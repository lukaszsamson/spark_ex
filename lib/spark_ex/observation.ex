defmodule SparkEx.Observation do
  @moduledoc """
  Collects metrics observed during query execution.

  Observations are identified by a name (sent to the server) and an opaque
  per-instance id (used to address metrics within this process). Use
  `SparkEx.DataFrame.observe/3` to attach an observation to a DataFrame plan,
  then access the metrics with `get/1` after an action is executed.

  ## Same-named observations

  Incoming `observed_metrics` are routed back to an `Observation` by `name`
  (the only key the server echoes). PySpark Connect matches each batch's
  metrics only against the observations attached to the *plan being
  executed* (`client/core.py`), so two distinct `Observation` objects that
  share a name but live on different plans never cross-talk. SparkEx routes
  through a single per-`{session_id, name}` ETS slot and cannot tell two
  same-named live observations apart at metric-arrival time. To avoid
  silently misrouting metrics, `register_observation/3` **raises** if a
  second still-unconsumed observation with the same name is attached in the
  same session — the caller must use distinct names (or consume/discard the
  first via `get/1`/`clear/1`) before reusing a name. See the audit note for
  FABLE-28.

  ## Lifecycle / reclamation

  Each attached observation owns a small set of ETS rows (route, attached
  marker, aliases, a session-membership marker, and — once an action runs —
  its metrics). These are reclaimed:

    * when its owning session terminates, via `clear_session/1` (called from
      the `SparkEx.Session` GenServer's terminate callback), and
    * explicitly via `clear/2` for callers that want to free a single
      observation eagerly.

  Routing tables (`{:obs_route, session_id, name}` and the legacy
  `{:metric_aliases, session_id, name}`) are namespaced by the originating
  session id so two sessions that observe the same `name` simultaneously do
  not overwrite each other's routing slot. When the session id is unknown
  the namespace falls back to a global slot — this preserves the prior
  behaviour for direct callers that don't go through `DataFrame.observe/3`.
  """

  alias Spark.Connect.{DataType, Expression}
  alias SparkEx.Internal.UUID

  @enforce_keys [:name, :id]
  defstruct [:name, :id]

  @type t :: %__MODULE__{name: String.t(), id: String.t()}

  @table :spark_ex_observations

  # ETS keys (all session-scoped where applicable):
  #   {:obs, id}                       -> metrics map (id is a per-instance UUID)
  #   {:obs_attached, id}              -> true (marker: observe/3 attached this id)
  #   {:obs_aliases, id}               -> [alias_name | nil]
  #   {:obs_session, session_id, id}   -> name (session-membership: reclaimed on terminate)
  #   {:obs_route, session_id, name}   -> id (the live un-consumed observe() for this name, per session)
  #   {:metric_aliases, session_id, name} -> [alias_name | nil] (legacy: raw-name observe)
  #   {:metric_legacy, session_id, name}  -> metrics map (legacy: raw-name observe)

  @doc """
  Creates a named observation. When called with no arguments, generates a UUID name.
  """
  @spec new(String.t() | nil) :: t()
  def new(name \\ nil)

  def new(nil) do
    %__MODULE__{name: UUID.generate_v4(), id: UUID.generate_v4()}
  end

  def new(name) when is_binary(name) and name != "" do
    %__MODULE__{name: name, id: UUID.generate_v4()}
  end

  @doc """
  Returns the observed metrics map for this observation.

  Matches PySpark Connect (`observation.py`): after the observation has been
  attached via `DataFrame.observe/3` but before any action has run, returns
  an empty map `%{}` (PySpark sets `_result = {}` in `_on`). Raises
  `[NO_OBSERVE_BEFORE_GET]` only when the observation was *never* attached
  (PySpark raises when `_result is None`).
  """
  @spec get(t()) :: map()
  def get(%__MODULE__{id: id, name: name}) do
    case :ets.lookup(@table, {:obs, id}) do
      [{{:obs, ^id}, metrics}] ->
        metrics

      [] ->
        # Attached but no action yet -> empty map; never attached -> raise.
        case :ets.lookup(@table, {:obs_attached, id}) do
          [{{:obs_attached, ^id}, _}] ->
            %{}

          [] ->
            raise ArgumentError,
                  "[NO_OBSERVE_BEFORE_GET] Observation \"#{name}\" was not attached. " <>
                    "Call DataFrame.observe/3 and execute an action first."
        end
    end
  end

  @doc false
  @spec register_observation(t(), [term()], String.t() | nil) :: :ok
  def register_observation(observation, metric_exprs, session_id \\ nil)

  def register_observation(%__MODULE__{id: id, name: name}, metric_exprs, session_id)
      when is_list(metric_exprs) do
    # Spark forbids reusing an Observation across multiple `observe()` calls
    # (see PySpark's REUSE_OBSERVATION assertion). Mark the id on first
    # registration; raise if it shows up again.
    unless :ets.insert_new(@table, {{:obs_attached, id}, true}) do
      raise ArgumentError,
            "[REUSE_OBSERVATION] Observation \"#{name}\" was already attached. " <>
              "Create a new SparkEx.Observation.new/1 for each observe/3 call."
    end

    # Metrics are routed back from the server by `name` alone through a
    # single per-(session, name) slot. If a different, still-live (route
    # not yet replaced and not cleared) observation already owns this
    # name in this session, a second attach would silently misroute one
    # of them (FABLE-28). PySpark avoids this by scoping routing to the
    # executed plan's observations; SparkEx cannot disambiguate two
    # same-named live observations at metric-arrival time, so it refuses
    # the collision up front.
    case :ets.lookup(@table, {:obs_route, session_id, name}) do
      [{{:obs_route, ^session_id, ^name}, existing_id}]
      when existing_id != id ->
        if observation_live?(existing_id) do
          # Roll back the attached marker we just set so the caller may
          # retry with a fresh Observation.
          :ets.delete(@table, {:obs_attached, id})

          raise ArgumentError,
                "[AMBIGUOUS_OBSERVATION] Another live Observation named \"#{name}\" is " <>
                  "already attached in this session. Same-named observations cannot be " <>
                  "routed unambiguously; use a distinct name, or read/clear the existing " <>
                  "one (Observation.get/1) before reusing the name."
        end

      _ ->
        :ok
    end

    aliases = aliases_from_exprs(metric_exprs)
    :ets.insert(@table, {{:obs_aliases, id}, aliases})
    :ets.insert(@table, {{:obs_route, session_id, name}, id})
    # Session-membership marker so `clear_session/1` can reclaim every row
    # this observation owns when the session terminates (FABLE-29).
    :ets.insert(@table, {{:obs_session, session_id, id}, name})
    :ok
  end

  # An observation is "live" while it is still attached (not cleared). Once
  # `clear/2` removes its attached marker the route may be safely reused.
  defp observation_live?(id) do
    :ets.member(@table, {:obs_attached, id})
  end

  @doc false
  @spec register_metric_aliases(String.t(), [term()], String.t() | nil) :: :ok
  def register_metric_aliases(name, metric_exprs, session_id \\ nil)

  def register_metric_aliases(name, metric_exprs, session_id)
      when is_binary(name) and is_list(metric_exprs) do
    aliases = aliases_from_exprs(metric_exprs)
    :ets.insert(@table, {{:metric_aliases, session_id, name}, aliases})
    :ok
  end

  @doc """
  Reclaims all ETS rows owned by a single observation.

  Deletes the metrics, attached marker, aliases, session-membership marker,
  and (only if it still points at this id) the routing slot. Frees the name
  for reuse. Idempotent.
  """
  @spec clear(t(), String.t() | nil) :: :ok
  def clear(observation, session_id \\ nil)

  def clear(%__MODULE__{id: id, name: name}, session_id) do
    if table_exists?() do
      :ets.delete(@table, {:obs, id})
      :ets.delete(@table, {:obs_attached, id})
      :ets.delete(@table, {:obs_aliases, id})
      :ets.delete(@table, {:obs_session, session_id, id})

      # Only clear the route if it still points at this observation: a later
      # same-named observation may have legitimately taken it over.
      case :ets.lookup(@table, {:obs_route, session_id, name}) do
        [{{:obs_route, ^session_id, ^name}, ^id}] ->
          :ets.delete(@table, {:obs_route, session_id, name})

        _ ->
          :ok
      end
    end

    :ok
  end

  @doc """
  Reclaims every observation row belonging to `session_id`.

  Called from the `SparkEx.Session` GenServer's terminate callback so a
  long-running session that
  creates observations in a loop does not leak ETS rows for the VM's
  lifetime (FABLE-29). Walks the `{:obs_session, session_id, id}` membership
  markers and deletes each observation's rows.
  """
  @spec clear_session(String.t() | nil) :: :ok
  def clear_session(session_id) do
    if table_exists?() do
      @table
      |> :ets.match_object({{:obs_session, session_id, :_}, :_})
      |> Enum.each(fn {{:obs_session, ^session_id, id}, name} ->
        :ets.delete(@table, {:obs, id})
        :ets.delete(@table, {:obs_attached, id})
        :ets.delete(@table, {:obs_aliases, id})
        :ets.delete(@table, {:obs_session, session_id, id})
        :ets.delete(@table, {:obs_route, session_id, name})
      end)

      # Legacy raw-name observe() slots are session-scoped too.
      :ets.match_delete(@table, {{:metric_aliases, session_id, :_}, :_})
      :ets.match_delete(@table, {{:metric_legacy, session_id, :_}, :_})
    end

    :ok
  end

  defp table_exists?, do: :ets.whereis(@table) != :undefined

  @doc false
  @spec store_observed_metrics(map(), String.t() | nil) :: :ok
  def store_observed_metrics(observed_metrics, session_id \\ nil)

  def store_observed_metrics(observed_metrics, session_id) when is_map(observed_metrics) do
    if map_size(observed_metrics) > 0 do
      Enum.each(observed_metrics, &store_one(&1, session_id))
    end

    :ok
  end

  defp store_one({name, metrics}, session_id) do
    case :ets.lookup(@table, {:obs_route, session_id, name}) do
      [{{:obs_route, ^session_id, ^name}, id}] ->
        normalized = maybe_apply_observation_aliases(id, metrics)
        # Last execution wins, with dict.update merge semantics: PySpark
        # Connect does `observation_result.update(...)` on every observed
        # metric batch (core.py), so re-executing the plan overwrites the
        # previously stored values (and adds any new keys).
        existing =
          case :ets.lookup(@table, {:obs, id}) do
            [{{:obs, ^id}, prev}] when is_map(prev) -> prev
            _ -> %{}
          end

        :ets.insert(@table, {{:obs, id}, Map.merge(existing, normalized)})

      _ ->
        # Legacy: observe() was called with a raw string name (no Observation
        # struct attached). Retrieval through Observation.get/1 isn't possible
        # in this case; we still keep the legacy slot for backward compatibility.
        :ets.insert(
          @table,
          {{:metric_legacy, session_id, name},
           maybe_apply_metric_aliases(session_id, name, metrics)}
        )
    end
  end

  @doc false
  @spec decode_literal(Expression.Literal.t() | nil) :: term()
  def decode_literal(nil), do: nil

  def decode_literal(%Expression.Literal{literal_type: {:null, _}}), do: nil
  def decode_literal(%Expression.Literal{literal_type: {:boolean, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:byte, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:short, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:integer, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:long, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:float, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:double, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:string, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:binary, v}}), do: v

  def decode_literal(%Expression.Literal{literal_type: {:date, days}}) when is_integer(days) do
    Date.add(~D[1970-01-01], days)
  end

  def decode_literal(%Expression.Literal{literal_type: {:timestamp, micros}})
      when is_integer(micros) do
    DateTime.add(~U[1970-01-01 00:00:00.000000Z], micros, :microsecond)
  end

  def decode_literal(%Expression.Literal{literal_type: {:timestamp_ntz, micros}})
      when is_integer(micros) do
    NaiveDateTime.add(~N[1970-01-01 00:00:00.000000], micros, :microsecond)
  end

  # 86_400 seconds in a day → upper bound (exclusive) for nanos within a day.
  @max_time_nanos 86_400 * 1_000_000_000

  def decode_literal(%Expression.Literal{
        literal_type: {:time, %Expression.Literal.Time{nano: nano}}
      })
      when is_integer(nano) and nano >= 0 and nano < @max_time_nanos do
    seconds = div(nano, 1_000_000_000)
    micros = div(rem(nano, 1_000_000_000), 1_000)

    seconds
    |> Time.from_seconds_after_midnight()
    |> Map.put(:microsecond, {micros, 6})
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:time, %Expression.Literal.Time{nano: nano}}
      })
      when is_integer(nano) do
    raise ArgumentError,
          "time literal nano-of-day must be in 0..#{@max_time_nanos - 1}, got: #{inspect(nano)}"
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:decimal, %Expression.Literal.Decimal{value: v}}
      })
      when is_binary(v) do
    Decimal.new(v)
  end

  def decode_literal(%Expression.Literal{
        literal_type:
          {:calendar_interval,
           %Expression.Literal.CalendarInterval{
             months: months,
             days: days,
             microseconds: micros
           }}
      }) do
    %{months: months || 0, days: days || 0, microseconds: micros || 0}
  end

  def decode_literal(%Expression.Literal{literal_type: {:year_month_interval, months}})
      when is_integer(months) do
    %{months: months}
  end

  def decode_literal(%Expression.Literal{literal_type: {:day_time_interval, micros}})
      when is_integer(micros) do
    %{microseconds: micros}
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:array, %Expression.Literal.Array{elements: elems}}
      }) do
    Enum.map(elems || [], &decode_literal/1)
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:map, %Expression.Literal.Map{keys: keys, values: values}}
      }) do
    keys = keys || []
    values = values || []

    keys
    |> Enum.map(&decode_literal/1)
    |> Enum.zip(Enum.map(values, &decode_literal/1))
    |> Map.new()
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:struct, %Expression.Literal.Struct{elements: elems, struct_type: st}}
      }) do
    decoded = Enum.map(elems || [], &decode_literal/1)
    field_names = struct_field_names(st)

    case field_names do
      [_ | _] when length(field_names) == length(decoded) ->
        field_names
        |> Enum.zip(decoded)
        |> Map.new()

      _ ->
        decoded
    end
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:specialized_array, %Expression.Literal.SpecializedArray{value_type: vt}}
      }) do
    decode_specialized_array(vt)
  end

  def decode_literal(other), do: other

  defp decode_specialized_array({:bools, %Spark.Connect.Bools{values: values}}), do: values || []
  defp decode_specialized_array({:ints, %Spark.Connect.Ints{values: values}}), do: values || []
  defp decode_specialized_array({:longs, %Spark.Connect.Longs{values: values}}), do: values || []

  defp decode_specialized_array({:floats, %Spark.Connect.Floats{values: values}}),
    do: values || []

  defp decode_specialized_array({:doubles, %Spark.Connect.Doubles{values: values}}),
    do: values || []

  defp decode_specialized_array({:strings, %Spark.Connect.Strings{values: values}}),
    do: values || []

  defp decode_specialized_array(_), do: []

  defp struct_field_names(%DataType{kind: {:struct, %DataType.Struct{fields: fields}}})
       when is_list(fields) do
    Enum.map(fields, & &1.name)
  end

  defp struct_field_names(_), do: []

  defp aliases_from_exprs(metric_exprs) do
    Enum.map(metric_exprs, fn
      {:alias, _, alias_name} when is_binary(alias_name) -> alias_name
      _ -> nil
    end)
  end

  defp maybe_apply_observation_aliases(id, metrics) when is_map(metrics) do
    case :ets.lookup(@table, {:obs_aliases, id}) do
      [{{:obs_aliases, ^id}, aliases}] when is_list(aliases) ->
        remap_positional_metrics(metrics, aliases)

      _ ->
        metrics
    end
  end

  defp maybe_apply_metric_aliases(session_id, name, metrics) when is_map(metrics) do
    case :ets.lookup(@table, {:metric_aliases, session_id, name}) do
      [{{:metric_aliases, ^session_id, ^name}, aliases}] when is_list(aliases) ->
        remap_positional_metrics(metrics, aliases)

      _ ->
        metrics
    end
  end

  defp remap_positional_metrics(metrics, aliases) do
    indexed_values =
      metrics
      |> Enum.map(fn {key, value} -> {metric_index(key), value} end)
      |> Enum.reject(fn {idx, _value} -> is_nil(idx) end)

    if indexed_values == [] do
      metrics
    else
      indexed_values
      |> Enum.sort_by(&elem(&1, 0))
      |> Map.new(fn {idx, value} ->
        metric_name =
          case Enum.at(aliases, idx - 1) do
            alias_name when is_binary(alias_name) and alias_name != "" -> alias_name
            _ -> "_#{idx}"
          end

        {metric_name, value}
      end)
    end
  end

  defp metric_index("_" <> rest) do
    case Integer.parse(rest) do
      {idx, ""} -> idx
      _ -> nil
    end
  end

  defp metric_index(_), do: nil
end
