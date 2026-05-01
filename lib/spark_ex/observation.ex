defmodule SparkEx.Observation do
  @moduledoc """
  Collects metrics observed during query execution.

  Observations are identified by a name (sent to the server) and an opaque
  per-instance id (used to address metrics within this process). Use
  `SparkEx.DataFrame.observe/3` to attach an observation to a DataFrame plan,
  then access the metrics with `get/1` after an action is executed.

  Two `Observation` structs that happen to share the same `name` will not
  collide: each carries its own `id` and metrics are stored under that id.
  """

  alias Spark.Connect.{DataType, Expression}
  alias SparkEx.Internal.UUID

  @enforce_keys [:name, :id]
  defstruct [:name, :id]

  @type t :: %__MODULE__{name: String.t(), id: String.t()}

  @table :spark_ex_observations

  # ETS keys:
  #   {:obs, id}            -> metrics map (per-instance store; first writer wins)
  #   {:obs_aliases, id}    -> [alias_name | nil]
  #   {:obs_route, name}    -> id (most-recent observe() with this name wins)
  #   {:metric_aliases, name} -> [alias_name | nil]   (legacy: raw-name observe)
  #   name                  -> metrics map            (legacy: raw-name observe)

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

  Raises if the observation was never attached via `DataFrame.observe/3`,
  or if no action has been executed against the attached plan yet.
  """
  @spec get(t()) :: map()
  def get(%__MODULE__{id: id, name: name}) do
    case :ets.lookup(@table, {:obs, id}) do
      [{{:obs, ^id}, metrics}] ->
        metrics

      [] ->
        raise ArgumentError,
              "[NO_OBSERVE_BEFORE_GET] Observation \"#{name}\" was not attached. " <>
                "Call DataFrame.observe/3 and execute an action first."
    end
  end

  @doc false
  @spec register_observation(t(), [term()]) :: :ok
  def register_observation(%__MODULE__{id: id, name: name}, metric_exprs)
      when is_list(metric_exprs) do
    aliases = aliases_from_exprs(metric_exprs)
    :ets.insert(@table, {{:obs_aliases, id}, aliases})
    :ets.insert(@table, {{:obs_route, name}, id})
    :ok
  end

  @doc false
  @spec register_metric_aliases(String.t(), [term()]) :: :ok
  def register_metric_aliases(name, metric_exprs)
      when is_binary(name) and is_list(metric_exprs) do
    aliases = aliases_from_exprs(metric_exprs)
    :ets.insert(@table, {{:metric_aliases, name}, aliases})
    :ok
  end

  @doc false
  @spec store_observed_metrics(map()) :: :ok
  def store_observed_metrics(observed_metrics) when is_map(observed_metrics) do
    if map_size(observed_metrics) > 0 do
      Enum.each(observed_metrics, &store_one/1)
    end

    :ok
  end

  defp store_one({name, metrics}) do
    case :ets.lookup(@table, {:obs_route, name}) do
      [{{:obs_route, ^name}, id}] ->
        normalized = maybe_apply_observation_aliases(id, metrics)
        # First writer wins: a single Observation captures the metrics from
        # the first execution of its plan, matching PySpark semantics.
        :ets.insert_new(@table, {{:obs, id}, normalized})

      _ ->
        # Legacy: observe() was called with a raw string name (no Observation
        # struct attached). Retrieval through Observation.get/1 isn't possible
        # in this case; we still keep the legacy slot for backward compatibility.
        :ets.insert(@table, {name, maybe_apply_metric_aliases(name, metrics)})
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

  def decode_literal(%Expression.Literal{
        literal_type: {:time, %Expression.Literal.Time{nano: nano}}
      })
      when is_integer(nano) do
    seconds = div(nano, 1_000_000_000)
    micros = div(rem(nano, 1_000_000_000), 1_000)

    seconds
    |> Time.from_seconds_after_midnight()
    |> Map.put(:microsecond, {micros, 6})
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
    case vt do
      {:bools, %Spark.Connect.Bools{values: values}} -> values || []
      {:ints, %Spark.Connect.Ints{values: values}} -> values || []
      {:longs, %Spark.Connect.Longs{values: values}} -> values || []
      {:floats, %Spark.Connect.Floats{values: values}} -> values || []
      {:doubles, %Spark.Connect.Doubles{values: values}} -> values || []
      {:strings, %Spark.Connect.Strings{values: values}} -> values || []
      _ -> []
    end
  end

  def decode_literal(other), do: other

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

  defp maybe_apply_metric_aliases(name, metrics) when is_map(metrics) do
    case :ets.lookup(@table, {:metric_aliases, name}) do
      [{{:metric_aliases, ^name}, aliases}] when is_list(aliases) ->
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
