defmodule SparkEx.Observation do
  @moduledoc """
  Collects metrics observed during query execution.

  Observations are identified by a name. Use `SparkEx.DataFrame.observe/3`
  to attach an observation to a DataFrame plan, then access the metrics
  with `get/1` after an action is executed.
  """

  alias Spark.Connect.Expression
  alias SparkEx.Internal.UUID

  @enforce_keys [:name]
  defstruct [:name]

  @type t :: %__MODULE__{name: String.t()}

  @table :spark_ex_observations

  @doc """
  Creates a named observation. When called with no arguments, generates a UUID name.
  """
  @spec new(String.t() | nil) :: t()
  def new(name \\ nil)

  def new(nil) do
    %__MODULE__{name: UUID.generate_v4()}
  end

  def new(name) when is_binary(name) and name != "" do
    %__MODULE__{name: name}
  end

  @doc """
  Returns the observed metrics map for this observation.

  Raises if the observation was never attached via `DataFrame.observe/3`.
  """
  @spec get(t()) :: map()
  def get(%__MODULE__{name: name}) do
    case :ets.lookup(@table, name) do
      [{^name, metrics}] ->
        metrics

      [] ->
        raise ArgumentError,
              "[NO_OBSERVE_BEFORE_GET] Observation \"#{name}\" was not attached. " <>
                "Call DataFrame.observe/3 and execute an action first."
    end
  end

  @doc false
  @spec register_metric_aliases(String.t(), [term()]) :: :ok
  def register_metric_aliases(name, metric_exprs) when is_binary(name) and is_list(metric_exprs) do
    aliases =
      Enum.map(metric_exprs, fn
        {:alias, _, alias_name} when is_binary(alias_name) -> alias_name
        _ -> nil
      end)

    :ets.insert(@table, {{:metric_aliases, name}, aliases})
    :ok
  end

  @doc false
  @spec store_observed_metrics(map()) :: :ok
  def store_observed_metrics(observed_metrics) when is_map(observed_metrics) do
    if map_size(observed_metrics) > 0 do
      Enum.each(observed_metrics, fn {name, metrics} ->
        :ets.insert(@table, {name, maybe_apply_metric_aliases(name, metrics)})
      end)
    end

    :ok
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
  def decode_literal(%Expression.Literal{literal_type: {:date, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:timestamp, v}}), do: v
  def decode_literal(%Expression.Literal{literal_type: {:timestamp_ntz, v}}), do: v

  def decode_literal(%Expression.Literal{
        literal_type: {:time, %Expression.Literal.Time{nano: v}}
      }),
      do: v

  def decode_literal(%Expression.Literal{
        literal_type: {:decimal, %Expression.Literal.Decimal{value: v}}
      }),
      do: v

  def decode_literal(%Expression.Literal{
        literal_type: {:array, %Expression.Literal.Array{elements: elems}}
      }) do
    Enum.map(elems, &decode_literal/1)
  end

  def decode_literal(%Expression.Literal{
        literal_type: {:map, %Expression.Literal.Map{keys: keys, values: values}}
      }) do
    keys
    |> Enum.map(&decode_literal/1)
    |> Enum.zip(Enum.map(values, &decode_literal/1))
    |> Map.new()
  end

  def decode_literal(%Expression.Literal{
         literal_type: {:struct, %Expression.Literal.Struct{elements: elems}}
       }) do
    Enum.map(elems, &decode_literal/1)
  end

  def decode_literal(other), do: other

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
