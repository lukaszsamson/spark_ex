defmodule SparkEx.Observation do
  @moduledoc """
  Collects metrics observed during query execution.

  Observations are identified by a name. Use `SparkEx.DataFrame.observe/3`
  to attach an observation to a DataFrame plan, then access the metrics
  with `get/1` after an action is executed.
  """

  alias Spark.Connect.Expression

  @enforce_keys [:name]
  defstruct [:name]

  @type t :: %__MODULE__{name: String.t()}

  @table :spark_ex_observations

  @doc """
  Creates a named observation.
  """
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name) and name != "" do
    ensure_table!()
    %__MODULE__{name: name}
  end

  @doc """
  Returns the observed metrics map for this observation.

  If no metrics are available yet, returns an empty map.
  """
  @spec get(t()) :: map()
  def get(%__MODULE__{name: name}) do
    ensure_table!()

    case :ets.lookup(@table, name) do
      [{^name, metrics}] -> metrics
      [] -> %{}
    end
  end

  @doc false
  @spec store_observed_metrics(map()) :: :ok
  def store_observed_metrics(observed_metrics) when is_map(observed_metrics) do
    if map_size(observed_metrics) > 0 do
      ensure_table!()

      Enum.each(observed_metrics, fn {name, metrics} ->
        :ets.insert(@table, {name, metrics})
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

  defp ensure_table!() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set])
          :ok
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end
end
