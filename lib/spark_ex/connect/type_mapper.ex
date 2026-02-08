defmodule SparkEx.Connect.TypeMapper do
  @moduledoc """
  Maps Spark Connect `DataType` protobuf variants to Explorer dtypes.

  Primitive types are mapped directly. Complex types (array, struct, map)
  and unsupported types fall back to `:string` (JSON representation).
  """

  alias Spark.Connect.DataType

  @doc """
  Converts a Spark Connect `DataType` to an Explorer dtype atom.

  ## Examples

      iex> TypeMapper.to_explorer_dtype(%DataType{kind: {:boolean, %DataType.Boolean{}}})
      {:ok, :boolean}

      iex> TypeMapper.to_explorer_dtype(%DataType{kind: {:array, %DataType.Array{}}})
      {:ok, :string}
  """
  @spec to_explorer_dtype(DataType.t()) :: {:ok, atom() | {atom(), term()}}
  def to_explorer_dtype(%DataType{kind: {tag, _value}} = dt) do
    {:ok, map_kind(tag, dt)}
  end

  def to_explorer_dtype(%DataType{kind: nil}) do
    {:ok, :null}
  end

  @doc """
  Converts a Spark Connect schema (`DataType.Struct`) to Explorer dtypes map.

  Returns a keyword list of `{column_name, dtype}` pairs.
  """
  @spec schema_to_dtypes(DataType.Struct.t()) :: {:ok, [{String.t(), atom() | {atom(), term()}}]}
  def schema_to_dtypes(%DataType.Struct{fields: fields}) do
    dtypes =
      Enum.map(fields, fn %DataType.StructField{name: name, data_type: dt} ->
        {:ok, dtype} = to_explorer_dtype(dt)
        {name, dtype}
      end)

    {:ok, dtypes}
  end

  # --- Primitive type mappings ---

  # Null
  defp map_kind(:null, _dt), do: :null

  # Boolean
  defp map_kind(:boolean, _dt), do: :boolean

  # Integer types — Spark byte/short/integer/long → Explorer signed integers
  defp map_kind(:byte, _dt), do: {:s, 8}
  defp map_kind(:short, _dt), do: {:s, 16}
  defp map_kind(:integer, _dt), do: {:s, 32}
  defp map_kind(:long, _dt), do: {:s, 64}

  # Float types
  defp map_kind(:float, _dt), do: {:f, 32}
  defp map_kind(:double, _dt), do: {:f, 64}

  # Decimal — map to string fallback (Explorer decimal support varies)
  defp map_kind(:decimal, _dt), do: :string

  # String types
  defp map_kind(:string, _dt), do: :string
  defp map_kind(:char, _dt), do: :string
  defp map_kind(:var_char, _dt), do: :string

  # Binary
  defp map_kind(:binary, _dt), do: :binary

  # Date and time types
  defp map_kind(:date, _dt), do: :date
  defp map_kind(:timestamp, _dt), do: {:datetime, :microsecond}
  defp map_kind(:timestamp_ntz, _dt), do: {:naive_datetime, :microsecond}
  defp map_kind(:time, _dt), do: {:time, :microsecond}

  # Interval types — fall back to string
  defp map_kind(:calendar_interval, _dt), do: :string
  defp map_kind(:year_month_interval, _dt), do: :string
  defp map_kind(:day_time_interval, _dt), do: :string

  # Complex types — fall back to string (JSON representation)
  defp map_kind(:array, _dt), do: :string
  defp map_kind(:struct, _dt), do: :string
  defp map_kind(:map, _dt), do: :string

  # Other types — fall back to string
  defp map_kind(:variant, _dt), do: :string
  defp map_kind(:udt, _dt), do: :string
  defp map_kind(:geometry, _dt), do: :string
  defp map_kind(:geography, _dt), do: :string
  defp map_kind(:unparsed, _dt), do: :string

  # Catch-all for future types
  defp map_kind(_unknown, _dt), do: :string
end
