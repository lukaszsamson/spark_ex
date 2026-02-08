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

  # --- Reverse mapping: Explorer dtype → Spark DDL type string ---

  @doc """
  Converts an Explorer dtype to a Spark DDL type string.

  ## Examples

      iex> TypeMapper.to_spark_ddl_type(:boolean)
      "BOOLEAN"

      iex> TypeMapper.to_spark_ddl_type({:s, 32})
      "INT"
  """
  @spec to_spark_ddl_type(atom() | {atom(), term()}) :: String.t()
  def to_spark_ddl_type(:null), do: "VOID"
  def to_spark_ddl_type(:boolean), do: "BOOLEAN"
  def to_spark_ddl_type({:s, 8}), do: "BYTE"
  def to_spark_ddl_type({:s, 16}), do: "SHORT"
  def to_spark_ddl_type({:s, 32}), do: "INT"
  def to_spark_ddl_type({:s, 64}), do: "LONG"
  def to_spark_ddl_type({:u, 8}), do: "SHORT"
  def to_spark_ddl_type({:u, 16}), do: "INT"
  def to_spark_ddl_type({:u, 32}), do: "LONG"
  def to_spark_ddl_type({:u, 64}), do: "LONG"
  def to_spark_ddl_type({:f, 32}), do: "FLOAT"
  def to_spark_ddl_type({:f, 64}), do: "DOUBLE"
  def to_spark_ddl_type(:string), do: "STRING"
  def to_spark_ddl_type(:binary), do: "BINARY"
  def to_spark_ddl_type(:date), do: "DATE"
  def to_spark_ddl_type({:datetime, _}), do: "TIMESTAMP"
  def to_spark_ddl_type({:naive_datetime, _}), do: "TIMESTAMP_NTZ"
  def to_spark_ddl_type({:time, _}), do: "STRING"
  def to_spark_ddl_type({:duration, _}), do: "STRING"
  def to_spark_ddl_type(:category), do: "STRING"
  def to_spark_ddl_type(_other), do: "STRING"

  @doc """
  Converts an Explorer.DataFrame schema to a Spark DDL schema string.

  ## Examples

      iex> TypeMapper.explorer_schema_to_ddl(%{"id" => {:s, 64}, "name" => :string})
      "id LONG, name STRING"
  """
  @spec explorer_schema_to_ddl(map() | [{String.t(), atom() | {atom(), term()}}]) :: String.t()
  def explorer_schema_to_ddl(dtypes) when is_map(dtypes) do
    dtypes
    |> Map.to_list()
    |> explorer_schema_to_ddl()
  end

  def explorer_schema_to_ddl(dtypes) when is_list(dtypes) do
    dtypes
    |> Enum.map_join(", ", fn {name, dtype} ->
      "#{name} #{to_spark_ddl_type(dtype)}"
    end)
  end
end
