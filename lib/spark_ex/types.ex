defmodule SparkEx.Types do
  @moduledoc """
  Elixir-friendly type construction for Spark schemas.

  Provides helpers to build structured schema types that can be passed
  to `SparkEx.Reader.schema/2` and `SparkEx.StreamReader.schema/2`.

  ## Examples

      import SparkEx.Types

      schema = struct_type([
        struct_field("id", :long),
        struct_field("name", :string),
        struct_field("score", :double, nullable: false)
      ])

      reader |> SparkEx.Reader.schema(schema) |> SparkEx.Reader.load("/data")
  """

  @type spark_type ::
          :null
          | :boolean
          | :byte
          | :short
          | :integer
          | :long
          | :float
          | :double
          | :string
          | {:string, String.t()}
          | {:char, non_neg_integer()}
          | {:varchar, non_neg_integer()}
          | :binary
          | :date
          | :time
          | :timestamp
          | :timestamp_ntz
          | :day_time_interval
          | :year_month_interval
          | :calendar_interval
          | {:decimal, non_neg_integer(), non_neg_integer()}
          | {:array, spark_type()}
          | {:map, spark_type(), spark_type()}
          | {:struct, [field()]}
          | :variant
          | :geometry
          | :geography

  @type field :: %{name: String.t(), type: spark_type(), nullable: boolean()}
  @type struct_type :: {:struct, [field()]}

  @doc """
  Creates a struct type (schema) from a list of fields.

  ## Examples

      struct_type([
        struct_field("id", :long),
        struct_field("name", :string)
      ])
  """
  @spec struct_type([field()]) :: struct_type()
  def struct_type(fields) when is_list(fields) do
    {:struct, fields}
  end

  @doc """
  Creates a struct field.

  ## Options

    * `:nullable` — whether the field can be null (default: `true`)

  ## Examples

      struct_field("id", :long)
      struct_field("name", :string, nullable: false)
  """
  @spec struct_field(String.t(), spark_type(), keyword()) :: field()
  def struct_field(name, type, opts \\ []) when is_binary(name) do
    %{
      name: name,
      type: type,
      nullable: Keyword.get(opts, :nullable, true)
    }
  end

  @doc """
  Creates an array type.

  ## Examples

      array_type(:string)
      array_type({:struct, fields})
  """
  @spec array_type(spark_type()) :: {:array, spark_type()}
  def array_type(element_type), do: {:array, element_type}

  @doc """
  Creates a map type.

  ## Examples

      map_type(:string, :long)
  """
  @spec map_type(spark_type(), spark_type()) :: {:map, spark_type(), spark_type()}
  def map_type(key_type, value_type), do: {:map, key_type, value_type}

  @doc """
  Converts a struct type to a DDL schema string.

  ## Examples

      iex> schema = struct_type([struct_field("id", :long), struct_field("name", :string)])
      iex> SparkEx.Types.to_ddl(schema)
      "id LONG, name STRING"
  """
  @spec to_ddl(struct_type()) :: String.t()
  def to_ddl({:struct, fields}) do
    fields
    |> Enum.map_join(", ", fn %{name: name, type: type} ->
      "#{name} #{type_to_ddl(type)}"
    end)
  end

  @doc """
  Converts a struct type to a JSON schema string (Spark JSON format).

  This produces the same JSON that PySpark's `StructType.json()` generates.

  ## Examples

      iex> schema = struct_type([struct_field("id", :long), struct_field("name", :string)])
      iex> SparkEx.Types.to_json(schema)
      ~s({"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]})
  """
  @spec to_json(struct_type()) :: String.t()
  def to_json({:struct, fields}) do
    json_fields =
      Enum.map(fields, fn %{name: name, type: type, nullable: nullable} ->
        %{
          "name" => name,
          "type" => type_to_json(type),
          "nullable" => nullable,
          "metadata" => %{}
        }
      end)

    Jason.encode!(%{"type" => "struct", "fields" => json_fields})
  end

  # --- DDL type conversion ---

  defp type_to_ddl(:null), do: "VOID"
  defp type_to_ddl(:boolean), do: "BOOLEAN"
  defp type_to_ddl(:byte), do: "BYTE"
  defp type_to_ddl(:short), do: "SHORT"
  defp type_to_ddl(:integer), do: "INT"
  defp type_to_ddl(:long), do: "LONG"
  defp type_to_ddl(:float), do: "FLOAT"
  defp type_to_ddl(:double), do: "DOUBLE"
  defp type_to_ddl(:string), do: "STRING"
  defp type_to_ddl({:string, collation}), do: "STRING COLLATE #{collation}"
  defp type_to_ddl({:char, length}), do: "CHAR(#{length})"
  defp type_to_ddl({:varchar, length}), do: "VARCHAR(#{length})"
  defp type_to_ddl(:binary), do: "BINARY"
  defp type_to_ddl(:date), do: "DATE"
  defp type_to_ddl(:time), do: "TIME"
  defp type_to_ddl(:timestamp), do: "TIMESTAMP"
  defp type_to_ddl(:timestamp_ntz), do: "TIMESTAMP_NTZ"
  defp type_to_ddl(:day_time_interval), do: "INTERVAL DAY TO SECOND"
  defp type_to_ddl(:year_month_interval), do: "INTERVAL YEAR TO MONTH"
  defp type_to_ddl(:calendar_interval), do: "INTERVAL"
  defp type_to_ddl({:decimal, precision, scale}), do: "DECIMAL(#{precision}, #{scale})"
  defp type_to_ddl({:array, element}), do: "ARRAY<#{type_to_ddl(element)}>"
  defp type_to_ddl({:map, key, value}), do: "MAP<#{type_to_ddl(key)}, #{type_to_ddl(value)}>"
  defp type_to_ddl(:variant), do: "VARIANT"
  defp type_to_ddl(:geometry), do: "GEOMETRY"
  defp type_to_ddl(:geography), do: "GEOGRAPHY"

  defp type_to_ddl({:struct, fields}) do
    inner =
      Enum.map_join(fields, ", ", fn %{name: name, type: type} ->
        "#{name}: #{type_to_ddl(type)}"
      end)

    "STRUCT<#{inner}>"
  end

  # --- JSON type conversion (Spark JSON format) ---

  defp type_to_json(:null), do: "void"
  defp type_to_json(:boolean), do: "boolean"
  defp type_to_json(:byte), do: "byte"
  defp type_to_json(:short), do: "short"
  defp type_to_json(:integer), do: "integer"
  defp type_to_json(:long), do: "long"
  defp type_to_json(:float), do: "float"
  defp type_to_json(:double), do: "double"
  defp type_to_json(:string), do: "string"

  defp type_to_json({:string, collation}) do
    %{"type" => "string", "collation" => collation}
  end

  defp type_to_json({:char, length}), do: "char(#{length})"
  defp type_to_json({:varchar, length}), do: "varchar(#{length})"
  defp type_to_json(:binary), do: "binary"
  defp type_to_json(:date), do: "date"
  defp type_to_json(:time), do: "time"
  defp type_to_json(:timestamp), do: "timestamp"
  defp type_to_json(:timestamp_ntz), do: "timestamp_ntz"
  defp type_to_json(:day_time_interval), do: "day-time interval"
  defp type_to_json(:year_month_interval), do: "year-month interval"
  defp type_to_json(:calendar_interval), do: "interval"

  defp type_to_json({:decimal, precision, scale}) do
    "decimal(#{precision},#{scale})"
  end

  defp type_to_json({:array, element}) do
    %{"type" => "array", "elementType" => type_to_json(element), "containsNull" => true}
  end

  defp type_to_json(:variant), do: "variant"
  defp type_to_json(:geometry), do: "geometry"
  defp type_to_json(:geography), do: "geography"

  defp type_to_json({:map, key, value}) do
    %{
      "type" => "map",
      "keyType" => type_to_json(key),
      "valueType" => type_to_json(value),
      "valueContainsNull" => true
    }
  end

  defp type_to_json({:struct, fields}) do
    json_fields =
      Enum.map(fields, fn %{name: name, type: type, nullable: nullable} ->
        %{
          "name" => name,
          "type" => type_to_json(type),
          "nullable" => nullable,
          "metadata" => %{}
        }
      end)

    %{"type" => "struct", "fields" => json_fields}
  end

  @doc false
  @spec schema_to_string(struct_type() | String.t()) :: String.t()
  def schema_to_string(schema) when is_binary(schema), do: schema
  def schema_to_string({:struct, _} = schema), do: to_ddl(schema)
end
