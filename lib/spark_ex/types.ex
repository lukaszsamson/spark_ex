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

  @type field :: %{
          name: String.t(),
          type: term(),
          nullable: boolean(),
          metadata: term()
        }
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
    * `:metadata` — metadata map (default: `%{}`)

  ## Examples

      struct_field("id", :long)
      struct_field("name", :string, nullable: false)
      struct_field("tags", :string, metadata: %{"comment" => "user tags"})
  """
  @spec struct_field(String.t(), term(), keyword()) :: field()
  def struct_field(name, type, opts \\ []) when is_binary(name) do
    %{
      name: name,
      type: type,
      nullable: Keyword.get(opts, :nullable, true),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc """
  Creates an array type.

  ## Examples

      array_type(:string)
      array_type({:struct, fields})
  """
  @spec array_type(spark_type(), keyword()) ::
          {:array, spark_type()} | {:array, spark_type(), boolean()}
  def array_type(element_type, opts \\ []) do
    case Keyword.get(opts, :contains_null, true) do
      true -> {:array, element_type}
      false -> {:array, element_type, false}
    end
  end

  @doc """
  Creates a map type.

  ## Examples

      map_type(:string, :long)
  """
  @spec map_type(spark_type(), spark_type(), keyword()) ::
          {:map, spark_type(), spark_type()} | {:map, spark_type(), spark_type(), boolean()}
  def map_type(key_type, value_type, opts \\ []) do
    case Keyword.get(opts, :value_contains_null, true) do
      true -> {:map, key_type, value_type}
      false -> {:map, key_type, value_type, false}
    end
  end

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
      Enum.map(fields, fn field ->
        %{
          "name" => field.name,
          "type" => type_to_json(field.type),
          "nullable" => field.nullable,
          "metadata" => Map.get(field, :metadata, %{})
        }
      end)

    Jason.encode!(%{"type" => "struct", "fields" => json_fields})
  end

  @doc """
  Converts a Spark Connect `DataType` protobuf value to Spark JSON schema string.

  This mirrors PySpark's `DataType.json()` output.
  """
  @spec data_type_to_json(Spark.Connect.DataType.t()) :: String.t()
  def data_type_to_json(%Spark.Connect.DataType{} = data_type) do
    data_type
    |> proto_type_to_json()
    |> Jason.encode!()
  end

  @doc """
  Converts a struct type to Spark Connect `DataType` protobuf.

  Preserves JSON-level fidelity (field nullability and metadata) for nested types.
  """
  @spec to_proto(struct_type()) :: Spark.Connect.DataType.t()
  def to_proto({:struct, _} = schema), do: type_to_proto(schema)

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
  defp type_to_ddl({:array, element, _contains_null}), do: "ARRAY<#{type_to_ddl(element)}>"
  defp type_to_ddl({:map, key, value}), do: "MAP<#{type_to_ddl(key)}, #{type_to_ddl(value)}>"

  defp type_to_ddl({:map, key, value, _vcn}),
    do: "MAP<#{type_to_ddl(key)}, #{type_to_ddl(value)}>"

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

  defp type_to_json({:array, element, contains_null}) do
    %{"type" => "array", "elementType" => type_to_json(element), "containsNull" => contains_null}
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

  defp type_to_json({:map, key, value, value_contains_null}) do
    %{
      "type" => "map",
      "keyType" => type_to_json(key),
      "valueType" => type_to_json(value),
      "valueContainsNull" => value_contains_null
    }
  end

  defp type_to_json({:struct, fields}) do
    json_fields =
      Enum.map(fields, fn field ->
        %{
          "name" => field.name,
          "type" => type_to_json(field.type),
          "nullable" => field.nullable,
          "metadata" => Map.get(field, :metadata, %{})
        }
      end)

    %{"type" => "struct", "fields" => json_fields}
  end

  defp proto_type_to_json(%Spark.Connect.DataType{kind: {kind, value}}) do
    case kind do
      :null -> "void"
      :boolean -> "boolean"
      :byte -> "byte"
      :short -> "short"
      :integer -> "integer"
      :long -> "long"
      :float -> "float"
      :double -> "double"
      :string -> string_proto_to_json(value)
      :char -> "char(#{value.length})"
      :var_char -> "varchar(#{value.length})"
      :binary -> "binary"
      :date -> "date"
      :time -> "time"
      :timestamp -> "timestamp"
      :timestamp_ntz -> "timestamp_ntz"
      :day_time_interval -> "day-time interval"
      :year_month_interval -> "year-month interval"
      :calendar_interval -> "interval"
      :decimal -> "decimal(#{value.precision},#{value.scale})"
      :array -> array_proto_to_json(value)
      :map -> map_proto_to_json(value)
      :struct -> struct_proto_to_json(value)
      :variant -> "variant"
      :geometry -> "geometry"
      :geography -> "geography"
      :unparsed -> value.data_type_string
      _ -> raise ArgumentError, "unsupported Spark.Connect.DataType kind: #{inspect(kind)}"
    end
  end

  defp string_proto_to_json(%Spark.Connect.DataType.String{collation: ""}), do: "string"

  defp string_proto_to_json(%Spark.Connect.DataType.String{collation: collation}) do
    %{"type" => "string", "collation" => collation}
  end

  defp array_proto_to_json(%Spark.Connect.DataType.Array{} = array) do
    %{
      "type" => "array",
      "elementType" => proto_type_to_json(array.element_type),
      "containsNull" => array.contains_null
    }
  end

  defp map_proto_to_json(%Spark.Connect.DataType.Map{} = map) do
    %{
      "type" => "map",
      "keyType" => proto_type_to_json(map.key_type),
      "valueType" => proto_type_to_json(map.value_type),
      "valueContainsNull" => map.value_contains_null
    }
  end

  defp struct_proto_to_json(%Spark.Connect.DataType.Struct{} = struct) do
    %{
      "type" => "struct",
      "fields" => Enum.map(struct.fields, &struct_field_proto_to_json/1)
    }
  end

  defp struct_field_proto_to_json(%Spark.Connect.DataType.StructField{} = field) do
    %{
      "name" => field.name,
      "type" => proto_type_to_json(field.data_type),
      "nullable" => field.nullable,
      "metadata" => decode_field_metadata(field.metadata)
    }
  end

  defp decode_field_metadata(nil), do: %{}
  defp decode_field_metadata(""), do: %{}

  defp decode_field_metadata(metadata) when is_binary(metadata) do
    case Jason.decode(metadata) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _ -> %{}
    end
  end

  # --- Spark Connect DataType protobuf conversion ---

  defp type_to_proto(:null),
    do: %Spark.Connect.DataType{kind: {:null, %Spark.Connect.DataType.NULL{}}}

  defp type_to_proto(:boolean),
    do: %Spark.Connect.DataType{kind: {:boolean, %Spark.Connect.DataType.Boolean{}}}

  defp type_to_proto(:byte),
    do: %Spark.Connect.DataType{kind: {:byte, %Spark.Connect.DataType.Byte{}}}

  defp type_to_proto(:short),
    do: %Spark.Connect.DataType{kind: {:short, %Spark.Connect.DataType.Short{}}}

  defp type_to_proto(:integer),
    do: %Spark.Connect.DataType{kind: {:integer, %Spark.Connect.DataType.Integer{}}}

  defp type_to_proto(:long),
    do: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}}

  defp type_to_proto(:float),
    do: %Spark.Connect.DataType{kind: {:float, %Spark.Connect.DataType.Float{}}}

  defp type_to_proto(:double),
    do: %Spark.Connect.DataType{kind: {:double, %Spark.Connect.DataType.Double{}}}

  defp type_to_proto(:string),
    do: %Spark.Connect.DataType{kind: {:string, %Spark.Connect.DataType.String{}}}

  defp type_to_proto({:string, collation}) do
    %Spark.Connect.DataType{
      kind: {:string, %Spark.Connect.DataType.String{collation: collation}}
    }
  end

  defp type_to_proto({:char, length}) do
    %Spark.Connect.DataType{kind: {:char, %Spark.Connect.DataType.Char{length: length}}}
  end

  defp type_to_proto({:varchar, length}) do
    %Spark.Connect.DataType{kind: {:var_char, %Spark.Connect.DataType.VarChar{length: length}}}
  end

  defp type_to_proto(:binary),
    do: %Spark.Connect.DataType{kind: {:binary, %Spark.Connect.DataType.Binary{}}}

  defp type_to_proto(:date),
    do: %Spark.Connect.DataType{kind: {:date, %Spark.Connect.DataType.Date{}}}

  defp type_to_proto(:time),
    do: %Spark.Connect.DataType{kind: {:time, %Spark.Connect.DataType.Time{}}}

  defp type_to_proto(:timestamp) do
    %Spark.Connect.DataType{kind: {:timestamp, %Spark.Connect.DataType.Timestamp{}}}
  end

  defp type_to_proto(:timestamp_ntz) do
    %Spark.Connect.DataType{kind: {:timestamp_ntz, %Spark.Connect.DataType.TimestampNTZ{}}}
  end

  defp type_to_proto(:day_time_interval) do
    %Spark.Connect.DataType{
      kind: {:day_time_interval, %Spark.Connect.DataType.DayTimeInterval{}}
    }
  end

  defp type_to_proto(:year_month_interval) do
    %Spark.Connect.DataType{
      kind: {:year_month_interval, %Spark.Connect.DataType.YearMonthInterval{}}
    }
  end

  defp type_to_proto(:calendar_interval) do
    %Spark.Connect.DataType{
      kind: {:calendar_interval, %Spark.Connect.DataType.CalendarInterval{}}
    }
  end

  defp type_to_proto({:decimal, precision, scale}) do
    %Spark.Connect.DataType{
      kind: {:decimal, %Spark.Connect.DataType.Decimal{precision: precision, scale: scale}}
    }
  end

  defp type_to_proto({:array, element}) do
    %Spark.Connect.DataType{
      kind:
        {:array,
         %Spark.Connect.DataType.Array{element_type: type_to_proto(element), contains_null: true}}
    }
  end

  defp type_to_proto({:array, element, contains_null}) do
    %Spark.Connect.DataType{
      kind:
        {:array,
         %Spark.Connect.DataType.Array{
           element_type: type_to_proto(element),
           contains_null: contains_null
         }}
    }
  end

  defp type_to_proto({:map, key, value}) do
    %Spark.Connect.DataType{
      kind:
        {:map,
         %Spark.Connect.DataType.Map{
           key_type: type_to_proto(key),
           value_type: type_to_proto(value),
           value_contains_null: true
         }}
    }
  end

  defp type_to_proto({:map, key, value, value_contains_null}) do
    %Spark.Connect.DataType{
      kind:
        {:map,
         %Spark.Connect.DataType.Map{
           key_type: type_to_proto(key),
           value_type: type_to_proto(value),
           value_contains_null: value_contains_null
         }}
    }
  end

  defp type_to_proto(:variant),
    do: %Spark.Connect.DataType{kind: {:variant, %Spark.Connect.DataType.Variant{}}}

  defp type_to_proto(:geometry),
    do: %Spark.Connect.DataType{kind: {:geometry, %Spark.Connect.DataType.Geometry{}}}

  defp type_to_proto(:geography),
    do: %Spark.Connect.DataType{kind: {:geography, %Spark.Connect.DataType.Geography{}}}

  defp type_to_proto({:struct, fields}) do
    proto_fields =
      Enum.map(fields, fn field ->
        %Spark.Connect.DataType.StructField{
          name: field.name,
          data_type: type_to_proto(field.type),
          nullable: field.nullable,
          metadata: encode_field_metadata(Map.get(field, :metadata, %{}))
        }
      end)

    %Spark.Connect.DataType{
      kind: {:struct, %Spark.Connect.DataType.Struct{fields: proto_fields}}
    }
  end

  defp encode_field_metadata(metadata) when is_binary(metadata), do: metadata
  defp encode_field_metadata(metadata) when is_map(metadata), do: Jason.encode!(metadata)
  defp encode_field_metadata(metadata), do: Jason.encode!(metadata)

  @doc false
  @spec schema_to_string(struct_type() | String.t()) :: String.t()
  def schema_to_string(schema) when is_binary(schema), do: schema
  def schema_to_string({:struct, _} = schema), do: to_ddl(schema)
end
