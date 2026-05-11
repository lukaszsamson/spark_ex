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

  @typedoc "A Spark Connect DataType protobuf struct."
  @type data_type_proto :: Spark.Connect.DataType.t()

  @typedoc "A Spark Connect StorageLevel protobuf struct."
  @type storage_level :: Spark.Connect.StorageLevel.t()

  @typedoc "A Spark Connect StreamingForeachFunction protobuf struct."
  @type foreach_function :: Spark.Connect.StreamingForeachFunction.t()

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
          | {:time, non_neg_integer()}
          | :timestamp
          | :timestamp_ntz
          | :day_time_interval
          | {:day_time_interval, non_neg_integer(), non_neg_integer()}
          | :year_month_interval
          | {:year_month_interval, non_neg_integer(), non_neg_integer()}
          | :calendar_interval
          | {:decimal, non_neg_integer(), non_neg_integer()}
          | {:array, spark_type()}
          | {:map, spark_type(), spark_type()}
          | {:struct, [field()]}
          | :variant
          | :geometry
          | {:geometry, integer()}
          | :geography
          | {:geography, integer()}

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
    Enum.map_join(fields, ", ", fn field ->
      suffix = if Map.get(field, :nullable, true) == false, do: " NOT NULL", else: ""
      "#{quote_identifier(field.name)} #{type_to_ddl(field.type)}#{suffix}"
    end)
  end

  @doc """
  Quotes a Spark SQL identifier with backticks when necessary.

  Identifiers that contain characters outside `[A-Za-z0-9_]` (or that
  match a reserved word) are wrapped in backticks. Embedded backticks
  are escaped by doubling.

  Plain identifiers like `"id"` or `"user_name"` are returned unchanged.
  """
  @spec quote_identifier(String.t()) :: String.t()
  def quote_identifier(name) when is_binary(name) do
    if needs_quoting?(name) do
      escaped = String.replace(name, "`", "``")
      "`" <> escaped <> "`"
    else
      name
    end
  end

  defp needs_quoting?(""), do: true

  defp needs_quoting?(name) do
    not Regex.match?(~r/\A[A-Za-z_][A-Za-z0-9_]*\z/, name) or reserved_word?(name)
  end

  # Conservative subset of Spark SQL reserved words that are likely to appear
  # as field names in user data. Quoting non-reserved identifiers is safe, so
  # this list errs on the side of inclusion when a name doubles as a keyword.
  @reserved_words ~w(
    select from where group order by having join on as case when then else end
    distinct union intersect except all any in not and or null true false
    insert update delete create table view drop alter index primary key
    references foreign default values into between like is exists with
    cast convert interval timestamp date time array map struct decimal
    string int integer bigint smallint tinyint float double boolean binary
    cross inner outer left right full natural using lateral table values
    rollup cube grouping limit offset fetch sort over partition window
    rows range unbounded preceding following current row
  )

  defp reserved_word?(name) do
    String.downcase(name) in @reserved_words
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
  @spec data_type_to_json(data_type_proto()) :: String.t()
  def data_type_to_json(%Spark.Connect.DataType{} = data_type) do
    data_type
    |> proto_type_to_json()
    |> Jason.encode!()
  end

  @doc """
  Converts a struct type to Spark Connect `DataType` protobuf.

  Preserves JSON-level fidelity (field nullability and metadata) for nested types.
  """
  @spec to_proto(struct_type()) :: data_type_proto()
  def to_proto({:struct, _} = schema), do: type_to_proto(schema)

  # --- DDL type conversion ---

  defp type_to_ddl(:null), do: "VOID"
  defp type_to_ddl(:boolean), do: "BOOLEAN"
  defp type_to_ddl(:byte), do: "TINYINT"
  defp type_to_ddl(:short), do: "SMALLINT"
  defp type_to_ddl(:integer), do: "INT"
  defp type_to_ddl(:long), do: "BIGINT"
  defp type_to_ddl(:float), do: "FLOAT"
  defp type_to_ddl(:double), do: "DOUBLE"
  defp type_to_ddl(:string), do: "STRING"
  defp type_to_ddl({:string, collation}), do: "STRING COLLATE #{collation}"
  defp type_to_ddl({:char, length}), do: "CHAR(#{length})"
  defp type_to_ddl({:varchar, length}), do: "VARCHAR(#{length})"
  defp type_to_ddl(:binary), do: "BINARY"
  defp type_to_ddl(:date), do: "DATE"
  defp type_to_ddl(:time), do: "TIME"
  defp type_to_ddl({:time, precision}) when is_integer(precision), do: "TIME(#{precision})"
  defp type_to_ddl(:timestamp), do: "TIMESTAMP"
  defp type_to_ddl(:timestamp_ntz), do: "TIMESTAMP_NTZ"
  defp type_to_ddl(:day_time_interval), do: "INTERVAL DAY TO SECOND"

  defp type_to_ddl({:day_time_interval, start_field, end_field}) do
    validate_day_time_interval_fields!(start_field, end_field)

    if start_field == end_field do
      "INTERVAL #{day_time_interval_field(start_field)}"
    else
      "INTERVAL #{day_time_interval_field(start_field)} TO #{day_time_interval_field(end_field)}"
    end
  end

  defp type_to_ddl(:year_month_interval), do: "INTERVAL YEAR TO MONTH"

  defp type_to_ddl({:year_month_interval, start_field, end_field}) do
    validate_year_month_interval_fields!(start_field, end_field)

    if start_field == end_field do
      "INTERVAL #{year_month_interval_field(start_field)}"
    else
      "INTERVAL #{year_month_interval_field(start_field)} TO #{year_month_interval_field(end_field)}"
    end
  end

  defp type_to_ddl(:calendar_interval), do: "INTERVAL"
  defp type_to_ddl({:decimal, precision, scale}), do: "DECIMAL(#{precision}, #{scale})"
  defp type_to_ddl({:array, element}), do: "ARRAY<#{type_to_ddl(element)}>"
  defp type_to_ddl({:array, element, _contains_null}), do: "ARRAY<#{type_to_ddl(element)}>"
  defp type_to_ddl({:map, key, value}), do: "MAP<#{type_to_ddl(key)}, #{type_to_ddl(value)}>"

  defp type_to_ddl({:map, key, value, _vcn}),
    do: "MAP<#{type_to_ddl(key)}, #{type_to_ddl(value)}>"

  defp type_to_ddl(:variant), do: "VARIANT"
  defp type_to_ddl(:geometry), do: "GEOMETRY"
  defp type_to_ddl({:geometry, srid}) when is_integer(srid), do: "GEOMETRY(#{srid})"
  defp type_to_ddl(:geography), do: "GEOGRAPHY"
  defp type_to_ddl({:geography, srid}) when is_integer(srid), do: "GEOGRAPHY(#{srid})"

  defp type_to_ddl({:struct, fields}) do
    inner =
      Enum.map_join(fields, ", ", fn field ->
        suffix = if Map.get(field, :nullable, true) == false, do: " NOT NULL", else: ""
        "#{quote_identifier(field.name)}: #{type_to_ddl(field.type)}#{suffix}"
      end)

    "STRUCT<#{inner}>"
  end

  # Spark uses these tokens for day-time interval start/end fields.
  # 0=DAY, 1=HOUR, 2=MINUTE, 3=SECOND.
  defp day_time_interval_field(0), do: "DAY"
  defp day_time_interval_field(1), do: "HOUR"
  defp day_time_interval_field(2), do: "MINUTE"
  defp day_time_interval_field(3), do: "SECOND"

  # 0=YEAR, 1=MONTH.
  defp year_month_interval_field(0), do: "YEAR"
  defp year_month_interval_field(1), do: "MONTH"

  # Lowercase forms used in PySpark's jsonValue / simpleString output.
  defp day_time_interval_json_field(0), do: "day"
  defp day_time_interval_json_field(1), do: "hour"
  defp day_time_interval_json_field(2), do: "minute"
  defp day_time_interval_json_field(3), do: "second"

  defp year_month_interval_json_field(0), do: "year"
  defp year_month_interval_json_field(1), do: "month"

  defp validate_day_time_interval_fields!(start_field, end_field) do
    unless is_integer(start_field) and start_field in 0..3 do
      raise ArgumentError,
            "invalid day_time_interval start_field: #{inspect(start_field)}. " <>
              "Expected an integer in 0..3 (0=DAY, 1=HOUR, 2=MINUTE, 3=SECOND)."
    end

    unless is_integer(end_field) and end_field in 0..3 do
      raise ArgumentError,
            "invalid day_time_interval end_field: #{inspect(end_field)}. " <>
              "Expected an integer in 0..3 (0=DAY, 1=HOUR, 2=MINUTE, 3=SECOND)."
    end

    if start_field > end_field do
      raise ArgumentError,
            "invalid day_time_interval field order: start_field (#{start_field}) " <>
              "must be <= end_field (#{end_field})."
    end

    :ok
  end

  defp validate_year_month_interval_fields!(start_field, end_field) do
    unless is_integer(start_field) and start_field in 0..1 do
      raise ArgumentError,
            "invalid year_month_interval start_field: #{inspect(start_field)}. " <>
              "Expected an integer in 0..1 (0=YEAR, 1=MONTH)."
    end

    unless is_integer(end_field) and end_field in 0..1 do
      raise ArgumentError,
            "invalid year_month_interval end_field: #{inspect(end_field)}. " <>
              "Expected an integer in 0..1 (0=YEAR, 1=MONTH)."
    end

    if start_field > end_field do
      raise ArgumentError,
            "invalid year_month_interval field order: start_field (#{start_field}) " <>
              "must be <= end_field (#{end_field})."
    end

    :ok
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

  # PySpark's StringType.jsonValue emits "string" for the UTF8_BINARY default
  # collation and "string collate <name>" otherwise; the value is always a
  # plain string, never a map.
  defp type_to_json({:string, ""}), do: "string"
  defp type_to_json({:string, "UTF8_BINARY"}), do: "string"
  defp type_to_json({:string, collation}), do: "string collate #{collation}"

  defp type_to_json({:char, length}), do: "char(#{length})"
  defp type_to_json({:varchar, length}), do: "varchar(#{length})"
  defp type_to_json(:binary), do: "binary"
  defp type_to_json(:date), do: "date"
  # TimeType default precision is 6.
  defp type_to_json(:time), do: "time(6)"
  defp type_to_json({:time, precision}) when is_integer(precision), do: "time(#{precision})"
  defp type_to_json(:timestamp), do: "timestamp"
  defp type_to_json(:timestamp_ntz), do: "timestamp_ntz"
  defp type_to_json(:day_time_interval), do: "interval day to second"

  defp type_to_json({:day_time_interval, start_field, end_field}) do
    validate_day_time_interval_fields!(start_field, end_field)

    if start_field == end_field do
      "interval #{day_time_interval_json_field(start_field)}"
    else
      "interval #{day_time_interval_json_field(start_field)} to #{day_time_interval_json_field(end_field)}"
    end
  end

  defp type_to_json(:year_month_interval), do: "interval year to month"

  defp type_to_json({:year_month_interval, start_field, end_field}) do
    validate_year_month_interval_fields!(start_field, end_field)

    if start_field == end_field do
      "interval #{year_month_interval_json_field(start_field)}"
    else
      "interval #{year_month_interval_json_field(start_field)} to #{year_month_interval_json_field(end_field)}"
    end
  end

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
  defp type_to_json({:geometry, srid}) when is_integer(srid), do: "geometry(#{srid})"
  defp type_to_json(:geography), do: "geography"
  defp type_to_json({:geography, srid}) when is_integer(srid), do: "geography(#{srid})"

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
    proto_kind_to_json(kind, value)
  end

  defp proto_kind_to_json(kind, _value)
       when kind in [
              :null,
              :boolean,
              :byte,
              :short,
              :integer,
              :long,
              :float,
              :double,
              :binary,
              :date,
              :timestamp,
              :timestamp_ntz,
              :calendar_interval,
              :variant
            ] do
    proto_scalar_kind_to_json(kind)
  end

  defp proto_kind_to_json(:string, value), do: string_proto_to_json(value)
  defp proto_kind_to_json(:char, value), do: "char(#{value.length})"
  defp proto_kind_to_json(:var_char, value), do: "varchar(#{value.length})"
  defp proto_kind_to_json(:time, value), do: time_proto_to_json(value)
  defp proto_kind_to_json(:decimal, value), do: "decimal(#{value.precision},#{value.scale})"
  defp proto_kind_to_json(:day_time_interval, value), do: day_time_interval_proto_to_json(value)

  defp proto_kind_to_json(:year_month_interval, value),
    do: year_month_interval_proto_to_json(value)

  defp proto_kind_to_json(:array, value), do: array_proto_to_json(value)
  defp proto_kind_to_json(:map, value), do: map_proto_to_json(value)
  defp proto_kind_to_json(:struct, value), do: struct_proto_to_json(value)
  defp proto_kind_to_json(:geometry, value), do: geometry_proto_to_json(value)
  defp proto_kind_to_json(:geography, value), do: geography_proto_to_json(value)
  defp proto_kind_to_json(:unparsed, value), do: value.data_type_string

  defp proto_kind_to_json(kind, _value),
    do: raise(ArgumentError, "unsupported Spark.Connect.DataType kind: #{inspect(kind)}")

  defp proto_scalar_kind_to_json(:null), do: "void"
  defp proto_scalar_kind_to_json(:boolean), do: "boolean"
  defp proto_scalar_kind_to_json(:byte), do: "byte"
  defp proto_scalar_kind_to_json(:short), do: "short"
  defp proto_scalar_kind_to_json(:integer), do: "integer"
  defp proto_scalar_kind_to_json(:long), do: "long"
  defp proto_scalar_kind_to_json(:float), do: "float"
  defp proto_scalar_kind_to_json(:double), do: "double"
  defp proto_scalar_kind_to_json(:binary), do: "binary"
  defp proto_scalar_kind_to_json(:date), do: "date"
  defp proto_scalar_kind_to_json(:timestamp), do: "timestamp"
  defp proto_scalar_kind_to_json(:timestamp_ntz), do: "timestamp_ntz"
  defp proto_scalar_kind_to_json(:calendar_interval), do: "interval"
  defp proto_scalar_kind_to_json(:variant), do: "variant"

  defp time_proto_to_json(%Spark.Connect.DataType.Time{precision: precision})
       when is_integer(precision),
       do: "time(#{precision})"

  # PySpark's TimeType always carries a precision (default 6). Mirror that on
  # decode so the JSON value round-trips through PySpark's parser unchanged.
  defp time_proto_to_json(_), do: "time(6)"

  defp day_time_interval_proto_to_json(%Spark.Connect.DataType.DayTimeInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and sf in 0..3 and is_integer(ef) and ef in 0..3 and sf == ef,
       do: "interval #{day_time_interval_json_field(sf)}"

  defp day_time_interval_proto_to_json(%Spark.Connect.DataType.DayTimeInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and sf in 0..3 and is_integer(ef) and ef in 0..3 and sf <= ef,
       do: "interval #{day_time_interval_json_field(sf)} to #{day_time_interval_json_field(ef)}"

  defp day_time_interval_proto_to_json(_), do: "interval day to second"

  defp year_month_interval_proto_to_json(%Spark.Connect.DataType.YearMonthInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and sf in 0..1 and is_integer(ef) and ef in 0..1 and sf == ef,
       do: "interval #{year_month_interval_json_field(sf)}"

  defp year_month_interval_proto_to_json(%Spark.Connect.DataType.YearMonthInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and sf in 0..1 and is_integer(ef) and ef in 0..1 and sf <= ef,
       do:
         "interval #{year_month_interval_json_field(sf)} to #{year_month_interval_json_field(ef)}"

  defp year_month_interval_proto_to_json(_), do: "interval year to month"

  # PySpark's jsonValue for spatial types uses the CRS string ("geometry({crs})"),
  # but the wire proto only carries an int32 SRID. We don't ship a SRID↔CRS
  # mapper, so emit the simpleString form ("geometry(<srid>)" / "geometry(any)")
  # which round-trips through Spark's DDL parser.
  defp geometry_proto_to_json(%Spark.Connect.DataType.Geometry{srid: srid})
       when is_integer(srid) and srid != 0,
       do: "geometry(#{srid})"

  defp geometry_proto_to_json(_), do: "geometry"

  defp geography_proto_to_json(%Spark.Connect.DataType.Geography{srid: srid})
       when is_integer(srid) and srid != 0,
       do: "geography(#{srid})"

  defp geography_proto_to_json(_), do: "geography"

  defp string_proto_to_json(%Spark.Connect.DataType.String{collation: c})
       when c in ["", "UTF8_BINARY"],
       do: "string"

  defp string_proto_to_json(%Spark.Connect.DataType.String{collation: collation}),
    do: "string collate #{collation}"

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

  defp type_to_proto({:time, precision}) when is_integer(precision) do
    %Spark.Connect.DataType{
      kind: {:time, %Spark.Connect.DataType.Time{precision: precision}}
    }
  end

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

  defp type_to_proto({:day_time_interval, start_field, end_field}) do
    validate_day_time_interval_fields!(start_field, end_field)

    %Spark.Connect.DataType{
      kind:
        {:day_time_interval,
         %Spark.Connect.DataType.DayTimeInterval{
           start_field: start_field,
           end_field: end_field
         }}
    }
  end

  defp type_to_proto(:year_month_interval) do
    %Spark.Connect.DataType{
      kind: {:year_month_interval, %Spark.Connect.DataType.YearMonthInterval{}}
    }
  end

  defp type_to_proto({:year_month_interval, start_field, end_field}) do
    validate_year_month_interval_fields!(start_field, end_field)

    %Spark.Connect.DataType{
      kind:
        {:year_month_interval,
         %Spark.Connect.DataType.YearMonthInterval{
           start_field: start_field,
           end_field: end_field
         }}
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

  defp type_to_proto({:geometry, srid}) when is_integer(srid) do
    %Spark.Connect.DataType{kind: {:geometry, %Spark.Connect.DataType.Geometry{srid: srid}}}
  end

  defp type_to_proto(:geography),
    do: %Spark.Connect.DataType{kind: {:geography, %Spark.Connect.DataType.Geography{}}}

  defp type_to_proto({:geography, srid}) when is_integer(srid) do
    %Spark.Connect.DataType{kind: {:geography, %Spark.Connect.DataType.Geography{srid: srid}}}
  end

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

  @doc """
  Parses a Spark DDL data-type string into a Spark Connect `DataType` protobuf
  for the simple primitive cases without a server round-trip.

  Supports primitive type names (e.g. `"INT"`, `"BIGINT"`, `"STRING"`),
  `DECIMAL(p, s)`, `DECIMAL(p)`, and `DECIMAL`. Returns `:error` for
  expressions outside that subset (arrays, structs, maps, intervals, etc.) so
  callers can fall back to the server's DDL parser.
  """
  @spec parse_ddl_type(String.t()) :: {:ok, data_type_proto()} | :error
  def parse_ddl_type(ddl) when is_binary(ddl) do
    trimmed = ddl |> String.trim() |> String.upcase()
    parse_trimmed_ddl(trimmed)
  end

  defp parse_trimmed_ddl(""), do: :error
  defp parse_trimmed_ddl("VOID"), do: {:ok, type_to_proto(:null)}
  defp parse_trimmed_ddl("NULL"), do: {:ok, type_to_proto(:null)}
  defp parse_trimmed_ddl("BOOLEAN"), do: {:ok, type_to_proto(:boolean)}
  defp parse_trimmed_ddl("BOOL"), do: {:ok, type_to_proto(:boolean)}
  defp parse_trimmed_ddl("TINYINT"), do: {:ok, type_to_proto(:byte)}
  defp parse_trimmed_ddl("BYTE"), do: {:ok, type_to_proto(:byte)}
  defp parse_trimmed_ddl("SMALLINT"), do: {:ok, type_to_proto(:short)}
  defp parse_trimmed_ddl("SHORT"), do: {:ok, type_to_proto(:short)}
  defp parse_trimmed_ddl("INT"), do: {:ok, type_to_proto(:integer)}
  defp parse_trimmed_ddl("INTEGER"), do: {:ok, type_to_proto(:integer)}
  defp parse_trimmed_ddl("BIGINT"), do: {:ok, type_to_proto(:long)}
  defp parse_trimmed_ddl("LONG"), do: {:ok, type_to_proto(:long)}
  defp parse_trimmed_ddl("FLOAT"), do: {:ok, type_to_proto(:float)}
  defp parse_trimmed_ddl("REAL"), do: {:ok, type_to_proto(:float)}
  defp parse_trimmed_ddl("DOUBLE"), do: {:ok, type_to_proto(:double)}
  defp parse_trimmed_ddl("STRING"), do: {:ok, type_to_proto(:string)}
  defp parse_trimmed_ddl("BINARY"), do: {:ok, type_to_proto(:binary)}
  defp parse_trimmed_ddl("DATE"), do: {:ok, type_to_proto(:date)}
  defp parse_trimmed_ddl("TIME"), do: {:ok, type_to_proto(:time)}
  defp parse_trimmed_ddl("TIMESTAMP"), do: {:ok, type_to_proto(:timestamp)}
  defp parse_trimmed_ddl("TIMESTAMP_NTZ"), do: {:ok, type_to_proto(:timestamp_ntz)}
  defp parse_trimmed_ddl("VARIANT"), do: {:ok, type_to_proto(:variant)}
  defp parse_trimmed_ddl("GEOMETRY"), do: {:ok, type_to_proto(:geometry)}
  defp parse_trimmed_ddl("GEOGRAPHY"), do: {:ok, type_to_proto(:geography)}
  defp parse_trimmed_ddl(trimmed), do: parse_decimal_ddl(trimmed)

  defp parse_decimal_ddl("DECIMAL"), do: {:ok, type_to_proto({:decimal, 10, 0})}
  defp parse_decimal_ddl("NUMERIC"), do: {:ok, type_to_proto({:decimal, 10, 0})}
  defp parse_decimal_ddl("DEC"), do: {:ok, type_to_proto({:decimal, 10, 0})}

  defp parse_decimal_ddl(ddl) do
    cond do
      result = decimal_match(ddl, "DECIMAL") -> result
      result = decimal_match(ddl, "NUMERIC") -> result
      result = decimal_match(ddl, "DEC") -> result
      true -> :error
    end
  end

  defp decimal_match(ddl, prefix) do
    case Regex.run(~r/^#{prefix}\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$/, ddl) do
      [_, p_str] ->
        p = String.to_integer(p_str)
        {:ok, type_to_proto({:decimal, p, 0})}

      [_, p_str, s_str] ->
        p = String.to_integer(p_str)
        s = String.to_integer(s_str)
        {:ok, type_to_proto({:decimal, p, s})}

      _ ->
        nil
    end
  end
end
