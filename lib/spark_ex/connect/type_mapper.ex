defmodule SparkEx.Connect.TypeMapper do
  @moduledoc false

  alias Spark.Connect.DataType

  @doc """
  Converts a Spark Connect `DataType` to an Explorer dtype atom.

  Spark types without a native Explorer dtype are mapped to the dtype of their
  Arrow wire representation: MAP → `{:list, {:struct, [key, value]}}`, VARIANT
  and GEOMETRY/GEOGRAPHY → their `{:struct, _}` layout, UDT → its `sql_type`.
  Returns `{:ok, nil}` only for types with no Explorer representation at all
  (year-month / calendar interval, unparsed, or a UDT without `sql_type`);
  callers building a frame should omit those dtypes and let Explorer infer.

  ## Examples

      iex> TypeMapper.to_explorer_dtype(%DataType{kind: {:boolean, %DataType.Boolean{}}})
      {:ok, :boolean}

      iex> element = %DataType{kind: {:integer, %DataType.Integer{}}}
      iex> TypeMapper.to_explorer_dtype(%DataType{kind: {:array, %DataType.Array{element_type: element}}})
      {:ok, {:list, {:s, 32}}}
  """
  @spec to_explorer_dtype(DataType.t() | nil) :: {:ok, atom() | {atom(), term()} | nil}
  def to_explorer_dtype(%DataType{kind: {tag, _value}} = dt) do
    {:ok, map_kind(tag, dt)}
  end

  def to_explorer_dtype(%DataType{kind: nil}) do
    {:ok, :null}
  end

  # A nested DataType slot can be unset on the wire; treat it as VOID/null.
  def to_explorer_dtype(nil), do: {:ok, :null}

  @doc """
  Converts a Spark Connect `DataType` directly to a Spark DDL type string.

  Uses direct mapping to preserve precision information (e.g., DECIMAL scale/precision)
  that would be lost in an Explorer dtype round-trip.
  """
  @spec data_type_to_ddl(DataType.t() | nil) :: String.t()
  def data_type_to_ddl(%DataType{kind: {tag, value}}) do
    direct_ddl(tag, value)
  end

  def data_type_to_ddl(%DataType{kind: nil}), do: "VOID"

  # Nested DataType fields can arrive as nil for forward-compat / partial responses.
  def data_type_to_ddl(nil), do: "VOID"

  @doc """
  Converts a Spark Connect schema (`DataType.Struct`) to Explorer dtypes map.

  Returns a keyword list of `{column_name, dtype}` pairs.

  Note: the `nullable` field from `StructField` is not preserved, as Explorer
  schemas do not carry nullability information.
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

  # Decimal
  defp map_kind(:decimal, %DataType{kind: {:decimal, %DataType.Decimal{precision: p, scale: s}}})
       when not is_nil(p) and not is_nil(s),
       do: {:decimal, p, s}

  defp map_kind(:decimal, %DataType{kind: {:decimal, %DataType.Decimal{precision: p}}})
       when not is_nil(p),
       do: {:decimal, p, 0}

  defp map_kind(:decimal, _dt), do: {:decimal, 10, 0}

  # String types
  defp map_kind(:string, _dt), do: :string
  defp map_kind(:char, _dt), do: :string
  defp map_kind(:var_char, _dt), do: :string

  # Binary
  defp map_kind(:binary, _dt), do: :binary

  # Date and time types
  defp map_kind(:date, _dt), do: :date
  # TIMESTAMP is tz-aware (UTC instant). Explorer's only tz-aware dtype is the
  # 3-tuple {:datetime, precision, tz}; the deprecated 2-tuple normalizes to a
  # naive dtype, collapsing TIMESTAMP into TIMESTAMP_NTZ and warning per use.
  defp map_kind(:timestamp, _dt), do: {:datetime, :microsecond, "Etc/UTC"}
  defp map_kind(:timestamp_ntz, _dt), do: {:naive_datetime, :microsecond}
  # Explorer 0.11's only time dtype is the bare atom `:time`; {:time, _} is not
  # a valid dtype and raises in check_dtypes!.
  defp map_kind(:time, _dt), do: :time

  # Interval types. Day-time intervals decode as %Explorer.Duration{}, whose
  # dtype is {:duration, precision}; mapping them to :string mismatches the
  # decoded cells and crashes the schema-policy rebuild. Year-month/calendar
  # intervals have no native Explorer dtype, so leave them unmapped (nil) and
  # let the rebuild infer from the decoded cells.
  defp map_kind(:calendar_interval, _dt), do: nil
  defp map_kind(:year_month_interval, _dt), do: nil
  defp map_kind(:day_time_interval, _dt), do: {:duration, :microsecond}

  # Complex types — preserve native nested structure
  defp map_kind(:array, %DataType{kind: {:array, %DataType.Array{element_type: et}}})
       when not is_nil(et) do
    case to_explorer_dtype(et) do
      # An element with no native dtype (e.g. ARRAY<MAP<...>>) can't be expressed
      # as {:list, dtype}; leave the whole list unmapped so cells are inferred.
      {:ok, nil} -> nil
      {:ok, dtype} -> {:list, dtype}
    end
  end

  defp map_kind(:array, _dt), do: {:list, :null}

  defp map_kind(:struct, %DataType{kind: {:struct, %DataType.Struct{fields: fields}}}) do
    field_dtypes =
      Enum.map(fields, fn %DataType.StructField{name: name, data_type: dt} ->
        {:ok, dtype} = to_explorer_dtype(dt)
        {name, dtype}
      end)

    # If any field has no native dtype, the {:struct, _} dtype would be invalid;
    # leave the whole struct unmapped so cells are inferred from decoded values.
    if Enum.any?(field_dtypes, fn {_name, dtype} -> is_nil(dtype) end) do
      nil
    else
      {:struct, field_dtypes}
    end
  end

  # A struct without a decoded field list cannot be represented safely: a
  # polars struct cast to `{:struct, []}` would silently drop every field.
  defp map_kind(:struct, _dt), do: nil

  # Map: Explorer has no native map dtype. Arrow encodes MAP<K, V> as
  # `map<struct<key: K, value: V>>`, which polars/Explorer reads as a list of
  # structs, so that is the structurally correct dtype for both empty and
  # non-empty frames (T-30). Collapses to nil when a component has no dtype.
  defp map_kind(:map, %DataType{kind: {:map, %DataType.Map{key_type: kt, value_type: vt}}}) do
    with {:ok, key_dtype} when not is_nil(key_dtype) <- to_explorer_dtype(kt),
         {:ok, value_dtype} when not is_nil(value_dtype) <- to_explorer_dtype(vt) do
      {:list, {:struct, [{"key", key_dtype}, {"value", value_dtype}]}}
    else
      _ -> nil
    end
  end

  # Variant is shipped over Arrow as `struct<value: binary, metadata: binary>`
  # (ArrowUtils.toArrowField), so mirror that structure.
  defp map_kind(:variant, _dt), do: {:struct, [{"value", :binary}, {"metadata", :binary}]}

  # A UDT travels as its `sql_type`; when the server omits it the column is
  # left unmapped so the decoded cells drive dtype inference.
  defp map_kind(:udt, %DataType{kind: {:udt, %DataType.UDT{sql_type: %DataType{} = sql_type}}}) do
    {:ok, dtype} = to_explorer_dtype(sql_type)
    dtype
  end

  defp map_kind(:udt, _dt), do: nil

  # Geometry / geography arrive as `struct<srid: int, wkb: binary>`.
  defp map_kind(:geometry, _dt), do: {:struct, [{"srid", {:s, 32}}, {"wkb", :binary}]}
  defp map_kind(:geography, _dt), do: {:struct, [{"srid", {:s, 32}}, {"wkb", :binary}]}

  # Unparsed types carry no structure we can map; leave them for inference.
  defp map_kind(:unparsed, _dt), do: nil

  # Catch-all for future types: leave unmapped so the rebuild infers from cells.
  defp map_kind(_unknown, _dt), do: nil

  # --- Direct DataType → DDL mapping (preserves precision) ---

  defp direct_ddl(:null, _), do: "VOID"
  defp direct_ddl(:boolean, _), do: "BOOLEAN"
  defp direct_ddl(:byte, _), do: "TINYINT"
  defp direct_ddl(:short, _), do: "SMALLINT"
  defp direct_ddl(:integer, _), do: "INT"
  defp direct_ddl(:long, _), do: "BIGINT"
  defp direct_ddl(:float, _), do: "FLOAT"
  defp direct_ddl(:double, _), do: "DOUBLE"

  defp direct_ddl(:string, %DataType.String{collation: c})
       when is_binary(c) and c != "" and c != "UTF8_BINARY" do
    "STRING COLLATE #{c}"
  end

  defp direct_ddl(:string, _), do: "STRING"

  defp direct_ddl(:char, %DataType.Char{length: length}) when is_integer(length) and length > 0 do
    "CHAR(#{length})"
  end

  defp direct_ddl(:char, _), do: "STRING"

  defp direct_ddl(:var_char, %DataType.VarChar{length: length})
       when is_integer(length) and length > 0 do
    "VARCHAR(#{length})"
  end

  defp direct_ddl(:var_char, _), do: "STRING"
  defp direct_ddl(:binary, _), do: "BINARY"
  defp direct_ddl(:date, _), do: "DATE"
  defp direct_ddl(:timestamp, _), do: "TIMESTAMP"
  defp direct_ddl(:timestamp_ntz, _), do: "TIMESTAMP_NTZ"

  defp direct_ddl(:time, %DataType.Time{precision: precision}) when is_integer(precision) do
    "TIME(#{precision})"
  end

  # PySpark's TimeType default precision is 6; render that explicitly so DDL
  # round-trips identically against PySpark's parser.
  defp direct_ddl(:time, _), do: "TIME(6)"

  defp direct_ddl(:decimal, %DataType.Decimal{precision: p, scale: s})
       when not is_nil(p) and not is_nil(s) do
    "DECIMAL(#{p}, #{s})"
  end

  defp direct_ddl(:decimal, %DataType.Decimal{precision: p}) when not is_nil(p) do
    "DECIMAL(#{p}, 0)"
  end

  defp direct_ddl(:decimal, _), do: "DECIMAL(10, 0)"

  defp direct_ddl(:calendar_interval, _), do: "INTERVAL"

  # PySpark sets endField = startField when end_field is absent
  # (connect/types.py:260-283), so start-only renders the single field.
  defp direct_ddl(:year_month_interval, %DataType.YearMonthInterval{
         start_field: sf,
         end_field: nil
       })
       when is_integer(sf) do
    "INTERVAL #{year_month_interval_field(sf)}"
  end

  defp direct_ddl(:year_month_interval, %DataType.YearMonthInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and is_integer(ef) and sf == ef do
    "INTERVAL #{year_month_interval_field(sf)}"
  end

  defp direct_ddl(:year_month_interval, %DataType.YearMonthInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and is_integer(ef) do
    "INTERVAL #{year_month_interval_field(sf)} TO #{year_month_interval_field(ef)}"
  end

  defp direct_ddl(:year_month_interval, _), do: "INTERVAL YEAR TO MONTH"

  # PySpark sets endField = startField when end_field is absent
  # (connect/types.py:260-283), so start-only renders the single field.
  defp direct_ddl(:day_time_interval, %DataType.DayTimeInterval{
         start_field: sf,
         end_field: nil
       })
       when is_integer(sf) do
    "INTERVAL #{day_time_interval_field(sf)}"
  end

  defp direct_ddl(:day_time_interval, %DataType.DayTimeInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and is_integer(ef) and sf == ef do
    "INTERVAL #{day_time_interval_field(sf)}"
  end

  defp direct_ddl(:day_time_interval, %DataType.DayTimeInterval{
         start_field: sf,
         end_field: ef
       })
       when is_integer(sf) and is_integer(ef) do
    "INTERVAL #{day_time_interval_field(sf)} TO #{day_time_interval_field(ef)}"
  end

  defp direct_ddl(:day_time_interval, _), do: "INTERVAL DAY TO SECOND"

  defp direct_ddl(:variant, _), do: "VARIANT"

  # MIXED_SRID (-1) renders as GEOMETRY(ANY) (PySpark GeometryType.simpleString
  # "geometry(any)"); SRID 0 is a valid Cartesian SRS rendered GEOMETRY(0), not
  # bare GEOMETRY.
  defp direct_ddl(:geometry, %DataType.Geometry{srid: -1}), do: "GEOMETRY(ANY)"

  defp direct_ddl(:geometry, %DataType.Geometry{srid: srid}) when is_integer(srid) do
    "GEOMETRY(#{srid})"
  end

  defp direct_ddl(:geometry, _), do: "GEOMETRY"

  defp direct_ddl(:geography, %DataType.Geography{srid: -1}), do: "GEOGRAPHY(ANY)"

  defp direct_ddl(:geography, %DataType.Geography{srid: srid}) when is_integer(srid) do
    "GEOGRAPHY(#{srid})"
  end

  defp direct_ddl(:geography, _), do: "GEOGRAPHY"

  defp direct_ddl(:array, %DataType.Array{element_type: element_type}) do
    "ARRAY<#{data_type_to_ddl(element_type)}>"
  end

  defp direct_ddl(:struct, %DataType.Struct{fields: fields}) do
    inner =
      fields
      |> Enum.map_join(", ", fn %DataType.StructField{
                                  name: name,
                                  data_type: data_type,
                                  nullable: nullable
                                } ->
        suffix = if nullable == false, do: " NOT NULL", else: ""
        "#{SparkEx.Types.quote_identifier(name)}: #{data_type_to_ddl(data_type)}#{suffix}"
      end)

    "STRUCT<#{inner}>"
  end

  defp direct_ddl(:map, %DataType.Map{key_type: key_type, value_type: value_type}) do
    "MAP<#{data_type_to_ddl(key_type)}, #{data_type_to_ddl(value_type)}>"
  end

  defp direct_ddl(_, _), do: "STRING"

  defp day_time_interval_field(0), do: "DAY"
  defp day_time_interval_field(1), do: "HOUR"
  defp day_time_interval_field(2), do: "MINUTE"
  defp day_time_interval_field(3), do: "SECOND"

  defp year_month_interval_field(0), do: "YEAR"
  defp year_month_interval_field(1), do: "MONTH"

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
  def to_spark_ddl_type({:s, 8}), do: "TINYINT"
  def to_spark_ddl_type({:s, 16}), do: "SMALLINT"
  def to_spark_ddl_type({:s, 32}), do: "INT"
  def to_spark_ddl_type({:s, 64}), do: "BIGINT"
  def to_spark_ddl_type({:u, 8}), do: "SMALLINT"
  def to_spark_ddl_type({:u, 16}), do: "INT"
  def to_spark_ddl_type({:u, 32}), do: "BIGINT"
  # Note: {:u, 64} (max 2^64-1) mapped to BIGINT (signed, max 2^63-1) is lossy.
  # Values above 2^63-1 will overflow. Spark has no unsigned 64-bit integer type.
  def to_spark_ddl_type({:u, 64}), do: "BIGINT"
  def to_spark_ddl_type({:f, 32}), do: "FLOAT"
  def to_spark_ddl_type({:f, 64}), do: "DOUBLE"
  def to_spark_ddl_type({:decimal, precision, scale}), do: "DECIMAL(#{precision}, #{scale})"
  def to_spark_ddl_type(:string), do: "STRING"
  def to_spark_ddl_type(:binary), do: "BINARY"
  def to_spark_ddl_type(:date), do: "DATE"
  # Explorer's real tz-aware datetime dtype is the 3-tuple {:datetime, p, tz};
  # PySpark's from_arrow_type maps a tz-aware Arrow timestamp to TimestampType.
  def to_spark_ddl_type({:datetime, _precision, _tz}), do: "TIMESTAMP"
  def to_spark_ddl_type({:naive_datetime, _}), do: "TIMESTAMP_NTZ"
  # Explorer's real time dtype is the bare atom :time (no precision tuple);
  # PySpark's from_arrow_type maps an Arrow time to TimeType.
  def to_spark_ddl_type(:time), do: "TIME"
  # Explorer durations map to a day-time interval; PySpark's from_arrow_type
  # yields DayTimeIntervalType for an Arrow duration.
  def to_spark_ddl_type({:duration, _}), do: "INTERVAL DAY TO SECOND"
  def to_spark_ddl_type(:category), do: "STRING"
  def to_spark_ddl_type({:list, element}), do: "ARRAY<#{to_spark_ddl_type(element)}>"

  def to_spark_ddl_type({:struct, fields}) when is_list(fields) do
    inner =
      fields
      |> Enum.map_join(", ", fn {name, dtype} ->
        "#{SparkEx.Types.quote_identifier(name)}: #{to_spark_ddl_type(dtype)}"
      end)

    "STRUCT<#{inner}>"
  end

  def to_spark_ddl_type({:map, key_dtype, value_dtype}),
    do: "MAP<#{to_spark_ddl_type(key_dtype)}, #{to_spark_ddl_type(value_dtype)}>"

  def to_spark_ddl_type(_other), do: "STRING"

  @doc """
  Converts an Explorer.DataFrame schema to a Spark DDL schema string.

  ## Examples

      iex> TypeMapper.explorer_schema_to_ddl([{"id", {:s, 64}}, {"name", :string}])
      "id BIGINT, name STRING"

  Map inputs are accepted but discouraged: map iteration order is unspecified,
  so the resulting DDL preserves the BEAM's traversal order rather than any
  user-intended column ordering. Pass a list of `{name, dtype}` tuples to
  guarantee positional schemas.
  """
  @spec explorer_schema_to_ddl(map() | [{String.t(), atom() | {atom(), term()}}]) :: String.t()
  def explorer_schema_to_ddl(dtypes) when is_map(dtypes) do
    IO.warn(
      "explorer_schema_to_ddl/1 with a map input has non-deterministic column ordering; " <>
        "pass a list of {name, dtype} tuples for positional schemas."
    )

    explorer_schema_to_ddl(Map.to_list(dtypes))
  end

  def explorer_schema_to_ddl(dtypes) when is_list(dtypes) do
    Enum.map_join(dtypes, ", ", fn
      {name, dtype} when is_binary(name) ->
        "#{SparkEx.Types.quote_identifier(name)} #{to_spark_ddl_type(dtype)}"

      other ->
        raise ArgumentError,
              "explorer_schema_to_ddl expected a list of {name, dtype} tuples with binary names, got element: #{inspect(other)}"
    end)
  end
end
