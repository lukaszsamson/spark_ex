defmodule SparkEx.BugsFableTypesTest do
  @moduledoc """
  Regression tests for the BUGS_FABLE type/decoder fixes:
  FABLE-05/06/13/14/15/16/17/54/56.
  """
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias SparkEx.Connect.ResultDecoder
  alias SparkEx.Connect.TypeMapper

  describe "FABLE-06: time dtype" do
    test "TIME maps to the bare :time atom (a valid Explorer dtype)" do
      dt = %DataType{kind: {:time, %DataType.Time{}}}
      assert {:ok, :time} = TypeMapper.to_explorer_dtype(dt)
    end
  end

  describe "FABLE-13: tz-aware TIMESTAMP" do
    test "TIMESTAMP maps to tz-aware 3-tuple, distinct from TIMESTAMP_NTZ" do
      ts = %DataType{kind: {:timestamp, %DataType.Timestamp{}}}
      ntz = %DataType{kind: {:timestamp_ntz, %DataType.TimestampNTZ{}}}

      assert {:ok, {:datetime, :microsecond, tz}} = TypeMapper.to_explorer_dtype(ts)
      assert is_binary(tz)
      assert {:ok, {:naive_datetime, :microsecond}} = TypeMapper.to_explorer_dtype(ntz)
      refute TypeMapper.to_explorer_dtype(ts) == TypeMapper.to_explorer_dtype(ntz)
    end
  end

  describe "FABLE-05: dtypes without a native Explorer mapping return nil" do
    test "day-time interval maps to a duration dtype" do
      dt = %DataType{kind: {:day_time_interval, %DataType.DayTimeInterval{}}}
      assert {:ok, {:duration, :microsecond}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "year-month / calendar interval and UDT without sql_type map to nil" do
      cases = [
        {:udt, %DataType.UDT{type: "udt"}},
        {:year_month_interval, %DataType.YearMonthInterval{}},
        {:calendar_interval, %DataType.CalendarInterval{}}
      ]

      for {tag, value} <- cases do
        dt = %DataType{kind: {tag, value}}
        assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt), "expected nil dtype for #{tag}"
      end
    end

    test "map / variant / udt / geometry / geography map to their Arrow wire layout (T-30)" do
      int = %DataType{kind: {:integer, %DataType.Integer{}}}
      str = %DataType{kind: {:string, %DataType.String{}}}
      geo = {:struct, [{"srid", {:s, 32}}, {"wkb", :binary}]}

      cases = [
        {{:map, %DataType.Map{key_type: str, value_type: int}},
         {:list, {:struct, [{"key", :string}, {"value", {:s, 32}}]}}},
        {{:variant, %DataType.Variant{}}, {:struct, [{"value", :binary}, {"metadata", :binary}]}},
        {{:udt, %DataType.UDT{type: "udt", sql_type: int}}, {:s, 32}},
        {{:geometry, %DataType.Geometry{}}, geo},
        {{:geography, %DataType.Geography{}}, geo}
      ]

      for {kind, expected} <- cases do
        assert {:ok, ^expected} = TypeMapper.to_explorer_dtype(%DataType{kind: kind})
      end
    end

    test "ARRAY of an element without an Explorer dtype collapses the list dtype to nil" do
      element = %DataType{kind: {:year_month_interval, %DataType.YearMonthInterval{}}}
      dt = %DataType{kind: {:array, %DataType.Array{element_type: element}}}
      assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt)
    end

    test "STRUCT with a field without an Explorer dtype collapses the struct dtype to nil" do
      fields = [
        %DataType.StructField{
          name: "v",
          data_type: %DataType{kind: {:calendar_interval, %DataType.CalendarInterval{}}}
        }
      ]

      dt = %DataType{kind: {:struct, %DataType.Struct{fields: fields}}}
      assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt)
    end

    test "a variant column can be built with its mapped dtype" do
      if Code.ensure_loaded?(Explorer.DataFrame) do
        struct = %DataType.Struct{
          fields: [
            %DataType.StructField{
              name: "m",
              data_type: %DataType{kind: {:variant, %DataType.Variant{}}}
            }
          ]
        }

        {:ok, dtypes} = TypeMapper.schema_to_dtypes(struct)
        cells = [%{"value" => <<1>>, "metadata" => <<2>>}, nil]

        df = Explorer.DataFrame.new([{"m", cells}], dtypes: dtypes)
        assert Explorer.DataFrame.n_rows(df) == 2

        assert Explorer.DataFrame.dtypes(Explorer.DataFrame.new([{"m", []}], dtypes: dtypes)) ==
                 Explorer.DataFrame.dtypes(df)
      end
    end
  end

  describe "FABLE-14: explorer dtype -> Spark DDL for real dtype shapes" do
    test "tz-aware datetime, bare time, duration map to TIMESTAMP/TIME/INTERVAL" do
      assert TypeMapper.to_spark_ddl_type({:datetime, :microsecond, "Etc/UTC"}) == "TIMESTAMP"
      assert TypeMapper.to_spark_ddl_type(:time) == "TIME"
      assert TypeMapper.to_spark_ddl_type({:duration, :microsecond}) == "INTERVAL DAY TO SECOND"
    end
  end

  describe "FABLE-16: geometry / geography SRID rendering" do
    test "data_type_to_ddl: SRID -1 -> ANY, SRID 0 valid, other SRID preserved" do
      assert ddl(:geometry, -1) == "GEOMETRY(ANY)"
      assert ddl(:geometry, 0) == "GEOMETRY(0)"
      assert ddl(:geometry, 4326) == "GEOMETRY(4326)"
      assert ddl(:geography, -1) == "GEOGRAPHY(ANY)"
      assert ddl(:geography, 0) == "GEOGRAPHY(0)"
    end

    test "data_type_to_json: SRID -1 -> any, SRID 0 valid" do
      assert json(:geometry, -1) == ~s|"geometry(any)"|
      assert json(:geometry, 0) == ~s|"geometry(0)"|
      assert json(:geography, -1) == ~s|"geography(any)"|
      assert json(:geography, 0) == ~s|"geography(0)"|
    end

    test "Types.type_to_json/type_to_ddl tuple forms render ANY / 0" do
      schema = {:struct, [SparkEx.Types.struct_field("g", {:geometry, -1})]}
      assert SparkEx.Types.to_ddl(schema) == "g GEOMETRY(ANY)"

      schema0 = {:struct, [SparkEx.Types.struct_field("g", {:geometry, 0})]}
      assert SparkEx.Types.to_ddl(schema0) == "g GEOMETRY(0)"
    end
  end

  describe "FABLE-17: data_type_to_json handles the :udt variant" do
    test "emits {\"type\":\"udt\", ...} instead of raising" do
      udt = %DataType{
        kind:
          {:udt,
           %DataType.UDT{
             type: "udt",
             python_class: "pyspark.ml.linalg.VectorUDT",
             serialized_python_class: "abc123",
             sql_type: %DataType{kind: {:double, %DataType.Double{}}}
           }}
      }

      json = SparkEx.Types.data_type_to_json(udt)
      decoded = Jason.decode!(json)
      assert decoded["type"] == "udt"
      assert decoded["pyClass"] == "pyspark.ml.linalg.VectorUDT"
      assert decoded["serializedClass"] == "abc123"
      assert decoded["sqlType"] == "double"
    end

    test "omits unset optional fields" do
      udt = %DataType{kind: {:udt, %DataType.UDT{type: "udt", jvm_class: "org.example.Foo"}}}
      decoded = udt |> SparkEx.Types.data_type_to_json() |> Jason.decode!()
      assert decoded == %{"type" => "udt", "class" => "org.example.Foo"}
    end
  end

  describe "FABLE-54: interval start_field only (end_field nil) -> single field" do
    test "data_type_to_ddl renders single field" do
      dt = %DataType{kind: {:day_time_interval, %DataType.DayTimeInterval{start_field: 1}}}
      assert TypeMapper.data_type_to_ddl(dt) == "INTERVAL HOUR"

      ym = %DataType{kind: {:year_month_interval, %DataType.YearMonthInterval{start_field: 1}}}
      assert TypeMapper.data_type_to_ddl(ym) == "INTERVAL MONTH"
    end

    test "data_type_to_json renders single field" do
      dt = %DataType{kind: {:day_time_interval, %DataType.DayTimeInterval{start_field: 1}}}
      assert SparkEx.Types.data_type_to_json(dt) == ~s|"interval hour"|

      ym = %DataType{kind: {:year_month_interval, %DataType.YearMonthInterval{start_field: 0}}}
      assert SparkEx.Types.data_type_to_json(ym) == ~s|"interval year"|
    end
  end

  describe "FABLE-15: UDT deserializers / CHAR padding parity" do
    test "CHAR columns are not transformed (server-padded values preserved)" do
      dt = %DataType{kind: {:char, %DataType.Char{length: 3}}}
      assert ResultDecoder.column_value_transform(dt) == nil
    end

    test "apply_row_transforms applies a registered UDT deserializer per cell" do
      udt = %DataType.UDT{type: "udt", python_class: "my.udt.Class"}

      :ok =
        SparkEx.Connect.UDTRegistry.register("my.udt.Class", fn v -> {:deserialized, v} end,
          replace?: true
        )

      struct = %DataType.Struct{
        fields: [
          %DataType.StructField{name: "u", data_type: %DataType{kind: {:udt, udt}}},
          %DataType.StructField{
            name: "x",
            data_type: %DataType{kind: {:integer, %DataType.Integer{}}}
          }
        ]
      }

      schema = %DataType{kind: {:struct, struct}}
      rows = [%{"u" => 1, "x" => 10}, %{"u" => 2, "x" => 20}]

      assert ResultDecoder.apply_row_transforms(rows, schema) == [
               %{"u" => {:deserialized, 1}, "x" => 10},
               %{"u" => {:deserialized, 2}, "x" => 20}
             ]
    after
      SparkEx.Connect.UDTRegistry.unregister("my.udt.Class")
    end

    test "apply_row_transforms is a no-op when the schema has no transform" do
      struct = %DataType.Struct{
        fields: [
          %DataType.StructField{
            name: "x",
            data_type: %DataType{kind: {:integer, %DataType.Integer{}}}
          }
        ]
      }

      schema = %DataType{kind: {:struct, struct}}
      rows = [%{"x" => 1}]
      assert ResultDecoder.apply_row_transforms(rows, schema) == rows
    end
  end

  # --- helpers ---

  defp ddl(tag, srid) do
    TypeMapper.data_type_to_ddl(%DataType{kind: {tag, geom_struct(tag, srid)}})
  end

  defp json(tag, srid) do
    SparkEx.Types.data_type_to_json(%DataType{kind: {tag, geom_struct(tag, srid)}})
  end

  defp geom_struct(:geometry, srid), do: %DataType.Geometry{srid: srid}
  defp geom_struct(:geography, srid), do: %DataType.Geography{srid: srid}
end
