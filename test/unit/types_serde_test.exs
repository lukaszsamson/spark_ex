defmodule SparkEx.TypesSerdeTest do
  use ExUnit.Case, async: true

  import SparkEx.Types

  test "to_json includes nested struct/array/map types" do
    schema =
      struct_type([
        struct_field("id", :long),
        struct_field("tags", array_type(:string)),
        struct_field("meta", map_type(:string, :long)),
        struct_field(
          "detail",
          struct_type([
            struct_field("score", :double),
            struct_field("created", :timestamp_ntz)
          ])
        )
      ])

    json = to_json(schema)
    decoded = Jason.decode!(json)

    assert decoded["type"] == "struct"
    assert length(decoded["fields"]) == 4

    detail = Enum.find(decoded["fields"], &(&1["name"] == "detail"))
    refute is_nil(detail)
    assert detail["type"]["type"] == "struct"

    tags = Enum.find(decoded["fields"], &(&1["name"] == "tags"))
    refute is_nil(tags)
    assert tags["type"]["type"] == "array"
    assert tags["type"]["elementType"] == "string"

    meta = Enum.find(decoded["fields"], &(&1["name"] == "meta"))
    refute is_nil(meta)
    assert meta["type"]["type"] == "map"
    assert meta["type"]["keyType"] == "string"
    assert meta["type"]["valueType"] == "long"
  end

  # ── Stream F2 (BUGS_PLAN_5): type-rendering parity with PySpark jsonValue/simpleString ──

  describe "F2: JSON/DDL parity with PySpark" do
    test "collated string jsonValue is 'string collate <name>', not a map" do
      assert to_json({:struct, [struct_field("s", {:string, "UNICODE"})]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "string collate UNICODE"
    end

    test "UTF8_BINARY collation collapses to plain 'string'" do
      assert to_json({:struct, [struct_field("s", {:string, "UTF8_BINARY"})]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "string"
    end

    test "TimeType json default precision is 6" do
      assert to_json({:struct, [struct_field("t", :time)]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "time(6)"
    end

    test "single-field day_time_interval renders without 'TO'" do
      assert to_ddl({:struct, [struct_field("d", {:day_time_interval, 0, 0})]}) ==
               "d INTERVAL DAY"

      assert to_json({:struct, [struct_field("d", {:day_time_interval, 0, 0})]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "interval day"
    end

    test "single-field year_month_interval renders without 'TO'" do
      assert to_ddl({:struct, [struct_field("y", {:year_month_interval, 1, 1})]}) ==
               "y INTERVAL MONTH"

      assert to_json({:struct, [struct_field("y", {:year_month_interval, 1, 1})]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "interval month"
    end

    test "multi-field intervals use 'interval X to Y' JSON form" do
      assert to_json({:struct, [struct_field("d", {:day_time_interval, 0, 3})]})
             |> Jason.decode!()
             |> get_in(["fields", Access.at(0), "type"]) == "interval day to second"
    end

    test "data_type_to_json round-trips collation, time, interval" do
      alias SparkEx.Types
      alias Spark.Connect.DataType

      str_dt = %DataType{
        kind: {:string, %DataType.String{collation: "UNICODE_CI"}}
      }

      assert Types.data_type_to_json(str_dt) == ~s|"string collate UNICODE_CI"|

      time_dt = %DataType{kind: {:time, %DataType.Time{}}}
      assert Types.data_type_to_json(time_dt) == ~s|"time(6)"|

      dti_dt = %DataType{
        kind: {:day_time_interval, %DataType.DayTimeInterval{start_field: 2, end_field: 2}}
      }

      assert Types.data_type_to_json(dti_dt) == ~s|"interval minute"|
    end
  end
end
