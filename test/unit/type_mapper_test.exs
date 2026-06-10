defmodule SparkEx.Connect.TypeMapperTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.TypeMapper
  alias Spark.Connect.DataType

  describe "to_explorer_dtype/1" do
    test "maps null type" do
      dt = %DataType{kind: {:null, %DataType.NULL{}}}
      assert {:ok, :null} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps boolean type" do
      dt = %DataType{kind: {:boolean, %DataType.Boolean{}}}
      assert {:ok, :boolean} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps byte to {:s, 8}" do
      dt = %DataType{kind: {:byte, %DataType.Byte{}}}
      assert {:ok, {:s, 8}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps short to {:s, 16}" do
      dt = %DataType{kind: {:short, %DataType.Short{}}}
      assert {:ok, {:s, 16}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps integer to {:s, 32}" do
      dt = %DataType{kind: {:integer, %DataType.Integer{}}}
      assert {:ok, {:s, 32}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps long to {:s, 64}" do
      dt = %DataType{kind: {:long, %DataType.Long{}}}
      assert {:ok, {:s, 64}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps float to {:f, 32}" do
      dt = %DataType{kind: {:float, %DataType.Float{}}}
      assert {:ok, {:f, 32}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps double to {:f, 64}" do
      dt = %DataType{kind: {:double, %DataType.Double{}}}
      assert {:ok, {:f, 64}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps decimal to Explorer decimal dtype" do
      dt = %DataType{kind: {:decimal, %DataType.Decimal{precision: 10, scale: 2}}}
      assert {:ok, {:decimal, 10, 2}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps string type" do
      dt = %DataType{kind: {:string, %DataType.String{}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps char to :string" do
      dt = %DataType{kind: {:char, %DataType.Char{length: 10}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps varchar to :string" do
      dt = %DataType{kind: {:var_char, %DataType.VarChar{length: 255}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps binary type" do
      dt = %DataType{kind: {:binary, %DataType.Binary{}}}
      assert {:ok, :binary} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps date type" do
      dt = %DataType{kind: {:date, %DataType.Date{}}}
      assert {:ok, :date} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps timestamp to tz-aware {:datetime, :microsecond, tz}" do
      dt = %DataType{kind: {:timestamp, %DataType.Timestamp{}}}
      assert {:ok, {:datetime, :microsecond, "Etc/UTC"}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps timestamp_ntz to {:naive_datetime, :microsecond}" do
      dt = %DataType{kind: {:timestamp_ntz, %DataType.TimestampNTZ{}}}
      assert {:ok, {:naive_datetime, :microsecond}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps time to bare :time atom" do
      dt = %DataType{kind: {:time, %DataType.Time{}}}
      assert {:ok, :time} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps array to native list dtype" do
      element = %DataType{kind: {:integer, %DataType.Integer{}}}
      dt = %DataType{kind: {:array, %DataType.Array{element_type: element, contains_null: false}}}
      assert {:ok, {:list, {:s, 32}}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps nested array to native nested list dtype" do
      inner_element = %DataType{kind: {:string, %DataType.String{}}}

      inner_array =
        %DataType{kind: {:array, %DataType.Array{element_type: inner_element}}}

      dt = %DataType{kind: {:array, %DataType.Array{element_type: inner_array}}}
      assert {:ok, {:list, {:list, :string}}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps struct to native struct dtype with field names" do
      fields = [
        %DataType.StructField{
          name: "id",
          data_type: %DataType{kind: {:long, %DataType.Long{}}}
        },
        %DataType.StructField{
          name: "name",
          data_type: %DataType{kind: {:string, %DataType.String{}}}
        }
      ]

      dt = %DataType{kind: {:struct, %DataType.Struct{fields: fields}}}

      assert {:ok, {:struct, [{"id", {:s, 64}}, {"name", :string}]}} =
               TypeMapper.to_explorer_dtype(dt)
    end

    test "maps empty struct to native struct dtype with empty fields" do
      dt = %DataType{kind: {:struct, %DataType.Struct{fields: []}}}
      assert {:ok, {:struct, []}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps map to nil (no native Explorer dtype; cells inferred)" do
      key = %DataType{kind: {:string, %DataType.String{}}}
      value = %DataType{kind: {:integer, %DataType.Integer{}}}

      dt =
        %DataType{
          kind:
            {:map, %DataType.Map{key_type: key, value_type: value, value_contains_null: false}}
        }

      assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps day-time interval to {:duration, :microsecond}; others to nil" do
      assert {:ok, {:duration, :microsecond}} =
               TypeMapper.to_explorer_dtype(%DataType{
                 kind: {:day_time_interval, struct(interval_module(:day_time_interval))}
               })

      for kind <- [:calendar_interval, :year_month_interval] do
        dt = %DataType{kind: {kind, struct(interval_module(kind))}}
        assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt)
      end
    end

    test "maps variant to nil (no native Explorer dtype; cells inferred)" do
      dt = %DataType{kind: {:variant, %DataType.Variant{}}}
      assert {:ok, nil} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps nil kind to :null" do
      dt = %DataType{kind: nil}
      assert {:ok, :null} = TypeMapper.to_explorer_dtype(dt)
    end
  end

  describe "schema_to_dtypes/1" do
    test "converts struct schema to dtype list" do
      schema = %DataType.Struct{
        fields: [
          %DataType.StructField{
            name: "id",
            data_type: %DataType{kind: {:long, %DataType.Long{}}},
            nullable: false
          },
          %DataType.StructField{
            name: "name",
            data_type: %DataType{kind: {:string, %DataType.String{}}},
            nullable: true
          },
          %DataType.StructField{
            name: "score",
            data_type: %DataType{kind: {:double, %DataType.Double{}}},
            nullable: true
          }
        ]
      }

      assert {:ok, dtypes} = TypeMapper.schema_to_dtypes(schema)
      assert dtypes == [{"id", {:s, 64}}, {"name", :string}, {"score", {:f, 64}}]
    end

    test "handles empty schema" do
      schema = %DataType.Struct{fields: []}
      assert {:ok, []} = TypeMapper.schema_to_dtypes(schema)
    end
  end

  describe "complex Explorer dtype to DDL" do
    test "maps list/struct/map dtypes recursively" do
      assert TypeMapper.to_spark_ddl_type({:list, :string}) == "ARRAY<STRING>"

      assert TypeMapper.to_spark_ddl_type({:struct, [{"age", {:s, 64}}, {"name", :string}]}) ==
               "STRUCT<age: BIGINT, name: STRING>"

      assert TypeMapper.to_spark_ddl_type({:map, :string, {:list, {:s, 32}}}) ==
               "MAP<STRING, ARRAY<INT>>"
    end

    test "builds schema DDL with complex nested fields" do
      dtypes = [
        {"id", {:s, 64}},
        {"tags", {:list, :string}},
        {"info", {:struct, [{"age", {:s, 32}}]}}
      ]

      assert TypeMapper.explorer_schema_to_ddl(dtypes) ==
               "id BIGINT, tags ARRAY<STRING>, info STRUCT<age: INT>"
    end
  end

  describe "data_type_to_ddl/1" do
    test "preserves complex Spark types" do
      array_dt =
        %DataType{
          kind:
            {:array,
             %DataType.Array{
               element_type: %DataType{kind: {:integer, %DataType.Integer{}}},
               contains_null: false
             }}
        }

      map_dt =
        %DataType{
          kind:
            {:map,
             %DataType.Map{
               key_type: %DataType{kind: {:string, %DataType.String{}}},
               value_type: %DataType{kind: {:double, %DataType.Double{}}},
               value_contains_null: true
             }}
        }

      struct_dt =
        %DataType{
          kind:
            {:struct,
             %DataType.Struct{
               fields: [
                 %DataType.StructField{
                   name: "tags",
                   data_type: array_dt,
                   nullable: true
                 },
                 %DataType.StructField{
                   name: "meta",
                   data_type: map_dt,
                   nullable: true
                 }
               ]
             }}
        }

      assert TypeMapper.data_type_to_ddl(array_dt) == "ARRAY<INT>"
      assert TypeMapper.data_type_to_ddl(map_dt) == "MAP<STRING, DOUBLE>"

      assert TypeMapper.data_type_to_ddl(struct_dt) ==
               "STRUCT<tags: ARRAY<INT>, meta: MAP<STRING, DOUBLE>>"
    end

    test "preserves CHAR length in DDL" do
      dt = %DataType{kind: {:char, %DataType.Char{length: 10}}}
      assert TypeMapper.data_type_to_ddl(dt) == "CHAR(10)"
    end

    test "preserves VARCHAR length in DDL" do
      dt = %DataType{kind: {:var_char, %DataType.VarChar{length: 255}}}
      assert TypeMapper.data_type_to_ddl(dt) == "VARCHAR(255)"
    end

    test "falls back to STRING for CHAR/VARCHAR with non-positive length" do
      # proto3 int32 defaults to 0 when omitted; 0 and negative lengths are invalid in Spark SQL.
      assert TypeMapper.data_type_to_ddl(%DataType{kind: {:char, %DataType.Char{}}}) == "STRING"

      assert TypeMapper.data_type_to_ddl(%DataType{kind: {:char, %DataType.Char{length: 0}}}) ==
               "STRING"

      assert TypeMapper.data_type_to_ddl(%DataType{kind: {:char, %DataType.Char{length: -1}}}) ==
               "STRING"

      assert TypeMapper.data_type_to_ddl(%DataType{kind: {:var_char, %DataType.VarChar{}}}) ==
               "STRING"

      assert TypeMapper.data_type_to_ddl(%DataType{
               kind: {:var_char, %DataType.VarChar{length: 0}}
             }) == "STRING"
    end

    test "preserves CHAR/VARCHAR length inside nested types" do
      char_dt = %DataType{kind: {:char, %DataType.Char{length: 5}}}
      var_char_dt = %DataType{kind: {:var_char, %DataType.VarChar{length: 32}}}

      array_dt = %DataType{
        kind: {:array, %DataType.Array{element_type: char_dt, contains_null: true}}
      }

      struct_dt = %DataType{
        kind:
          {:struct,
           %DataType.Struct{
             fields: [
               %DataType.StructField{name: "code", data_type: char_dt, nullable: true},
               %DataType.StructField{name: "label", data_type: var_char_dt, nullable: true}
             ]
           }}
      }

      map_dt = %DataType{
        kind:
          {:map,
           %DataType.Map{
             key_type: char_dt,
             value_type: var_char_dt,
             value_contains_null: true
           }}
      }

      assert TypeMapper.data_type_to_ddl(array_dt) == "ARRAY<CHAR(5)>"
      assert TypeMapper.data_type_to_ddl(struct_dt) == "STRUCT<code: CHAR(5), label: VARCHAR(32)>"
      assert TypeMapper.data_type_to_ddl(map_dt) == "MAP<CHAR(5), VARCHAR(32)>"
    end
  end

  defp interval_module(:calendar_interval), do: DataType.CalendarInterval
  defp interval_module(:year_month_interval), do: DataType.YearMonthInterval
  defp interval_module(:day_time_interval), do: DataType.DayTimeInterval
end
