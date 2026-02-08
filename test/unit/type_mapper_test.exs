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

    test "maps decimal to :string" do
      dt = %DataType{kind: {:decimal, %DataType.Decimal{precision: 10, scale: 2}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
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

    test "maps timestamp to {:datetime, :microsecond}" do
      dt = %DataType{kind: {:timestamp, %DataType.Timestamp{}}}
      assert {:ok, {:datetime, :microsecond}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps timestamp_ntz to {:naive_datetime, :microsecond}" do
      dt = %DataType{kind: {:timestamp_ntz, %DataType.TimestampNTZ{}}}
      assert {:ok, {:naive_datetime, :microsecond}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps time to {:time, :microsecond}" do
      dt = %DataType{kind: {:time, %DataType.Time{}}}
      assert {:ok, {:time, :microsecond}} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps array to :string (JSON fallback)" do
      element = %DataType{kind: {:integer, %DataType.Integer{}}}
      dt = %DataType{kind: {:array, %DataType.Array{element_type: element, contains_null: false}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps struct to :string (JSON fallback)" do
      dt = %DataType{kind: {:struct, %DataType.Struct{fields: []}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps map to :string (JSON fallback)" do
      key = %DataType{kind: {:string, %DataType.String{}}}
      value = %DataType{kind: {:integer, %DataType.Integer{}}}

      dt =
        %DataType{
          kind:
            {:map, %DataType.Map{key_type: key, value_type: value, value_contains_null: false}}
        }

      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
    end

    test "maps interval types to :string" do
      for kind <- [:calendar_interval, :year_month_interval, :day_time_interval] do
        dt = %DataType{kind: {kind, struct(interval_module(kind))}}
        assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
      end
    end

    test "maps variant to :string" do
      dt = %DataType{kind: {:variant, %DataType.Variant{}}}
      assert {:ok, :string} = TypeMapper.to_explorer_dtype(dt)
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

  defp interval_module(:calendar_interval), do: DataType.CalendarInterval
  defp interval_module(:year_month_interval), do: DataType.YearMonthInterval
  defp interval_module(:day_time_interval), do: DataType.DayTimeInterval
end
