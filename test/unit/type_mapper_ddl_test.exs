defmodule SparkEx.Unit.TypeMapperDDLTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.TypeMapper

  describe "to_spark_ddl_type/1" do
    test "maps primitive Explorer types" do
      assert TypeMapper.to_spark_ddl_type(:boolean) == "BOOLEAN"
      assert TypeMapper.to_spark_ddl_type(:string) == "STRING"
      assert TypeMapper.to_spark_ddl_type(:binary) == "BINARY"
      assert TypeMapper.to_spark_ddl_type(:date) == "DATE"
      assert TypeMapper.to_spark_ddl_type(:null) == "VOID"
    end

    test "maps signed integer types" do
      assert TypeMapper.to_spark_ddl_type({:s, 8}) == "BYTE"
      assert TypeMapper.to_spark_ddl_type({:s, 16}) == "SHORT"
      assert TypeMapper.to_spark_ddl_type({:s, 32}) == "INT"
      assert TypeMapper.to_spark_ddl_type({:s, 64}) == "LONG"
    end

    test "maps unsigned integer types to widened signed types" do
      assert TypeMapper.to_spark_ddl_type({:u, 8}) == "SHORT"
      assert TypeMapper.to_spark_ddl_type({:u, 16}) == "INT"
      assert TypeMapper.to_spark_ddl_type({:u, 32}) == "LONG"
      assert TypeMapper.to_spark_ddl_type({:u, 64}) == "LONG"
    end

    test "maps float types" do
      assert TypeMapper.to_spark_ddl_type({:f, 32}) == "FLOAT"
      assert TypeMapper.to_spark_ddl_type({:f, 64}) == "DOUBLE"
    end

    test "maps datetime types" do
      assert TypeMapper.to_spark_ddl_type({:datetime, :microsecond}) == "TIMESTAMP"
      assert TypeMapper.to_spark_ddl_type({:naive_datetime, :microsecond}) == "TIMESTAMP_NTZ"
    end

    test "maps unknown types to STRING" do
      assert TypeMapper.to_spark_ddl_type(:category) == "STRING"
      assert TypeMapper.to_spark_ddl_type({:time, :microsecond}) == "STRING"
      assert TypeMapper.to_spark_ddl_type({:duration, :microsecond}) == "STRING"
    end
  end

  describe "explorer_schema_to_ddl/1" do
    test "converts single column schema" do
      assert TypeMapper.explorer_schema_to_ddl(%{"id" => {:s, 64}}) == "id LONG"
    end

    test "converts multi-column schema preserving explicit list order" do
      dtypes = [{"name", :string}, {"id", {:s, 32}}, {"active", :boolean}]
      ddl = TypeMapper.explorer_schema_to_ddl(dtypes)
      assert ddl == "name STRING, id INT, active BOOLEAN"
    end

    test "converts empty schema" do
      assert TypeMapper.explorer_schema_to_ddl(%{}) == ""
    end

    test "handles float columns" do
      dtypes = [{"score", {:f, 64}}, {"weight", {:f, 32}}]
      ddl = TypeMapper.explorer_schema_to_ddl(dtypes)
      assert ddl == "score DOUBLE, weight FLOAT"
    end
  end
end
