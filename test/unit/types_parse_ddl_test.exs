defmodule SparkEx.TypesParseDdlTest do
  use ExUnit.Case, async: true

  alias SparkEx.Types
  alias Spark.Connect.DataType

  describe "parse_ddl_type/1 primitives" do
    test "boolean aliases" do
      assert {:ok, %DataType{kind: {:boolean, _}}} = Types.parse_ddl_type("BOOLEAN")
      assert {:ok, %DataType{kind: {:boolean, _}}} = Types.parse_ddl_type("bool")
    end

    test "integer aliases" do
      assert {:ok, %DataType{kind: {:byte, _}}} = Types.parse_ddl_type("TINYINT")
      assert {:ok, %DataType{kind: {:byte, _}}} = Types.parse_ddl_type("byte")
      assert {:ok, %DataType{kind: {:short, _}}} = Types.parse_ddl_type("SMALLINT")
      assert {:ok, %DataType{kind: {:short, _}}} = Types.parse_ddl_type("Short")
      assert {:ok, %DataType{kind: {:integer, _}}} = Types.parse_ddl_type("INT")
      assert {:ok, %DataType{kind: {:integer, _}}} = Types.parse_ddl_type("integer")
      assert {:ok, %DataType{kind: {:long, _}}} = Types.parse_ddl_type("BIGINT")
      assert {:ok, %DataType{kind: {:long, _}}} = Types.parse_ddl_type("LONG")
    end

    test "float aliases" do
      assert {:ok, %DataType{kind: {:float, _}}} = Types.parse_ddl_type("FLOAT")
      assert {:ok, %DataType{kind: {:float, _}}} = Types.parse_ddl_type("real")
      assert {:ok, %DataType{kind: {:double, _}}} = Types.parse_ddl_type("DOUBLE")
    end

    test "string and binary" do
      assert {:ok, %DataType{kind: {:string, _}}} = Types.parse_ddl_type("STRING")
      assert {:ok, %DataType{kind: {:binary, _}}} = Types.parse_ddl_type("binary")
    end

    test "temporal types" do
      assert {:ok, %DataType{kind: {:date, _}}} = Types.parse_ddl_type("DATE")
      assert {:ok, %DataType{kind: {:time, _}}} = Types.parse_ddl_type("time")
      assert {:ok, %DataType{kind: {:timestamp, _}}} = Types.parse_ddl_type("TIMESTAMP")
      assert {:ok, %DataType{kind: {:timestamp_ntz, _}}} = Types.parse_ddl_type("timestamp_ntz")
    end

    test "void / null" do
      assert {:ok, %DataType{kind: {:null, _}}} = Types.parse_ddl_type("VOID")
      assert {:ok, %DataType{kind: {:null, _}}} = Types.parse_ddl_type("null")
    end

    test "variant / geometry / geography" do
      assert {:ok, %DataType{kind: {:variant, _}}} = Types.parse_ddl_type("VARIANT")
      assert {:ok, %DataType{kind: {:geometry, _}}} = Types.parse_ddl_type("geometry")
      assert {:ok, %DataType{kind: {:geography, _}}} = Types.parse_ddl_type("Geography")
    end

    test "trims surrounding whitespace and is case-insensitive" do
      assert {:ok, %DataType{kind: {:integer, _}}} = Types.parse_ddl_type("  int  ")
      assert {:ok, %DataType{kind: {:long, _}}} = Types.parse_ddl_type("BigInt")
    end
  end

  describe "parse_ddl_type/1 decimal" do
    test "DECIMAL with no args defaults to (10, 0)" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 10, scale: 0}}}} =
               Types.parse_ddl_type("DECIMAL")
    end

    test "DEC and NUMERIC are aliases" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 10, scale: 0}}}} =
               Types.parse_ddl_type("dec")

      assert {:ok, %DataType{kind: {:decimal, %{precision: 10, scale: 0}}}} =
               Types.parse_ddl_type("numeric")
    end

    test "DECIMAL(p) sets scale to 0" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 18, scale: 0}}}} =
               Types.parse_ddl_type("DECIMAL(18)")
    end

    test "DECIMAL(p, s) preserves precision and scale" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 38, scale: 9}}}} =
               Types.parse_ddl_type("DECIMAL(38, 9)")
    end

    test "DECIMAL(p,s) without spaces" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 12, scale: 4}}}} =
               Types.parse_ddl_type("decimal(12,4)")
    end

    test "DECIMAL with extra surrounding whitespace" do
      assert {:ok, %DataType{kind: {:decimal, %{precision: 12, scale: 4}}}} =
               Types.parse_ddl_type("  decimal( 12 , 4 )  ")
    end
  end

  describe "parse_ddl_type/1 negative cases" do
    test "rejects array DDL" do
      assert :error = Types.parse_ddl_type("ARRAY<INT>")
    end

    test "rejects struct DDL" do
      assert :error = Types.parse_ddl_type("STRUCT<id: INT, name: STRING>")
    end

    test "rejects map DDL" do
      assert :error = Types.parse_ddl_type("MAP<STRING, INT>")
    end

    test "rejects multi-field schema strings" do
      assert :error = Types.parse_ddl_type("id INT, name STRING")
    end

    test "rejects empty string" do
      assert :error = Types.parse_ddl_type("")
    end

    test "rejects unknown type names" do
      assert :error = Types.parse_ddl_type("FOOBAR")
    end

    test "rejects malformed decimal" do
      assert :error = Types.parse_ddl_type("DECIMAL(abc)")
      assert :error = Types.parse_ddl_type("DECIMAL(10,)")
    end
  end
end
