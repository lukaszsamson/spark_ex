defmodule SparkEx.BugsPlan5.StreamDTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias SparkEx.Connect.{Client, TypeMapper}
  alias SparkEx.Internal.UUID

  describe "UUID validation (CLAUDE-72)" do
    test "valid_uuid? accepts non-v4 UUIDs" do
      assert UUID.valid_uuid?("550e8400-e29b-11d4-a716-446655440000")
      assert UUID.valid_uuid?("00000000-0000-0000-0000-000000000000")
      assert UUID.valid_uuid?("01876543-89ab-7cde-8123-456789abcdef")
      assert UUID.valid_uuid?(UUID.generate_v4())
    end

    test "valid_uuid? rejects garbage" do
      refute UUID.valid_uuid?("not-a-uuid")
      refute UUID.valid_uuid?("")
      refute UUID.valid_uuid?(nil)
      refute UUID.valid_uuid?(123)
    end
  end

  describe "tag validation (CLAUDE-71)" do
    test "validate_tag rejects empty and comma-bearing tags" do
      assert :ok = Client.validate_tag("normal-tag")
      assert {:error, :empty} = Client.validate_tag("")
      assert {:error, :contains_comma} = Client.validate_tag("a,b")
      assert {:error, :not_a_string} = Client.validate_tag(:atom)
    end

    test "validate_tags walks the list" do
      assert :ok = Client.validate_tags(["a", "b"])
      assert {:error, {:invalid_tag, :empty, ""}} = Client.validate_tags(["a", ""])
      assert {:error, {:invalid_tag, :contains_comma, "x,y"}} = Client.validate_tags(["x,y"])
      assert {:error, {:invalid_tags, :nope}} = Client.validate_tags(:nope)
    end
  end

  describe "TypeMapper nil-DataType guards (CLAUDE-70)" do
    test "to_explorer_dtype handles nil" do
      assert {:ok, :null} = TypeMapper.to_explorer_dtype(nil)
      assert {:ok, :null} = TypeMapper.to_explorer_dtype(%DataType{kind: nil})
    end

    test "data_type_to_ddl handles nil" do
      assert "VOID" = TypeMapper.data_type_to_ddl(nil)
      assert "VOID" = TypeMapper.data_type_to_ddl(%DataType{kind: nil})
    end

    test "schema_to_dtypes tolerates StructField with nil data_type" do
      schema = %DataType.Struct{
        fields: [
          %DataType.StructField{name: "a", data_type: nil, nullable: true},
          %DataType.StructField{
            name: "b",
            data_type: %DataType{kind: {:integer, %DataType.Integer{}}},
            nullable: true
          }
        ]
      }

      assert {:ok, [{"a", :null}, {"b", {:s, 32}}]} = TypeMapper.schema_to_dtypes(schema)
    end

    test "data_type_to_ddl handles struct/array with nil nested data_type" do
      arr = %DataType{kind: {:array, %DataType.Array{element_type: nil}}}
      assert "ARRAY<VOID>" = TypeMapper.data_type_to_ddl(arr)

      struct_dt = %DataType{
        kind:
          {:struct,
           %DataType.Struct{
             fields: [%DataType.StructField{name: "a", data_type: nil, nullable: true}]
           }}
      }

      assert "STRUCT<a: VOID>" = TypeMapper.data_type_to_ddl(struct_dt)
    end
  end

  describe "explorer_schema_to_ddl input validation (CLAUDE-46)" do
    test "raises ArgumentError on a non-tuple list element" do
      assert_raise ArgumentError, ~r/list of \{name, dtype\} tuples/, fn ->
        TypeMapper.explorer_schema_to_ddl(["not-a-tuple"])
      end
    end
  end
end
