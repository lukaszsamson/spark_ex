defmodule SparkEx.TypesSerdeRoundtripTest do
  use ExUnit.Case, async: true

  import SparkEx.Types

  test "nested array/map JSON serialization" do
    schema =
      struct_type([
        struct_field("id", :long),
        struct_field("attrs", map_type(:string, array_type(:integer)))
      ])

    json = to_json(schema)
    decoded = Jason.decode!(json)

    assert decoded["type"] == "struct"

    attrs = Enum.find(decoded["fields"], &(&1["name"] == "attrs"))
    assert attrs["type"]["type"] == "map"
    assert attrs["type"]["keyType"] == "string"
    assert attrs["type"]["valueType"]["type"] == "array"
    assert attrs["type"]["valueType"]["elementType"] == "integer"
  end

  test "string collation JSON serialization" do
    schema =
      struct_type([
        struct_field("name", {:string, "UNICODE_CI"})
      ])

    json = to_json(schema)
    decoded = Jason.decode!(json)

    field = hd(decoded["fields"])
    assert field["type"]["type"] == "string"
    assert field["type"]["collation"] == "UNICODE_CI"
  end
end
