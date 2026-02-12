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
    assert detail["type"]["type"] == "struct"

    tags = Enum.find(decoded["fields"], &(&1["name"] == "tags"))
    assert tags["type"]["type"] == "array"
    assert tags["type"]["elementType"] == "string"

    meta = Enum.find(decoded["fields"], &(&1["name"] == "meta"))
    assert meta["type"]["type"] == "map"
    assert meta["type"]["keyType"] == "string"
    assert meta["type"]["valueType"] == "long"
  end
end
