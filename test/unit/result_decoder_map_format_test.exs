defmodule SparkEx.Connect.ResultDecoderMapFormatTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.ResultDecoder
  alias Spark.Connect.DataType

  defp t(kind_atom, kind_struct), do: %DataType{kind: {kind_atom, kind_struct}}

  defp string_type, do: t(:string, %DataType.String{})
  defp int_type, do: t(:integer, %DataType.Integer{})

  defp map_type(kt, vt), do: t(:map, %DataType.Map{key_type: kt, value_type: vt})
  defp array_type(et), do: t(:array, %DataType.Array{element_type: et})

  defp struct_type(fields) do
    t(
      :struct,
      %DataType.Struct{
        fields:
          Enum.map(fields, fn {name, dt} ->
            %DataType.StructField{name: name, data_type: dt, nullable: true}
          end)
      }
    )
  end

  defp schema(fields), do: struct_type(fields)

  describe "convert_map_columns/2" do
    test "converts a top-level map column to an Elixir map" do
      rows = [%{"m" => [%{"key" => "a", "value" => 1}, %{"key" => "b", "value" => 2}]}]
      schema = schema([{"m", map_type(string_type(), int_type())}])

      assert [%{"m" => %{"a" => 1, "b" => 2}}] = ResultDecoder.convert_map_columns(rows, schema)
    end

    test "leaves nil map values and non-map columns untouched" do
      rows = [%{"m" => nil, "s" => "x"}]
      schema = schema([{"m", map_type(string_type(), int_type())}, {"s", string_type()}])

      assert [%{"m" => nil, "s" => "x"}] = ResultDecoder.convert_map_columns(rows, schema)
    end

    test "converts maps nested in arrays and structs recursively" do
      rows = [
        %{
          "am" => [[%{"key" => "x", "value" => 1}]],
          "st" => %{"inner" => [%{"key" => "k", "value" => [%{"key" => "d", "value" => 9}]}]}
        }
      ]

      schema =
        schema([
          {"am", array_type(map_type(string_type(), int_type()))},
          {"st",
           struct_type([
             {"inner", map_type(string_type(), map_type(string_type(), int_type()))}
           ])}
        ])

      assert [%{"am" => [%{"x" => 1}], "st" => %{"inner" => %{"k" => %{"d" => 9}}}}] =
               ResultDecoder.convert_map_columns(rows, schema)
    end

    test "genuine key/value structs are not converted" do
      rows = [%{"kv" => [%{"key" => "a", "value" => 1}]}]

      schema =
        schema([
          {"kv", array_type(struct_type([{"key", string_type()}, {"value", int_type()}]))}
        ])

      assert [%{"kv" => [%{"key" => "a", "value" => 1}]}] =
               ResultDecoder.convert_map_columns(rows, schema)
    end

    test "duplicate map keys collapse with last entry winning" do
      rows = [%{"m" => [%{"key" => "a", "value" => 1}, %{"key" => "a", "value" => 2}]}]
      schema = schema([{"m", map_type(string_type(), int_type())}])

      assert [%{"m" => %{"a" => 2}}] = ResultDecoder.convert_map_columns(rows, schema)
    end

    test "passes rows through when schema is nil or not a struct" do
      rows = [%{"m" => [%{"key" => "a", "value" => 1}]}]

      assert ^rows = ResultDecoder.convert_map_columns(rows, nil)
      assert ^rows = ResultDecoder.convert_map_columns(rows, string_type())
    end
  end
end
