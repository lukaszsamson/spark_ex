defmodule SparkEx.Wave4SessionTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType

  defp dt(kind), do: %DataType{kind: kind}

  defp struct_type(fields) do
    dt(
      {:struct,
       %DataType.Struct{
         fields:
           Enum.map(fields, fn {name, type} ->
             %DataType.StructField{name: name, data_type: type, nullable: true}
           end)
       }}
    )
  end

  defp array_of(type), do: dt({:array, %DataType.Array{element_type: type, contains_null: true}})

  defp map_of(key, value),
    do: dt({:map, %DataType.Map{key_type: key, value_type: value, value_contains_null: true}})

  defp decimal_t, do: dt({:decimal, %DataType.Decimal{precision: 10, scale: 2}})
  defp timestamp_t, do: dt({:timestamp, %DataType.Timestamp{}})
  defp timestamp_ntz_t, do: dt({:timestamp_ntz, %DataType.TimestampNTZ{}})
  defp date_t, do: dt({:date, %DataType.Date{}})
  defp binary_t, do: dt({:binary, %DataType.Binary{}})
  defp string_t, do: dt({:string, %DataType.String{}})

  describe "T-32 JSON projection scalar coercion" do
    test "coerces scalars nested in a struct" do
      schema =
        struct_type([
          {"s",
           struct_type([
             {"dec", decimal_t()},
             {"ts", timestamp_t()},
             {"ntz", timestamp_ntz_t()},
             {"d", date_t()},
             {"b", binary_t()},
             {"txt", string_t()}
           ])}
        ])

      json =
        Jason.encode!(%{
          "dec" => 1.25,
          "ts" => "2024-01-02T03:04:05.000Z",
          "ntz" => "2024-01-02T03:04:05.000",
          "d" => "2024-01-02",
          "b" => Base.encode64("hi"),
          "txt" => "2024-01-02"
        })

      assert [%{"s" => decoded}] =
               SparkEx.Session.__decode_json_projection_rows__([%{"s" => json}], schema)

      assert Decimal.equal?(decoded["dec"], Decimal.new("1.25"))
      assert decoded["ts"] == ~U[2024-01-02 03:04:05.000Z]
      assert decoded["ntz"] == ~N[2024-01-02 03:04:05.000]
      assert decoded["d"] == ~D[2024-01-02]
      assert decoded["b"] == "hi"
      # STRING stays a plain string even when it looks like a date.
      assert decoded["txt"] == "2024-01-02"
    end

    test "coerces scalars in arrays and map values, including nils" do
      schema =
        struct_type([
          {"arr", array_of(date_t())},
          {"mp", map_of(string_t(), decimal_t())},
          {"nested", array_of(struct_type([{"ts", timestamp_t()}]))},
          {"holes", array_of(binary_t())}
        ])

      row = %{
        "arr" => Jason.encode!(["2024-01-02", "2024-02-03"]),
        "mp" => Jason.encode!(%{"a" => "1.50"}),
        "nested" => Jason.encode!([%{"ts" => "2024-01-02T03:04:05Z"}]),
        "holes" => Jason.encode!([nil])
      }

      assert [decoded] = SparkEx.Session.__decode_json_projection_rows__([row], schema)
      assert decoded["arr"] == [~D[2024-01-02], ~D[2024-02-03]]
      assert [%{"key" => "a", "value" => value}] = decoded["mp"]
      assert Decimal.equal?(value, Decimal.new("1.50"))
      assert [%{"ts" => ~U[2024-01-02 03:04:05Z]}] = decoded["nested"]
      assert decoded["holes"] == [nil]
    end

    test "leaves unparseable scalars untouched" do
      schema = struct_type([{"s", struct_type([{"d", date_t()}, {"ts", timestamp_t()}])}])

      row = %{"s" => Jason.encode!(%{"d" => "not-a-date", "ts" => "nope"})}

      assert [%{"s" => %{"d" => "not-a-date", "ts" => "nope"}}] =
               SparkEx.Session.__decode_json_projection_rows__([row], schema)
    end

    test "decimals rendered as JSON integers become Decimal" do
      schema = struct_type([{"arr", array_of(decimal_t())}])

      assert [%{"arr" => [d]}] =
               SparkEx.Session.__decode_json_projection_rows__(
                 [%{"arr" => Jason.encode!([7])}],
                 schema
               )

      assert Decimal.equal?(d, Decimal.new(7))
    end
  end

  describe "T-33 unique column schema helper" do
    test "renames duplicate top-level columns positionally" do
      schema = struct_type([{"a", string_t()}, {"a", string_t()}])

      assert %DataType{kind: {:struct, %DataType.Struct{fields: fields}}} =
               SparkEx.Session.__unique_columns_map_format_schema__(schema)

      assert Enum.map(fields, & &1.name) == ["a", "a_1"]
    end
  end
end
