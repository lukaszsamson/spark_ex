defmodule SparkEx.Unit.CreateDataFrameTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.TypeMapper

  describe "explorer_schema_to_ddl from Explorer.DataFrame" do
    test "generates DDL from Explorer.DataFrame dtypes" do
      df = Explorer.DataFrame.new(%{"id" => [1, 2], "name" => ["a", "b"]})
      dtypes = Explorer.DataFrame.dtypes(df)
      ddl = TypeMapper.explorer_schema_to_ddl(dtypes)

      # Explorer infers s64 for integers, string for strings
      assert ddl =~ "id LONG"
      assert ddl =~ "name STRING"
    end

    test "generates DDL for boolean and float columns" do
      df =
        Explorer.DataFrame.new(%{
          "flag" => [true, false],
          "score" => [1.5, 2.5]
        })

      dtypes = Explorer.DataFrame.dtypes(df)
      ddl = TypeMapper.explorer_schema_to_ddl(dtypes)

      assert ddl =~ "flag BOOLEAN"
      assert ddl =~ "score DOUBLE"
    end
  end

  describe "Explorer.DataFrame Arrow IPC serialization" do
    test "dump_ipc_stream produces valid binary" do
      df = Explorer.DataFrame.new(%{"id" => [1, 2, 3], "name" => ["a", "b", "c"]})

      assert {:ok, ipc_bytes} = Explorer.DataFrame.dump_ipc_stream(df)
      assert is_binary(ipc_bytes)
      assert byte_size(ipc_bytes) > 0
    end

    test "IPC stream round-trips correctly" do
      df = Explorer.DataFrame.new(%{"x" => [10, 20, 30], "y" => [1.1, 2.2, 3.3]})

      {:ok, ipc_bytes} = Explorer.DataFrame.dump_ipc_stream(df)
      {:ok, restored} = Explorer.DataFrame.load_ipc_stream(ipc_bytes)

      assert Explorer.DataFrame.n_rows(restored) == 3
      assert Explorer.DataFrame.names(restored) |> Enum.sort() == ["x", "y"]
    end
  end

  describe "list_of_maps conversion" do
    test "converts list of maps to Explorer.DataFrame" do
      data = [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}]

      columns =
        data
        |> hd()
        |> Map.keys()
        |> Enum.map(fn key ->
          values = Enum.map(data, fn row -> Map.get(row, key) end)
          {to_string(key), values}
        end)
        |> Map.new()

      df = Explorer.DataFrame.new(columns)
      assert Explorer.DataFrame.n_rows(df) == 2
    end
  end
end
