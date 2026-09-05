defmodule SparkEx.Wave5SessionTest do
  use ExUnit.Case, async: true

  alias SparkEx.Session

  describe "T-47: map-row schema inference sorts the key union" do
    test "collect_ordered_keys returns the sorted stringified union" do
      rows = [%{"z" => 1, "a" => 2}, %{"m" => 3, "a" => 4}]
      assert Session.__collect_ordered_keys__(rows) == ["a", "m", "z"]
    end

    test "atom keys are stringified before sorting" do
      rows = [%{z: 1, a: 2}, %{"m" => 3}]
      assert Session.__collect_ordered_keys__(rows) == ["a", "m", "z"]
    end

    test "order is independent of insertion order and map size (> 32 keys)" do
      keys = Enum.map(1..40, fn i -> "c#{i}" end)
      forward = Map.new(keys, fn k -> {k, 1} end)
      reverse = keys |> Enum.reverse() |> Map.new(fn k -> {k, 1} end)

      assert Session.__collect_ordered_keys__([forward]) ==
               Session.__collect_ordered_keys__([reverse])

      assert Session.__collect_ordered_keys__([forward]) == Enum.sort(keys)
    end

    test "inferred local relation columns come out sorted" do
      {:ok, {:local_relation, _ipc, schema, df}} =
        Session.__prepare_local_data__([%{"z" => 1, "a" => "x"}, %{"z" => 2, "a" => "y"}])

      assert Explorer.DataFrame.names(df) == ["a", "z"]
      assert schema =~ ~r/\Aa\s/
    end

    test "an explicit DDL schema keeps its own column order" do
      {:ok, {:sql_relation, query, nil}} =
        Session.__prepare_local_data__([%{"z" => 1, "a" => "x"}], schema: "z BIGINT, a STRING")

      assert query =~ "'z BIGINT, a STRING'"
    end
  end

  describe "T-49: too-short column-name lists are padded with _N" do
    test "tuple rows pad the tail following PySpark's 1-based index" do
      {:ok, {:local_relation, _ipc, _schema, df}} =
        Session.__prepare_local_data__([{1, "b", true}], schema: ["a"])

      # Tuple rows are positional: supplied names first, then `_N` padding.
      assert Explorer.DataFrame.names(df) == ["a", "_2", "_3"]
    end

    test "exactly matching name lists are unchanged" do
      {:ok, {:local_relation, _ipc, _schema, df}} =
        Session.__prepare_local_data__([{1, "b"}], schema: ["x", "y"])

      assert Explorer.DataFrame.names(df) == ["x", "y"]
    end

    test "a too-long column-name list is still an error" do
      assert {:error, {:invalid_schema, message}} =
               Session.__prepare_local_data__([{1, "b"}], schema: ["a", "b", "c"])

      assert message =~ "length 3"
    end

    test "list rows are padded like tuple rows" do
      {:ok, {:local_relation, _ipc, _schema, df}} =
        Session.__prepare_local_data__([[1, 2]], schema: ["a"])

      assert Explorer.DataFrame.names(df) == ["a", "_2"]
    end

    test "map rows with a mismatched column-name list stay an error" do
      assert {:error, {:invalid_schema, _}} =
               Session.__prepare_local_data__([%{"a" => 1, "b" => 2}], schema: ["only"])
    end
  end

  describe "T-48: all-null columns cannot be inferred" do
    test "a column that is nil in every row is rejected" do
      assert {:error, {:cannot_determine_type, "note"}} =
               Session.__prepare_local_data__([
                 %{"id" => 1, "note" => nil},
                 %{"id" => 2, "note" => nil}
               ])
    end

    test "atom keys report the stringified column name" do
      assert {:error, {:cannot_determine_type, "note"}} =
               Session.__prepare_local_data__([%{id: 1, note: nil}])
    end

    test "a partially-null column is fine" do
      assert {:ok, {:local_relation, _ipc, _schema, _df}} =
               Session.__prepare_local_data__([
                 %{"id" => 1, "note" => nil},
                 %{"id" => 2, "note" => "x"}
               ])
    end

    test "nested all-null types are rejected too (ARRAY<NULL>)" do
      assert {:error, {:cannot_determine_type, "tags"}} =
               Session.__check_determinable_columns__([%{"tags" => [nil]}])
    end

    test "an explicit schema bypasses the check" do
      assert {:ok, _} =
               Session.__prepare_local_data__([%{"id" => 1, "note" => nil}],
                 schema: "id BIGINT, note STRING"
               )
    end

    test "check_determinable_columns is :ok when every column has a value" do
      assert :ok = Session.__check_determinable_columns__([%{"a" => 1}, %{"a" => nil}])
    end
  end
end
