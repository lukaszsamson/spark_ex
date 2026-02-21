defmodule SparkEx.SqlFormatterTest do
  use ExUnit.Case, async: true

  alias SparkEx.SqlFormatter

  describe "format/2 with nil args" do
    test "returns sql unchanged" do
      assert SqlFormatter.format("SELECT 1", nil) == "SELECT 1"
    end
  end

  describe "format/2 with positional args" do
    test "replaces single placeholder" do
      assert SqlFormatter.format("SELECT ? AS a", [1]) == "SELECT 1 AS a"
    end

    test "replaces multiple placeholders" do
      assert SqlFormatter.format("SELECT ? AS a, ? AS b", [1, 2]) ==
               "SELECT 1 AS a, 2 AS b"
    end

    test "quotes string values" do
      assert SqlFormatter.format("SELECT ? AS a", ["hello"]) ==
               "SELECT 'hello' AS a"
    end

    test "escapes single quotes in strings" do
      assert SqlFormatter.format("SELECT ? AS a", ["it's"]) ==
               "SELECT 'it''s' AS a"
    end

    test "handles nil as NULL" do
      assert SqlFormatter.format("SELECT ? AS a", [nil]) == "SELECT NULL AS a"
    end

    test "handles booleans" do
      assert SqlFormatter.format("SELECT ?, ?", [true, false]) == "SELECT TRUE, FALSE"
    end

    test "handles dates" do
      date = ~D[2024-01-15]
      assert SqlFormatter.format("SELECT ?", [date]) == "SELECT DATE '2024-01-15'"
    end

    test "handles floats" do
      result = SqlFormatter.format("SELECT ?", [3.14])
      assert result =~ "SELECT 3.14"
    end

    test "raises when more placeholders than args" do
      assert_raise ArgumentError, ~r/not enough args/, fn ->
        SqlFormatter.format("SELECT ? AS a, ? AS b", [1])
      end
    end

    test "raises when more args than placeholders" do
      assert_raise ArgumentError, ~r/not enough placeholders/, fn ->
        SqlFormatter.format("SELECT ? AS a", [1, 2])
      end
    end

    test "does not replace ? inside string literals" do
      assert SqlFormatter.format("SELECT '?' AS literal, ? AS v", [42]) ==
               "SELECT '?' AS literal, 42 AS v"
    end

    test "handles escaped quotes in string literals" do
      assert SqlFormatter.format("SELECT 'it''s a ?' AS literal, ? AS v", [1]) ==
               "SELECT 'it''s a ?' AS literal, 1 AS v"
    end

    test "empty args with no placeholders" do
      assert SqlFormatter.format("SELECT 1", []) == "SELECT 1"
    end
  end

  describe "format/2 with named args" do
    test "replaces single named placeholder" do
      assert SqlFormatter.format("SELECT :id AS id", %{id: 42}) ==
               "SELECT 42 AS id"
    end

    test "replaces multiple named placeholders" do
      assert SqlFormatter.format("SELECT :id AS id, :name AS name", %{id: 1, name: "alice"}) ==
               "SELECT 1 AS id, 'alice' AS name"
    end

    test "does not corrupt overlapping names" do
      result =
        SqlFormatter.format("SELECT :id_2 AS x, :id AS y", %{id: 1, id_2: 2})

      assert result == "SELECT 2 AS x, 1 AS y"
    end

    test "does not replace inside string literals" do
      result =
        SqlFormatter.format("SELECT ':id' AS literal, :id AS v", %{id: 42})

      assert result == "SELECT ':id' AS literal, 42 AS v"
    end

    test "raises on unresolved named placeholders" do
      assert_raise ArgumentError, ~r/unresolved named placeholders/, fn ->
        SqlFormatter.format("SELECT :id AS id, :name AS name", %{id: 1})
      end
    end

    test "raises on unused named args" do
      assert_raise ArgumentError, ~r/unused named args/, fn ->
        SqlFormatter.format("SELECT :id AS id", %{id: 1, unused: 2})
      end
    end

    test "handles string keys in args map" do
      assert SqlFormatter.format("SELECT :id AS id", %{"id" => 42}) ==
               "SELECT 42 AS id"
    end

    test "replaces same placeholder used multiple times" do
      result =
        SqlFormatter.format("SELECT :id AS a, :id AS b", %{id: 42})

      assert result == "SELECT 42 AS a, 42 AS b"
    end
  end

  describe "format/2 with invalid args" do
    test "raises for non-map, non-list, non-nil args" do
      assert_raise ArgumentError, fn ->
        SqlFormatter.format("SELECT 1", :invalid)
      end
    end
  end
end
