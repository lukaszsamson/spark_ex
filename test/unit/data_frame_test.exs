defmodule SparkEx.DataFrameTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame

  describe "struct" do
    test "holds session and plan" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      assert df.session == self()
      assert df.plan == {:sql, "SELECT 1", nil}
    end
  end

  describe "SparkEx.sql/3" do
    test "creates a DataFrame with SQL plan" do
      df = SparkEx.sql(self(), "SELECT 1")
      assert %DataFrame{plan: {:sql, "SELECT 1", nil}} = df
    end

    test "creates a DataFrame with SQL and named args" do
      df = SparkEx.sql(self(), "SELECT :id", args: %{id: 1})
      assert %DataFrame{plan: {:sql, "SELECT :id", %{id: 1}}} = df
    end

    test "creates a DataFrame with SQL and positional args" do
      df = SparkEx.sql(self(), "SELECT ?", args: [42])
      assert %DataFrame{plan: {:sql, "SELECT ?", [42]}} = df
    end

    test "raises for invalid SQL args type" do
      assert_raise ArgumentError, ~r/expected :args to be a list, map, or nil/, fn ->
        SparkEx.sql(self(), "SELECT ?", args: MapSet.new([1, 2, 3]))
      end
    end
  end

  describe "SparkEx.range/3" do
    test "creates a range DataFrame with defaults" do
      df = SparkEx.range(self(), 10)
      assert %DataFrame{plan: {:range, 0, 10, 1, nil}} = df
    end

    test "creates a range DataFrame with options" do
      df = SparkEx.range(self(), 100, start: 10, step: 5, num_partitions: 4)
      assert %DataFrame{plan: {:range, 10, 100, 5, 4}} = df
    end
  end
end
