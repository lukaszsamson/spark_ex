defmodule SparkEx.GroupedDataTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.GroupedData
  alias SparkEx.Functions

  describe "group_by/2 + agg/2" do
    test "creates aggregate plan from group_by and agg" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([Functions.sum(Functions.col("salary"))])

      assert {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}],
              [{:fn, "sum", [{:col, "salary"}], false}]} = unwrap_plan(result)
    end

    test "group_by with Column structs" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by([Functions.col("dept"), Functions.col("role")])
        |> GroupedData.agg([Functions.count(Functions.col("id"))])

      assert {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}, {:col, "role"}],
              [{:fn, "count", [{:col, "id"}], false}]} = unwrap_plan(result)
    end

    test "group_by with atom column names" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by([:dept])
        |> GroupedData.agg([Functions.avg(Functions.col("salary"))])

      assert {:aggregate, {:sql, "SELECT * FROM t", nil}, :groupby, [{:col, "dept"}],
              [{:fn, "avg", [{:col, "salary"}], false}]} = unwrap_plan(result)
    end

    test "multiple aggregate expressions" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([
          Functions.sum(Functions.col("salary")),
          Functions.avg(Functions.col("age")),
          Functions.count(Functions.col("id"))
        ])

      assert {:aggregate, _, :groupby, _, agg_exprs} = unwrap_plan(result)
      assert length(agg_exprs) == 3
    end

    test "preserves session through group_by + agg" do
      session = self()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([Functions.count(Functions.col("id"))])

      assert result.session == session
    end

    test "raises when aggregate list is empty" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/at least one aggregate column/, fn ->
        GroupedData.agg(grouped, [])
      end
    end

    test "raises when aggregate list contains non-Column values" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, [Functions.count(Functions.col("id")), :bad])
      end
    end
  end

  describe "agg/2 pair-form" do
    test "valid pair list {col, func_name} produces remote call with correct function name" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([{"salary", "max"}])

      assert {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}],
              [{:alias, {:fn, "max", [{:col, "salary"}], false}, _}]} = unwrap_plan(result)
    end

    test "valid map %{col => func_name} produces remote call with correct function name" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg(%{"salary" => "avg"})

      assert {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}],
              [{:alias, {:fn, "avg", [{:col, "salary"}], false}, _}]} = unwrap_plan(result)
    end

    test "raises ArgumentError for boolean func_name true in pair list" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, [{"x", true}])
      end
    end

    test "raises ArgumentError for boolean func_name false in pair list" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, [{"x", false}])
      end
    end

    test "raises ArgumentError for nil func_name in pair list" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, [{"x", nil}])
      end
    end

    test "raises ArgumentError for boolean func_name true in map form" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, %{"x" => true})
      end
    end
  end
end
