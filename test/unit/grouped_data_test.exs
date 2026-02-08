defmodule SparkEx.GroupedDataTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.GroupedData
  alias SparkEx.Functions

  describe "group_by/2 + agg/2" do
    test "creates aggregate plan from group_by and agg" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([Functions.sum(Functions.col("salary"))])

      assert %DataFrame{
               plan:
                 {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}],
                  [{:fn, "sum", [{:col, "salary"}], false}]}
             } = result
    end

    test "group_by with Column structs" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}

      result =
        df
        |> DataFrame.group_by([Functions.col("dept"), Functions.col("role")])
        |> GroupedData.agg([Functions.count(Functions.col("id"))])

      assert %DataFrame{
               plan:
                 {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}, {:col, "role"}],
                  [{:fn, "count", [{:col, "id"}], false}]}
             } = result
    end

    test "group_by with atom column names" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}

      result =
        df
        |> DataFrame.group_by([:dept])
        |> GroupedData.agg([Functions.avg(Functions.col("salary"))])

      assert %DataFrame{
               plan: {:aggregate, {:sql, _, _}, :groupby, [{:col, "dept"}], _}
             } = result
    end

    test "multiple aggregate expressions" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([
          Functions.sum(Functions.col("salary")),
          Functions.avg(Functions.col("age")),
          Functions.count(Functions.col("id"))
        ])

      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result
      assert length(agg_exprs) == 3
    end

    test "preserves session through group_by + agg" do
      session = self()
      df = %DataFrame{session: session, plan: {:sql, "SELECT * FROM t", nil}}

      result =
        df
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([Functions.count(Functions.col("id"))])

      assert result.session == session
    end

    test "raises when aggregate list is empty" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/at least one aggregate column/, fn ->
        GroupedData.agg(grouped, [])
      end
    end

    test "raises when aggregate list contains non-Column values" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      grouped = DataFrame.group_by(df, ["dept"])

      assert_raise ArgumentError, ~r/expected all aggregate expressions/, fn ->
        GroupedData.agg(grouped, [Functions.count(Functions.col("id")), :bad])
      end
    end
  end
end
