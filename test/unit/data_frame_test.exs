defmodule SparkEx.DataFrameTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions

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

    test "supports start/end signature" do
      df = SparkEx.range(self(), 10, 20)
      assert %DataFrame{plan: {:range, 10, 20, 1, nil}} = df
    end

    test "supports start/end/step signature with opts" do
      df = SparkEx.range(self(), 10, 20, 2, num_partitions: 3)
      assert %DataFrame{plan: {:range, 10, 20, 2, 3}} = df
    end
  end

  describe "select/2" do
    test "creates project plan from Column structs" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.select(df, [Functions.col("a"), Functions.col("b")])
      assert %DataFrame{plan: {:project, {:sql, _, _}, [{:col, "a"}, {:col, "b"}]}} = result
    end

    test "creates project plan from string column names" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.select(df, ["a", "b"])
      assert %DataFrame{plan: {:project, {:sql, _, _}, [{:col, "a"}, {:col, "b"}]}} = result
    end

    test "creates project plan from atom column names" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.select(df, [:a, :b])
      assert %DataFrame{plan: {:project, {:sql, _, _}, [{:col, "a"}, {:col, "b"}]}} = result
    end
  end

  describe "filter/2" do
    test "creates filter plan from Column condition" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      condition = Column.gt(Functions.col("age"), Functions.lit(18))
      result = DataFrame.filter(df, condition)

      assert %DataFrame{
               plan: {:filter, {:sql, _, _}, {:fn, ">", [{:col, "age"}, {:lit, 18}], false}}
             } = result
    end
  end

  describe "with_column/3" do
    test "creates with_columns plan" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      expr = Column.plus(Functions.col("a"), Functions.lit(1))
      result = DataFrame.with_column(df, "a_plus_1", expr)

      assert %DataFrame{
               plan:
                 {:with_columns, {:sql, _, _},
                  [{:alias, {:fn, "+", [{:col, "a"}, {:lit, 1}], false}, "a_plus_1"}]}
             } = result
    end
  end

  describe "drop/2" do
    test "creates drop plan from string names" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.drop(df, ["temp", "debug"])
      assert %DataFrame{plan: {:drop, {:sql, _, _}, ["temp", "debug"]}} = result
    end

    test "creates drop plan from atom names" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.drop(df, [:temp, :debug])
      assert %DataFrame{plan: {:drop, {:sql, _, _}, ["temp", "debug"]}} = result
    end
  end

  describe "order_by/2" do
    test "creates sort plan from Column with sort order" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.order_by(df, [Column.desc(Functions.col("age"))])

      assert %DataFrame{
               plan: {:sort, {:sql, _, _}, [{:sort_order, {:col, "age"}, :desc, nil}]}
             } = result
    end

    test "creates sort plan from string names (ascending default)" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.order_by(df, ["name"])

      assert %DataFrame{
               plan: {:sort, {:sql, _, _}, [{:sort_order, {:col, "name"}, :asc, nil}]}
             } = result
    end

    test "wraps bare Column in ascending sort order" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.order_by(df, [Functions.col("name")])

      assert %DataFrame{
               plan: {:sort, {:sql, _, _}, [{:sort_order, {:col, "name"}, :asc, nil}]}
             } = result
    end
  end

  describe "limit/2" do
    test "creates limit plan" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.limit(df, 10)
      assert %DataFrame{plan: {:limit, {:sql, _, _}, 10}} = result
    end
  end

  describe "chaining transforms" do
    test "builds up nested plan through pipeline" do
      df =
        %DataFrame{session: self(), plan: {:read_data_source, "parquet", ["/p"], nil, %{}}}
        |> DataFrame.select(["name", "age"])
        |> DataFrame.filter(Column.gt(Functions.col("age"), Functions.lit(18)))
        |> DataFrame.order_by([Column.desc(Functions.col("age"))])
        |> DataFrame.limit(10)

      # Plan should be: limit -> sort -> filter -> project -> read
      assert {:limit, {:sort, {:filter, {:project, {:read_data_source, _, _, _, _}, _}, _}, _},
              10} =
               df.plan
    end
  end
end
