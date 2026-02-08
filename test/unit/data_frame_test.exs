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

  describe "join/4" do
    test "creates inner join plan with Column condition" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      condition = Column.eq(Functions.col("a.id"), Functions.col("b.id"))
      result = DataFrame.join(df1, df2, condition)

      assert %DataFrame{
               plan:
                 {:join, {:sql, "SELECT * FROM a", _}, {:sql, "SELECT * FROM b", _},
                  {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false}, :inner, []}
             } = result
    end

    test "creates left join plan" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      condition = Column.eq(Functions.col("id"), Functions.col("id"))
      result = DataFrame.join(df1, df2, condition, :left)

      assert %DataFrame{plan: {:join, _, _, _, :left, []}} = result
    end

    test "creates join plan with using columns" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.join(df1, df2, ["id", "name"], :inner)

      assert %DataFrame{plan: {:join, _, _, nil, :inner, ["id", "name"]}} = result
    end

    test "creates join plan with single using column name" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.join(df1, df2, "id", :inner)

      assert %DataFrame{plan: {:join, _, _, nil, :inner, ["id"]}} = result
    end

    test "creates join plan with list of Column conditions" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}

      result =
        DataFrame.join(
          df1,
          df2,
          [
            Column.eq(Functions.col("a.id"), Functions.col("b.id")),
            Column.eq(Functions.col("a.dept"), Functions.col("b.dept"))
          ],
          :inner
        )

      assert %DataFrame{
               plan:
                 {:join, _, _,
                  {:fn, "and",
                   [
                     {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false},
                     {:fn, "==", [{:col, "a.dept"}, {:col, "b.dept"}], false}
                   ], false}, :inner, []}
             } = result
    end

    test "creates cross join" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.join(df1, df2, [], :cross)

      assert %DataFrame{plan: {:join, _, _, nil, :cross, []}} = result
    end

    test "normalizes join type aliases" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}

      assert %DataFrame{plan: {:join, _, _, nil, :full, _}} =
               DataFrame.join(df1, df2, ["id"], :outer)

      assert %DataFrame{plan: {:join, _, _, nil, :left, _}} =
               DataFrame.join(df1, df2, ["id"], :left_outer)

      assert %DataFrame{plan: {:join, _, _, nil, :right, _}} =
               DataFrame.join(df1, df2, ["id"], :rightouter)

      assert %DataFrame{plan: {:join, _, _, nil, :left_semi, _}} =
               DataFrame.join(df1, df2, ["id"], :semi)

      assert %DataFrame{plan: {:join, _, _, nil, :left_anti, _}} =
               DataFrame.join(df1, df2, ["id"], "left_anti")
    end

    test "raises on unsupported join type" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/invalid join type/, fn ->
        DataFrame.join(df1, df2, ["id"], :sideways)
      end
    end

    test "raises on invalid join key type" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/expected join keys/, fn ->
        DataFrame.join(df1, df2, %{id: 1}, :inner)
      end
    end

    test "raises when joining DataFrames from different sessions" do
      df1 = %DataFrame{session: :session_a, plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: :session_b, plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.join(df1, df2, ["id"], :inner)
      end
    end
  end

  describe "distinct/1" do
    test "creates deduplicate plan" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.distinct(df)

      assert %DataFrame{plan: {:deduplicate, {:sql, _, _}, [], true}} = result
    end
  end

  describe "union/2" do
    test "creates union all plan" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.union(df1, df2)

      assert %DataFrame{
               plan:
                 {:set_operation, {:sql, "SELECT * FROM a", _}, {:sql, "SELECT * FROM b", _},
                  :union, true}
             } = result
    end

    test "raises when unioning DataFrames from different sessions" do
      df1 = %DataFrame{session: :session_a, plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: :session_b, plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union(df1, df2)
      end
    end
  end

  describe "union_distinct/2" do
    test "creates union distinct plan" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.union_distinct(df1, df2)

      assert %DataFrame{plan: {:set_operation, _, _, :union, false}} = result
    end

    test "raises when union_distinct DataFrames are from different sessions" do
      df1 = %DataFrame{session: :session_a, plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: :session_b, plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union_distinct(df1, df2)
      end
    end
  end

  describe "intersect/2" do
    test "creates intersect plan" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.intersect(df1, df2)

      assert %DataFrame{plan: {:set_operation, _, _, :intersect, false}} = result
    end

    test "raises when intersecting DataFrames from different sessions" do
      df1 = %DataFrame{session: :session_a, plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: :session_b, plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.intersect(df1, df2)
      end
    end
  end

  describe "except/2" do
    test "creates except plan" do
      df1 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM b", nil}}
      result = DataFrame.except(df1, df2)

      assert %DataFrame{plan: {:set_operation, _, _, :except, false}} = result
    end

    test "raises when excepting DataFrames from different sessions" do
      df1 = %DataFrame{session: :session_a, plan: {:sql, "SELECT * FROM a", nil}}
      df2 = %DataFrame{session: :session_b, plan: {:sql, "SELECT * FROM b", nil}}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.except(df1, df2)
      end
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
