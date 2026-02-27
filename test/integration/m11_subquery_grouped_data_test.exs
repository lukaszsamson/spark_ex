defmodule SparkEx.Integration.M11SubqueryGroupedDataTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.GroupedData
  alias SparkEx.TableArg

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp numeric_df(session) do
    SparkEx.sql(
      session,
      "SELECT * FROM VALUES (1, 10, 1.5, 'x'), (1, 20, 2.5, 'y') AS t(dept, salary, bonus, name)"
    )
  end

  defp assert_agg_values(rows, prefix, expected_values) do
    [row] = rows

    keys =
      row
      |> Map.keys()
      |> Enum.filter(&String.starts_with?(&1, "#{prefix}("))

    assert length(keys) == length(expected_values)

    values =
      keys
      |> Enum.map(&row[&1])
      |> Enum.map(fn
        %Decimal{} = value -> Decimal.to_float(value)
        value -> value
      end)
      |> Enum.sort()

    assert values == Enum.sort(expected_values)
  end

  describe "subquery parity" do
    @tag min_spark: "4.0"
    test "scalar subquery executes", %{session: session} do
      subquery =
        SparkEx.range(session, 1)
        |> DataFrame.select([Functions.lit(1)])

      df =
        SparkEx.range(session, 1)
        |> DataFrame.select([Column.alias_(DataFrame.scalar(subquery), "b")])

      assert {:ok, [%{"b" => 1}]} = DataFrame.collect(df)
    end

    @tag min_spark: "4.0"
    test "exists subquery executes", %{session: session} do
      has_rows = DataFrame.exists(SparkEx.range(session, 1))

      no_rows =
        DataFrame.exists(
          SparkEx.range(session, 1)
          |> DataFrame.filter(Functions.lit(false))
        )

      df =
        SparkEx.range(session, 1)
        |> DataFrame.select([
          Column.alias_(has_rows, "has_rows"),
          Column.alias_(no_rows, "no_rows")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["has_rows"] == true
      assert row["no_rows"] == false
    end

    test "table_arg subquery encodes with table options", %{session: session} do
      table_arg =
        SparkEx.range(session, 2)
        |> DataFrame.as_table()
        |> TableArg.partition_by(["id"])
        |> TableArg.order_by(["id"])

      expr = TableArg.to_subquery_expr(table_arg)
      {encoded, _counter} = SparkEx.Connect.PlanEncoder.encode({:sql, "SELECT ?", [expr]}, 0)

      assert %Spark.Connect.Plan{
               op_type: {:root, %Spark.Connect.Relation{rel_type: {:with_relations, wr}}}
             } =
               encoded

      assert %Spark.Connect.Relation{rel_type: {:sql, _}} = wr.root
      assert length(wr.references) == 1
    end
  end

  describe "grouped numeric aggregates" do
    test "sum expands numeric columns", %{session: session} do
      df = numeric_df(session)

      assert {:ok, rows} =
               df
               |> DataFrame.group_by(["dept"])
               |> GroupedData.sum()
               |> DataFrame.collect()

      assert_agg_values(rows, "sum", [2, 30, 4.0])
    end

    test "avg and mean expand numeric columns", %{session: session} do
      df = numeric_df(session)

      assert {:ok, avg_rows} =
               df
               |> DataFrame.group_by(["dept"])
               |> GroupedData.avg()
               |> DataFrame.collect()

      assert_agg_values(avg_rows, "avg", [1.0, 15.0, 2.0])

      assert {:ok, mean_rows} =
               df
               |> DataFrame.group_by(["dept"])
               |> GroupedData.mean()
               |> DataFrame.collect()

      assert_agg_values(mean_rows, "avg", [1.0, 15.0, 2.0])
    end
  end
end
