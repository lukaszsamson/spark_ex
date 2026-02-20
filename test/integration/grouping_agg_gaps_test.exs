defmodule SparkEx.Integration.GroupingAggGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.GroupedData

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp sample_df(session) do
    SparkEx.sql(
      session,
      """
      SELECT * FROM VALUES
        ('eng', 'Alice', 100),
        ('eng', 'Bob', 200),
        ('hr', 'Carol', 150),
        ('hr', 'Dave', 250),
        ('sales', 'Eve', 300)
      AS t(dept, name, salary)
      """
    )
  end

  # ── multiple simultaneous aggregation functions ──

  describe "multiple aggregation functions" do
    test "applies multiple agg functions simultaneously", %{session: session} do
      df =
        sample_df(session)
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([
          Column.alias_(Functions.count(Functions.col("salary")), "cnt"),
          Column.alias_(Functions.sum(Functions.col("salary")), "total"),
          Column.alias_(Functions.avg(Functions.col("salary")), "average"),
          Column.alias_(Functions.min(Functions.col("salary")), "min_sal"),
          Column.alias_(Functions.max(Functions.col("salary")), "max_sal")
        ])
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert eng["cnt"] == 2
      assert eng["total"] == 300
      assert eng["average"] == 150.0
      assert eng["min_sal"] == 100
      assert eng["max_sal"] == 200
    end
  end

  # ── group_by_ordinal / order_by_ordinal ──

  describe "group by ordinal and order by ordinal via SQL" do
    test "GROUP BY column position", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT dept, sum(salary) AS total
        FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150) AS t(dept, salary)
        GROUP BY 1
        ORDER BY 1
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.find(rows, &(&1["dept"] == "eng"))["total"] == 300
    end

    test "ORDER BY column position", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (3, 'c'), (1, 'a'), (2, 'b') AS t(id, name)
        ORDER BY 1
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["id"]) == [1, 2, 3]
    end
  end

  # ── pivot ──

  describe "pivot" do
    test "pivot with explicit values", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('eng', 2023, 100), ('eng', 2024, 200), ('hr', 2023, 50) AS t(dept, year, salary)"
        )
        |> DataFrame.group_by(["dept"])
        |> GroupedData.pivot("year", [2023, 2024])
        |> GroupedData.agg([Functions.sum(Functions.col("salary"))])
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert eng["2023"] == 100
      assert eng["2024"] == 200
    end
  end

  # ── aggregation with complex expressions ──

  describe "aggregation with complex expressions" do
    test "agg with arithmetic expressions", %{session: session} do
      df =
        sample_df(session)
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg([
          Column.alias_(
            Column.multiply(Functions.avg(Functions.col("salary")), Functions.lit(1.1)),
            "avg_with_bonus"
          )
        ])
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert_in_delta eng["avg_with_bonus"], 165.0, 0.01
    end
  end

  # ── rollup via DataFrame API ──

  describe "rollup via DataFrame API" do
    test "rollup produces subtotals and grand total", %{session: session} do
      df =
        sample_df(session)
        |> DataFrame.rollup(["dept"])
        |> GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("salary")), "total")
        ])
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      # Should include each dept + a grand total row (null dept)
      assert Enum.any?(rows, &is_nil(&1["dept"]))
      grand_total = Enum.find(rows, &is_nil(&1["dept"]))
      assert grand_total["total"] == 1000
    end
  end

  # ── cube via DataFrame API ──

  describe "cube via DataFrame API" do
    test "cube produces all grouping combinations", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('a', 'x', 1), ('a', 'y', 2), ('b', 'x', 3) AS t(g1, g2, v)"
        )
        |> DataFrame.cube(["g1", "g2"])
        |> GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("v")), "total")
        ])

      assert {:ok, rows} = DataFrame.collect(df)
      # Cube for 2 dims: 2*2 individual + 2 single-dim + 1 grand total = 7+
      assert length(rows) >= 6
    end
  end

  # ── grouping / grouping_id via SQL ──

  describe "grouping and grouping_id functions" do
    test "grouping() identifies aggregate rows", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT dept, grouping(dept) AS is_total, sum(salary) AS total
        FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150) AS t(dept, salary)
        GROUP BY ROLLUP(dept)
        ORDER BY dept NULLS LAST
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      # Dept rows: grouping=0, grand total: grouping=1
      grand = Enum.find(rows, &(&1["is_total"] == 1))
      assert grand["total"] == 450
    end

    test "grouping_id() returns bitmask", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT g1, g2, grouping_id(g1, g2) AS gid, sum(v) AS total
        FROM VALUES ('a', 'x', 1), ('a', 'y', 2), ('b', 'x', 3) AS t(g1, g2, v)
        GROUP BY CUBE(g1, g2)
        ORDER BY gid, g1, g2
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      gids = Enum.map(rows, & &1["gid"]) |> Enum.uniq() |> Enum.sort()
      # gid: 0 (both present), 1 (g2 rolled up), 2 (g1 rolled up), 3 (both rolled up)
      assert gids == [0, 1, 2, 3]
    end
  end

  # ── aggregation with filter clause ──

  describe "aggregation with filter clause via SQL" do
    test "count with FILTER WHERE", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          count(*) AS total,
          count(*) FILTER (WHERE salary > 150) AS high_salary_count
        FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150), ('hr', 250) AS t(dept, salary)
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["total"] == 4
      assert row["high_salary_count"] == 2
    end

    test "sum with FILTER WHERE", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          sum(salary) AS total,
          sum(salary) FILTER (WHERE dept = 'eng') AS eng_total
        FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150) AS t(dept, salary)
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["total"] == 450
      assert row["eng_total"] == 300
    end
  end

  # ── convenience group agg methods ──

  describe "GroupedData convenience methods" do
    test "count/2 returns group sizes", %{session: session} do
      df =
        sample_df(session)
        |> DataFrame.group_by(["dept"])
        |> GroupedData.count()
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert eng["count"] == 2
    end

    test "min/2 and max/2 return group extremes", %{session: session} do
      min_df =
        sample_df(session)
        |> DataFrame.group_by(["dept"])
        |> GroupedData.min()
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(min_df)
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert eng["min(salary)"] == 100

      max_df =
        sample_df(session)
        |> DataFrame.group_by(["dept"])
        |> GroupedData.max()
        |> DataFrame.order_by(["dept"])

      assert {:ok, max_rows} = DataFrame.collect(max_df)
      eng_max = Enum.find(max_rows, &(&1["dept"] == "eng"))
      assert eng_max["max(salary)"] == 200
    end
  end
end
