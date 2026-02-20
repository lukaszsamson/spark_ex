defmodule SparkEx.Integration.SubqueryLateralGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Scalar subquery variants ──

  describe "scalar subquery" do
    test "uncorrelated scalar subquery with temp view", %{session: session} do
      SparkEx.sql(
        session,
        "CREATE OR REPLACE TEMPORARY VIEW nums AS SELECT * FROM VALUES (10), (20), (30) AS t(v)"
      )
      |> DataFrame.collect()

      subquery =
        SparkEx.sql(session, "SELECT max(v) FROM nums")
        |> DataFrame.scalar()

      df =
        SparkEx.range(session, 1)
        |> DataFrame.select([Column.alias_(subquery, "max_v")])

      assert {:ok, [%{"max_v" => 30}]} = DataFrame.collect(df)
    end

    test "scalar subquery against local relation", %{session: session} do
      {:ok, local_df} =
        SparkEx.create_dataframe(session, [%{"x" => 5}, %{"x" => 15}], schema: "x INT")

      subquery = DataFrame.scalar(
        local_df
        |> DataFrame.select([Functions.max(Functions.col("x"))])
      )

      df =
        SparkEx.range(session, 1)
        |> DataFrame.select([Column.alias_(subquery, "max_x")])

      assert {:ok, [%{"max_x" => 15}]} = DataFrame.collect(df)
    end

    test "correlated scalar subquery referencing outer columns", %{session: session} do
      # Use SQL to express a correlated subquery since the DataFrame API
      # doesn't directly support correlated references
      df =
        SparkEx.sql(session, """
        SELECT id, (SELECT max(v) FROM VALUES (1),(2),(3) AS sub(v) WHERE sub.v <= outer_t.id) AS max_v
        FROM VALUES (1), (2), (3) AS outer_t(id)
        ORDER BY id
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      assert Enum.map(rows, & &1["max_v"]) == [1, 2, 3]
    end
  end

  # ── Exists subquery variants ──

  describe "exists subquery" do
    test "exists with temp view", %{session: session} do
      SparkEx.sql(
        session,
        "CREATE OR REPLACE TEMPORARY VIEW lookup AS SELECT * FROM VALUES (1), (3), (5) AS t(id)"
      )
      |> DataFrame.collect()

      exists_col = DataFrame.exists(SparkEx.sql(session, "SELECT * FROM lookup"))

      df =
        SparkEx.range(session, 1)
        |> DataFrame.select([Column.alias_(exists_col, "has_data")])

      assert {:ok, [%{"has_data" => true}]} = DataFrame.collect(df)
    end
  end

  # ── In subquery variants ──

  describe "in_subquery" do
    test "in_subquery with computed subquery", %{session: session} do
      subquery =
        SparkEx.sql(session, "SELECT * FROM VALUES (2), (4), (6) AS t(id)")
        |> DataFrame.select([
          Column.alias_(Column.multiply(Functions.col("id"), Functions.lit(2)), "id")
        ])

      df =
        SparkEx.range(session, 15)
        |> DataFrame.filter(DataFrame.in_subquery(subquery, [Functions.col("id")]))
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      # subquery produces 4, 8, 12
      assert Enum.map(rows, & &1["id"]) == [4, 8, 12]
    end
  end

  # ── Lateral join variants ──

  describe "lateral join" do
    test "lateral join inner (default)", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      right = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(rid)")

      df =
        DataFrame.lateral_join(
          left,
          right,
          Column.eq(Functions.col("id"), Functions.col("rid"))
        )
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["id"]) == [1, 2]
    end

    test "lateral join left produces all left rows", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      right = SparkEx.sql(session, "SELECT * FROM VALUES (1) AS t(rid)")

      df =
        DataFrame.lateral_join(
          left,
          right,
          Column.eq(Functions.col("id"), Functions.col("rid")),
          :left
        )
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      # Non-matching rows should have null rid
      assert Enum.find(rows, &(&1["id"] == 3))["rid"] == nil
    end

    test "lateral join cross produces Cartesian product", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(id)")
      right = SparkEx.sql(session, "SELECT * FROM VALUES ('a'), ('b') AS t(tag)")

      df =
        DataFrame.lateral_join(left, right, Functions.lit(true), :cross)
        |> DataFrame.order_by(["id", "tag"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 4
    end

    test "multiple lateral joins chained", %{session: session} do
      base = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(id)")
      right1 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(r1_id, r1_val)")
      right2 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 100), (2, 200) AS t(r2_id, r2_val)")

      df =
        base
        |> DataFrame.lateral_join(
          right1,
          Column.eq(Functions.col("id"), Functions.col("r1_id"))
        )
        |> DataFrame.lateral_join(
          right2,
          Column.eq(Functions.col("id"), Functions.col("r2_id"))
        )
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.map(rows, & &1["r1_val"]) == ["x", "y"]
      assert Enum.map(rows, & &1["r2_val"]) == [100, 200]
    end
  end

  # ── Subqueries via SQL in various positions ──

  describe "subquery in various positions (SQL)" do
    test "subquery in join condition", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT a.id, b.name
        FROM VALUES (1), (2), (3) AS a(id)
        JOIN VALUES (1, 'Alice'), (2, 'Bob') AS b(id, name)
          ON a.id = b.id AND a.id IN (SELECT v FROM VALUES (1), (2) AS s(v))
        ORDER BY a.id
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["name"]) == ["Alice", "Bob"]
    end

    test "subquery in with_columns expression", %{session: session} do
      base = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(id)")

      sub =
        SparkEx.sql(session, "SELECT max(v) FROM VALUES (10), (20), (30) AS t(v)")
        |> DataFrame.scalar()

      df =
        base
        |> DataFrame.with_column("max_val", sub)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.all?(rows, &(&1["max_val"] == 30))
    end

    test "scalar subquery inside lateral join via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT t.id,
               (SELECT count(*) FROM VALUES (1),(2),(3) AS s(v) WHERE s.v <= t.id) AS cnt
        FROM VALUES (1), (2), (3) AS t(id)
        ORDER BY t.id
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["cnt"]) == [1, 2, 3]
    end

    test "lateral join with aggregation and correlated predicates via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT o.dept, sub.total
        FROM VALUES ('eng'), ('hr') AS o(dept)
        JOIN LATERAL (
          SELECT sum(salary) AS total
          FROM VALUES ('eng', 100), ('eng', 200), ('hr', 50) AS e(dept, salary)
          WHERE e.dept = o.dept
        ) sub
        ORDER BY o.dept
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      eng = Enum.find(rows, &(&1["dept"] == "eng"))
      assert eng["total"] == 300
    end

    test "nested lateral joins via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT a.id, b.doubled, c.quadrupled
        FROM VALUES (1), (2) AS a(id)
        JOIN LATERAL (SELECT a.id * 2 AS doubled) b
        JOIN LATERAL (SELECT b.doubled * 2 AS quadrupled) c
        ORDER BY a.id
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, &{&1["id"], &1["doubled"], &1["quadrupled"]}) == [
               {1, 2, 4},
               {2, 4, 8}
             ]
    end

    test "lateral join with table valued function via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT t.id, e.col
        FROM VALUES (1), (2) AS t(id)
        JOIN LATERAL explode(array(t.id, t.id * 10)) AS e(col)
        ORDER BY t.id, e.col
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 4
      assert Enum.map(rows, &{&1["id"], &1["col"]}) == [{1, 1}, {1, 10}, {2, 2}, {2, 20}]
    end

    test "subquery in unpivot source via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM (
          SELECT * FROM VALUES (1, 10, 20) AS t(id, c1, c2)
        )
        UNPIVOT (val FOR col IN (c1, c2))
        ORDER BY col
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.map(rows, &{&1["col"], &1["val"]}) == [{"c1", 10}, {"c2", 20}]
    end
  end
end
