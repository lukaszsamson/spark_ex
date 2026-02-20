defmodule SparkEx.Integration.DataFrameOpsGapsTest do
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

  # ── drop_duplicates with column subset ──

  describe "drop_duplicates/2 with subset" do
    test "deduplicates on specified columns only", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a', 10), (1, 'a', 20), (2, 'b', 30) AS t(id, name, value)"
        )
        |> DataFrame.drop_duplicates(["id", "name"])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2]
    end

    test "drop_duplicates with no subset deduplicates all columns", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a'), (1, 'a'), (2, 'b') AS t(id, name)"
        )
        |> DataFrame.drop_duplicates()
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end
  end

  # ── column names with dots ──

  describe "column names with dots" do
    test "drop column with dot in name", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 1 AS `a.b`, 2 AS c")
        |> DataFrame.drop(["a.b"])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert Map.keys(row) == ["c"]
    end
  end

  # ── lateral column alias ──

  describe "lateral column alias" do
    test "referencing earlier column in same select via SQL", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT 1 AS a, a + 1 AS b"
        )

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["a"] == 1
      assert row["b"] == 2
    end
  end

  # ── duplicated column names ──

  describe "duplicated column names" do
    @tag :skip
    @tag :explorer_limitation
    test "DataFrame with duplicate column names can be collected", %{session: session} do
      # SKIP: Explorer/Polars NIF panics on duplicate column names in Arrow IPC.
      # See EXPLORER_TODO.md for details.
      df = SparkEx.sql(session, "SELECT 1 AS x, 2 AS x")

      assert {:ok, [row]} = DataFrame.collect(df)
      assert map_size(row) >= 1
    end
  end

  # ── sample with seed ──

  describe "sample with random seed" do
    test "deterministic sampling with explicit seed", %{session: session} do
      df = SparkEx.range(session, 100)

      assert {:ok, sample1} = df |> DataFrame.sample(0.3, seed: 42) |> DataFrame.collect()
      assert {:ok, sample2} = df |> DataFrame.sample(0.3, seed: 42) |> DataFrame.collect()

      assert sample1 == sample2
      assert length(sample1) > 0
      assert length(sample1) < 100
    end
  end

  # ── repartition_by_expression ──

  describe "repartition_by_expression" do
    test "repartition using column expressions", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.repartition(4, [Functions.col("id")])

      assert {:ok, 20} = DataFrame.count(df)
    end
  end

  # ── repartition_by_range ──

  describe "repartition_by_range" do
    test "range-based repartitioning", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.repartition_by_range(4, [Functions.col("id")])

      assert {:ok, 20} = DataFrame.count(df)
    end

    test "repartition_by_range with sort direction", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.repartition_by_range(4, [Column.desc(Functions.col("id"))])

      assert {:ok, 20} = DataFrame.count(df)
    end
  end

  # ── offset / limit+offset ──

  describe "offset/2" do
    test "skips first N rows", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.order_by(["id"])
        |> DataFrame.offset(5)
        |> DataFrame.limit(3)

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [5, 6, 7]
    end
  end

  # ── tail ──

  describe "tail/2" do
    test "returns last N rows", %{session: session} do
      df = SparkEx.range(session, 10) |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.tail(df, 3) |> DataFrame.collect()
      ids = Enum.map(rows, & &1["id"])
      assert ids == [7, 8, 9]
    end
  end

  # ── same_semantics / semantic_hash ──

  describe "same_semantics/2 and semantic_hash/1" do
    test "same plan yields same semantics", %{session: session} do
      df1 =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(5)))

      df2 =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(5)))

      assert {:ok, true} = DataFrame.same_semantics(df1, df2)
    end

    test "different plans yield different semantics", %{session: session} do
      df1 = SparkEx.range(session, 10)
      df2 = SparkEx.range(session, 20)

      assert {:ok, false} = DataFrame.same_semantics(df1, df2)
    end

    test "semantic_hash returns an integer", %{session: session} do
      df = SparkEx.range(session, 10)

      assert {:ok, hash} = DataFrame.semantic_hash(df)
      assert is_integer(hash)
    end

    test "same plans produce same hash", %{session: session} do
      df1 = SparkEx.range(session, 10)
      df2 = SparkEx.range(session, 10)

      assert {:ok, h1} = DataFrame.semantic_hash(df1)
      assert {:ok, h2} = DataFrame.semantic_hash(df2)
      assert h1 == h2
    end
  end

  # ── subquery_alias ──

  describe "alias_/2 (subquery alias)" do
    test "aliases a DataFrame as subquery", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
        |> DataFrame.alias_("sub")

      # Should be able to reference the alias in subsequent operations
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end
  end

  # ── head / first ──

  describe "head/3 and first/2" do
    test "head returns first N rows", %{session: session} do
      df = SparkEx.range(session, 10) |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.head(df, 3)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [0, 1, 2]
    end

    test "first returns single row", %{session: session} do
      df = SparkEx.range(session, 10) |> DataFrame.order_by(["id"])

      assert {:ok, row} = DataFrame.first(df)
      assert row["id"] == 0
    end
  end

  # ── rollup / cube ──

  describe "rollup/2" do
    test "multi-level rollup aggregation", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('a', 'x', 1), ('a', 'y', 2), ('b', 'x', 3) AS t(g1, g2, v)"
        )
        |> DataFrame.rollup(["g1", "g2"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("v")), "total")
        ])
        |> DataFrame.order_by(["g1", "g2"])

      assert {:ok, rows} = DataFrame.collect(df)

      # Rollup produces subtotals + grand total: (a,x), (a,y), (a,null), (b,x), (b,null), (null,null)
      assert length(rows) >= 4
      # Grand total row should exist
      assert Enum.any?(rows, fn r -> r["g1"] == nil and r["g2"] == nil end)
    end
  end

  describe "cube/2" do
    test "multi-dimensional cube aggregation", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('a', 'x', 1), ('a', 'y', 2), ('b', 'x', 3) AS t(g1, g2, v)"
        )
        |> DataFrame.cube(["g1", "g2"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("v")), "total")
        ])
        |> DataFrame.order_by(["g1", "g2"])

      assert {:ok, rows} = DataFrame.collect(df)
      # Cube produces all combinations of grouping
      assert length(rows) >= 6
      # Grand total
      assert Enum.any?(rows, fn r -> r["g1"] == nil and r["g2"] == nil end)
      # Group by g2 only
      assert Enum.any?(rows, fn r -> r["g1"] == nil and r["g2"] != nil end)
    end
  end

  # ── cross_join ──

  describe "cross_join/2" do
    test "Cartesian product without condition", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(a)")
      right = SparkEx.sql(session, "SELECT * FROM VALUES ('x'), ('y') AS t(b)")

      df = DataFrame.cross_join(left, right) |> DataFrame.order_by(["a", "b"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 4
    end
  end

  # ── where (alias for filter) ──

  describe "where/2" do
    test "filters same as filter/2", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.where(Column.gt(Functions.col("id"), Functions.lit(7)))

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [8, 9]
    end
  end

  # ── to_local_iterator ──

  describe "to_local_iterator/2" do
    test "returns a lazy stream of rows", %{session: session} do
      {:ok, local_df} =
        SparkEx.create_dataframe(session, [
          %{"id" => 0},
          %{"id" => 1},
          %{"id" => 2},
          %{"id" => 3},
          %{"id" => 4}
        ])

      assert {:ok, stream} = DataFrame.to_local_iterator(local_df)
      rows = Enum.to_list(stream)
      assert length(rows) == 5
      ids = rows |> Enum.map(& &1["id"]) |> Enum.sort()
      assert ids == [0, 1, 2, 3, 4]
    end

    test "supports incremental consumption with Stream.take", %{session: session} do
      {:ok, local_df} = SparkEx.create_dataframe(session, Enum.map(0..20, &%{"id" => &1}))

      assert {:ok, stream} = DataFrame.to_local_iterator(local_df)
      ids = stream |> Stream.take(3) |> Enum.map(& &1["id"])
      assert length(ids) == 3
      assert Enum.all?(ids, &is_integer/1)
    end
  end
end
