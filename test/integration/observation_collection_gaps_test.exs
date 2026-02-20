defmodule SparkEx.Integration.ObservationCollectionGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.Observation
  alias SparkEx.StreamReader

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── observe with string expression ──

  describe "observe with string expression" do
    test "observe using expr string for metric", %{session: session} do
      obs = Observation.new("expr_test_#{System.unique_integer([:positive])}")

      df =
        SparkEx.range(session, 10)
        |> DataFrame.observe(obs, [
          Column.alias_(Functions.count(Functions.lit(1)), "row_count"),
          Column.alias_(Functions.max(Functions.col("id")), "max_id")
        ])

      assert {:ok, _} = DataFrame.collect(df)

      metrics = Observation.get(obs)
      assert metrics["row_count"] == 10
      assert metrics["max_id"] == 9
    end
  end

  describe "observe streaming guard" do
    test "raises when observing a streaming dataframe", %{session: session} do
      streaming_df = StreamReader.rate(session, rows_per_second: 1)

      assert_raise ArgumentError, ~r/Streaming DataFrame with Observation is not supported/, fn ->
        DataFrame.observe(streaming_df, "stream_obs", [
          Column.alias_(Functions.count(Functions.lit(1)), "n")
        ])
      end
    end
  end

  # ── observe with same name on different DataFrames ──

  describe "observe with same name on different DataFrames" do
    test "two observations with different names collect independently", %{session: session} do
      obs1 = Observation.new("obs_a_#{System.unique_integer([:positive])}")
      obs2 = Observation.new("obs_b_#{System.unique_integer([:positive])}")

      df1 =
        SparkEx.range(session, 5)
        |> DataFrame.observe(obs1, [Column.alias_(Functions.sum(Functions.col("id")), "total")])

      df2 =
        SparkEx.range(session, 10)
        |> DataFrame.observe(obs2, [Column.alias_(Functions.sum(Functions.col("id")), "total")])

      assert {:ok, _} = DataFrame.collect(df1)
      assert {:ok, _} = DataFrame.collect(df2)

      assert Observation.get(obs1)["total"] == 10
      assert Observation.get(obs2)["total"] == 45
    end
  end

  # ── observe with struct type ──

  describe "observe with complex aggregate types" do
    test "observe with min/max on strings", %{session: session} do
      obs = Observation.new("str_obs_#{System.unique_integer([:positive])}")

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('Alice'), ('Bob'), ('Carol') AS t(name)"
        )
        |> DataFrame.observe(obs, [
          Column.alias_(Functions.min(Functions.col("name")), "min_name"),
          Column.alias_(Functions.max(Functions.col("name")), "max_name")
        ])

      assert {:ok, _} = DataFrame.collect(df)

      metrics = Observation.get(obs)
      assert metrics["min_name"] == "Alice"
      assert metrics["max_name"] == "Carol"
    end

    test "observe with multiple numeric aggregates", %{session: session} do
      obs = Observation.new("multi_obs_#{System.unique_integer([:positive])}")

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1.0), (2.0), (3.0), (4.0), (5.0) AS t(v)"
        )
        |> DataFrame.observe(obs, [
          Column.alias_(Functions.sum(Functions.col("v")), "total"),
          Column.alias_(Functions.avg(Functions.col("v")), "mean"),
          Column.alias_(Functions.count(Functions.col("v")), "n")
        ])

      assert {:ok, _} = DataFrame.collect(df)

      metrics = Observation.get(obs)
      assert metrics["n"] == 5

      mean_val =
        case metrics["mean"] do
          v when is_binary(v) -> String.to_float(v)
          %Decimal{} = d -> Decimal.to_float(d)
          v when is_number(v) -> v
        end

      assert_in_delta mean_val, 3.0, 0.01
    end
  end

  # ── collect from empty DataFrame ──

  describe "collect from empty DataFrame" do
    test "returns empty list", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2) AS t(id) WHERE 1 = 0"
        )

      assert {:ok, []} = DataFrame.collect(df)
    end

    test "empty DataFrame preserves schema", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT CAST(1 AS INT) AS id, CAST('x' AS STRING) AS name WHERE 1 = 0"
        )

      assert {:ok, schema} = DataFrame.schema(df)
      {:struct, struct} = schema.kind
      names = Enum.map(struct.fields, & &1.name)
      assert names == ["id", "name"]

      assert {:ok, []} = DataFrame.collect(df)
    end
  end

  # ── collect from null-only DataFrame ──

  describe "collect from null-only DataFrame" do
    test "collects nulls correctly", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (CAST(NULL AS INT), CAST(NULL AS STRING)),
          (CAST(NULL AS INT), CAST(NULL AS STRING))
        AS t(id, name)
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["id"] == nil))
      assert Enum.all?(rows, &(&1["name"] == nil))
    end
  end

  # ── collect with duplicated column names ──

  describe "collect with duplicated column names" do
    @tag :skip
    @tag :explorer_limitation
    test "handles duplicate column names in result", %{session: session} do
      # SKIP: Explorer/Polars NIF panics on duplicate column names in Arrow IPC.
      # See EXPLORER_TODO.md for details.
      df = SparkEx.sql(session, "SELECT 1 AS x, 2 AS x, 3 AS y")

      assert {:ok, [row]} = DataFrame.collect(df)
      assert Map.has_key?(row, "y")
      assert row["y"] == 3
    end
  end

  # ── take/3 ──

  describe "take/3" do
    test "returns specified number of rows", %{session: session} do
      df = SparkEx.range(session, 100) |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.take(df, 5)
      assert length(rows) == 5
      ids = Enum.map(rows, & &1["id"])
      assert ids == [0, 1, 2, 3, 4]
    end
  end

  # ── count/1 ──

  describe "count/1" do
    test "returns exact row count", %{session: session} do
      df = SparkEx.range(session, 42)

      assert {:ok, 42} = DataFrame.count(df)
    end

    test "count after filter", %{session: session} do
      df =
        SparkEx.range(session, 100)
        |> DataFrame.filter(Column.lt(Functions.col("id"), Functions.lit(10)))

      assert {:ok, 10} = DataFrame.count(df)
    end
  end

  # ── is_empty/1 ──

  describe "is_empty/1" do
    test "returns true for empty DataFrame", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Functions.lit(false))

      assert {:ok, true} = DataFrame.is_empty(df)
    end

    test "returns false for non-empty DataFrame", %{session: session} do
      df = SparkEx.range(session, 10)

      assert {:ok, false} = DataFrame.is_empty(df)
    end
  end

  # ── tree_string ──

  describe "tree_string/2" do
    test "returns plan tree string", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(5)))

      assert {:ok, tree} = DataFrame.tree_string(df)
      assert is_binary(tree)
      assert String.contains?(tree, "id")
    end
  end
end
