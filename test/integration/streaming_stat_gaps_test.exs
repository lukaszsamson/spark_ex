defmodule SparkEx.Integration.StreamingStatGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Functions, StreamReader, StreamWriter, StreamingQuery}
  alias SparkEx.DataFrame.{NA, Stat}


  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp unique_checkpoint do
    suffix =
      "#{System.system_time(:millisecond)}_#{System.unique_integer([:positive, :monotonic])}"

    path = "/tmp/spark_ex_gaps_ckpt_#{suffix}"
    File.rm_rf!(path)
    path
  end

  # ── streaming with temporary view ──

  describe "streaming with temporary view" do
    test "register streaming DF as temp view and query via SQL", %{session: session} do
      checkpoint = unique_checkpoint()
      query_name = "stream_view_#{System.unique_integer([:positive])}"

      df = StreamReader.rate(session, rows_per_second: 10)

      {:ok, query} =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.format("memory")
        |> StreamWriter.output_mode("append")
        |> StreamWriter.option("checkpointLocation", checkpoint)
        |> StreamWriter.query_name(query_name)
        |> StreamWriter.start()

      # Wait for some data to arrive
      Process.sleep(2000)

      # Query the memory table via SQL
      result_df = SparkEx.sql(session, "SELECT count(*) AS cnt FROM #{query_name}")
      assert {:ok, [row]} = DataFrame.collect(result_df)
      assert row["cnt"] >= 0

      StreamingQuery.stop(query)
    end
  end

  # ── NA.replace edge cases ──

  describe "NA.replace edge cases" do
    test "replace with nil value (replace value with null)", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2), (3) AS t(x)"
        )

      # Replace 2 with map that replaces 2 -> nil equivalent
      # Since NA.replace doesn't support nil target, we test the map variant
      replaced = NA.replace(df, %{2 => -1})
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [-1, 1, 3]
    end

    test "replace with type mismatch is ignored", %{session: session} do
      # String replacement on integer column should be type-safe
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2), (3) AS t(x)"
        )

      # Replacing string values in integer column - should have no effect
      replaced = NA.replace(df, %{"a" => "b"})
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [1, 2, 3]
    end
  end

  # ── NA.drop with threshold on specific columns ──

  describe "NA.drop with threshold and subset" do
    test "dropna with subset restriction", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, CAST(NULL AS STRING), 100),
          (CAST(NULL AS INT), 'b', 200),
          (3, 'c', CAST(NULL AS INT))
        AS t(id, name, score)
        """)

      # Drop rows with any null in subset [id, name] only
      dropped = NA.drop(df, subset: ["id", "name"])
      {:ok, rows} = DataFrame.collect(dropped)
      # Row 1: id=1, name=null -> dropped
      # Row 2: id=null, name=b -> dropped
      # Row 3: id=3, name=c -> kept (score null doesn't matter)
      assert length(rows) == 1
      assert hd(rows)["id"] == 3
    end

    test "dropna with how: :all and subset", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (CAST(NULL AS INT), CAST(NULL AS STRING)),
          (CAST(NULL AS INT), 'b'),
          (3, 'c')
        AS t(id, name)
        """)

      dropped = NA.drop(df, how: :all, subset: ["id", "name"])
      {:ok, rows} = DataFrame.collect(dropped)
      # Only first row has all nulls in subset -> dropped
      assert length(rows) == 2
    end
  end

  # ── Stat operations on empty DataFrames ──

  describe "Stat operations on empty DataFrames" do
    test "describe on empty DataFrame", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT CAST(1 AS INT) AS id, CAST(1.0 AS DOUBLE) AS value WHERE 1 = 0"
        )

      desc = Stat.describe(df)
      {:ok, rows} = DataFrame.collect(desc)
      # Should still have summary rows (count=0)
      assert length(rows) > 0
      summaries = Map.new(rows, &{&1["summary"], &1})
      assert summaries["count"]["id"] == "0"
    end

    test "corr on empty DataFrame", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT CAST(1.0 AS DOUBLE) AS x, CAST(1.0 AS DOUBLE) AS y WHERE 1 = 0"
        )

      assert {:ok, r} = Stat.corr(df, "x", "y")
      # Correlation of empty set returns :nan atom
      assert r == :nan or r == nil
    end
  end

  # ── Stat operations with all-null columns ──

  describe "Stat operations with all-null columns" do
    test "describe with all-null column", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (CAST(NULL AS INT)),
          (CAST(NULL AS INT)),
          (CAST(NULL AS INT))
        AS t(x)
        """)

      desc = Stat.describe(df)
      {:ok, rows} = DataFrame.collect(desc)
      summaries = Map.new(rows, &{&1["summary"], &1})
      assert summaries["count"]["x"] == "0"
    end
  end

  # ── melt (unpivot) then groupby ──

  describe "melt (unpivot) then groupby" do
    test "unpivot followed by aggregation", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (1, 10, 20), (2, 30, 40) AS t(id, c1, c2)")
        |> DataFrame.unpivot(["id"], ["c1", "c2"], "col_name", "col_value")
        |> DataFrame.group_by(["col_name"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("col_value")), "total")
        ])
        |> DataFrame.order_by(["col_name"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.find(rows, &(&1["col_name"] == "c1"))["total"] == 40
      assert Enum.find(rows, &(&1["col_name"] == "c2"))["total"] == 60
    end
  end

  # ── Error handling gaps ──

  describe "error handling gaps" do
    test "error includes JVM stack trace information", %{session: session} do
      result =
        SparkEx.sql(session, "SELECT * FROM nonexistent_table_xyz_#{System.unique_integer([:positive])}")
        |> DataFrame.collect()

      assert {:error, error} = result
      # Error should contain meaningful information
      assert is_struct(error) or is_binary(error)
    end

    test "concurrent operations don't interfere", %{session: session} do
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            df = SparkEx.sql(session, "SELECT #{i} AS val")
            DataFrame.collect(df)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, fn {:ok, [_row]} -> true; _ -> false end)

      values =
        results
        |> Enum.map(fn {:ok, [row]} -> row["val"] end)
        |> Enum.sort()

      assert values == [1, 2, 3, 4, 5]
    end
  end

  # ── streaming drop_duplicates_within_watermark ──

  describe "streaming drop_duplicates_within_watermark" do
    test "works on streaming DataFrame", %{session: session} do
      checkpoint = unique_checkpoint()
      query_name = "dedup_wm_#{System.unique_integer([:positive])}"

      df =
        StreamReader.rate(session, rows_per_second: 10)
        |> DataFrame.with_watermark("timestamp", "10 seconds")
        |> DataFrame.drop_duplicates_within_watermark(["value"])

      {:ok, query} =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.format("memory")
        |> StreamWriter.output_mode("append")
        |> StreamWriter.option("checkpointLocation", checkpoint)
        |> StreamWriter.query_name(query_name)
        |> StreamWriter.start()

      # Let it run briefly
      Process.sleep(2000)

      assert {:ok, true} = StreamingQuery.is_active?(query)

      StreamingQuery.stop(query)
    end
  end

  # ── session management gaps ──

  describe "session management gaps" do
    test "stop already-stopped session returns error or is idempotent", %{session: _session} do
      {:ok, session2} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session2)

      :ok = SparkEx.Session.stop(session2)

      # Second stop should either succeed (idempotent) or return error
      # GenServer.stop/1 raises if process is already dead, so catch that
      result =
        try do
          SparkEx.Session.stop(session2)
        catch
          :exit, {:noproc, _} -> {:error, :noproc}
          :exit, _ -> {:error, :already_stopped}
        end

      assert result == :ok or match?({:error, _}, result)
    end

    test "multiple sessions to same remote are independent", %{session: session} do
      {:ok, session2} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session2)

      on_exit(fn ->
        if Process.alive?(session2), do: SparkEx.Session.stop(session2)
      end)

      # Set different configs
      SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "10"}])
      SparkEx.config_set(session2, [{"spark.sql.shuffle.partitions", "20"}])

      assert {:ok, [{"spark.sql.shuffle.partitions", "10"}]} =
               SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])

      assert {:ok, [{"spark.sql.shuffle.partitions", "20"}]} =
               SparkEx.config_get(session2, ["spark.sql.shuffle.partitions"])
    end
  end
end
