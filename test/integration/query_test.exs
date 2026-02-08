defmodule SparkEx.Integration.QueryTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    # Unlink so the GenServer doesn't die when the test process exits
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "sql + collect" do
    test "SELECT 1 returns single row", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert [%{"n" => 1}] = rows
    end

    test "SELECT multiple columns", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS a, 'hello' AS b, true AS c")
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert [row] = rows
      assert row["a"] == 1
      assert row["b"] == "hello"
      assert row["c"] == true
    end

    test "SELECT with multiple rows", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, letter)")

      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 3
    end

    test "SQL with named args", %{session: session} do
      df = SparkEx.sql(session, "SELECT :id AS id, :name AS name", args: %{id: 7, name: "alice"})
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert [%{"id" => 7, "name" => "alice"}] = rows
    end

    test "SQL with positional args", %{session: session} do
      df = SparkEx.sql(session, "SELECT ? AS id, ? AS name", args: [9, "bob"])
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert [%{"id" => 9, "name" => "bob"}] = rows
    end

    test "SQL with named expression args", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT element_at(:m, 'a') AS v FROM range(1)",
          args: %{m: SparkEx.Functions.expr("map('a', 1)")}
        )

      assert {:ok, [%{"v" => 1}]} = SparkEx.DataFrame.collect(df)
    end

    test "SQL with positional expression args", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT element_at(?, 1) AS v FROM range(1)",
          args: [SparkEx.Functions.expr("array(7)")]
        )

      assert {:ok, [%{"v" => 7}]} = SparkEx.DataFrame.collect(df)
    end

    test "SQL with invalid args type raises", %{session: session} do
      assert_raise ArgumentError, ~r/expected :args to be a list, map, or nil/, fn ->
        SparkEx.sql(session, "SELECT 1", args: MapSet.new([1, 2, 3]))
      end
    end
  end

  describe "range" do
    test "creates range DataFrame and collects", %{session: session} do
      df = SparkEx.range(session, 5)
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 5
      ids = Enum.map(rows, & &1["id"])
      assert ids == [0, 1, 2, 3, 4]
    end

    test "range with start and step", %{session: session} do
      df = SparkEx.range(session, 10, start: 2, step: 3)
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [2, 5, 8]
    end
  end

  describe "take" do
    test "returns limited rows", %{session: session} do
      df = SparkEx.range(session, 100)
      assert {:ok, rows} = SparkEx.DataFrame.take(df, 3)
      assert length(rows) == 3
    end
  end

  describe "count" do
    test "returns row count", %{session: session} do
      df = SparkEx.range(session, 42)
      assert {:ok, 42} = SparkEx.DataFrame.count(df)
    end
  end

  describe "schema" do
    test "returns schema for SQL query", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n, 'hello' AS s")
      assert {:ok, schema} = SparkEx.DataFrame.schema(df)
      assert schema != nil
    end
  end

  describe "explain" do
    test "returns explain string", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")
      assert {:ok, explain_str} = SparkEx.DataFrame.explain(df)
      assert is_binary(explain_str)
      assert String.length(explain_str) > 0
    end

    test "extended explain mode", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")
      assert {:ok, explain_str} = SparkEx.DataFrame.explain(df, :extended)
      assert is_binary(explain_str)
    end

    test "all explain modes return non-empty output", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")

      for mode <- [:simple, :extended, :codegen, :cost, :formatted] do
        assert {:ok, explain_str} = SparkEx.DataFrame.explain(df, mode)
        assert is_binary(explain_str)
        assert String.length(explain_str) > 0
      end
    end

    test "invalid explain mode returns error", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")
      assert {:error, {:invalid_explain_mode, :unknown}} = SparkEx.DataFrame.explain(df, :unknown)
    end
  end

  describe "config" do
    test "set and get config", %{session: session} do
      assert :ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "10"}])
      assert {:ok, pairs} = SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])
      assert [{"spark.sql.shuffle.partitions", "10"}] = pairs
    end

    test "unknown key returns structured error", %{session: session} do
      key = "spark_ex.this.key.should.not.exist"
      assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.config_get(session, [key])
      assert error.error_class == "SQL_CONF_NOT_FOUND"
      assert error.sql_state == "42K0I"
    end
  end

  describe "session continuity" do
    test "tracks server-side session id across requests", %{session: session} do
      state_before = SparkEx.Session.get_state(session)
      assert state_before.server_side_session_id in [nil, ""]

      assert {:ok, _} = SparkEx.spark_version(session)
      state_after_version = SparkEx.Session.get_state(session)
      assert is_binary(state_after_version.server_side_session_id)
      assert state_after_version.server_side_session_id != ""

      _ = SparkEx.sql(session, "SELECT 1") |> SparkEx.DataFrame.collect()
      state_after_collect = SparkEx.Session.get_state(session)

      assert state_after_collect.server_side_session_id ==
               state_after_version.server_side_session_id
    end
  end

  describe "chunking behavior" do
    test "collect succeeds for query shape likely to trigger chunking", %{session: session} do
      :ok =
        SparkEx.config_set(session, [
          {"spark.connect.session.resultChunking.maxChunkSize", "1024"}
        ])

      df =
        SparkEx.sql(session, "SELECT id, CAST(id + 0.5 AS DOUBLE) AS d FROM range(0, 2000, 1, 4)")

      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 2000
      assert hd(rows)["id"] == 0
      assert List.last(rows)["id"] == 1999
    end
  end

  describe "error handling" do
    test "invalid SQL returns structured error", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM nonexistent_table_xyz_12345")
      result = SparkEx.DataFrame.collect(df)

      assert {:error, error} = result
      assert %SparkEx.Error.Remote{} = error
      assert error.grpc_status != nil
    end
  end
end
