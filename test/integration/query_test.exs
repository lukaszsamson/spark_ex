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
  end

  describe "config" do
    test "set and get config", %{session: session} do
      assert :ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "10"}])
      assert {:ok, pairs} = SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])
      assert [{"spark.sql.shuffle.partitions", "10"}] = pairs
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
