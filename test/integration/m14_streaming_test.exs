defmodule SparkEx.Integration.M14.StreamingTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.{DataFrame, StreamReader, StreamWriter, StreamingQuery, StreamingQueryManager}

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp unique_checkpoint do
    suffix = "#{System.system_time(:millisecond)}_#{System.unique_integer([:positive, :monotonic])}"
    path = "/tmp/spark_ex_streaming_ckpt_#{suffix}"
    # Ensure clean checkpoint directory
    File.rm_rf!(path)
    path
  end

  defp start_rate_query(session, opts \\ []) do
    checkpoint = Keyword.get(opts, :checkpoint, unique_checkpoint())
    query_name = Keyword.get(opts, :query_name, nil)
    format = Keyword.get(opts, :format, "memory")
    output_mode = Keyword.get(opts, :output_mode, "append")
    trigger_opts = Keyword.get(opts, :trigger, nil)

    df = StreamReader.rate(session, rows_per_second: 10)

    writer =
      df
      |> DataFrame.write_stream()
      |> StreamWriter.format(format)
      |> StreamWriter.output_mode(output_mode)
      |> StreamWriter.option("checkpointLocation", checkpoint)

    writer = if query_name, do: StreamWriter.query_name(writer, query_name), else: writer
    writer = if trigger_opts, do: StreamWriter.trigger(writer, trigger_opts), else: writer

    StreamWriter.start(writer)
  end

  defp stop_query(query) do
    try do
      StreamingQuery.stop(query)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  describe "stream read + is_streaming" do
    test "streaming DataFrame reports is_streaming true", %{session: session} do
      df = StreamReader.rate(session, rows_per_second: 1)
      assert {:ok, true} = DataFrame.is_streaming(df)
    end

    test "batch DataFrame reports is_streaming false", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, false} = DataFrame.is_streaming(df)
    end
  end

  describe "start/stop query" do
    test "start and stop a streaming query", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "start_stop_test")

      on_exit(fn -> stop_query(query) end)

      assert %StreamingQuery{} = query
      assert is_binary(query.query_id)
      assert is_binary(query.run_id)

      assert {:ok, true} = StreamingQuery.is_active?(query)
      assert :ok = StreamingQuery.stop(query)

      # After stop, query should not be active
      # Give Spark a moment to process the stop
      Process.sleep(500)
      assert {:ok, false} = StreamingQuery.is_active?(query)
    end
  end

  describe "query status" do
    test "returns status fields", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "status_test")

      on_exit(fn -> stop_query(query) end)

      # Let the query process a bit
      Process.sleep(1000)

      assert {:ok, status} = StreamingQuery.status(query)
      assert is_map(status)
      assert Map.has_key?(status, :message)
      assert Map.has_key?(status, :is_active)
      assert status.is_active == true

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "await_termination" do
    test "returns terminated status after stop", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "await_test")

      on_exit(fn -> stop_query(query) end)

      # Stop the query first, then await termination without timeout
      :ok = StreamingQuery.stop(query)
      Process.sleep(500)

      # After stopping, await_termination should return true (terminated)
      assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 5000)
    end
  end

  describe "process_all_available" do
    test "completes without error", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "process_all_test")

      on_exit(fn -> stop_query(query) end)

      Process.sleep(500)
      assert :ok = StreamingQuery.process_all_available(query)

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "query manager" do
    test "active returns list of queries", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "mgr_active_test")

      on_exit(fn -> stop_query(query) end)

      Process.sleep(500)

      assert {:ok, queries} = StreamingQueryManager.active(session)
      assert is_list(queries)
      assert length(queries) >= 1

      query_ids = Enum.map(queries, & &1.query_id)
      assert query.query_id in query_ids

      :ok = StreamingQuery.stop(query)
    end

    test "get returns specific query", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "mgr_get_test")

      on_exit(fn -> stop_query(query) end)

      Process.sleep(500)

      assert {:ok, found} = StreamingQueryManager.get(session, query.query_id)
      assert %StreamingQuery{} = found
      assert found.query_id == query.query_id

      :ok = StreamingQuery.stop(query)
    end

    test "await_any_termination after stop", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "mgr_await_test")

      on_exit(fn -> stop_query(query) end)

      # Stop the query first, then await
      :ok = StreamingQuery.stop(query)
      Process.sleep(500)

      assert {:ok, true} = StreamingQueryManager.await_any_termination(session, timeout: 5000)
    end

    test "reset_terminated succeeds", %{session: session} do
      assert :ok = StreamingQueryManager.reset_terminated(session)
    end
  end

  describe "recent_progress" do
    test "returns progress list", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "progress_test")

      on_exit(fn -> stop_query(query) end)

      # Let query run for a bit to generate progress
      Process.sleep(2000)

      assert {:ok, progress} = StreamingQuery.recent_progress(query)
      assert is_list(progress)

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "explain" do
    test "returns explain string", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "explain_test")

      on_exit(fn -> stop_query(query) end)

      Process.sleep(500)

      assert {:ok, plan} = StreamingQuery.explain(query)
      assert is_binary(plan)
      assert String.length(plan) > 0

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "exception" do
    test "returns nil when no exception", %{session: session} do
      {:ok, query} = start_rate_query(session, query_name: "exception_test")

      on_exit(fn -> stop_query(query) end)

      Process.sleep(500)

      assert {:ok, nil} = StreamingQuery.exception(query)

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "memory sink" do
    test "query results via SQL on memory table", %{session: session} do
      table_name = "memory_sink_test_#{System.unique_integer([:positive])}"
      {:ok, query} = start_rate_query(session, query_name: table_name, format: "memory")

      on_exit(fn -> stop_query(query) end)

      # Let some data accumulate
      Process.sleep(3000)

      # Query the memory table via SQL
      result_df = SparkEx.sql(session, "SELECT * FROM #{table_name}")
      assert {:ok, rows} = DataFrame.collect(result_df)
      assert length(rows) > 0

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "to_table" do
    test "starts query writing to a table", %{session: session} do
      table_name = "to_table_test_#{System.unique_integer([:positive])}"
      checkpoint = unique_checkpoint()

      df = StreamReader.rate(session, rows_per_second: 10)

      writer =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.output_mode("append")
        |> StreamWriter.option("checkpointLocation", checkpoint)

      {:ok, query} = StreamWriter.to_table(writer, table_name)

      on_exit(fn -> stop_query(query) end)

      assert %StreamingQuery{} = query
      assert {:ok, true} = StreamingQuery.is_active?(query)

      :ok = StreamingQuery.stop(query)
    end
  end

  describe "convenience methods" do
    test "SparkEx.read_stream returns StreamReader", %{session: session} do
      reader = SparkEx.read_stream(session)
      assert %StreamReader{} = reader
      assert reader.session == session
    end

    test "DataFrame.write_stream returns StreamWriter", %{session: session} do
      df = StreamReader.rate(session, rows_per_second: 1)
      writer = DataFrame.write_stream(df)
      assert %StreamWriter{} = writer
      assert writer.df == df
    end
  end
end
