defmodule SparkEx.Integration.StreamingSinksTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, GroupedData, StreamReader, StreamWriter, StreamingQuery}
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

  defp unique_checkpoint do
    suffix =
      "#{System.system_time(:millisecond)}_#{System.unique_integer([:positive, :monotonic])}"

    path = "/tmp/spark_ex_streaming_sink_ckpt_#{suffix}"
    File.rm_rf!(path)
    path
  end

  defp start_file_sink(session, format) do
    path = "/tmp/spark_ex_streaming_#{format}_#{System.unique_integer([:positive])}"
    checkpoint = unique_checkpoint()

    df = StreamReader.rate(session, rows_per_second: 10)

    writer =
      df
      |> DataFrame.write_stream()
      |> StreamWriter.format(format)
      |> StreamWriter.output_mode("append")
      |> StreamWriter.option("checkpointLocation", checkpoint)
      |> StreamWriter.path(path)

    {path, StreamWriter.start(writer)}
  end

  describe "file sinks" do
    test "streaming parquet sink writes files", %{session: session} do
      {path, {:ok, query}} = start_file_sink(session, "parquet")
      on_exit(fn -> StreamingQuery.stop(query) end)

      Process.sleep(2000)
      assert {:ok, true} = StreamingQuery.is_active?(query)
      assert :ok = StreamingQuery.stop(query)

      assert {:ok, rows} = SparkEx.Reader.parquet(session, path) |> DataFrame.collect()
      assert length(rows) > 0
    end

    test "streaming json sink writes files", %{session: session} do
      {path, {:ok, query}} = start_file_sink(session, "json")
      on_exit(fn -> StreamingQuery.stop(query) end)

      Process.sleep(2000)
      assert {:ok, true} = StreamingQuery.is_active?(query)
      assert :ok = StreamingQuery.stop(query)

      assert {:ok, rows} = SparkEx.Reader.json(session, path) |> DataFrame.collect()
      assert length(rows) > 0
    end

    test "streaming csv sink writes files", %{session: session} do
      {path, {:ok, query}} = start_file_sink(session, "csv")
      on_exit(fn -> StreamingQuery.stop(query) end)

      Process.sleep(2000)
      assert {:ok, true} = StreamingQuery.is_active?(query)
      assert :ok = StreamingQuery.stop(query)

      assert {:ok, rows} =
               SparkEx.Reader.csv(session, path, header: false, infer_schema: true)
               |> DataFrame.collect()

      assert length(rows) > 0
    end
  end

  describe "watermark + window" do
    test "windowed aggregation with watermark writes to memory sink", %{session: session} do
      checkpoint = unique_checkpoint()
      query_name = "stream_window_#{System.unique_integer([:positive])}"

      df =
        StreamReader.rate(session, rows_per_second: 10)
        |> DataFrame.with_watermark("timestamp", "10 seconds")
        |> DataFrame.group_by([Functions.expr("window(timestamp, '10 seconds')")])
        |> GroupedData.agg([Functions.count(Functions.col("value"))])

      writer =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.format("memory")
        |> StreamWriter.output_mode("complete")
        |> StreamWriter.option("checkpointLocation", checkpoint)
        |> StreamWriter.query_name(query_name)
        |> StreamWriter.trigger(once: true)

      {:ok, query} = StreamWriter.start(writer)
      on_exit(fn -> StreamingQuery.stop(query) end)

      assert {:ok, _} = StreamingQuery.await_termination(query, timeout: 20_000)
      assert {:ok, progress} = StreamingQuery.last_progress(query)
      assert is_binary(progress)

      assert :ok = StreamingQuery.stop(query)
    end
  end
end
