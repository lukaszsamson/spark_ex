defmodule SparkEx.Integration.Spark42RealTimeTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"
  @moduletag skip:
               if(System.get_env("SPARK_EX_TEST_PROVIDERS") == "1",
                 do: false,
                 else: "requires the prepared Spark 4.2 provider fixtures"
               )

  alias SparkEx.{DataFrame, Session, StreamReader, StreamingQuery, StreamWriter}

  test "real-time trigger consumes a supported source through the real-time reader" do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)

    checkpoint = Path.join(System.tmp_dir!(), "spark42-rtm-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(checkpoint) end)

    # The provider rejects ordinary micro-batch planning and next/0, proving
    # this trigger reaches Spark's SupportsRealTimeMode/SupportsRealTimeRead path.
    input =
      session
      |> StreamReader.new()
      |> StreamReader.format("org.apache.spark.sql.connector.read.Spark42RealTimeProvider")
      |> StreamReader.load()

    assert {:ok, query} =
             input
             |> DataFrame.write_stream()
             |> StreamWriter.format("console")
             |> StreamWriter.output_mode("update")
             |> StreamWriter.option("checkpointLocation", checkpoint)
             |> StreamWriter.trigger(real_time: "5 seconds")
             |> StreamWriter.start()

    try do
      # Connect's serialized progress exposes counts on sources/sink; the
      # derived top-level numInputRows printed in Spark logs is not on this wire.
      progress = await_input(query, System.monotonic_time(:millisecond) + 30_000)
      assert progress["sink"]["numOutputRows"] == 3
      assert [%{"numInputRows" => 3}] = progress["sources"]
      assert {:ok, true} = StreamingQuery.is_active?(query)
    after
      assert :ok = StreamingQuery.stop(query)
    end

    assert {:ok, false} = StreamingQuery.is_active?(query)
  end

  defp await_input(query, deadline) do
    assert {:ok, progress} = StreamingQuery.recent_progress(query)

    case Enum.find(progress, &(get_in(&1, ["sink", "numOutputRows"]) == 3)) do
      nil ->
        assert System.monotonic_time(:millisecond) < deadline,
               "real-time source did not report its three input rows: #{inspect(progress)}"

        assert {:ok, true} = StreamingQuery.is_active?(query)
        Process.sleep(100)
        await_input(query, deadline)

      progress ->
        progress
    end
  end
end
