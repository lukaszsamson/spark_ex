defmodule SparkEx.Integration.ResultChunkingTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.1"

  alias SparkEx.Connect.Client
  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{ExecutePlanResponse, SparkConnectService.Stub}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "execute request chunking options drive arrow chunk metadata", %{session: session} do
    :ok =
      SparkEx.config_set(session, [{"spark.connect.session.resultChunking.maxChunkSize", "1024"}])

    try do
      state = SparkEx.Session.get_state(session)

      {plan, _counter} =
        PlanEncoder.encode(
          {:sql, "select id, CAST(id + 0.5 AS DOUBLE) as value from range(0, 2000, 1, 4)", nil},
          0
        )

      request =
        Client.build_execute_request(
          state,
          plan,
          [],
          nil,
          false,
          allow_arrow_batch_chunking: true,
          preferred_arrow_chunk_size: 1024
        )

      assert {:ok, stream} = Stub.execute_plan(state.channel, request, timeout: 60_000)

      arrow_batches =
        Enum.reduce_while(stream, [], fn
          {:ok, %ExecutePlanResponse{response_type: {:arrow_batch, batch}}}, acc ->
            {:cont, [batch | acc]}

          {:ok, %ExecutePlanResponse{response_type: {:result_complete, _}}}, acc ->
            {:halt, Enum.reverse(acc)}

          {:ok, _other}, acc ->
            {:cont, acc}

          {:error, error}, _acc ->
            flunk("unexpected execute_plan stream error: #{inspect(error)}")
        end)

      assert arrow_batches != []
      assert Enum.any?(arrow_batches, fn batch -> batch.num_chunks_in_batch > 1 end)

      Enum.each(arrow_batches, fn chunk ->
        assert chunk.chunk_index >= 0
        assert chunk.chunk_index < chunk.num_chunks_in_batch
        assert byte_size(chunk.data) <= 1024
      end)
    after
      :ok = SparkEx.config_unset(session, ["spark.connect.session.resultChunking.maxChunkSize"])
    end
  end
end
