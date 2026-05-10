defmodule SparkEx.Test.DecoderFakesSmokeTest do
  @moduledoc """
  Sanity tests for `SparkEx.Test.DecoderFakes`. Stream C will use these
  builders to inject ResultComplete + trailing frames, RetryInfo errors,
  and metrics deterministically.
  """

  use ExUnit.Case, async: true

  alias SparkEx.Test.DecoderFakes
  alias SparkEx.Connect.{ResultDecoder, Errors}
  alias Spark.Connect.ExecutePlanResponse

  test "result_complete + observed metrics round-trip through decoder" do
    obs =
      DecoderFakes.observed(
        observed: [
          {"obs1", [{"total", %Spark.Connect.Expression.Literal{literal_type: {:long, 5}}}]}
        ],
        response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
      )

    assert {:ok, result} = ResultDecoder.decode_stream([obs])
    assert result.observed_metrics == %{"obs1" => %{"total" => 5}}
  end

  test "execution metrics frame is consumed without error" do
    frame =
      DecoderFakes.metrics(
        execution: %{{"scan", 1} => %{"numRows" => 10}},
        response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
      )

    assert {:ok, result} = ResultDecoder.decode_stream([frame])
    assert result.execution_metrics[{"scan", 1}] == %{"numRows" => 10}
  end

  test "retryable_error builds a RetryInfo-bearing GRPC error decodable by Errors" do
    {:error, %GRPC.RPCError{} = err} = DecoderFakes.retryable_error(14, "unavailable", 250)

    session = %SparkEx.Session{
      channel: nil,
      session_id: "sess",
      user_id: "u",
      client_type: "test"
    }

    remote = Errors.from_grpc_error(err, session)
    assert remote.retry_delay_ms == 250
  end

  test "controllable/0 lets a producer interleave frames and finalize" do
    {push, finalize, stream} = DecoderFakes.controllable()
    push.(DecoderFakes.arrow_batch(<<>>, row_count: 0))
    push.(DecoderFakes.result_complete())
    finalize.()

    assert {:error, _} = ResultDecoder.decode_stream(stream)
    # Note: empty Arrow IPC bytes hit Explorer's decoder path; what we're
    # really validating is that the stream terminates after `finalize/0`.
  end
end
