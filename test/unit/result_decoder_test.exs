defmodule SparkEx.Connect.ResultDecoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.ResultDecoder
  alias Spark.Connect.ExecutePlanResponse

  describe "decode_stream/1" do
    test "returns empty rows for empty stream" do
      stream = []
      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.rows == []
      assert result.schema == nil
      assert result.server_side_session_id == nil
    end

    test "tracks server_side_session_id from responses" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           server_side_session_id: "ssid-123",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.server_side_session_id == "ssid-123"
    end

    test "handles gRPC errors in stream" do
      stream = [
        {:error, %GRPC.RPCError{status: 13, message: "internal error"}}
      ]

      assert {:error, %GRPC.RPCError{status: 13}} = ResultDecoder.decode_stream(stream)
    end

    test "handles generic errors in stream" do
      stream = [{:error, :timeout}]
      assert {:error, :timeout} = ResultDecoder.decode_stream(stream)
    end

    test "ignores sql_command_result, execution_progress, metrics, nil response types" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type: {:sql_command_result, %ExecutePlanResponse.SqlCommandResult{}}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:execution_progress, %ExecutePlanResponse.ExecutionProgress{}}
         }},
        {:ok, %ExecutePlanResponse{response_type: {:metrics, %ExecutePlanResponse.Metrics{}}}},
        {:ok, %ExecutePlanResponse{response_type: nil}}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.rows == []
    end
  end
end
