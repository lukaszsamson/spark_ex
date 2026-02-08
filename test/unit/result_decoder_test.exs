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

    test "returns error for incomplete chunked arrow batch" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1, 2, 3>>,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }}
      ]

      assert {:error, {:incomplete_arrow_batch, %{expected_chunks: 2, received_chunks: 1}}} =
               ResultDecoder.decode_stream(stream)
    end

    test "returns error for invalid first chunk index" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1, 2, 3>>,
                start_offset: 0,
                chunk_index: 1,
                num_chunks_in_batch: 2
              }}
         }}
      ]

      assert {:error, {:invalid_arrow_batch, message}} = ResultDecoder.decode_stream(stream)
      assert message =~ "Expected chunk index 0"
    end

    test "enriches streamed grpc errors when session is provided" do
      stream = [{:error, %GRPC.RPCError{status: 3, message: "bad request"}}]

      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session",
        user_id: "test",
        client_type: "test"
      }

      assert {:error, %SparkEx.Error.Remote{} = error} = ResultDecoder.decode_stream(stream, session)
      assert error.grpc_status == 3
      assert error.message == "bad request"
    end
  end
end
