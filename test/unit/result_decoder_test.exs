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

    test "captures checkpoint command result" do
      result =
        %Spark.Connect.CheckpointCommandResult{
          relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-1"}
        }

      stream = [
        {:ok, %ExecutePlanResponse{response_type: {:checkpoint_command_result, result}}}
      ]

      assert {:ok, decoded} = ResultDecoder.decode_stream(stream)
      assert decoded.command_result == {:checkpoint, result}
      assert decoded.command_results == [{:checkpoint, result}]
    end

    test "accumulates multiple command results in arrival order" do
      first =
        %Spark.Connect.CheckpointCommandResult{
          relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-1"}
        }

      second =
        %Spark.Connect.CheckpointCommandResult{
          relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-2"}
        }

      stream = [
        {:ok, %ExecutePlanResponse{response_type: {:checkpoint_command_result, first}}},
        {:ok, %ExecutePlanResponse{response_type: {:checkpoint_command_result, second}}}
      ]

      assert {:ok, decoded} = ResultDecoder.decode_stream(stream)
      assert decoded.command_result == {:checkpoint, second}
      assert decoded.command_results == [{:checkpoint, first}, {:checkpoint, second}]
    end

    test "captures pipeline, ml, get_resources, create_resource_profile command results" do
      pipe = %Spark.Connect.PipelineCommandResult{}
      pipe_event = %Spark.Connect.PipelineEventResult{}
      ml = %Spark.Connect.MlCommandResult{}
      gres = %Spark.Connect.GetResourcesCommandResult{}
      crp = %Spark.Connect.CreateResourceProfileCommandResult{profile_id: 7}

      stream = [
        {:ok, %ExecutePlanResponse{response_type: {:pipeline_command_result, pipe}}},
        {:ok, %ExecutePlanResponse{response_type: {:pipeline_event_result, pipe_event}}},
        {:ok, %ExecutePlanResponse{response_type: {:ml_command_result, ml}}},
        {:ok, %ExecutePlanResponse{response_type: {:get_resources_command_result, gres}}},
        {:ok, %ExecutePlanResponse{response_type: {:create_resource_profile_command_result, crp}}}
      ]

      assert {:ok, decoded} = ResultDecoder.decode_stream(stream)

      assert decoded.command_results == [
               {:pipeline, pipe},
               {:pipeline_event, pipe_event},
               {:ml, ml},
               {:get_resources, gres},
               {:create_resource_profile, crp}
             ]
    end

    test "returns unsupported_response_type error for opaque extension responses" do
      stream = [
        {:ok, %ExecutePlanResponse{response_type: {:extension, %Google.Protobuf.Any{}}}}
      ]

      assert {:error, {:unsupported_response_type, :extension}} =
               ResultDecoder.decode_stream(stream)
    end

    test "captures observed metrics" do
      metrics =
        %ExecutePlanResponse.ObservedMetrics{
          name: "obs1",
          keys: ["total"],
          values: [%Spark.Connect.Expression.Literal{literal_type: {:long, 5}}]
        }

      stream = [
        {:ok,
         %ExecutePlanResponse{
           observed_metrics: [metrics],
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.observed_metrics == %{"obs1" => %{"total" => 5}}
    end

    test "captures positional observed metrics when keys are absent" do
      metrics =
        %ExecutePlanResponse.ObservedMetrics{
          name: "obs1",
          values: [
            %Spark.Connect.Expression.Literal{literal_type: {:long, 5}},
            %Spark.Connect.Expression.Literal{literal_type: {:long, 15}}
          ]
        }

      stream = [
        {:ok,
         %ExecutePlanResponse{
           observed_metrics: [metrics],
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.observed_metrics == %{"obs1" => %{"_1" => 5, "_2" => 15}}
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

    test "returns error for invalid continuation chunk index" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1>>,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<2>>,
                start_offset: 0,
                chunk_index: 2,
                num_chunks_in_batch: 2
              }}
         }}
      ]

      assert {:error, {:invalid_arrow_batch, message}} = ResultDecoder.decode_stream(stream)
      assert message =~ "Expected chunk index 1"
    end

    test "returns error for mismatched continuation num_chunks_in_batch" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1>>,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<2>>,
                start_offset: 0,
                chunk_index: 1,
                num_chunks_in_batch: 3
              }}
         }}
      ]

      assert {:error, {:invalid_arrow_batch, message}} = ResultDecoder.decode_stream(stream)
      assert message =~ "Expected num_chunks_in_batch 2"
    end

    test "returns error for mismatched continuation row_count" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1>>,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 2,
                data: <<2>>,
                start_offset: 0,
                chunk_index: 1,
                num_chunks_in_batch: 2
              }}
         }}
      ]

      assert {:error, {:invalid_arrow_batch, message}} = ResultDecoder.decode_stream(stream)
      assert message =~ "Expected consistent row_count 1"
    end

    test "returns error for mismatched continuation start_offset" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<1>>,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: <<2>>,
                start_offset: 10,
                chunk_index: 1,
                num_chunks_in_batch: 2
              }}
         }}
      ]

      assert {:error, {:invalid_arrow_batch, message}} = ResultDecoder.decode_stream(stream)
      assert message =~ "Expected consistent start_offset"
    end

    test "enriches streamed grpc errors when session is provided" do
      stream = [{:error, %GRPC.RPCError{status: 3, message: "bad request"}}]

      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session",
        user_id: "test",
        client_type: "test"
      }

      assert {:error, %SparkEx.Error.Remote{} = error} =
               ResultDecoder.decode_stream(stream, session)

      assert error.grpc_status == 3
      assert error.message == "bad request"
    end

    test "captures execution metrics" do
      metrics =
        %ExecutePlanResponse.Metrics{
          metrics: [
            %ExecutePlanResponse.Metrics.MetricObject{
              name: "scan",
              plan_id: 1,
              execution_metrics: %{
                "numRows" => %ExecutePlanResponse.Metrics.MetricValue{value: 10}
              }
            }
          ]
        }

      stream = [
        {:ok,
         %ExecutePlanResponse{
           metrics: metrics,
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream(stream)
      assert result.execution_metrics[{"scan", 1}] == %{"numRows" => 10}
    end

    @tag skip:
           unless(
             File.exists?(
               Path.expand("../../checklist/payloads/arrow_duplicate_columns.ipc", __DIR__)
             ),
             do: "missing payload fixture"
           )
    test "returns arrow_decode_failed when Explorer to_rows panics on duplicate-field payload" do
      payload_path =
        Path.expand("../../checklist/payloads/arrow_duplicate_columns.ipc", __DIR__)

      payload = File.read!(payload_path)

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: payload,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 1
              }}
         }}
      ]

      assert {:error, {:arrow_decode_failed, _reason}} = ResultDecoder.decode_stream(stream)
    end
  end

  describe "decode_stream_arrow/2" do
    test "returns arrow bytes" do
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
                num_chunks_in_batch: 1
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream_arrow(stream)
      assert result.arrow == <<1, 2, 3>>
      assert result.arrow_batches == [<<1, 2, 3>>]
      assert result.observed_metrics == %{}
      assert result.execution_metrics == %{}
    end

    test "preserves multiple arrow batches in order" do
      first_batch = build_ipc_data([1])
      second_batch = build_ipc_data([2])
      assert first_batch != <<>>
      assert second_batch != <<>>

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: first_batch,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 1
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: second_batch,
                start_offset: 1,
                chunk_index: 0,
                num_chunks_in_batch: 1
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, result} = ResultDecoder.decode_stream_arrow(stream)
      assert result.arrow == [first_batch, second_batch]
      assert result.arrow_batches == [first_batch, second_batch]
    end

    test "returns an error when result_complete arrives mid-chunked batch" do
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
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:error, {:incomplete_arrow_batch, %{expected_chunks: 2, received_chunks: 1}}} =
               ResultDecoder.decode_stream_arrow(stream)
    end
  end

  describe "rows_stream/2" do
    test "decodes rows lazily from execute plan stream" do
      ipc_data = build_multi_row_ipc_data(3)
      assert ipc_data != <<>>

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{data: ipc_data, row_count: 3, start_offset: 0}}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      row_stream = ResultDecoder.rows_stream(stream)
      assert Enum.take(row_stream, 2) == [{:ok, %{"id" => 1}}, {:ok, %{"id" => 2}}]
    end

    test "reassembles chunked arrow batches while streaming rows" do
      ipc_data = build_multi_row_ipc_data(2)
      assert ipc_data != <<>>
      split = div(byte_size(ipc_data), 2)
      {first, second} = :erlang.split_binary(ipc_data, split)

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                data: first,
                row_count: 2,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                data: second,
                row_count: 2,
                start_offset: 0,
                chunk_index: 1,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert Enum.to_list(ResultDecoder.rows_stream(stream)) == [
               {:ok, %{"id" => 1}},
               {:ok, %{"id" => 2}}
             ]
    end

    test "emits an error element instead of raising when result_complete arrives mid-chunked batch" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                data: <<1, 2, 3>>,
                row_count: 1,
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 2
              }}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert [{:error, {:incomplete_arrow_batch, %{expected_chunks: 2, received_chunks: 1}}}] =
               Enum.to_list(ResultDecoder.rows_stream(stream))
    end
  end

  describe "column_value_transform/1" do
    test "char(n) strips trailing spaces" do
      dt = %Spark.Connect.DataType{
        kind: {:char, %Spark.Connect.DataType.Char{length: 3}}
      }

      fun = ResultDecoder.column_value_transform(dt)
      assert is_function(fun, 1)
      assert fun.("c  ") == "c"
    end

    test "varchar(n) preserves trailing spaces" do
      dt = %Spark.Connect.DataType{
        kind: {:var_char, %Spark.Connect.DataType.VarChar{length: 3}}
      }

      assert ResultDecoder.column_value_transform(dt) == nil
    end

    test "string returns nil (no transform)" do
      dt = %Spark.Connect.DataType{kind: {:string, %Spark.Connect.DataType.String{}}}
      assert ResultDecoder.column_value_transform(dt) == nil
    end
  end

  defp build_multi_row_ipc_data(n) do
    build_ipc_data(Enum.to_list(1..n))
  end

  defp build_ipc_data(ids) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      df = Explorer.DataFrame.new(%{"id" => ids})

      case Explorer.DataFrame.dump_ipc_stream(df) do
        {:ok, data} -> data
        _ -> <<>>
      end
    else
      <<>>
    end
  end
end
