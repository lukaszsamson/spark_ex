defmodule SparkEx.DataFrame.ToExplorerTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame

  describe "to_explorer/2 plan construction" do
    test "to_explorer is listed as an action" do
      # Ensure to_explorer/2 exists on the module
      assert function_exported?(DataFrame, :to_explorer, 2)
      assert function_exported?(DataFrame, :to_explorer, 1)
    end
  end

  describe "result decoder explorer mode" do
    alias SparkEx.Connect.ResultDecoder
    alias Spark.Connect.DataType
    alias Spark.Connect.ExecutePlanResponse

    test "decode_stream_explorer returns Explorer.DataFrame" do
      ipc_data = build_test_ipc_data()

      if ipc_data != <<>> do
        batch = %ExecutePlanResponse.ArrowBatch{
          data: ipc_data,
          row_count: 1,
          start_offset: 0
        }

        stream = [
          {:ok,
           %ExecutePlanResponse{
             response_type: {:arrow_batch, batch},
             server_side_session_id: "test"
           }},
          {:ok,
           %ExecutePlanResponse{
             response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
           }}
        ]

        {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
        assert %Explorer.DataFrame{} = result.dataframe
        assert Explorer.DataFrame.n_rows(result.dataframe) == 1
      end
    end

    test "decode_stream_explorer enforces max_rows limit" do
      ipc_data = build_multi_row_ipc_data(5)

      if ipc_data != <<>> do
        batch = %ExecutePlanResponse.ArrowBatch{
          data: ipc_data,
          row_count: 5,
          start_offset: 0
        }

        stream = [
          {:ok,
           %ExecutePlanResponse{
             response_type: {:arrow_batch, batch},
             server_side_session_id: "test"
           }},
          {:ok,
           %ExecutePlanResponse{
             response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
           }}
        ]

        result = ResultDecoder.decode_stream_explorer(stream, nil, max_rows: 3)
        assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :rows}} = result
      end
    end

    test "decode_stream_explorer enforces max_bytes limit" do
      ipc_data = build_test_ipc_data()

      if ipc_data != <<>> do
        batch = %ExecutePlanResponse.ArrowBatch{
          data: ipc_data,
          row_count: 1,
          start_offset: 0
        }

        stream = [
          {:ok,
           %ExecutePlanResponse{
             response_type: {:arrow_batch, batch},
             server_side_session_id: "test"
           }},
          {:ok,
           %ExecutePlanResponse{
             response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
           }}
        ]

        # Set max_bytes very low to trigger limit
        result = ResultDecoder.decode_stream_explorer(stream, nil, max_bytes: 1)
        assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :bytes}} = result
      end
    end

    test "decode_stream_explorer returns empty dataframe for no batches" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}},
           server_side_session_id: "test"
         }}
      ]

      {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert %Explorer.DataFrame{} = result.dataframe
    end

    test "decode_stream_explorer preserves schema for empty result set" do
      schema =
        %DataType{
          kind:
            {:struct,
             %DataType.Struct{
               fields: [
                 %DataType.StructField{
                   name: "id",
                   data_type: %DataType{kind: {:long, %DataType.Long{}}},
                   nullable: false
                 },
                 %DataType.StructField{
                   name: "name",
                   data_type: %DataType{kind: {:string, %DataType.String{}}},
                   nullable: true
                 }
               ]
             }}
        }

      stream = [
        {:ok, %ExecutePlanResponse{schema: schema}},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert Explorer.DataFrame.n_rows(result.dataframe) == 0
      assert Explorer.DataFrame.names(result.dataframe) == ["id", "name"]
    end

    test "decode_stream_explorer concatenates multiple batches" do
      ipc_data1 = build_test_ipc_data()
      ipc_data2 = build_test_ipc_data()

      if ipc_data1 != <<>> do
        stream = [
          {:ok,
           %ExecutePlanResponse{
             response_type:
               {:arrow_batch,
                %ExecutePlanResponse.ArrowBatch{
                  data: ipc_data1,
                  row_count: 1,
                  start_offset: 0
                }},
             server_side_session_id: "test"
           }},
          {:ok,
           %ExecutePlanResponse{
             response_type:
               {:arrow_batch,
                %ExecutePlanResponse.ArrowBatch{
                  data: ipc_data2,
                  row_count: 1,
                  start_offset: 1
                }},
             server_side_session_id: "test"
           }},
          {:ok,
           %ExecutePlanResponse{
             response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
           }}
        ]

        {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
        assert Explorer.DataFrame.n_rows(result.dataframe) == 2
      end
    end

    test "returns error for incomplete chunked arrow batch in explorer mode" do
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
               ResultDecoder.decode_stream_explorer(stream, nil)
    end

    test "returns error for invalid first chunk index in explorer mode" do
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

      assert {:error, {:invalid_arrow_batch, message}} =
               ResultDecoder.decode_stream_explorer(stream, nil)

      assert message =~ "Expected chunk index 0"
    end

    test "returns error for invalid continuation chunk index in explorer mode" do
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

      assert {:error, {:invalid_arrow_batch, message}} =
               ResultDecoder.decode_stream_explorer(stream, nil)

      assert message =~ "Expected chunk index 1"
    end

    test "returns error for mismatched continuation num_chunks in explorer mode" do
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

      assert {:error, {:invalid_arrow_batch, message}} =
               ResultDecoder.decode_stream_explorer(stream, nil)

      assert message =~ "Expected num_chunks_in_batch 2"
    end

    test "returns error for mismatched continuation row_count in explorer mode" do
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

      assert {:error, {:invalid_arrow_batch, message}} =
               ResultDecoder.decode_stream_explorer(stream, nil)

      assert message =~ "Expected consistent row_count 1"
    end

    test "returns error for mismatched continuation start_offset in explorer mode" do
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

      assert {:error, {:invalid_arrow_batch, message}} =
               ResultDecoder.decode_stream_explorer(stream, nil)

      assert message =~ "Expected consistent start_offset"
    end
  end

  defp build_test_ipc_data do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      df = Explorer.DataFrame.new(%{"a" => [1]})

      case Explorer.DataFrame.dump_ipc_stream(df) do
        {:ok, data} -> data
        _ -> <<>>
      end
    else
      <<>>
    end
  end

  defp build_multi_row_ipc_data(n) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      df = Explorer.DataFrame.new(%{"a" => Enum.to_list(1..n)})

      case Explorer.DataFrame.dump_ipc_stream(df) do
        {:ok, data} -> data
        _ -> <<>>
      end
    else
      <<>>
    end
  end
end
