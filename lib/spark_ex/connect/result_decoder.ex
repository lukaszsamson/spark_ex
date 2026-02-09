defmodule SparkEx.Connect.ResultDecoder do
  @moduledoc """
  Decodes ExecutePlan response streams into Elixir data.

  Handles Arrow IPC batch reassembly from chunked gRPC responses
  and converts to list of maps.
  """

  @compile {:no_warn_undefined, Explorer.DataFrame}

  alias Spark.Connect.ExecutePlanResponse
  alias SparkEx.Connect.Errors
  alias SparkEx.Connect.TypeMapper

  @type decode_result :: %{
          rows: [map()],
          schema: term() | nil,
          server_side_session_id: String.t() | nil
        }

  @type explorer_result :: %{
          dataframe: Explorer.DataFrame.t(),
          schema: term() | nil,
          server_side_session_id: String.t() | nil
        }

  @type decode_error ::
          {:missing_dependency, :explorer}
          | {:incomplete_arrow_batch, map()}
          | {:invalid_arrow_batch, String.t()}
          | {:invalid_arrow_batch_row_count,
             %{expected: non_neg_integer(), got: non_neg_integer()}}
          | {:arrow_decode_failed, term()}

  @doc """
  Consumes an ExecutePlan response stream and returns decoded rows.

  The stream is a `GRPC.Client.Stream` that yields `ExecutePlanResponse` messages.
  """
  @spec decode_stream(Enumerable.t()) :: {:ok, decode_result()} | {:error, term()}
  def decode_stream(stream) do
    decode_stream(stream, nil)
  end

  @doc """
  Same as `decode_stream/1`, but enriches streamed gRPC errors when a session is provided.
  """
  @spec decode_stream(Enumerable.t(), SparkEx.Session.t() | nil) ::
          {:ok, decode_result()} | {:error, term()}
  def decode_stream(stream, session) do
    state = %{
      rows: [],
      current_chunked_batch: nil,
      schema: nil,
      server_side_session_id: nil,
      num_records: 0,
      command_result: nil
    }

    result =
      Enum.reduce_while(stream, {:ok, state}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {:ok, state} ->
          state = %{
            state
            | server_side_session_id: resp.server_side_session_id || state.server_side_session_id
          }

          state = if resp.schema, do: %{state | schema: resp.schema}, else: state

          case resp.response_type do
            {:arrow_batch, batch} ->
              case handle_arrow_batch(state, batch) do
                {:ok, state} -> {:cont, {:ok, state}}
                {:error, _} = error -> {:halt, error}
              end

            {:result_complete, _} ->
              {:cont, {:ok, state}}

            {:sql_command_result, _} ->
              {:cont, {:ok, state}}

            {:write_stream_operation_start_result, result} ->
              {:cont, {:ok, %{state | command_result: {:write_stream_start, result}}}}

            {:streaming_query_command_result, result} ->
              {:cont, {:ok, %{state | command_result: {:streaming_query, result}}}}

            {:streaming_query_manager_command_result, result} ->
              {:cont, {:ok, %{state | command_result: {:streaming_query_manager, result}}}}

            {:streaming_query_listener_events_result, result} ->
              {:cont, {:ok, %{state | command_result: {:listener_events, result}}}}

            {:execution_progress, progress} ->
              :telemetry.execute(
                [:spark_ex, :result, :progress],
                %{num_inflight_tasks: progress.num_inflight_tasks || 0},
                %{
                  stages:
                    Enum.map(progress.stages || [], fn stage ->
                      %{
                        stage_id: stage.stage_id,
                        num_tasks: stage.num_tasks,
                        num_completed_tasks: stage.num_completed_tasks,
                        input_bytes_read: stage.input_bytes_read,
                        done: stage.done
                      }
                    end)
                }
              )

              {:cont, {:ok, state}}

            {:metrics, _} ->
              {:cont, {:ok, state}}

            nil ->
              {:cont, {:ok, state}}

            _other ->
              {:cont, {:ok, state}}
          end

        {:error, %GRPC.RPCError{} = error}, {:ok, _state} ->
          err =
            if session do
              Errors.from_grpc_error(error, session)
            else
              error
            end

          {:halt, {:error, err}}

        {:error, reason}, {:ok, _state} ->
          {:halt, {:error, reason}}
      end)

    case result do
      {:ok, state} ->
        case state.current_chunked_batch do
          nil ->
            {:ok,
             %{
               rows: state.rows,
               schema: state.schema,
               server_side_session_id: state.server_side_session_id,
               command_result: state.command_result
             }}

          current ->
            {:error,
             {:incomplete_arrow_batch,
              %{
                expected_chunks: current.expected_chunks,
                received_chunks: length(current.parts)
              }}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Consumes an ExecutePlan response stream and returns an `Explorer.DataFrame`.

  Decodes each Arrow IPC batch to an Explorer DataFrame and concatenates them.
  Enforces row and byte limits to prevent OOM.

  ## Options

  - `:max_rows` — maximum number of rows to collect (default: 10_000)
  - `:max_bytes` — maximum total bytes of Arrow data (default: 67_108_864 / 64 MB)
  """
  @spec decode_stream_explorer(Enumerable.t(), SparkEx.Session.t() | nil, keyword()) ::
          {:ok, explorer_result()} | {:error, term()}
  def decode_stream_explorer(stream, session, opts \\ []) do
    max_rows = Keyword.get(opts, :max_rows, 10_000)
    max_bytes = Keyword.get(opts, :max_bytes, 67_108_864)

    state = %{
      dataframes: [],
      current_chunked_batch: nil,
      schema: nil,
      server_side_session_id: nil,
      num_records: 0,
      total_bytes: 0,
      max_rows: max_rows,
      max_bytes: max_bytes
    }

    result =
      Enum.reduce_while(stream, {:ok, state}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {:ok, state} ->
          state = %{
            state
            | server_side_session_id: resp.server_side_session_id || state.server_side_session_id
          }

          state = if resp.schema, do: %{state | schema: resp.schema}, else: state

          case resp.response_type do
            {:arrow_batch, batch} ->
              case handle_arrow_batch_explorer(state, batch) do
                {:ok, state} -> {:cont, {:ok, state}}
                {:error, _} = error -> {:halt, error}
              end

            {:execution_progress, progress} ->
              :telemetry.execute(
                [:spark_ex, :result, :progress],
                %{num_inflight_tasks: progress.num_inflight_tasks || 0},
                %{
                  stages:
                    Enum.map(progress.stages || [], fn stage ->
                      %{
                        stage_id: stage.stage_id,
                        num_tasks: stage.num_tasks,
                        num_completed_tasks: stage.num_completed_tasks,
                        input_bytes_read: stage.input_bytes_read,
                        done: stage.done
                      }
                    end)
                }
              )

              {:cont, {:ok, state}}

            _ ->
              {:cont, {:ok, state}}
          end

        {:error, %GRPC.RPCError{} = error}, {:ok, _state} ->
          err =
            if session do
              Errors.from_grpc_error(error, session)
            else
              error
            end

          {:halt, {:error, err}}

        {:error, reason}, {:ok, _state} ->
          {:halt, {:error, reason}}
      end)

    case result do
      {:ok, state} ->
        case state.current_chunked_batch do
          nil ->
            finalize_explorer_result(state)

          current ->
            {:error,
             {:incomplete_arrow_batch,
              %{
                expected_chunks: current.expected_chunks,
                received_chunks: length(current.parts)
              }}}
        end

      {:error, _} = error ->
        error
    end
  end

  # --- Arrow batch handling (rows mode) ---

  defp handle_arrow_batch(state, %ExecutePlanResponse.ArrowBatch{} = batch) do
    chunk_index = batch.chunk_index || 0
    num_chunks = batch.num_chunks_in_batch || 0

    case state.current_chunked_batch do
      nil ->
        with :ok <- validate_batch_start_offset(state, batch),
             :ok <- validate_new_batch_chunk_index(chunk_index) do
          if num_chunks > 1 do
            {:ok,
             %{
               state
               | current_chunked_batch: %{
                   expected_chunks: num_chunks,
                   next_chunk_index: 1,
                   row_count: batch.row_count,
                   start_offset: batch.start_offset,
                   parts: [batch.data]
                 }
             }}
          else
            decode_and_append_batch(state, batch.data, batch.row_count)
          end
        end

      current ->
        with :ok <- validate_continuation_chunk_index(current, chunk_index),
             :ok <- validate_continuation_num_chunks(current, num_chunks),
             :ok <- validate_continuation_row_count(current, batch.row_count),
             :ok <- validate_continuation_start_offset(current, batch.start_offset) do
          updated = %{
            current
            | next_chunk_index: current.next_chunk_index + 1,
              parts: current.parts ++ [batch.data]
          }

          if length(updated.parts) == updated.expected_chunks do
            assembled = IO.iodata_to_binary(updated.parts)
            state = %{state | current_chunked_batch: nil}
            decode_and_append_batch(state, assembled, updated.row_count)
          else
            {:ok, %{state | current_chunked_batch: updated}}
          end
        end
    end
  end

  defp validate_batch_start_offset(_state, %ExecutePlanResponse.ArrowBatch{start_offset: nil}),
    do: :ok

  defp validate_batch_start_offset(state, %ExecutePlanResponse.ArrowBatch{start_offset: offset})
       when is_integer(offset) do
    if offset == state.num_records do
      :ok
    else
      {:error,
       {:invalid_arrow_batch,
        "Expected arrow batch to start at row offset #{state.num_records}, got #{offset}"}}
    end
  end

  defp validate_new_batch_chunk_index(0), do: :ok

  defp validate_new_batch_chunk_index(idx) do
    {:error, {:invalid_arrow_batch, "Expected chunk index 0, got #{idx}"}}
  end

  defp validate_continuation_chunk_index(current, chunk_index) do
    if chunk_index == current.next_chunk_index do
      :ok
    else
      {:error,
       {:invalid_arrow_batch,
        "Expected chunk index #{current.next_chunk_index}, got #{chunk_index}"}}
    end
  end

  defp validate_continuation_num_chunks(_current, 0), do: :ok

  defp validate_continuation_num_chunks(current, num_chunks) do
    if num_chunks == current.expected_chunks do
      :ok
    else
      {:error,
       {:invalid_arrow_batch,
        "Expected num_chunks_in_batch #{current.expected_chunks}, got #{num_chunks}"}}
    end
  end

  defp validate_continuation_row_count(current, row_count) do
    if row_count == current.row_count do
      :ok
    else
      {:error,
       {:invalid_arrow_batch,
        "Expected consistent row_count #{current.row_count}, got #{row_count}"}}
    end
  end

  defp validate_continuation_start_offset(_current, nil), do: :ok

  defp validate_continuation_start_offset(current, start_offset) do
    if start_offset == current.start_offset do
      :ok
    else
      {:error,
       {:invalid_arrow_batch,
        "Expected consistent start_offset #{inspect(current.start_offset)}, got #{inspect(start_offset)}"}}
    end
  end

  defp decode_and_append_batch(state, ipc_data, expected_row_count) do
    with {:ok, rows} <- decode_single_batch(ipc_data),
         :ok <- validate_row_count(rows, expected_row_count) do
      num_rows = length(rows)

      :telemetry.execute(
        [:spark_ex, :result, :batch],
        %{row_count: num_rows, bytes: byte_size(ipc_data)},
        %{batch_index: state.num_records}
      )

      {:ok,
       %{
         state
         | rows: state.rows ++ rows,
           num_records: state.num_records + num_rows
       }}
    end
  end

  defp validate_row_count(rows, expected_row_count) do
    actual = length(rows)

    if actual == expected_row_count do
      :ok
    else
      {:error, {:invalid_arrow_batch_row_count, %{expected: expected_row_count, got: actual}}}
    end
  end

  # --- Arrow IPC decode ---
  # Each Arrow batch from Spark is a standalone IPC stream (schema + record batch).
  # Decode each batch into rows.

  defp decode_single_batch(ipc_data) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      case Explorer.DataFrame.load_ipc_stream(ipc_data) do
        {:ok, df} ->
          {:ok, Explorer.DataFrame.to_rows(df)}

        {:error, err_stream} ->
          decode_with_fallback(ipc_data, err_stream)
      end
    else
      {:error, {:missing_dependency, :explorer}}
    end
  end

  defp decode_with_fallback(ipc_data, err_stream) do
    case Explorer.DataFrame.load_ipc(ipc_data) do
      {:ok, df} ->
        {:ok, Explorer.DataFrame.to_rows(df)}

      {:error, _} ->
        {:error, {:arrow_decode_failed, err_stream}}
    end
  end

  # --- Arrow batch handling (Explorer mode) ---

  defp handle_arrow_batch_explorer(state, %ExecutePlanResponse.ArrowBatch{} = batch) do
    chunk_index = batch.chunk_index || 0
    num_chunks = batch.num_chunks_in_batch || 0

    case state.current_chunked_batch do
      nil ->
        with :ok <- validate_batch_start_offset(state, batch),
             :ok <- validate_new_batch_chunk_index(chunk_index) do
          if num_chunks > 1 do
            {:ok,
             %{
               state
               | current_chunked_batch: %{
                   expected_chunks: num_chunks,
                   next_chunk_index: 1,
                   row_count: batch.row_count,
                   start_offset: batch.start_offset,
                   parts: [batch.data]
                 }
             }}
          else
            decode_and_append_batch_explorer(state, batch.data, batch.row_count)
          end
        end

      current ->
        with :ok <- validate_continuation_chunk_index(current, chunk_index),
             :ok <- validate_continuation_num_chunks(current, num_chunks),
             :ok <- validate_continuation_row_count(current, batch.row_count),
             :ok <- validate_continuation_start_offset(current, batch.start_offset) do
          updated = %{
            current
            | next_chunk_index: current.next_chunk_index + 1,
              parts: current.parts ++ [batch.data]
          }

          if length(updated.parts) == updated.expected_chunks do
            assembled = IO.iodata_to_binary(updated.parts)
            state = %{state | current_chunked_batch: nil}
            decode_and_append_batch_explorer(state, assembled, updated.row_count)
          else
            {:ok, %{state | current_chunked_batch: updated}}
          end
        end
    end
  end

  defp decode_and_append_batch_explorer(state, ipc_data, expected_row_count) do
    batch_bytes = byte_size(ipc_data)
    new_total_bytes = state.total_bytes + batch_bytes

    if exceeds_limit?(new_total_bytes, state.max_bytes) do
      {:error,
       %SparkEx.Error.LimitExceeded{
         limit_type: :bytes,
         limit_value: state.max_bytes,
         actual_value: new_total_bytes,
         remediation:
           "Use DataFrame.limit/2 to reduce result size, or pass max_bytes: <value> to increase the limit"
       }}
    else
      with {:ok, df} <- decode_single_batch_explorer(ipc_data) do
        num_rows = Explorer.DataFrame.n_rows(df)

        if num_rows != expected_row_count do
          {:error,
           {:invalid_arrow_batch_row_count, %{expected: expected_row_count, got: num_rows}}}
        else
          new_num_records = state.num_records + num_rows

          if exceeds_limit?(new_num_records, state.max_rows) do
            {:error,
             %SparkEx.Error.LimitExceeded{
               limit_type: :rows,
               limit_value: state.max_rows,
               actual_value: new_num_records,
               remediation:
                 "Use DataFrame.limit/2 to reduce result size, or pass max_rows: <value> to increase the limit"
             }}
          else
            :telemetry.execute(
              [:spark_ex, :result, :batch],
              %{row_count: num_rows, bytes: batch_bytes},
              %{batch_index: state.num_records, mode: :explorer}
            )

            {:ok,
             %{
               state
               | dataframes: state.dataframes ++ [df],
                 num_records: new_num_records,
                 total_bytes: new_total_bytes
             }}
          end
        end
      end
    end
  end

  defp decode_single_batch_explorer(ipc_data) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      case Explorer.DataFrame.load_ipc_stream(ipc_data) do
        {:ok, df} ->
          {:ok, df}

        {:error, err_stream} ->
          case Explorer.DataFrame.load_ipc(ipc_data) do
            {:ok, df} -> {:ok, df}
            {:error, _} -> {:error, {:arrow_decode_failed, err_stream}}
          end
      end
    else
      {:error, {:missing_dependency, :explorer}}
    end
  end

  defp finalize_explorer_result(%{dataframes: []} = state) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      empty_df = build_empty_dataframe_from_schema(state.schema)

      {:ok,
       %{
         dataframe: empty_df,
         schema: state.schema,
         server_side_session_id: state.server_side_session_id
       }}
    else
      {:error, {:missing_dependency, :explorer}}
    end
  end

  defp finalize_explorer_result(%{dataframes: [single]} = state) do
    dataframe = apply_schema_policy(single, state.schema)

    {:ok,
     %{
       dataframe: dataframe,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id
     }}
  end

  defp finalize_explorer_result(%{dataframes: dfs} = state) do
    combined = Explorer.DataFrame.concat_rows(dfs)
    dataframe = apply_schema_policy(combined, state.schema)

    {:ok,
     %{
       dataframe: dataframe,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id
     }}
  end

  defp exceeds_limit?(_actual, :infinity), do: false
  defp exceeds_limit?(actual, limit), do: actual > limit

  defp build_empty_dataframe_from_schema(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    {:ok, dtypes} = TypeMapper.schema_to_dtypes(struct)

    columns =
      dtypes
      |> Enum.map(fn {name, _dtype} -> {name, []} end)
      |> Map.new()

    Explorer.DataFrame.new(columns, dtypes: dtypes)
  end

  defp build_empty_dataframe_from_schema(_), do: Explorer.DataFrame.new([])

  defp apply_schema_policy(df, %Spark.Connect.DataType{kind: {:struct, struct}}) do
    fallback_columns =
      struct.fields
      |> Enum.filter(fn field ->
        case field.data_type do
          %Spark.Connect.DataType{kind: {tag, _}} -> fallback_string_type?(tag)
          _ -> false
        end
      end)
      |> Enum.map(& &1.name)
      |> MapSet.new()

    if MapSet.size(fallback_columns) == 0 do
      df
    else
      names = Explorer.DataFrame.names(df)
      row_count = Explorer.DataFrame.n_rows(df)

      columns =
        names
        |> Enum.map(fn name ->
          series = Explorer.DataFrame.pull(df, name)
          values = Explorer.Series.to_list(series)

          normalized =
            if MapSet.member?(fallback_columns, name) do
              Enum.map(values, &json_string_fallback/1)
            else
              values
            end

          {name, normalized}
        end)
        |> Map.new()

      {:ok, mapped_dtypes} = TypeMapper.schema_to_dtypes(struct)

      dtypes =
        mapped_dtypes
        |> Enum.filter(fn {name, _} -> name in names end)

      if map_size(columns) == 0 and row_count == 0 do
        build_empty_dataframe_from_schema(%Spark.Connect.DataType{kind: {:struct, struct}})
      else
        Explorer.DataFrame.new(columns, dtypes: dtypes)
      end
    end
  end

  defp apply_schema_policy(df, _), do: df

  defp fallback_string_type?(tag) do
    tag in [
      :decimal,
      :calendar_interval,
      :year_month_interval,
      :day_time_interval,
      :array,
      :struct,
      :map,
      :variant,
      :udt,
      :geometry,
      :geography,
      :unparsed
    ]
  end

  defp json_string_fallback(nil), do: nil
  defp json_string_fallback(value) when is_binary(value), do: value

  defp json_string_fallback(value) when is_list(value) or is_map(value) or is_tuple(value) do
    value
    |> normalize_json_value()
    |> Jason.encode!()
  end

  defp json_string_fallback(value), do: to_string(value)

  defp normalize_json_value(value) when is_list(value) do
    Enum.map(value, &normalize_json_value/1)
  end

  defp normalize_json_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&normalize_json_value/1)
  end

  defp normalize_json_value(value) when is_map(value) do
    value =
      if is_struct(value) do
        Map.from_struct(value)
      else
        value
      end

    Map.new(value, fn {k, v} -> {to_string(k), normalize_json_value(v)} end)
  end

  defp normalize_json_value(value), do: value
end
