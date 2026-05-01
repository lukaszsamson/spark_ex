defmodule SparkEx.Connect.ResultDecoder do
  @moduledoc false

  alias Spark.Connect.ExecutePlanResponse
  alias SparkEx.Connect.Errors
  alias SparkEx.Connect.SessionIntegrity
  alias SparkEx.Connect.TypeMapper

  @type decode_result :: %{
          rows: [map()],
          schema: term() | nil,
          server_side_session_id: String.t() | nil,
          command_result: term() | nil,
          command_results: [term()],
          observed_metrics: map(),
          execution_metrics: map()
        }

  @type unsupported_response_type :: atom()

  @type explorer_result :: %{
          dataframe: Explorer.DataFrame.t(),
          schema: term() | nil,
          server_side_session_id: String.t() | nil,
          observed_metrics: map(),
          execution_metrics: map()
        }

  @type arrow_result :: %{
          arrow: binary() | [binary()],
          arrow_batches: [binary()],
          schema: term() | nil,
          server_side_session_id: String.t() | nil,
          observed_metrics: map(),
          execution_metrics: map()
        }

  @type decode_error ::
          {:missing_dependency, :explorer}
          | {:incomplete_arrow_batch, map()}
          | {:invalid_arrow_batch, String.t()}
          | {:invalid_arrow_batch_row_count,
             %{expected: non_neg_integer(), got: non_neg_integer()}}
          | {:arrow_decode_failed, term()}
          | {:unsupported_response_type, atom()}

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
      command_result: nil,
      command_results: [],
      observed_metrics: %{},
      execution_metrics: %{}
    }

    result =
      Enum.reduce_while(stream, {:ok, state}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {:ok, state} ->
          case check_response_integrity(resp, session, state) do
            {:ok, state} ->
              state = if resp.schema, do: %{state | schema: resp.schema}, else: state

              observed_metrics =
                merge_observed_metrics(state.observed_metrics, resp.observed_metrics)

              execution_metrics =
                merge_execution_metrics(state.execution_metrics, resp.metrics)

              state = %{
                state
                | observed_metrics: observed_metrics,
                  execution_metrics: execution_metrics
              }

              case dispatch_response_type(resp.response_type, state, session) do
                {:cont, state} -> {:cont, {:ok, state}}
                {:error, _} = error -> {:halt, error}
              end

            {:error, _} = error ->
              {:halt, error}
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
               rows: Enum.reverse(state.rows),
               schema: state.schema,
               server_side_session_id: state.server_side_session_id,
               command_result: state.command_result,
               command_results: Enum.reverse(state.command_results),
               observed_metrics: state.observed_metrics,
               execution_metrics: state.execution_metrics
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
  Returns a lazy row stream decoded from ExecutePlan responses.

  Rows are emitted batch-by-batch as Arrow batches arrive. Each emitted
  element is `{:ok, row}` for a successfully decoded row, or
  `{:error, reason}` when the underlying gRPC stream fails, integrity
  validation rejects a response, or batch decoding fails. After an
  `{:error, _}` element is emitted the stream halts; no further elements
  follow.

  When `session` is non-nil, the same session-id / server-session-id
  invariants enforced by `decode_stream/2` are applied here so that a
  drifting `to_local_iterator` consumer sees an integrity error rather
  than silently merging foreign-session rows.
  """
  @spec rows_stream(Enumerable.t(), SparkEx.Session.t() | nil) :: Enumerable.t()
  def rows_stream(stream, session \\ nil) do
    Stream.transform(
      stream,
      fn ->
        %{
          current_chunked_batch: nil,
          num_records: 0,
          server_side_session_id: nil,
          errored: false
        }
      end,
      fn
        _event, %{errored: true} = state ->
          {:halt, state}

        {:ok, %ExecutePlanResponse{} = resp}, state ->
          case check_response_integrity(resp, session, state) do
            {:ok, state} -> handle_rows_stream_response(resp, state)
            {:error, reason} -> emit_rows_stream_error(reason, state)
          end

        {:error, %GRPC.RPCError{} = error}, state ->
          err = if session, do: Errors.from_grpc_error(error, session), else: error
          emit_rows_stream_error(err, state)

        {:error, reason}, state ->
          emit_rows_stream_error(reason, state)
      end,
      fn state ->
        case state do
          %{errored: true} ->
            :ok

          %{current_chunked_batch: nil} ->
            :ok

          _current ->
            # Source enumerable terminated mid-chunked batch without a
            # `result_complete` marker. The truncation has already been
            # surfaced upstream (or the consumer halted early); we no
            # longer raise from the after-fun so that the stream remains
            # safe to enumerate.
            :ok
        end
      end
    )
  end

  defp handle_rows_stream_response(%ExecutePlanResponse{} = resp, state) do
    case resp.response_type do
      {:arrow_batch, %ExecutePlanResponse.ArrowBatch{} = batch} ->
        case handle_arrow_batch_rows_stream(state, batch) do
          {:ok, rows, next_state} ->
            {Enum.map(rows, &{:ok, &1}), next_state}

          {:error, reason} ->
            emit_rows_stream_error(reason, state)
        end

      {:result_complete, _} ->
        if state.current_chunked_batch do
          emit_rows_stream_error(
            incomplete_arrow_batch_error(state.current_chunked_batch),
            state
          )
        else
          {:halt, state}
        end

      _ ->
        {[], state}
    end
  end

  defp emit_rows_stream_error(reason, state) do
    {[{:error, reason}], %{state | errored: true}}
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
      max_bytes: max_bytes,
      observed_metrics: %{},
      execution_metrics: %{}
    }

    result =
      Enum.reduce_while(stream, {:ok, state}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {:ok, state} ->
          case check_response_integrity(resp, session, state) do
            {:ok, state} ->
              state = if resp.schema, do: %{state | schema: resp.schema}, else: state

              observed_metrics =
                merge_observed_metrics(state.observed_metrics, resp.observed_metrics)

              execution_metrics =
                merge_execution_metrics(state.execution_metrics, resp.metrics)

              state = %{
                state
                | observed_metrics: observed_metrics,
                  execution_metrics: execution_metrics
              }

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
                      session_id: session && session.session_id,
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

            {:error, _} = error ->
              {:halt, error}
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

  @doc """
  Decodes an ExecutePlan response stream into an Arrow IPC payload.

  `arrow` preserves the raw Arrow IPC payloads received from the server.
  Single-batch results return a binary. Multi-batch results return the ordered
  list of per-batch binaries, and `arrow_batches` exposes the same list
  explicitly.

  ## Options

  - `:max_rows` — maximum number of rows to collect (default: `:infinity`)
  - `:max_bytes` — maximum total bytes of Arrow data (default: `:infinity`)
  """
  @spec decode_stream_arrow(Enumerable.t(), SparkEx.Session.t() | nil, keyword()) ::
          {:ok, arrow_result()} | {:error, term()}
  def decode_stream_arrow(stream, session \\ nil, opts \\ [])

  def decode_stream_arrow(stream, session, opts) do
    max_rows = Keyword.get(opts, :max_rows, :infinity)
    max_bytes = Keyword.get(opts, :max_bytes, :infinity)

    state = %{
      arrow_parts: [],
      current_chunked_batch: nil,
      schema: nil,
      server_side_session_id: nil,
      num_records: 0,
      total_bytes: 0,
      max_rows: max_rows,
      max_bytes: max_bytes,
      observed_metrics: %{},
      execution_metrics: %{}
    }

    result =
      Enum.reduce_while(stream, {:ok, state}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {:ok, state} ->
          case check_response_integrity(resp, session, state) do
            {:ok, state} ->
              state = maybe_set_schema(state, resp.schema)

              observed_metrics =
                merge_observed_metrics(state.observed_metrics, resp.observed_metrics)

              execution_metrics =
                merge_execution_metrics(state.execution_metrics, resp.metrics)

              state = %{
                state
                | observed_metrics: observed_metrics,
                  execution_metrics: execution_metrics
              }

              case resp.response_type do
                {:arrow_batch, %ExecutePlanResponse.ArrowBatch{} = batch} ->
                  case handle_arrow_batch_arrow(state, batch) do
                    {:ok, state} -> {:cont, {:ok, state}}
                    {:error, _} = error -> {:halt, error}
                  end

                {:result_complete, _} ->
                  case state.current_chunked_batch do
                    nil ->
                      arrow_state =
                        state
                        |> Map.put_new(:arrow_parts, [])
                        |> Map.put(:current_chunked_batch, nil)

                      {:halt, finalize_arrow_result(arrow_state)}

                    current ->
                      {:halt, {:error, incomplete_arrow_batch_error(current)}}
                  end

                _ ->
                  {:cont, {:ok, state}}
              end

            {:error, _} = error ->
              {:halt, error}
          end

        {:error, %GRPC.RPCError{} = error}, {:ok, _state} ->
          err = if session, do: Errors.from_grpc_error(error, session), else: error
          {:halt, {:error, err}}

        {:error, reason}, {:ok, _state} ->
          {:halt, {:error, reason}}
      end)

    case result do
      {:ok, %{arrow: _} = arrow_result} ->
        {:ok, arrow_result}

      {:ok, state} ->
        finalize_state =
          state
          |> Map.put_new(:current_chunked_batch, nil)
          |> Map.put_new(:arrow_parts, [])

        case finalize_state.current_chunked_batch do
          nil ->
            finalize_arrow_result(finalize_state)

          current ->
            {:error, incomplete_arrow_batch_error(current)}
        end

      {:error, _} = error ->
        error
    end
  end

  # --- Response-type dispatch (rows mode) ---
  #
  # Returns `{:cont, state}` to continue, `{:error, reason}` to halt.
  # Recognized command-result variants are accumulated in `state.command_results`
  # (in arrival order) and `state.command_result` is updated to the latest one
  # for back-compat with single-result callers.

  defp dispatch_response_type({:arrow_batch, batch}, state, _session) do
    case handle_arrow_batch(state, batch) do
      {:ok, state} -> {:cont, state}
      {:error, _} = error -> error
    end
  end

  defp dispatch_response_type({:result_complete, _}, state, _session), do: {:cont, state}

  defp dispatch_response_type({:sql_command_result, result}, state, _session) do
    relation = Map.get(result || %{}, :relation)
    {:cont, push_command_result(state, {:sql_command, relation})}
  end

  defp dispatch_response_type({:write_stream_operation_start_result, result}, state, _),
    do: {:cont, push_command_result(state, {:write_stream_start, result})}

  defp dispatch_response_type({:streaming_query_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:streaming_query, result})}

  defp dispatch_response_type({:streaming_query_manager_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:streaming_query_manager, result})}

  defp dispatch_response_type({:streaming_query_listener_events_result, result}, state, _),
    do: {:cont, push_command_result(state, {:listener_events, result})}

  defp dispatch_response_type({:checkpoint_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:checkpoint, result})}

  defp dispatch_response_type({:create_resource_profile_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:create_resource_profile, result})}

  defp dispatch_response_type({:ml_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:ml, result})}

  defp dispatch_response_type({:get_resources_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:get_resources, result})}

  defp dispatch_response_type({:pipeline_command_result, result}, state, _),
    do: {:cont, push_command_result(state, {:pipeline, result})}

  defp dispatch_response_type({:pipeline_event_result, result}, state, _),
    do: {:cont, push_command_result(state, {:pipeline_event, result})}

  defp dispatch_response_type({:pipeline_query_function_execution_signal, result}, state, _),
    do: {:cont, push_command_result(state, {:pipeline_query_function_execution_signal, result})}

  defp dispatch_response_type({:execution_progress, progress}, state, session) do
    emit_progress_telemetry(progress, session)
    {:cont, state}
  end

  defp dispatch_response_type({:metrics, _}, state, _session), do: {:cont, state}
  defp dispatch_response_type(nil, state, _session), do: {:cont, state}

  defp dispatch_response_type({:extension, _ext}, _state, _session) do
    {:error, {:unsupported_response_type, :extension}}
  end

  defp dispatch_response_type({tag, _}, _state, _session) when is_atom(tag) do
    {:error, {:unsupported_response_type, tag}}
  end

  defp push_command_result(state, tagged_tuple) do
    %{
      state
      | command_result: tagged_tuple,
        command_results: [tagged_tuple | state.command_results]
    }
  end

  defp emit_progress_telemetry(progress, session) do
    :telemetry.execute(
      [:spark_ex, :result, :progress],
      %{num_inflight_tasks: progress.num_inflight_tasks || 0},
      %{
        session_id: session && session.session_id,
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
              parts: [batch.data | current.parts]
          }

          if updated.next_chunk_index == updated.expected_chunks do
            assembled = updated.parts |> Enum.reverse() |> IO.iodata_to_binary()
            state = %{state | current_chunked_batch: nil}
            decode_and_append_batch(state, assembled, updated.row_count)
          else
            {:ok, %{state | current_chunked_batch: updated}}
          end
        end
    end
  end

  defp handle_arrow_batch_rows_stream(state, %ExecutePlanResponse.ArrowBatch{} = batch) do
    chunk_index = batch.chunk_index || 0
    num_chunks = batch.num_chunks_in_batch || 0

    case state.current_chunked_batch do
      nil ->
        with :ok <- validate_batch_start_offset(state, batch),
             :ok <- validate_new_batch_chunk_index(chunk_index) do
          if num_chunks > 1 do
            {:ok, [],
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
            decode_batch_for_rows_stream(state, batch.data, batch.row_count)
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
              parts: [batch.data | current.parts]
          }

          if updated.next_chunk_index == updated.expected_chunks do
            assembled = updated.parts |> Enum.reverse() |> IO.iodata_to_binary()
            state = %{state | current_chunked_batch: nil}
            decode_batch_for_rows_stream(state, assembled, updated.row_count)
          else
            {:ok, [], %{state | current_chunked_batch: updated}}
          end
        end
    end
  end

  defp decode_batch_for_rows_stream(state, ipc_data, expected_row_count) do
    with {:ok, rows} <- decode_single_batch(ipc_data),
         :ok <- validate_row_count(rows, expected_row_count) do
      num_rows = length(rows)

      :telemetry.execute(
        [:spark_ex, :result, :batch],
        %{row_count: num_rows, bytes: byte_size(ipc_data)},
        %{batch_index: state.num_records}
      )

      {:ok, rows, %{state | num_records: state.num_records + num_rows}}
    end
  end

  defp handle_arrow_batch_arrow(state, %ExecutePlanResponse.ArrowBatch{} = batch) do
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
            check_arrow_limits_and_append(state, batch.data, batch.row_count)
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
              parts: [batch.data | current.parts]
          }

          if updated.next_chunk_index == updated.expected_chunks do
            assembled = updated.parts |> Enum.reverse() |> IO.iodata_to_binary()
            state = %{state | current_chunked_batch: nil}
            check_arrow_limits_and_append(state, assembled, updated.row_count)
          else
            {:ok, %{state | current_chunked_batch: updated}}
          end
        end
    end
  end

  defp check_arrow_limits_and_append(state, data, row_count) do
    batch_bytes = byte_size(data)
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
      new_num_records = state.num_records + row_count

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
        {:ok,
         %{
           state
           | arrow_parts: [data | state.arrow_parts],
             num_records: new_num_records,
             total_bytes: new_total_bytes
         }}
      end
    end
  end

  defp finalize_arrow_result(state) do
    arrow_batches = Enum.reverse(state.arrow_parts)

    arrow =
      case arrow_batches do
        [] -> <<>>
        [single_batch] -> single_batch
        batches -> batches
      end

    {:ok,
     %{
       arrow: arrow,
       arrow_batches: arrow_batches,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id,
       observed_metrics: state.observed_metrics,
       execution_metrics: state.execution_metrics
     }}
  end

  defp incomplete_arrow_batch_error(current) do
    {:incomplete_arrow_batch,
     %{expected_chunks: current.expected_chunks, received_chunks: length(current.parts)}}
  end

  # Validates the per-response session integrity invariants and updates
  # the running server-side-session-id pin in the decoder state.
  #
  # Returns `{:ok, state}` (with `server_side_session_id` populated when
  # the response carries one) or `{:error, reason}` to halt the stream.
  # When `session` is `nil` (used by tests / the bare 1-arity entrypoint)
  # the cross-session checks are skipped.
  #
  # Note: protobuf string fields default to "" when unset, which is truthy
  # in Elixir. We treat "" as "absent" here so a response with no
  # server_side_session_id can't overwrite the previously-pinned value
  # (and later trigger a false `server_session_changed` error).
  defp check_response_integrity(resp, session, state) do
    response_server_id = nonempty_id(Map.get(resp, :server_side_session_id))

    case do_validate(response_server_id, session, resp, state) do
      :ok ->
        new_state_server_id = response_server_id || state.server_side_session_id
        {:ok, %{state | server_side_session_id: new_state_server_id}}

      {:error, _} = error ->
        error
    end
  end

  defp do_validate(_response_server_id, nil, _resp, _state), do: :ok

  defp do_validate(response_server_id, %SparkEx.Session{} = session, resp, state) do
    with :ok <- SessionIntegrity.validate_session_id(resp, session) do
      baseline = session.server_side_session_id || state.server_side_session_id

      case SessionIntegrity.validate_server_session_id(response_server_id, baseline) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  # Catch-all for callers that pass an unrelated value (e.g. test fakes that
  # mock `:get_state` to return an atom). Treat it as "no session" so the
  # integrity invariants are simply skipped rather than crashing the stream.
  defp do_validate(_response_server_id, _other, _resp, _state), do: :ok

  defp nonempty_id(nil), do: nil
  defp nonempty_id(""), do: nil
  defp nonempty_id(id) when is_binary(id), do: id

  defp maybe_set_schema(state, nil), do: state

  defp maybe_set_schema(state, schema) do
    %{state | schema: schema}
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
         | rows: Enum.reverse(rows, state.rows),
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
    case safe_load_ipc_stream(ipc_data) do
      {:ok, df} ->
        safe_dataframe_to_rows(df)

      {:error, err_stream} ->
        decode_with_fallback(ipc_data, err_stream)
    end
  end

  defp decode_with_fallback(ipc_data, err_stream) do
    case safe_load_ipc(ipc_data) do
      {:ok, df} ->
        safe_dataframe_to_rows(df)

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
              parts: [batch.data | current.parts]
          }

          if updated.next_chunk_index == updated.expected_chunks do
            assembled = updated.parts |> Enum.reverse() |> IO.iodata_to_binary()
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
               | dataframes: [df | state.dataframes],
                 num_records: new_num_records,
                 total_bytes: new_total_bytes
             }}
          end
        end
      end
    end
  end

  defp decode_single_batch_explorer(ipc_data) do
    case safe_load_ipc_stream(ipc_data) do
      {:ok, df} ->
        {:ok, df}

      {:error, err_stream} ->
        case safe_load_ipc(ipc_data) do
          {:ok, df} -> {:ok, df}
          {:error, _} -> {:error, {:arrow_decode_failed, err_stream}}
        end
    end
  end

  defp safe_load_ipc_stream(ipc_data) do
    try do
      Explorer.DataFrame.load_ipc_stream(ipc_data)
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp safe_load_ipc(ipc_data) do
    try do
      Explorer.DataFrame.load_ipc(ipc_data)
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp safe_dataframe_to_rows(df) do
    try do
      {:ok, Explorer.DataFrame.to_rows(df)}
    rescue
      error -> {:error, {:arrow_decode_failed, error}}
    catch
      kind, reason -> {:error, {:arrow_decode_failed, {kind, reason}}}
    end
  end

  defp finalize_explorer_result(%{dataframes: []} = state) do
    empty_df = build_empty_dataframe_from_schema(state.schema)

    {:ok,
     %{
       dataframe: empty_df,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id,
       observed_metrics: state.observed_metrics,
       execution_metrics: state.execution_metrics
     }}
  end

  defp finalize_explorer_result(%{dataframes: [single]} = state) do
    dataframe = apply_schema_policy(single, state.schema)

    {:ok,
     %{
       dataframe: dataframe,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id,
       observed_metrics: state.observed_metrics,
       execution_metrics: state.execution_metrics
     }}
  end

  defp finalize_explorer_result(%{dataframes: dfs} = state) do
    combined =
      dfs
      |> Enum.reverse()
      |> Explorer.DataFrame.concat_rows()

    dataframe = apply_schema_policy(combined, state.schema)

    {:ok,
     %{
       dataframe: dataframe,
       schema: state.schema,
       server_side_session_id: state.server_side_session_id,
       observed_metrics: state.observed_metrics,
       execution_metrics: state.execution_metrics
     }}
  end

  defp exceeds_limit?(_actual, :infinity), do: false
  defp exceeds_limit?(actual, limit), do: actual > limit

  defp build_empty_dataframe_from_schema(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    {:ok, dtypes} = TypeMapper.schema_to_dtypes(struct)
    # Ordered list (not map) so column order survives wide schemas.
    columns = Enum.map(dtypes, fn {name, _dtype} -> {name, []} end)
    Explorer.DataFrame.new(columns, dtypes: dtypes)
  end

  defp build_empty_dataframe_from_schema(_), do: Explorer.DataFrame.new([])

  defp apply_schema_policy(df, %Spark.Connect.DataType{kind: {:struct, struct}}) do
    column_transforms =
      struct.fields
      |> Enum.flat_map(fn %Spark.Connect.DataType.StructField{name: name, data_type: dt} ->
        case column_value_transform(dt) do
          nil -> []
          fun -> [{name, fun}]
        end
      end)
      |> Map.new()

    if map_size(column_transforms) == 0 do
      df
    else
      names = Explorer.DataFrame.names(df)
      row_count = Explorer.DataFrame.n_rows(df)

      # Use an ordered keyword list of `{name, values}` pairs (not a map)
      # so wide schemas (>32 cols) preserve column order through
      # `Explorer.DataFrame.new/2`. Maps would re-bucket by hash and shuffle.
      columns =
        Enum.map(names, fn name ->
          series = Explorer.DataFrame.pull(df, name)
          values = Explorer.Series.to_list(series)

          normalized =
            case Map.fetch(column_transforms, name) do
              {:ok, fun} -> Enum.map(values, fun)
              :error -> values
            end

          {name, normalized}
        end)

      {:ok, mapped_dtypes} = TypeMapper.schema_to_dtypes(struct)

      dtypes =
        mapped_dtypes
        |> Enum.filter(fn {name, _} -> name in names end)

      if columns == [] and row_count == 0 do
        build_empty_dataframe_from_schema(%Spark.Connect.DataType{kind: {:struct, struct}})
      else
        Explorer.DataFrame.new(columns, dtypes: dtypes)
      end
    end
  end

  defp apply_schema_policy(df, _), do: df

  # Per-column post-decode transformation. Returns nil for columns that need no
  # rewriting, or a 1-arity function applied to each value otherwise.
  #
  # - char(n): strip read-side space padding (matches CharVarcharCodegenUtils).
  # - varchar(n): strip trailing spaces (Spark stores varchar as space-padded
  #   when written through fixed-width sinks).
  # - variant / udt / geometry / geography / unparsed: pass the raw decoded
  #   value through unchanged. Earlier code Jason-encoded list/map values,
  #   which lost native structure for callers who can handle it.
  defp column_value_transform(%Spark.Connect.DataType{kind: {:char, _}}),
    do: &strip_trailing_spaces/1

  defp column_value_transform(%Spark.Connect.DataType{kind: {:var_char, _}}),
    do: &strip_trailing_spaces/1

  defp column_value_transform(%Spark.Connect.DataType{
         kind: {:udt, %Spark.Connect.DataType.UDT{} = udt}
       }) do
    case SparkEx.Connect.UDTRegistry.lookup_deserializer(udt) do
      nil -> &raw_passthrough/1
      fun when is_function(fun, 1) -> fun
    end
  end

  defp column_value_transform(%Spark.Connect.DataType{kind: {tag, _}})
       when tag in [
              :calendar_interval,
              :year_month_interval,
              :day_time_interval,
              :variant,
              :geometry,
              :geography,
              :unparsed
            ],
       do: &raw_passthrough/1

  defp column_value_transform(_), do: nil

  defp strip_trailing_spaces(nil), do: nil
  defp strip_trailing_spaces(value) when is_binary(value), do: String.trim_trailing(value, " ")
  defp strip_trailing_spaces(other), do: other

  defp raw_passthrough(value), do: value

  defp merge_observed_metrics(acc, nil), do: acc

  defp merge_observed_metrics(acc, observed_metrics) when is_list(observed_metrics) do
    if Enum.empty?(observed_metrics) do
      acc
    else
      Enum.reduce(observed_metrics, acc, &merge_observed_metric/2)
    end
  end

  defp merge_observed_metric(%ExecutePlanResponse.ObservedMetrics{name: name}, acc)
       when name == "" or is_nil(name),
       do: acc

  defp merge_observed_metric(%ExecutePlanResponse.ObservedMetrics{} = metric, acc) do
    keys = metric.keys || []
    values = metric.values || []

    entry =
      if keys == [] do
        values
        |> Enum.with_index(1)
        |> Map.new(fn {value, index} ->
          {"_#{index}", SparkEx.Observation.decode_literal(value)}
        end)
      else
        keys
        |> Enum.with_index()
        |> Enum.map(fn {key, index} ->
          {key, SparkEx.Observation.decode_literal(Enum.at(values, index))}
        end)
        |> Map.new()
      end

    Map.update(acc, metric.name, entry, fn existing ->
      Map.merge(existing, entry, fn _key, _left, right -> right end)
    end)
  end

  defp merge_execution_metrics(acc, nil), do: acc

  defp merge_execution_metrics(acc, %ExecutePlanResponse.Metrics{metrics: metrics}) do
    if metrics == [] do
      acc
    else
      Enum.reduce(metrics, acc, fn metric, acc ->
        key = metric_key(metric)
        value = execution_metric_value(metric)
        Map.put(acc, key, value)
      end)
    end
  end

  defp metric_key(%ExecutePlanResponse.Metrics.MetricObject{name: name, plan_id: plan_id}) do
    {name, plan_id}
  end

  defp execution_metric_value(%ExecutePlanResponse.Metrics.MetricObject{
         execution_metrics: metrics
       }) do
    Map.new(metrics, fn {k, v} -> {k, v.value} end)
  end
end
