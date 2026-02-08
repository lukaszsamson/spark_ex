defmodule SparkEx.Connect.ResultDecoder do
  @moduledoc """
  Decodes ExecutePlan response streams into Elixir data.

  Handles Arrow IPC batch reassembly from chunked gRPC responses
  and converts to list of maps.
  """

  alias Spark.Connect.ExecutePlanResponse
  alias SparkEx.Connect.Errors

  @type decode_result :: %{
          rows: [map()],
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
      num_records: 0
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

            {:execution_progress, _} ->
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
               server_side_session_id: state.server_side_session_id
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

  # --- Arrow batch handling ---

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
end
