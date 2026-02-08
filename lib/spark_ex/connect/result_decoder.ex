defmodule SparkEx.Connect.ResultDecoder do
  @moduledoc """
  Decodes ExecutePlan response streams into Elixir data.

  Handles Arrow IPC batch reassembly from chunked gRPC responses
  and converts to list of maps.
  """

  alias Spark.Connect.ExecutePlanResponse

  @type decode_result :: %{
          rows: [map()],
          schema: term() | nil,
          server_side_session_id: String.t() | nil
        }

  @doc """
  Consumes an ExecutePlan response stream and returns decoded rows.

  The stream is a `GRPC.Client.Stream` that yields `ExecutePlanResponse` messages.
  """
  @spec decode_stream(Enumerable.t()) :: {:ok, decode_result()} | {:error, term()}
  def decode_stream(stream) do
    state = %{
      batches: [],
      chunks: %{},
      schema: nil,
      server_side_session_id: nil
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
              {:cont, {:ok, handle_arrow_batch(state, batch)}}

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
          {:halt, {:error, error}}

        {:error, reason}, {:ok, _state} ->
          {:halt, {:error, reason}}
      end)

    case result do
      {:ok, state} ->
        rows = decode_batches(state.batches)

        {:ok,
         %{
           rows: rows,
           schema: state.schema,
           server_side_session_id: state.server_side_session_id
         }}

      {:error, _} = error ->
        error
    end
  end

  # --- Arrow batch handling ---

  defp handle_arrow_batch(state, %ExecutePlanResponse.ArrowBatch{} = batch) do
    case {batch.chunk_index, batch.num_chunks_in_batch} do
      {nil, nil} ->
        # Single (non-chunked) batch
        %{state | batches: state.batches ++ [batch.data]}

      {_idx, nil} ->
        %{state | batches: state.batches ++ [batch.data]}

      {chunk_idx, num_chunks} when is_integer(chunk_idx) and is_integer(num_chunks) ->
        # Chunked batch — collect chunks and reassemble when complete
        key = batch.start_offset || 0
        chunks = Map.get(state.chunks, key, %{expected: num_chunks, parts: %{}})
        chunks = %{chunks | parts: Map.put(chunks.parts, chunk_idx, batch.data)}

        if map_size(chunks.parts) == num_chunks do
          # All chunks received — reassemble in order
          assembled =
            0..(num_chunks - 1)
            |> Enum.map(&Map.fetch!(chunks.parts, &1))
            |> IO.iodata_to_binary()

          state = %{state | chunks: Map.delete(state.chunks, key)}
          %{state | batches: state.batches ++ [assembled]}
        else
          %{state | chunks: Map.put(state.chunks, key, chunks)}
        end
    end
  end

  # --- Arrow IPC decode ---
  #
  # Each Arrow batch from Spark is a standalone IPC stream (schema + record batch).
  # We decode each batch individually and concatenate the rows.

  defp decode_batches([]), do: []

  defp decode_batches(batch_data_list) do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      Enum.flat_map(batch_data_list, &decode_single_batch/1)
    else
      []
    end
  end

  defp decode_single_batch(ipc_data) do
    case Explorer.DataFrame.load_ipc_stream(ipc_data) do
      {:ok, df} ->
        Explorer.DataFrame.to_rows(df)

      {:error, _} ->
        case Explorer.DataFrame.load_ipc(ipc_data) do
          {:ok, df} -> Explorer.DataFrame.to_rows(df)
          {:error, _} -> []
        end
    end
  end
end
