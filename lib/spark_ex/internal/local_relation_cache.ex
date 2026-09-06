defmodule SparkEx.Internal.LocalRelationCache do
  @moduledoc false

  alias Explorer.DataFrame, as: DF
  alias SparkEx.Error.Remote

  # Only the current batch and ordered hashes survive an upload. The source
  # DataFrame remains native; no full-relation IPC binary is required.
  def upload(df, schema, params, client_state, upload_batch) do
    schema = schema || ""
    schema_hash = hash(schema)

    acc = %{
      client_state: client_state,
      batch: [],
      batch_size: 0,
      seen: %{},
      hashes: [],
      total_size: byte_size(schema),
      limit: Map.get(params, :size_limit),
      batch_limit: params.batch_of_chunks_size_bytes,
      chunk_limit: min(params.chunk_size_bytes, params.batch_of_chunks_size_bytes),
      upload: upload_batch
    }

    with :ok <- check_size(acc),
         {:ok, acc} <- enqueue(acc, schema_hash, schema),
         {:ok, acc} <- chunks(df, 0, DF.n_rows(df), params.chunk_size_rows, acc),
         {:ok, acc} <- flush(acc) do
      {:ok, Enum.reverse(acc.hashes), schema_hash, acc.client_state}
    else
      {:error, reason, state} -> {:error, reason, state}
      {:error, reason} -> {:error, reason, client_state}
    end
  end

  defp chunks(df, 0, 0, _rows, acc), do: encode_slice(df, 0, 0, acc)
  defp chunks(_df, offset, total, _rows, acc) when offset >= total, do: {:ok, acc}

  defp chunks(df, offset, total, rows, acc) do
    count = min(rows, total - offset)

    with {:ok, acc} <- encode_slice(df, offset, count, acc) do
      chunks(df, offset + count, total, rows, acc)
    end
  end

  defp encode_slice(df, offset, count, acc) do
    case encode(df, offset, count) do
      {:ok, bytes} when byte_size(bytes) > acc.chunk_limit and count > 1 ->
        # Estimates cannot bound variable-width rows. Split an oversized slice
        # until it fits, retaining only its range, not its serialized parent.
        split_slice(df, offset, count, acc)

      {:ok, bytes} ->
        accept_chunk(acc, bytes)

      {:error, reason} ->
        {:error, reason, acc.client_state}
    end
  end

  defp split_slice(df, offset, count, acc) do
    left = div(count, 2)

    with {:ok, acc} <- encode_slice(df, offset, left, acc) do
      encode_slice(df, offset + left, count - left, acc)
    end
  end

  defp encode(df, offset, count) do
    case df |> DF.slice(offset, count) |> DF.dump_ipc_stream() do
      {:ok, _} = result -> result
      {:error, reason} -> {:error, {:arrow_encode_error, reason}}
    end
  rescue
    error -> {:error, {:arrow_encode_error, error}}
  end

  defp accept_chunk(acc, bytes) do
    acc = %{acc | total_size: acc.total_size + byte_size(bytes)}

    case check_size(acc) do
      :ok ->
        hash = hash(bytes)
        enqueue(%{acc | hashes: [hash | acc.hashes]}, hash, bytes)

      {:error, reason} ->
        {:error, reason, acc.client_state}
    end
  end

  defp check_size(%{limit: nil}), do: :ok
  defp check_size(%{total_size: size, limit: limit}) when size <= limit, do: :ok

  defp check_size(%{total_size: size, limit: limit}) do
    {:error,
     %Remote{
       error_class: "LOCAL_RELATION_SIZE_LIMIT_EXCEEDED",
       message: "Local relation size #{size} exceeds limit #{limit}",
       message_parameters: %{"actualSize" => to_string(size), "sizeLimit" => to_string(limit)}
     }}
  end

  defp enqueue(acc, hash, bytes) do
    if Map.has_key?(acc.seen, hash) do
      {:ok, acc}
    else
      with {:ok, acc} <- flush_before(acc, byte_size(bytes)) do
        acc = %{
          acc
          | batch: [{"cache/#{hash}", bytes} | acc.batch],
            batch_size: acc.batch_size + byte_size(bytes),
            seen: Map.put(acc.seen, hash, true)
        }

        # A single row (or the schema) can exceed the byte budget. It is sent
        # alone, matching the existing upload contract; it is never combined
        # with another artifact in an oversized batch.
        if acc.batch_size >= acc.batch_limit, do: flush(acc), else: {:ok, acc}
      end
    end
  end

  defp flush_before(%{batch: []} = acc, _bytes), do: {:ok, acc}

  defp flush_before(acc, bytes) do
    if acc.batch_size + bytes > acc.batch_limit, do: flush(acc), else: {:ok, acc}
  end

  defp flush(%{batch: []} = acc), do: {:ok, acc}

  defp flush(acc) do
    case acc.upload.(acc.client_state, Enum.reverse(acc.batch)) do
      {:ok, state} -> {:ok, %{acc | client_state: state, batch: [], batch_size: 0}}
      {:error, reason, state} -> {:error, reason, state}
      {:error, reason} -> {:error, reason, acc.client_state}
    end
  end

  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
