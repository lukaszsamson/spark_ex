defmodule SparkEx.LocalRelationCacheStreamTest do
  use ExUnit.Case, async: true

  alias SparkEx.Internal.LocalRelationCache

  defp params(extra) do
    Map.merge(
      %{
        chunk_size_bytes: 4096,
        chunk_size_rows: 2,
        batch_of_chunks_size_bytes: 8192,
        size_limit: nil
      },
      extra
    )
  end

  test "explicit chunk sizes cannot enlarge the server batch budget" do
    configs = SparkEx.Session.__parse_local_relation_configs__([])
    configs = %{configs | batch_of_chunks_size_bytes: 1024}

    assert {:ok, %{chunk_size_bytes: 1024, batch_of_chunks_size_bytes: 1024}} =
             SparkEx.Session.__local_relation_chunk_params__([cache_chunk_size: 8192], configs)

    assert {:error, {:invalid_local_relation_batch_size, 0}} =
             SparkEx.Session.__local_relation_chunk_params__(
               [cache_chunk_size: 8192],
               %{configs | batch_of_chunks_size_bytes: 0}
             )
  end

  test "ordered repeated chunks count toward size while uploads deduplicate" do
    df = Explorer.DataFrame.new(%{"id" => [1, 1, 1]})
    uploader = fn collected, batch -> {:ok, collected ++ batch} end

    assert {:ok, [same, same, same], schema_hash, artifacts} =
             LocalRelationCache.upload(
               df,
               "id BIGINT",
               params(%{chunk_size_rows: 1}),
               [],
               uploader
             )

    assert Enum.map(artifacts, &elem(&1, 0)) == ["cache/#{schema_hash}", "cache/#{same}"]
    chunk = artifacts |> List.last() |> elem(1)
    limit = byte_size("id BIGINT") + 2 * byte_size(chunk)

    assert {:error, %SparkEx.Error.Remote{message_parameters: error}, _} =
             LocalRelationCache.upload(
               df,
               "id BIGINT",
               params(%{chunk_size_rows: 1, size_limit: limit}),
               [],
               uploader
             )

    assert error["actualSize"] == to_string(limit + byte_size(chunk))
  end

  test "variable-width rows split to the batch budget and round-trip in order" do
    df =
      Explorer.DataFrame.new(%{
        "id" => 0..29,
        "v" => List.duplicate(String.duplicate("x", 300), 30)
      })

    uploader = fn collected, batch ->
      assert Enum.sum(Enum.map(batch, fn {_, bytes} -> byte_size(bytes) end)) <= 4096
      {:ok, Map.merge(collected, Map.new(batch))}
    end

    assert {:ok, hashes, _, artifacts} =
             LocalRelationCache.upload(
               df,
               "id BIGINT, v STRING",
               params(%{
                 chunk_size_bytes: 1_000_000,
                 chunk_size_rows: 30,
                 batch_of_chunks_size_bytes: 4096
               }),
               %{},
               uploader
             )

    ids =
      Enum.flat_map(hashes, fn hash ->
        {:ok, chunk} = Explorer.DataFrame.load_ipc_stream(artifacts["cache/#{hash}"])
        Explorer.DataFrame.to_columns(chunk)["id"]
      end)

    assert ids == Enum.to_list(0..29)
  end

  test "upload errors stop consumption and preserve the last successful state" do
    df = Explorer.DataFrame.new(%{"id" => 0..99})

    uploader = fn
      0, _batch -> {:ok, 1}
      1, _batch -> {:error, :upload_failed}
    end

    assert {:error, :upload_failed, 1} =
             LocalRelationCache.upload(
               df,
               "id BIGINT",
               params(%{batch_of_chunks_size_bytes: 1}),
               0,
               uploader
             )
  end

  test "an upload failure retains identity learned inside the current callback" do
    df = Explorer.DataFrame.new(%{"id" => [1]})

    uploader = fn %{server_id: nil}, _batch ->
      # ArtifactStatus can pin the identity before AddArtifacts fails.
      {:error, :upload_failed, %{server_id: "observed-server"}}
    end

    assert {:error, :upload_failed, %{server_id: "observed-server"}} =
             LocalRelationCache.upload(df, "id BIGINT", params(%{}), %{server_id: nil}, uploader)
  end

  test "serialized binary retention stays bounded across many upload batches" do
    # Native input is deliberately much larger than an upload batch. Only
    # hashes/byte counts escape the worker, so the measurement cannot be hidden
    # by retaining the uploaded payloads in the test process.
    df =
      Explorer.DataFrame.new(%{
        "id" => 0..799,
        "v" => List.duplicate(String.duplicate("x", 8192), 800)
      })

    task =
      Task.async(fn ->
        uploader = fn stats, batch ->
          :erlang.garbage_collect()
          {:binary, binaries} = Process.info(self(), :binary)
          retained = Enum.sum(Enum.map(binaries, fn {_, size, _} -> size end))
          bytes = Enum.sum(Enum.map(batch, fn {_, payload} -> byte_size(payload) end))
          assert bytes <= 65_536

          {:ok,
           %{
             peak: max(stats.peak, retained),
             total: stats.total + bytes,
             batches: stats.batches + 1
           }}
        end

        LocalRelationCache.upload(
          df,
          "id BIGINT, v STRING",
          params(%{chunk_size_bytes: 16_384, batch_of_chunks_size_bytes: 65_536}),
          %{peak: 0, total: 0, batches: 0},
          uploader
        )
      end)

    assert {:ok, hashes, _, stats} = Task.await(task, 15_000)
    assert length(hashes) == 800
    assert stats.batches > 50
    assert stats.total > 6_000_000
    assert stats.peak < 1_000_000
  end
end
