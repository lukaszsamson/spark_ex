defmodule SparkEx.BugsPlan4.StreamBTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias Spark.Connect.{ExecutePlanResponse, Plan}

  # ── B1: Reattachable response stream is lazy ──
  #
  # The decoder must short-circuit on the first limit-exceeded element and
  # never pull subsequent responses out of the upstream stream. This is the
  # OOM-prevention contract the rest of `decode_stream_*` relies on.

  describe "B1 — reattachable execute streams responses lazily" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-b1",
        server_side_session_id: "server-side-b1",
        user_id: "u",
        client_type: "elixir/test"
      }

      %{session: session}
    end

    test "decode_stream_explorer with max_bytes halts before pulling later responses", %{
      session: session
    } do
      # 100 batches of 1 KiB each. With max_bytes: 512 the very first batch
      # already exceeds the limit, so the decoder must short-circuit without
      # pulling responses 2-100. Pre-refactor `collect_with_reattach` would
      # accumulate the entire stream into a list before the limit check ever
      # ran — defeating the OOM-protection contract on `max_bytes`/`max_rows`.
      pulled = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        upstream =
          Stream.unfold(0, fn
            i when i >= 100 ->
              nil

            i ->
              :counters.add(pulled, 1, 1)

              elem =
                {:ok,
                 %ExecutePlanResponse{
                   response_id: "r#{i}",
                   response_type:
                     {:arrow_batch,
                      %ExecutePlanResponse.ArrowBatch{
                        row_count: 1,
                        data: :binary.copy(<<0>>, 1024),
                        start_offset: i,
                        chunk_index: 0,
                        num_chunks_in_batch: 1
                      }}
                 }}

              {elem, i + 1}
          end)

        {:ok, upstream}
      end

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :bytes}} =
               Client.execute_plan_explorer(session, %Plan{},
                 max_bytes: 512,
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: fn _ -> {:ok, []} end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      # Only the very first arrow batch is pulled before the decoder halts.
      pulled_count = :counters.get(pulled, 1)
      assert pulled_count <= 2, "expected lazy halt, but pulled #{pulled_count} responses"
    end

    test "non-limit decode error from middle of upstream stops further pulls", %{
      session: session
    } do
      # If the decoder halts on a non-LimitExceeded error mid-stream, the
      # upstream must not keep producing responses that nobody will read.
      pulled = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        upstream =
          Stream.unfold(0, fn
            i when i >= 50 ->
              nil

            i ->
              :counters.add(pulled, 1, 1)

              elem =
                if i == 0 do
                  # A malformed arrow_batch (chunk_index past expected) makes
                  # `dispatch_response_type/3` return an integrity error,
                  # which halts decode_stream/2. Plan-5 Stream C made the
                  # decoder skip unknown response_type tags for forward-compat,
                  # so we drive the lazy-halt assertion through a real decode
                  # error instead.
                  {:ok,
                   %ExecutePlanResponse{
                     response_id: "boom",
                     response_type:
                       {:arrow_batch,
                        %ExecutePlanResponse.ArrowBatch{
                          row_count: 0,
                          data: <<>>,
                          start_offset: 0,
                          chunk_index: 7,
                          num_chunks_in_batch: 1
                        }}
                   }}
                else
                  # Filler that nobody should ever pull — invalid arrow data
                  # would crash the decoder if it kept going.
                  {:ok,
                   %ExecutePlanResponse{
                     response_id: "r#{i}",
                     response_type:
                       {:arrow_batch,
                        %ExecutePlanResponse.ArrowBatch{
                          row_count: 0,
                          data: <<>>,
                          start_offset: 0,
                          chunk_index: 0,
                          num_chunks_in_batch: 1
                        }}
                   }}
                end

              {elem, i + 1}
          end)

        {:ok, upstream}
      end

      assert {:error, {:invalid_arrow_batch, _}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: fn _ -> {:ok, []} end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      pulled_count = :counters.get(pulled, 1)
      assert pulled_count <= 3, "expected lazy halt, but pulled #{pulled_count} responses"
    end
  end

  # ── B2: copy_from_local_to_fs streams via lazy artifact path ──
  #
  # `read_local_file/1` would slurp the whole file into memory before the
  # AddArtifacts request stream began. The fix routes through the existing
  # `{:file, path, size}` artifact shape so `Client.build_add_artifacts_requests/3`
  # chunks the file lazily.

  describe "B2 — copy_from_local_to_fs uses lazy file artifact path" do
    test "passes {:file, path, size} to add_artifacts instead of in-memory bytes" do
      tmp = Path.join(System.tmp_dir!(), "spark_ex_b2_#{System.unique_integer([:positive])}.bin")
      contents = :binary.copy(<<0xAB>>, 4096)
      File.write!(tmp, contents)
      on_exit(fn -> File.rm(tmp) end)

      parent = self()

      session_pid =
        spawn_link(fn ->
          receive do
            {:"$gen_call", from, {:add_artifacts, artifacts}} ->
              send(parent, {:add_artifacts_received, artifacts})

              summaries =
                Enum.map(artifacts, fn {name, _payload} -> {name, true} end)

              GenServer.reply(from, {:ok, summaries})
          end
        end)

      dest = "/tmp/spark_ex_b2_dest.bin"
      assert :ok = SparkEx.Session.copy_from_local_to_fs(session_pid, tmp, dest)

      assert_receive {:add_artifacts_received, artifacts}
      assert [{name, payload}] = artifacts
      assert name == "forward_to_fs/tmp/spark_ex_b2_dest.bin"

      # Critical: payload is the lazy file tuple, not in-memory bytes.
      assert {:file, ^tmp, 4096} = payload
    end

    test "non-existent local file surfaces :file_read_error" do
      assert {:error, {:file_read_error, "/nonexistent/spark_ex_b2_missing", :enoent}} =
               SparkEx.Session.copy_from_local_to_fs(
                 self(),
                 "/nonexistent/spark_ex_b2_missing",
                 "/tmp/anything.bin"
               )
    end

    test "lazy file artifacts get chunked by build_add_artifacts_requests" do
      # Build a >chunk_size temp file so the chunked path is exercised end-to-end.
      tmp =
        Path.join(System.tmp_dir!(), "spark_ex_b2_big_#{System.unique_integer([:positive])}.bin")

      chunk_size = 4096
      total_bytes = chunk_size * 5
      File.write!(tmp, :binary.copy(<<0xCD>>, total_bytes))
      on_exit(fn -> File.rm(tmp) end)

      session = %SparkEx.Session{
        channel: nil,
        session_id: "s",
        server_side_session_id: nil,
        user_id: "u",
        client_type: "t"
      }

      requests =
        Client.build_add_artifacts_requests(
          session,
          [{"forward_to_fs/tmp/big.bin", {:file, tmp, total_bytes}}],
          chunk_size
        )

      # First request is the begin-chunk frame; the rest are continuation chunks.
      # Five 4 KiB chunks produces one begin + four chunk requests.
      assert length(requests) == 5

      [first | rest] = requests
      assert match?({:begin_chunk, _}, first.payload)

      Enum.each(rest, fn req ->
        assert match?({:chunk, _}, req.payload)
      end)
    end
  end
end
