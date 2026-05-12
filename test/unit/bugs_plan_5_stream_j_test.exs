defmodule SparkEx.BugsPlan5.StreamJTest do
  @moduledoc """
  Regression tests for BUGS_PLAN_5 Stream J cleanup sweep.
  """

  use ExUnit.Case, async: true

  alias Spark.Connect.StorageLevel
  alias SparkEx.Connect.TypeMapper
  alias SparkEx.Internal.UUID

  describe "explorer_schema_to_ddl (CLAUDE-02/03)" do
    test "list input renders BIGINT for {:s, 64}" do
      assert TypeMapper.explorer_schema_to_ddl([{"id", {:s, 64}}, {"name", :string}]) ==
               "id BIGINT, name STRING"
    end

    test "map input emits a deprecation warning" do
      io =
        ExUnit.CaptureIO.capture_io(:stderr, fn ->
          assert TypeMapper.explorer_schema_to_ddl(%{"id" => {:s, 64}}) =~ "id BIGINT"
        end)

      assert io =~ "non-deterministic"
    end
  end

  describe "Trigger.Once deprecation warning (CLAUDE-40)" do
    test "encode_command warns but still encodes :once trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      stderr =
        ExUnit.CaptureIO.capture_io(:stderr, fn ->
          SparkEx.Connect.CommandEncoder.encode_command(
            {:write_stream_operation_start, df_plan,
             [format: "console", output_mode: "append", trigger: :once]},
            0
          )
        end)

      assert stderr =~ "Trigger.Once was removed in Spark 4"
    end
  end

  describe "is_cached? StorageLevel.NONE (GPT-19)" do
    defmodule StorageLevelNoneSession do
      use GenServer

      def start_link(storage_level), do: GenServer.start_link(__MODULE__, storage_level, [])

      @impl true
      def init(storage_level), do: {:ok, storage_level}

      @impl true
      def handle_call({:analyze_get_storage_level, _plan}, _from, level) do
        {:reply, {:ok, level}, level}
      end
    end

    test "replication=1 with all use_* false → is_cached returns false" do
      # StorageLevel.NONE has replication=1 by default; only use_* flags matter.
      {:ok, session} =
        StorageLevelNoneSession.start_link(%StorageLevel{
          use_disk: false,
          use_memory: false,
          use_off_heap: false,
          deserialized: false,
          replication: 1
        })

      df = SparkEx.DataFrame.new(session, {:sql, "SELECT 1", nil})
      assert {:ok, false} = SparkEx.DataFrame.is_cached(df)
    end
  end

  describe "config coercion (CLAUDE-57/81)" do
    test "Client.config_set/2 raises ArgumentError when a value is nil" do
      # coerce_config_string(nil) raises before any RPC is issued, so a fake
      # session struct (channel=nil) is sufficient.
      fake_session = %SparkEx.Session{
        session_id: "00000000-0000-0000-0000-000000000000",
        user_id: "u",
        client_type: "test",
        server_side_session_id: nil,
        channel: nil
      }

      assert_raise ArgumentError, ~r/config key\/value cannot be nil/, fn ->
        SparkEx.Connect.Client.config_set(fake_session, [{"spark.some.key", nil}])
      end
    end
  end

  describe "is_modifiable parser strict (CLAUDE-41)" do
    test "exact 'true'/'false' map to booleans; whitespace/mixed-case do not" do
      alias SparkEx.Connect.Client

      assert Client.parse_is_modifiable_value("k", "true") == true
      assert Client.parse_is_modifiable_value("k", "false") == false
      assert Client.parse_is_modifiable_value("k", "") == nil
      assert Client.parse_is_modifiable_value("k", nil) == nil
      # Strict — no trim/downcase; these return nil with a Logger.warning
      assert Client.parse_is_modifiable_value("k", " true ") == nil
      assert Client.parse_is_modifiable_value("k", "True") == nil
      assert Client.parse_is_modifiable_value("k", "TRUE") == nil
    end
  end

  describe "clone_session UUID validation (CLAUDE-21/65)" do
    test "UUID.valid_uuid? rejects empty / non-UUID strings" do
      refute UUID.valid_uuid?("not-a-uuid")
      refute UUID.valid_uuid?("")
    end
  end

  describe "release_execute response shape (CLAUDE-62)" do
    test "@doc mentions operation_id field" do
      {:docs_v1, _, _, _, _, _, fns} = Code.fetch_docs(SparkEx.Connect.Client)

      doc =
        Enum.find_value(fns, fn
          {{:function, :release_execute, 3}, _, _, %{"en" => text}, _} -> text
          _ -> nil
        end)

      assert is_binary(doc)
      assert doc =~ "operation_id"
    end
  end

  describe "artifact_status_full (CLAUDE-56)" do
    test "Client exports artifact_status_full/2" do
      Code.ensure_loaded!(SparkEx.Connect.Client)
      assert function_exported?(SparkEx.Connect.Client, :artifact_status_full, 2)
    end
  end

  # CLAUDE-78 — empty artifacts must take the batch path (BatchedArtifact),
  # never the chunked path (BeginChunkedArtifact). PySpark's `_add_artifacts`
  # routes `size > CHUNK_SIZE` to chunked and everything else to batched;
  # SparkEx's `producer_stream/2` does the same. The previously-dead
  # `if total_bytes == 0` branch inside `file_chunk_request_stream/5` is
  # now gone — these tests pin the reachable wire shape.
  describe "empty artifact wire shape (CLAUDE-78)" do
    setup do
      session = %SparkEx.Session{
        session_id: "00000000-0000-0000-0000-000000000000",
        user_id: "u",
        client_type: "test",
        server_side_session_id: nil,
        channel: nil
      }

      {:ok, session: session}
    end

    test "empty in-memory binary uses BatchedArtifact, not BeginChunkedArtifact",
         %{session: session} do
      [request] =
        SparkEx.Connect.Client.build_add_artifacts_requests(session, [{"empty.jar", ""}])

      assert {:batch, %Spark.Connect.AddArtifactsRequest.Batch{artifacts: [single]}} =
               request.payload

      assert single.name == "empty.jar"
      assert single.data.data == ""
      # `:erlang.crc32(<<>>) == 0`, which is also the int64 proto default;
      # protobuf does not serialize the field on the wire — PySpark's
      # `zlib.crc32(b"") == 0` behaves identically.
      assert single.data.crc == 0
    end

    test "empty file uses BatchedArtifact, not BeginChunkedArtifact",
         %{session: session} do
      path = Path.join(System.tmp_dir!(), "spark_ex_empty_#{System.unique_integer([:positive])}")
      File.write!(path, "")

      try do
        [request] =
          SparkEx.Connect.Client.build_add_artifacts_requests(
            session,
            [{"empty.jar", {:file, path, 0}}]
          )

        assert {:batch, %Spark.Connect.AddArtifactsRequest.Batch{artifacts: [single]}} =
                 request.payload

        assert single.name == "empty.jar"
        assert single.data.data == ""
      after
        File.rm!(path)
      end
    end

    test "chunked path only triggers when total_bytes > chunk_size",
         %{session: session} do
      path = Path.join(System.tmp_dir!(), "spark_ex_big_#{System.unique_integer([:positive])}")
      # 3.5 chunks of 1024 bytes (1 KiB) → spans BeginChunked + 3 follow-up chunks.
      data = :crypto.strong_rand_bytes(3_500)
      File.write!(path, data)

      try do
        requests =
          SparkEx.Connect.Client.build_add_artifacts_requests(
            session,
            [{"big.jar", {:file, path, byte_size(data)}}],
            1024
          )

        # First request is the BeginChunkedArtifact, rest are chunks.
        [first | rest] = requests
        assert {:begin_chunk, begin} = first.payload
        # 3500 bytes / 1024 = ceil(3.418) = 4 chunks.
        assert begin.num_chunks == 4
        assert begin.total_bytes == 3500
        assert byte_size(begin.initial_chunk.data) == 1024
        assert length(rest) == 3
        for req <- rest, do: assert({:chunk, _} = req.payload)
      after
        File.rm!(path)
      end
    end
  end

  # CLAUDE-67 — Spark Connect's `ResponseSchema` is `DataType` whose
  # top-level `kind` is *required* to be `:struct`. PySpark asserts this
  # (`assert isinstance(schema, StructType)` in core.py) and otherwise
  # derives a struct schema from the Arrow stream itself. SparkEx falls
  # back to a 0-column empty DataFrame for any non-Struct top-level
  # schema (including `kind: nil` and `state.schema == nil`), which is
  # the right shape: a 1-column VOID frame would diverge from PySpark
  # and would mis-shape downstream callers that count columns.
  describe "empty-dataframe schema fallback (CLAUDE-67)" do
    alias Spark.Connect.{DataType, ExecutePlanResponse}
    alias SparkEx.Connect.ResultDecoder

    test "no schema frame, no arrow batches → 0-column empty DataFrame" do
      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert Explorer.DataFrame.n_rows(result.dataframe) == 0
      assert Explorer.DataFrame.names(result.dataframe) == []
      # `state.schema` was never set, so it is preserved as nil on the result.
      assert result.schema == nil
    end

    test "non-Struct top-level schema (server protocol violation) → 0-column frame" do
      # A wrapper DataType with `kind: nil` (or any non-Struct kind at the
      # top level) is a server bug. PySpark would assert-fail; SparkEx
      # falls back to an empty 0-column frame so callers don't crash.
      bogus_schema = %DataType{kind: nil}

      stream = [
        {:ok, %ExecutePlanResponse{schema: bogus_schema}},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert Explorer.DataFrame.n_rows(result.dataframe) == 0
      assert Explorer.DataFrame.names(result.dataframe) == []
    end

    test "Struct schema with zero fields → 0-column frame (Spark `STRUCT<>`)" do
      struct_schema = %DataType{
        kind: {:struct, %DataType.Struct{fields: []}}
      }

      stream = [
        {:ok, %ExecutePlanResponse{schema: struct_schema}},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      {:ok, result} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert Explorer.DataFrame.n_rows(result.dataframe) == 0
      assert Explorer.DataFrame.names(result.dataframe) == []
    end
  end
end
