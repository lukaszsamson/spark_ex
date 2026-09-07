defmodule SparkEx.PlanCompressionTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder

  test "encodes compressed_operation plan" do
    data = "compressed"
    op_type = Spark.Connect.Plan.CompressedOperation.OpType.OP_TYPE_RELATION
    codec = Spark.Connect.CompressionCodec.COMPRESSION_CODEC_UNSPECIFIED

    {plan, _} = PlanEncoder.encode({:compressed_operation, data, op_type, codec}, 0)

    assert %Spark.Connect.Plan{op_type: {:compressed_operation, compressed}} = plan
    assert compressed.data == data
    assert compressed.op_type == op_type
    assert compressed.compression_codec == codec
  end
end

defmodule SparkEx.Unit.PlanCompressionTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{AnalyzePlanRequest, Command, Plan, Relation, SQL, SqlCommand}
  alias SparkEx.Connect.{Client, PlanCompression}

  # Exercise protocol decisions even on OTP releases without the optional ZSTD
  # module. Live integration tests verify the actual server codec or fallback.
  defmodule TestCodec do
    def compress(data), do: :zlib.compress(data)
    def decompress(data), do: :zlib.uncompress(data)
  end

  defp codec, do: if(PlanCompression.codec_available?(), do: :zstd, else: TestCodec)

  defmodule IneffectiveCodec do
    def compress(data), do: data
  end

  defmodule BrokenCodec do
    def compress(_data), do: raise("codec failure")
  end

  defp session,
    do: %SparkEx.Session{session_id: "client", user_id: "test", server_side_session_id: nil}

  defp plan,
    do: %Plan{
      op_type:
        {:root, %Relation{rel_type: {:sql, %SQL{query: String.duplicate("SELECT 1;", 1000)}}}}
    }

  test "relation and command compression round trip preserves operation type and codec" do
    command = %Plan{
      op_type:
        {:command,
         %Command{
           command_type: {:sql_command, %SqlCommand{sql: String.duplicate("SELECT 1;", 1000)}}
         }}
    }

    for {original, operation, module} <- [
          {plan(), :OP_TYPE_RELATION, Relation},
          {command, :OP_TYPE_COMMAND, Command}
        ] do
      assert %Plan{op_type: {:compressed_operation, compressed}} =
               PlanCompression.compress(original, %{plan_compression: {0, codec()}})

      assert compressed.op_type == operation
      assert compressed.compression_codec == :COMPRESSION_CODEC_ZSTD

      assert module.decode(IO.iodata_to_binary(apply(codec(), :decompress, [compressed.data]))) ==
               elem(original.op_type, 1)
    end
  end

  test "threshold is strict and ineffective or unavailable codecs preserve the plan" do
    original = plan()
    size = original.op_type |> elem(1) |> Relation.encode() |> IO.iodata_length()
    assert PlanCompression.compress(original, %{plan_compression: {size, :zstd}}) == original
    assert PlanCompression.compress(original, %{plan_compression: :disabled}) == original

    for codec <- [IneffectiveCodec, BrokenCodec, MissingCodec] do
      assert PlanCompression.compress(original, %{plan_compression: {0, codec}}) == original
    end
  end

  test "execute and both same-semantics analyze operands use compressed plans" do
    state = %{session() | plan_compression: {0, codec()}}

    assert %{plan: %Plan{op_type: {:compressed_operation, _}}} =
             Client.build_execute_request(state, plan(), [], nil, false)

    request = %AnalyzePlanRequest{
      analyze:
        {:same_semantics,
         %AnalyzePlanRequest.SameSemantics{target_plan: plan(), other_plan: plan()}}
    }

    assert %{analyze: {:same_semantics, result}} =
             PlanCompression.compress_analyze(request, state)

    assert {:compressed_operation, _} = result.target_plan.op_type
    assert {:compressed_operation, _} = result.other_plan.op_type
  end

  test "analysis requests with bare relations cannot use Plan compression" do
    relation = elem(plan().op_type, 1)

    for {kind, message} <- [
          {:persist, %AnalyzePlanRequest.Persist{relation: relation}},
          {:unpersist, %AnalyzePlanRequest.Unpersist{relation: relation}},
          {:get_storage_level, %AnalyzePlanRequest.GetStorageLevel{relation: relation}}
        ] do
      request = %AnalyzePlanRequest{analyze: {kind, message}}

      assert PlanCompression.compress_analyze(request, %{plan_compression: {0, codec()}}) ==
               request
    end
  end

  test "negotiation pins server identity and caches its configuration" do
    reader = fn _, _, _ ->
      {:ok,
       [
         {"spark.connect.session.planCompression.threshold", "16"},
         {"spark.connect.session.planCompression.defaultAlgorithm", "ZSTD"}
       ], "server"}
    end

    assert {:ok, state} =
             PlanCompression.prepare_session(session(),
               enabled: true,
               codec_available: true,
               config_reader: reader
             )

    assert state.plan_compression == {16, :zstd}
    assert state.server_side_session_id == "server"

    assert {:ok, ^state} =
             PlanCompression.prepare_session(state,
               enabled: true,
               codec_available: true,
               config_reader: fn _, _, _ -> flunk("cached configuration fetched twice") end
             )
  end

  test "disabled or missing codec does not query server" do
    reader = fn _, _, _ -> flunk("unnecessary config RPC") end

    for opts <- [[enabled: false, codec_available: true], [enabled: true, codec_available: false]] do
      assert {:ok, %{plan_compression: :disabled}} =
               PlanCompression.prepare_session(session(), opts ++ [config_reader: reader])
    end
  end

  test "old servers and missing or invalid configs fall back, integrity and size errors propagate" do
    for response <- [
          {:ok, [], nil},
          {:ok,
           [
             {"spark.connect.session.planCompression.threshold", "invalid"},
             {"spark.connect.session.planCompression.defaultAlgorithm", "ZSTD"}
           ], nil},
          {:error, %SparkEx.Error.Remote{grpc_status: 12}}
        ] do
      assert {:ok, %{plan_compression: :disabled}} =
               PlanCompression.prepare_session(session(),
                 enabled: true,
                 codec_available: true,
                 config_reader: fn _, _, _ -> response end
               )
    end

    for reason <- [
          {:session_id_mismatch, %{}},
          {:server_session_changed, %{}},
          %SparkEx.Error.Remote{grpc_status: 8, message: "maxPlanSize exceeded"}
        ] do
      assert {:error, ^reason} =
               PlanCompression.prepare_session(session(),
                 enabled: true,
                 codec_available: true,
                 config_reader: fn _, _, _ -> {:error, reason} end
               )
    end

    state = %{session() | server_side_session_id: "original"}

    assert {:error, {:server_session_changed, _}} =
             PlanCompression.prepare_session(state,
               enabled: true,
               codec_available: true,
               config_reader: fn _, _, _ -> {:ok, [], "rotated"} end
             )
  end
end
