defmodule SparkEx.Integration.PlanCompressionTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Session}

  setup do
    {:ok, session} =
      SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"), plan_compression: true)

    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "opting in executes queries on every supported server", %{session: session} do
    df = SparkEx.sql(session, "SELECT 42 AS answer")
    assert {:ok, _} = DataFrame.schema(df)
    assert {:ok, [%{"answer" => 42}]} = DataFrame.collect(df)
    state = Session.get_state(session)
    assert state.plan_compression_enabled
    assert state.plan_compression != nil

    {:ok, version} = Session.spark_version(session)

    if Version.compare(version, "4.1.0") == :lt or
         not SparkEx.Connect.PlanCompression.codec_available?() do
      assert state.plan_compression == :disabled
    end
  end

  @tag min_spark: "4.1"
  test "negotiated compression executes commands and analyzes relations", %{session: session} do
    assert :ok =
             Session.config_set(session, [
               {"spark.connect.session.planCompression.threshold", "0"},
               {"spark.connect.session.planCompression.defaultAlgorithm", "ZSTD"}
             ])

    value = String.duplicate("plan-compression", 1000)
    df = SparkEx.sql(session, "SELECT '#{value}' AS value")
    assert {:ok, _} = DataFrame.schema(df)
    assert {:ok, [%{"value" => ^value}]} = DataFrame.collect(df)
    state = Session.get_state(session)

    expected =
      if SparkEx.Connect.PlanCompression.codec_available?(), do: {0, :zstd}, else: :disabled

    assert state.plan_compression == expected
    assert is_binary(state.server_side_session_id)

    name = "compression_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             SparkEx.sql(
               session,
               "CREATE OR REPLACE TEMP VIEW #{name} AS SELECT '#{value}' AS value"
             )
             |> DataFrame.collect()

    assert {:ok, [%{"value" => ^value}]} =
             SparkEx.sql(session, "SELECT * FROM #{name}") |> DataFrame.collect()

    assert :ok =
             Session.config_set(session, [
               {"spark.connect.session.planCompression.threshold", "-1"}
             ])

    assert {:ok, [%{"value" => ^value}]} = DataFrame.collect(df)
    assert Session.get_state(session).plan_compression == :disabled
  end

  @tag min_spark: "4.1"
  test "server parse rejection is returned without retrying an uncompressed operation", %{
    session: session
  } do
    plan = %Spark.Connect.Plan{
      op_type:
        {:compressed_operation,
         %Spark.Connect.Plan.CompressedOperation{
           data: "invalid zstd",
           op_type: :OP_TYPE_RELATION,
           compression_codec: :COMPRESSION_CODEC_ZSTD
         }}
    }

    assert {:error, error} =
             SparkEx.Connect.Client.execute_plan(Session.get_state(session), plan,
               reattachable: false
             )

    assert error.error_class == "CONNECT_INVALID_PLAN.CANNOT_PARSE"
  end
end
