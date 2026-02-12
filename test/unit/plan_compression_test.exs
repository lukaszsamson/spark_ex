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
