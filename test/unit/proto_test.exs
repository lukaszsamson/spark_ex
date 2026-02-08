defmodule SparkEx.ProtoTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{AnalyzePlanRequest, AnalyzePlanResponse, UserContext}

  describe "proto modules" do
    test "AnalyzePlanRequest struct is available with expected fields" do
      req = %AnalyzePlanRequest{
        session_id: "test-session-id",
        user_context: %UserContext{user_id: "test-user"},
        client_type: "test/0.1",
        analyze: {:spark_version, %AnalyzePlanRequest.SparkVersion{}}
      }

      assert req.session_id == "test-session-id"
      assert req.user_context.user_id == "test-user"
      assert req.client_type == "test/0.1"
      assert {:spark_version, %AnalyzePlanRequest.SparkVersion{}} = req.analyze
    end

    test "AnalyzePlanRequest encodes and decodes (roundtrip)" do
      req = %AnalyzePlanRequest{
        session_id: "roundtrip-session",
        user_context: %UserContext{user_id: "roundtrip-user"},
        client_type: "elixir/test",
        analyze: {:spark_version, %AnalyzePlanRequest.SparkVersion{}}
      }

      encoded = Protobuf.encode(req)
      decoded = Protobuf.decode(encoded, AnalyzePlanRequest)

      assert decoded.session_id == "roundtrip-session"
      assert decoded.user_context.user_id == "roundtrip-user"
      assert decoded.client_type == "elixir/test"
      assert {:spark_version, %AnalyzePlanRequest.SparkVersion{}} = decoded.analyze
    end

    test "AnalyzePlanResponse.SparkVersion decodes correctly" do
      resp = %AnalyzePlanResponse{
        session_id: "resp-session",
        server_side_session_id: "server-123",
        result: {:spark_version, %AnalyzePlanResponse.SparkVersion{version: "4.1.1"}}
      }

      encoded = Protobuf.encode(resp)
      decoded = Protobuf.decode(encoded, AnalyzePlanResponse)

      assert decoded.session_id == "resp-session"
      assert decoded.server_side_session_id == "server-123"
      assert {:spark_version, %{version: "4.1.1"}} = decoded.result
    end
  end
end
