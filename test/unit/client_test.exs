defmodule SparkEx.Connect.ClientTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client

  test "analyze_explain/3 returns error for unknown explain mode" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test"
    }

    assert {:error, {:invalid_explain_mode, :invalid_mode}} =
             Client.analyze_explain(session, %Spark.Connect.Plan{}, :invalid_mode)
  end

  test "build_execute_request/6 uses session chunking defaults" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test",
      allow_arrow_batch_chunking: false,
      preferred_arrow_chunk_size: 2048
    }

    request =
      Client.build_execute_request(
        session,
        %Spark.Connect.Plan{},
        [],
        nil,
        false
      )

    assert [
             %Spark.Connect.ExecutePlanRequest.RequestOption{
               request_option:
                 {:result_chunking_options,
                  %Spark.Connect.ResultChunkingOptions{
                    allow_arrow_batch_chunking: false,
                    preferred_arrow_chunk_size: 2048
                  }}
             }
           ] = request.request_options
  end

  test "build_execute_request/6 allows per-request chunking overrides" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test",
      allow_arrow_batch_chunking: true,
      preferred_arrow_chunk_size: nil
    }

    request =
      Client.build_execute_request(
        session,
        %Spark.Connect.Plan{},
        [],
        nil,
        false,
        allow_arrow_batch_chunking: false,
        preferred_arrow_chunk_size: 8192
      )

    assert [
             %Spark.Connect.ExecutePlanRequest.RequestOption{
               request_option:
                 {:result_chunking_options,
                  %Spark.Connect.ResultChunkingOptions{
                    allow_arrow_batch_chunking: false,
                    preferred_arrow_chunk_size: 8192
                  }}
             }
           ] = request.request_options
  end

  test "build_execute_request/6 validates preferred chunk size" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test"
    }

    assert_raise ArgumentError, fn ->
      Client.build_execute_request(
        session,
        %Spark.Connect.Plan{},
        [],
        nil,
        false,
        preferred_arrow_chunk_size: 0
      )
    end
  end
end
