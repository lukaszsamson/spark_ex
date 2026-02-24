defmodule SparkEx.Connect.ClientTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias SparkEx.SqlFormatter
  alias Google.Protobuf.Any

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

  test "build_execute_request/6 includes user context extensions" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test"
    }

    extension = %Any{type_url: "type.example/test", value: <<1, 2, 3>>}
    on_exit(fn -> SparkEx.UserContextExtensions.clear_user_context_extensions() end)
    :ok = SparkEx.UserContextExtensions.add_threadlocal_user_context_extension(extension)

    request =
      Client.build_execute_request(
        session,
        %Spark.Connect.Plan{},
        [],
        nil,
        false
      )

    assert request.user_context.user_id == "test"
    assert [^extension] = request.user_context.extensions
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

  test "clone_session/2 rejects non-string new_session_id" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test"
    }

    assert {:error, {:invalid_new_session_id, 123}} = Client.clone_session(session, 123)
  end

  test "SqlFormatter formats positional args" do
    sql = "SELECT * FROM t WHERE id = ? AND name = ?"

    assert SqlFormatter.format(sql, [42, "O'Reilly"]) ==
             "SELECT * FROM t WHERE id = 42 AND name = 'O''Reilly'"
  end

  test "SqlFormatter formats named args" do
    sql = "SELECT * FROM t WHERE id = :id AND active = :active"

    assert SqlFormatter.format(sql, %{id: 7, active: true}) ==
             "SELECT * FROM t WHERE id = 7 AND active = TRUE"
  end
end
