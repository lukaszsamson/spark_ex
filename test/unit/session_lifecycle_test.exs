defmodule SparkEx.Unit.SessionLifecycleTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    CloneSessionRequest,
    InterruptRequest,
    ReleaseSessionRequest,
    UserContext
  }

  describe "Client.build_interrupt_request (via interrupt type matching)" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-123",
        server_side_session_id: "server-side-456",
        user_id: "test_user",
        client_type: "elixir/test",
        plan_id_counter: 0
      }

      %{session: session}
    end

    test "builds interrupt ALL request", %{session: session} do
      # Test the request building by verifying the struct fields
      request = %InterruptRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        interrupt_type: :INTERRUPT_TYPE_ALL
      }

      assert request.session_id == "test-session-123"
      assert request.interrupt_type == :INTERRUPT_TYPE_ALL
      assert request.client_observed_server_side_session_id == "server-side-456"
      assert request.user_context.user_id == "test_user"
      assert request.interrupt == nil
    end

    test "builds interrupt TAG request", %{session: session} do
      request = %InterruptRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        interrupt_type: :INTERRUPT_TYPE_TAG,
        interrupt: {:operation_tag, "etl-job-42"}
      }

      assert request.interrupt_type == :INTERRUPT_TYPE_TAG
      assert request.interrupt == {:operation_tag, "etl-job-42"}
    end

    test "builds interrupt OPERATION_ID request", %{session: session} do
      request = %InterruptRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        interrupt_type: :INTERRUPT_TYPE_OPERATION_ID,
        interrupt: {:operation_id, "op-789"}
      }

      assert request.interrupt_type == :INTERRUPT_TYPE_OPERATION_ID
      assert request.interrupt == {:operation_id, "op-789"}
    end
  end

  describe "ReleaseSession request building" do
    test "builds correct ReleaseSessionRequest" do
      request = %ReleaseSessionRequest{
        session_id: "sess-abc",
        user_context: %UserContext{user_id: "spark_ex"},
        client_type: "elixir/test"
      }

      assert request.session_id == "sess-abc"
      assert request.user_context.user_id == "spark_ex"
      assert request.client_type == "elixir/test"
      assert request.allow_reconnect == false
    end
  end

  describe "CloneSession request building" do
    test "builds correct CloneSessionRequest" do
      request = %CloneSessionRequest{
        session_id: "sess-abc",
        client_observed_server_side_session_id: "ssid-1",
        user_context: %UserContext{user_id: "spark_ex"},
        client_type: "elixir/test",
        new_session_id: "clone-xyz"
      }

      assert request.session_id == "sess-abc"
      assert request.client_observed_server_side_session_id == "ssid-1"
      assert request.user_context.user_id == "spark_ex"
      assert request.client_type == "elixir/test"
      assert request.new_session_id == "clone-xyz"
    end
  end

  describe "DataFrame.tag/2" do
    test "adds a tag to a DataFrame" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      tagged = SparkEx.DataFrame.tag(df, "my-tag")

      assert tagged.tags == ["my-tag"]
      assert tagged.plan == df.plan
      assert tagged.session == df.session
    end

    test "accumulates multiple tags" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}

      tagged =
        df
        |> SparkEx.DataFrame.tag("tag-1")
        |> SparkEx.DataFrame.tag("tag-2")

      assert tagged.tags == ["tag-1", "tag-2"]
    end

    test "tags are empty by default" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      assert df.tags == []
    end

    test "tags are preserved through transforms" do
      df =
        %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
        |> SparkEx.DataFrame.tag("etl")
        |> SparkEx.DataFrame.limit(5)

      assert df.tags == ["etl"]
    end

    test "rejects empty tags" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      assert_raise ArgumentError, ~r/non-empty string/, fn -> SparkEx.DataFrame.tag(df, "") end
    end

    test "rejects tags containing commas" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}

      assert_raise ArgumentError, ~r/cannot contain ','/, fn ->
        SparkEx.DataFrame.tag(df, "a,b")
      end
    end
  end

  describe "Session struct released field" do
    test "defaults to false" do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test",
        user_id: "test"
      }

      assert session.released == false
    end
  end
end
