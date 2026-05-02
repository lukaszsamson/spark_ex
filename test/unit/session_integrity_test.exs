defmodule SparkEx.Connect.SessionIntegrityTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.ExecutePlanResponse
  alias SparkEx.Connect.ResultDecoder
  alias SparkEx.Connect.SessionIntegrity

  defp session(session_id, server_side_session_id \\ nil) do
    %SparkEx.Session{
      session_id: session_id,
      server_side_session_id: server_side_session_id,
      user_id: "u",
      client_type: "test"
    }
  end

  describe "validate_session_id/2" do
    test "passes when response and session ids match" do
      assert :ok =
               SessionIntegrity.validate_session_id(
                 %{session_id: "s1"},
                 session("s1")
               )
    end

    test "passes when response carries no session_id" do
      assert :ok = SessionIntegrity.validate_session_id(%{session_id: nil}, session("s1"))
      assert :ok = SessionIntegrity.validate_session_id(%{session_id: ""}, session("s1"))
    end

    test "rejects mismatched session_id" do
      assert {:error, {:session_id_mismatch, %{expected: "s1", got: "s2"}}} =
               SessionIntegrity.validate_session_id(%{session_id: "s2"}, session("s1"))
    end
  end

  describe "validate_server_session_id/2" do
    test "pins the first non-empty id" do
      assert {:ok, "ssid-1"} = SessionIntegrity.validate_server_session_id("ssid-1", nil)
    end

    test "passes when same as pinned" do
      assert {:ok, "ssid-1"} = SessionIntegrity.validate_server_session_id("ssid-1", "ssid-1")
    end

    test "ignores nil/empty when already pinned" do
      assert {:ok, "ssid-1"} = SessionIntegrity.validate_server_session_id(nil, "ssid-1")
      assert {:ok, "ssid-1"} = SessionIntegrity.validate_server_session_id("", "ssid-1")
    end

    test "rejects when pinned id changes" do
      assert {:error, {:server_session_changed, %{pinned: "a", got: "b"}}} =
               SessionIntegrity.validate_server_session_id("b", "a")
    end
  end

  describe "session_changed_error?/1" do
    test "matches by error_class" do
      assert SessionIntegrity.session_changed_error?(%SparkEx.Error.Remote{
               error_class: "INVALID_HANDLE.SESSION_CHANGED"
             })
    end

    test "matches by message substring as fallback" do
      assert SessionIntegrity.session_changed_error?(%SparkEx.Error.Remote{
               message: "boom: INVALID_HANDLE.SESSION_CHANGED at server"
             })
    end

    test "ignores unrelated errors" do
      refute SessionIntegrity.session_changed_error?(%SparkEx.Error.Remote{
               message: "something else"
             })

      refute SessionIntegrity.session_changed_error?(:other)
    end

    test "matches local server-side rotation tuples" do
      assert SessionIntegrity.session_changed_error?(
               {:server_session_changed, %{pinned: "a", got: "b"}}
             )
    end

    test "unwraps {:error, _} replies" do
      assert SessionIntegrity.session_changed_error?(
               {:error, %SparkEx.Error.Remote{error_class: "INVALID_HANDLE.SESSION_CHANGED"}}
             )

      assert SessionIntegrity.session_changed_error?(
               {:error, {:server_session_changed, %{pinned: "a", got: "b"}}}
             )

      refute SessionIntegrity.session_changed_error?({:error, :session_released})
    end
  end

  describe "decode_stream integrity enforcement" do
    test "halts when response session_id does not match the client session" do
      sess = session("client-1")

      stream = [
        {:ok,
         %ExecutePlanResponse{
           session_id: "evil-2",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:error, {:session_id_mismatch, %{expected: "client-1", got: "evil-2"}}} =
               ResultDecoder.decode_stream(stream, sess)
    end

    test "halts when server-side session id rotates mid-stream" do
      sess = session("c", "ssid-1")

      stream = [
        {:ok,
         %ExecutePlanResponse{
           session_id: "c",
           server_side_session_id: "ssid-2",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:error, {:server_session_changed, %{pinned: "ssid-1", got: "ssid-2"}}} =
               ResultDecoder.decode_stream(stream, sess)
    end

    test "treats empty server_side_session_id as absent (does not overwrite the pin)" do
      # Protobuf string fields default to "" when unset. An empty value
      # must not clobber the pinned id and trigger a later false-positive
      # `server_session_changed` error.
      sess = session("c", "ssid-1")

      stream = [
        {:ok,
         %ExecutePlanResponse{
           session_id: "c",
           server_side_session_id: "",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }},
        {:ok,
         %ExecutePlanResponse{
           session_id: "c",
           server_side_session_id: "ssid-1",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, %{server_side_session_id: "ssid-1"}} =
               ResultDecoder.decode_stream(stream, sess)
    end

    test "passes when session_id matches and server-side id is consistent" do
      sess = session("c", nil)

      stream = [
        {:ok,
         %ExecutePlanResponse{
           session_id: "c",
           server_side_session_id: "ssid-1",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }},
        {:ok,
         %ExecutePlanResponse{
           session_id: "c",
           server_side_session_id: "ssid-1",
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, %{server_side_session_id: "ssid-1"}} =
               ResultDecoder.decode_stream(stream, sess)
    end
  end
end
