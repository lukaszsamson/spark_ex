defmodule SparkEx.SessionCloneCleanupTest do
  use ExUnit.Case, async: true

  alias SparkEx.Session

  test "cleanup_cloned_session_on_start_failure releases cloned session id" do
    state = %Session{
      channel: :channel_ref,
      session_id: "source-session",
      user_id: "user-1",
      client_type: "elixir/test"
    }

    clone_info = %{
      new_session_id: "clone-session-id",
      new_server_side_session_id: "clone-server-id"
    }

    parent = self()

    release_fun = fn cleanup_session ->
      send(parent, {:release_called_with, cleanup_session})
      {:ok, "released-server-id"}
    end

    assert :ok = Session.cleanup_cloned_session_on_start_failure(state, clone_info, release_fun)

    assert_receive {:release_called_with,
                    %Session{
                      session_id: "clone-session-id",
                      server_side_session_id: "clone-server-id",
                      user_id: "user-1",
                      client_type: "elixir/test",
                      channel: :channel_ref
                    }}
  end

  test "cleanup_cloned_session_on_start_failure propagates release errors" do
    state = %Session{
      channel: :channel_ref,
      session_id: "source-session",
      user_id: "user-1",
      client_type: "elixir/test"
    }

    clone_info = %{new_session_id: "clone-session-id", new_server_side_session_id: nil}

    assert {:error, :release_failed} =
             Session.cleanup_cloned_session_on_start_failure(
               state,
               clone_info,
               fn _cleanup_session -> {:error, :release_failed} end
             )
  end
end
