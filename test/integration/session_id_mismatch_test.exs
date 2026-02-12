defmodule SparkEx.Integration.SessionIdMismatchTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  test "server-side session id mismatch returns error" do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    SparkEx.Session.update_server_side_session_id(session, "mismatch")

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.spark_version(session)

    assert error.error_class in [
             "INVALID_HANDLE.SESSION_CHANGED",
             "INVALID_HANDLE.SESSION_NOT_FOUND",
             "SESSION_NOT_FOUND",
             "INTERNAL_ERROR"
           ]
  end
end
