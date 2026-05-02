defmodule SparkEx.Integration.SessionIdMismatchTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.0"

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  test "server-side session id mismatch returns error" do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    SparkEx.Session.__update_server_side_session_id__(session, "mismatch")

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.spark_version(session)

    assert error.error_class in [
             "INVALID_HANDLE.SESSION_CHANGED",
             "INVALID_HANDLE.SESSION_NOT_FOUND",
             "SESSION_NOT_FOUND",
             "INTERNAL_ERROR"
           ]

    if error.error_class == "INVALID_HANDLE.SESSION_CHANGED" do
      # SESSION_CHANGED means the server rotated the session; the client
      # must close locally so subsequent RPCs short-circuit with
      # `:session_closed` instead of re-issuing doomed requests.
      assert {:error, :session_closed} = SparkEx.spark_version(session)
    end
  end
end
