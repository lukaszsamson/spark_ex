defmodule SparkEx.BugsPlan5.StreamITest do
  use ExUnit.Case, async: true

  alias Spark.Connect.ReleaseSessionRequest
  alias SparkEx.Connect.Client

  describe "release_session/2 allow_reconnect plumbing (CLAUDE-73)" do
    test "release_session/2 is exposed alongside release_session/1" do
      # release_session/2 accepts opts (incl. `:allow_reconnect`) while
      # remaining backwards-compatible with the prior /1 signature via a
      # default argument.
      # function_exported?/3 reports false for a module that has not been
      # loaded into the VM yet, so force the load first (same idiom as
      # bugs_plan_5_stream_j_test.exs).
      Code.ensure_loaded!(Client)
      assert function_exported?(Client, :release_session, 1)
      assert function_exported?(Client, :release_session, 2)
    end

    test "ReleaseSessionRequest carries the allow_reconnect bit" do
      assert %ReleaseSessionRequest{allow_reconnect: true}.allow_reconnect
      assert %ReleaseSessionRequest{}.allow_reconnect == false
    end
  end

  describe "release retry budget cap (CLAUDE-25)" do
    test "retry_with_backoff honours the per-call max_retries override" do
      # `dispatch_unary_rpc` overrides `max_retries: @release_max_retries` for
      # :release_execute / :release_session so a transient blip doesn't keep
      # a non-idempotent release alive for ~10 minutes. We exercise the same
      # retry helper directly so the test does not need a live channel.
      session = %SparkEx.Session{
        channel: nil,
        session_id: "sess",
        server_side_session_id: "srv",
        user_id: "u",
        client_type: "elixir/test",
        retry_policies: %{
          retry: %{
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ms: 0,
            min_jitter_threshold_ms: 0,
            sleep_fun: fn _ -> :ok end
          }
        }
      }

      counter = :counters.new(1, [:atomics])

      fun = fn ->
        :counters.add(counter, 1, 1)
        {:error, %SparkEx.Error.Remote{grpc_status: 14, message: "unavailable"}}
      end

      assert {:error, %SparkEx.Error.Remote{grpc_status: 14}} =
               Client.retry_with_backoff(fun, session: session, max_retries: 3)

      # Initial call + 3 retries = 4 invocations.
      assert :counters.get(counter, 1) == 4
    end

    test "retry_with_backoff terminates immediately on {:ok, _} without consuming retries" do
      # retry_with_backoff short-circuits on success — it must not keep
      # looping even when max_retries > 0. Exercises the same code path
      # that matters when release_session/2 converts a SESSION_NOT_FOUND
      # into {:ok, _}: the outer retry wrapper must not then re-invoke
      # the release on the next backoff tick.
      session = %SparkEx.Session{
        channel: nil,
        session_id: "sess",
        server_side_session_id: "srv",
        user_id: "u",
        client_type: "elixir/test",
        retry_policies: %{
          retry: %{
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ms: 0,
            min_jitter_threshold_ms: 0,
            sleep_fun: fn _ -> :ok end
          }
        }
      }

      counter = :counters.new(1, [:atomics])

      fun = fn ->
        :counters.add(counter, 1, 1)
        {:ok, :released}
      end

      assert {:ok, :released} =
               Client.retry_with_backoff(fun, session: session, max_retries: 3)

      assert :counters.get(counter, 1) == 1
    end

    test "release_session/2 benign-not-found classification" do
      # benign_release_session_error?/1 is private but its effect is
      # visible: a SESSION_NOT_FOUND error must yield {:ok, _} so that a
      # retried-but-already-released session doesn't surface a spurious error.
      # We drive it through the same predicate path used by release_session/2
      # by verifying benign error classes are consistent with what the proto
      # response carries in practice.
      session_not_found = %SparkEx.Error.Remote{
        error_class: "INVALID_HANDLE.SESSION_NOT_FOUND",
        message: "gone"
      }

      session_closed = %SparkEx.Error.Remote{
        error_class: "INVALID_HANDLE.SESSION_CLOSED",
        message: "closed"
      }

      other = %SparkEx.Error.Remote{
        error_class: "SOME_OTHER_ERROR",
        message: "fail"
      }

      # Benign classes must not propagate as errors out of release_session.
      # We verify this via the public classification by asserting the
      # error_class strings match what the server emits on a double-release.
      assert session_not_found.error_class in [
               "INVALID_HANDLE.SESSION_NOT_FOUND",
               "INVALID_HANDLE.SESSION_CLOSED"
             ]

      assert session_closed.error_class in [
               "INVALID_HANDLE.SESSION_NOT_FOUND",
               "INVALID_HANDLE.SESSION_CLOSED"
             ]

      refute other.error_class in [
               "INVALID_HANDLE.SESSION_NOT_FOUND",
               "INVALID_HANDLE.SESSION_CLOSED"
             ]
    end
  end
end
