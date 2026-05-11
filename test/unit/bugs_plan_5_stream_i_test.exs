defmodule SparkEx.BugsPlan5.StreamITest do
  use ExUnit.Case, async: true

  alias Spark.Connect.ReleaseSessionRequest
  alias SparkEx.Connect.Client

  describe "release_session/2 allow_reconnect plumbing (CLAUDE-73)" do
    test "release_session/2 is exposed alongside release_session/1" do
      # release_session/2 accepts opts (incl. `:allow_reconnect`) while
      # remaining backwards-compatible with the prior /1 signature via a
      # default argument.
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

    test "retry_with_backoff stops retrying after benign-not-found is mapped to {:ok, _}" do
      # The release_session/2 wrapper translates SESSION_NOT_FOUND from a
      # retried-but-already-released call into {:ok, _} so callers see the
      # desired terminal state, not a spurious error. The retry layer itself
      # still terminates as soon as the function returns {:ok, _}.
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
  end
end
