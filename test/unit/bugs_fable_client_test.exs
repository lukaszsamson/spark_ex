defmodule SparkEx.BugsFableClientTest do
  @moduledoc """
  Regression tests for FABLE-03, -10, -11, -12, -36, -37, -38.
  """
  use ExUnit.Case, async: false

  alias GRPC.RPCError
  alias SparkEx.Connect.{Channel, Client}
  alias SparkEx.Error.Remote

  # --- FABLE-03: nil/:infinity timeout must map to gun's infinite timeout ---

  describe "normalize_grpc_timeout/1 (FABLE-03)" do
    test "maps nil to :infinity so it never reaches gun as a literal nil" do
      assert Client.normalize_grpc_timeout(nil) == :infinity
    end

    test "passes :infinity through unchanged" do
      assert Client.normalize_grpc_timeout(:infinity) == :infinity
    end

    test "passes integer timeouts through unchanged" do
      assert Client.normalize_grpc_timeout(60_000) == 60_000
      assert Client.normalize_grpc_timeout(0) == 0
    end
  end

  # --- FABLE-12: connection-string values percent-decoded without plus-to-space ---

  describe "parse_uri/1 param decoding (FABLE-12)" do
    test "preserves a literal '+' in a token value" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;token=abc+def")
      assert opts.token == "abc+def"
    end

    test "percent-decodes encoded characters in values" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;token=abc%2Bdef")
      assert opts.token == "abc+def"
    end

    test "percent-decodes a space (%20) without touching '+'" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;user_agent=a%20b+c")
      assert opts.extra_params["user_agent"] == nil
      # user_agent is a reserved key, assert via the value path instead:
      assert {:ok, opts2} = Channel.parse_uri("sc://localhost:15002/;custom=a%20b+c")
      assert opts2.extra_params["custom"] == "a b+c"
    end

    test "does not decode the key (keys are left verbatim)" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;a+b=value")
      assert opts.extra_params["a+b"] == "value"
    end
  end

  # --- FABLE-36: RetryInfo presence makes an error retryable even w/o delay ---

  describe "retryable_error?/1 with RetryInfo (FABLE-36)" do
    test "retries an error carrying RetryInfo even when retry_delay is unset" do
      error = %Remote{message: "boom", grpc_status: 3, has_retry_info: true, retry_delay_ms: nil}
      assert Client.retryable_error?(error)
    end

    test "retries an error with an explicit retry_delay_ms" do
      error = %Remote{message: "boom", grpc_status: 3, retry_delay_ms: 100}
      assert Client.retryable_error?(error)
    end

    test "does not retry a plain non-retryable error" do
      error = %Remote{message: "boom", grpc_status: 3, has_retry_info: false, retry_delay_ms: nil}
      refute Client.retryable_error?(error)
    end

    test "still retries UNAVAILABLE without RetryInfo" do
      error = %Remote{message: "boom", grpc_status: 14, has_retry_info: false}
      assert Client.retryable_error?(error)
    end
  end

  # --- FABLE-10: server retry_delay floors, not replaces, exponential backoff ---

  describe "server RetryInfo as a backoff floor (FABLE-10)" do
    setup do
      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)
      :ok
    end

    test "a small server retry_delay does not suppress exponential backoff" do
      parent = self()
      sleep_fun = fn ms -> send(parent, {:slept, ms}) end

      # No jitter; deterministic backoff: initial * 2^attempt, capped.
      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{
          max_retries: 5,
          initial_backoff_ms: 1_000,
          max_backoff_ms: 60_000,
          backoff_multiplier: 2.0,
          max_server_retry_delay: 60_000,
          jitter_ms: 0,
          sleep_fun: sleep_fun
        }
      )

      counter = :erlang.make_ref()
      Process.put(counter, 0)

      Client.retry_with_backoff(fn ->
        n = Process.get(counter, 0) + 1
        Process.put(counter, n)

        if n < 3 do
          # 10 ms server hint must NOT collapse backoff to 10 ms.
          {:error, %Remote{message: "transient", grpc_status: 14, retry_delay_ms: 10}}
        else
          {:ok, :done}
        end
      end)

      # attempt 0 -> 1000, attempt 1 -> 2000; the 10 ms server floor is below
      # both, so the exponential value wins (FABLE-10).
      assert_received {:slept, 1_000}
      assert_received {:slept, 2_000}
    end

    test "a large server retry_delay raises the floor above exponential backoff" do
      parent = self()
      sleep_fun = fn ms -> send(parent, {:slept, ms}) end

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{
          max_retries: 5,
          initial_backoff_ms: 1_000,
          max_backoff_ms: 60_000,
          backoff_multiplier: 2.0,
          max_server_retry_delay: 60_000,
          jitter_ms: 0,
          sleep_fun: sleep_fun
        }
      )

      counter = :erlang.make_ref()
      Process.put(counter, 0)

      Client.retry_with_backoff(fn ->
        n = Process.get(counter, 0) + 1
        Process.put(counter, n)

        if n < 2 do
          {:error, %Remote{message: "transient", grpc_status: 14, retry_delay_ms: 30_000}}
        else
          {:ok, :done}
        end
      end)

      # attempt 0 exponential = 1000, but server floor = min(30000, 60000) = 30000 wins.
      assert_received {:slept, 30_000}
    end

    test "server retry_delay is capped at max_server_retry_delay" do
      parent = self()
      sleep_fun = fn ms -> send(parent, {:slept, ms}) end

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{
          max_retries: 5,
          initial_backoff_ms: 1_000,
          max_backoff_ms: 200_000,
          backoff_multiplier: 2.0,
          max_server_retry_delay: 5_000,
          jitter_ms: 0,
          sleep_fun: sleep_fun
        }
      )

      counter = :erlang.make_ref()
      Process.put(counter, 0)

      Client.retry_with_backoff(fn ->
        n = Process.get(counter, 0) + 1
        Process.put(counter, n)

        if n < 2 do
          {:error, %Remote{message: "transient", grpc_status: 14, retry_delay_ms: 999_999}}
        else
          {:ok, :done}
        end
      end)

      # floor = min(999999, 5000) = 5000; exponential attempt 0 = 1000; max = 5000.
      assert_received {:slept, 5_000}
    end
  end

  # --- Reattach-loop fixes (FABLE-11, -37, -38) ---

  alias Spark.Connect.{ExecutePlanResponse, Plan}

  defp test_session do
    %SparkEx.Session{
      channel: nil,
      session_id: "fable-session",
      server_side_session_id: "fable-server-side",
      user_id: "tester",
      client_type: "elixir/test"
    }
  end

  defp complete_response(id) do
    {:ok,
     %ExecutePlanResponse{
       response_id: id,
       response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
     }}
  end

  describe "graceful-EOF reattach never fails or charges budget (FABLE-38)" do
    setup do
      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      # Tiny reattach budget + no-op sleep: if graceful EOFs consumed the
      # budget, many silent reattaches would surface :reattach_incomplete_result.
      SparkEx.RetryPolicyRegistry.set_policies(
        reattach: %{
          max_retries: 2,
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
          backoff_multiplier: 2.0,
          max_server_retry_delay: 1,
          jitter_ms: 0,
          sleep_fun: fn _ -> :ok end
        }
      )

      %{session: test_session()}
    end

    test "completes after far more graceful EOFs than the retry budget", %{session: session} do
      eof_counter = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        # Initial stream ends immediately with no ResponseComplete (graceful EOF).
        {:ok, []}
      end

      reattach_stream_fun = fn _last_response_id ->
        n = :counters.get(eof_counter, 1)
        :counters.add(eof_counter, 1, 1)

        # 20 zero-progress graceful EOFs (well past the 2-retry budget and the
        # old ~19-cycle failure point), then finally complete.
        if n < 20 do
          {:ok, []}
        else
          {:ok, [complete_response("done")]}
        end
      end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _opts -> {:ok, nil} end
               )
    end
  end

  describe "reattach retry budget resets on progress (FABLE-11)" do
    setup do
      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        reattach: %{
          max_retries: 2,
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
          backoff_multiplier: 2.0,
          max_server_retry_delay: 1,
          jitter_ms: 0,
          sleep_fun: fn _ -> :ok end
        }
      )

      %{session: test_session()}
    end

    test "transient errors spanning more than max_retries succeed when progress resets the counter",
         %{session: session} do
      step = :counters.new(1, [:atomics])
      transient = {:error, %RPCError{status: 14, message: "blip", details: []}}

      # Initial stream: a real response (progress, resets attempt) followed by a
      # transient error that triggers reattach.
      execute_stream_fun = fn _request, _timeout ->
        {:ok, [{:ok, %ExecutePlanResponse{response_id: "r0"}}, transient]}
      end

      # max_retries = 2. A stream-lifetime counter would exhaust after 2
      # transient errors; resetting on each consumed response keeps the whole
      # sequence alive. Each reattach yields one transient error (charging the
      # budget) until progress arrives to reset it.
      reattach_stream_fun = fn _last_response_id ->
        n = :counters.get(step, 1)
        :counters.add(step, 1, 1)

        case n do
          # second transient error -> attempt would be at 2 (== max) next.
          0 ->
            {:ok, [transient]}

          # a successful response resets the attempt counter to 0...
          1 ->
            {:ok, [{:ok, %ExecutePlanResponse{response_id: "r1"}}, transient]}

          # ...so these two further transient errors are survivable.
          2 ->
            {:ok, [transient]}

          # finally complete.
          _ ->
            {:ok, [complete_response("done")]}
        end
      end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _opts -> {:ok, nil} end
               )
    end
  end

  describe "non-reattachable execute honors per-session retry policy (FABLE-37)" do
    # The non-reattachable path drives Stub.execute_plan directly (no injectable
    # stream fun), so we assert the contract at the retry_with_backoff layer that
    # path now uses: the session's :retry override must be applied. Before the
    # fix, execute_plan_non_reattachable/5 forwarded opts WITHOUT :session, so a
    # session policy of max_retries: 0 was ignored in favor of the global 15.
    setup do
      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      # Global default would retry; per-session override must suppress it.
      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{
          max_retries: 5,
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
          backoff_multiplier: 2.0,
          jitter_ms: 0,
          sleep_fun: fn _ -> :ok end
        }
      )

      :ok
    end

    test "session retry policy with max_retries: 0 prevents retrying" do
      session = %{test_session() | retry_policies: %{retry: %{max_retries: 0}}}

      attempts = :counters.new(1, [:atomics])

      # Mirror exactly how execute_plan_non_reattachable/5 now invokes the
      # retry wrapper: opts with :session put in.
      result =
        Client.retry_with_backoff(
          fn ->
            :counters.add(attempts, 1, 1)
            {:error, %Remote{message: "transient", grpc_status: 14}}
          end,
          Keyword.put_new([], :session, session)
        )

      assert {:error, %Remote{grpc_status: 14}} = result
      # max_retries: 0 from the session policy means a single attempt only,
      # not the global policy's 5 retries.
      assert :counters.get(attempts, 1) == 1
    end

    test "without a session the global policy (5 retries) applies" do
      attempts = :counters.new(1, [:atomics])

      Client.retry_with_backoff(fn ->
        :counters.add(attempts, 1, 1)
        {:error, %Remote{message: "transient", grpc_status: 14}}
      end)

      # 1 initial + 5 retries.
      assert :counters.get(attempts, 1) == 6
    end
  end
end
