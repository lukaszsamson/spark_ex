defmodule SparkEx.Connect.RetryTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias SparkEx.Error.Remote

  test "retries transient failures and succeeds" do
    parent = self()

    jitter_fun = fn capped -> capped end
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 3,
        initial_backoff_ms: 10,
        max_backoff_ms: 40,
        backoff_multiplier: 2.0,
        jitter_fun: jitter_fun,
        sleep_fun: sleep_fun
      }
    )

    attempt_counter = :erlang.make_ref()
    Process.put(attempt_counter, 0)

    result =
      Client.retry_with_backoff(fn ->
        attempt = Process.get(attempt_counter, 0) + 1
        Process.put(attempt_counter, attempt)

        if attempt < 3 do
          {:error, %Remote{message: "temporary", grpc_status: 14}}
        else
          {:ok, :done}
        end
      end)

    assert result == {:ok, :done}
    assert Process.get(attempt_counter) == 3
    assert_received {:slept, 10}
    assert_received {:slept, 20}
    refute_received {:slept, 40}
  end

  test "does not retry non-transient failures" do
    sleep_fun = fn _ms -> flunk("sleep should not be called") end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)
    SparkEx.RetryPolicyRegistry.set_policies(retry: %{sleep_fun: sleep_fun})

    result =
      Client.retry_with_backoff(fn ->
        {:error, %Remote{message: "bad request", grpc_status: 3}}
      end)

    assert {:error, %Remote{grpc_status: 3}} = result
  end

  test "returns last transient error after max retries" do
    parent = self()
    jitter_fun = fn capped -> capped end
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 2,
        initial_backoff_ms: 5,
        max_backoff_ms: 20,
        backoff_multiplier: 2.0,
        jitter_fun: jitter_fun,
        sleep_fun: sleep_fun
      }
    )

    attempt_counter = :erlang.make_ref()
    Process.put(attempt_counter, 0)

    result =
      Client.retry_with_backoff(fn ->
        attempt = Process.get(attempt_counter, 0) + 1
        Process.put(attempt_counter, attempt)
        {:error, %Remote{message: "still unavailable", grpc_status: 14}}
      end)

    assert {:error, %Remote{grpc_status: 14}} = result
    assert Process.get(attempt_counter) == 3
    assert_received {:slept, 5}
    assert_received {:slept, 10}
    refute_received {:slept, 20}
  end

  test "uses retry_delay_ms override and returns to backoff" do
    parent = self()
    jitter_fun = fn capped -> capped end
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 5,
        initial_backoff_ms: 10,
        max_backoff_ms: 80,
        backoff_multiplier: 2.0,
        jitter_fun: jitter_fun,
        sleep_fun: sleep_fun
      }
    )

    attempt_counter = :erlang.make_ref()
    Process.put(attempt_counter, 0)

    result =
      Client.retry_with_backoff(fn ->
        attempt = Process.get(attempt_counter, 0) + 1
        Process.put(attempt_counter, attempt)

        retry_delay_ms =
          if attempt <= 2 do
            5_000
          else
            0
          end

        {:error, %Remote{message: "unavailable", grpc_status: 14, retry_delay_ms: retry_delay_ms}}
      end)

    assert {:error, %Remote{grpc_status: 14}} = result
    assert Process.get(attempt_counter) == 6
    assert_received {:slept, 5_000}
    assert_received {:slept, 5_000}
    assert_received {:slept, 40}
    assert_received {:slept, 80}
    assert_received {:slept, 80}
  end

  test "does not retry DEADLINE_EXCEEDED" do
    sleep_fun = fn _ms -> flunk("sleep should not be called") end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)
    SparkEx.RetryPolicyRegistry.set_policies(retry: %{sleep_fun: sleep_fun})

    result =
      Client.retry_with_backoff(fn ->
        {:error, %Remote{message: "deadline exceeded", grpc_status: 4}}
      end)

    assert {:error, %Remote{grpc_status: 4}} = result
  end

  test "retries INTERNAL with INVALID_CURSOR.DISCONNECTED error class" do
    parent = self()
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
        sleep_fun: sleep_fun
      }
    )

    result =
      Client.retry_with_backoff(fn ->
        {:error,
         %Remote{
           message: "cursor dropped",
           grpc_status: 13,
           error_class: "INVALID_CURSOR.DISCONNECTED"
         }}
      end)

    assert {:error, %Remote{grpc_status: 13}} = result
    assert_received {:slept, _}
  end

  test "does not retry INTERNAL without INVALID_CURSOR.DISCONNECTED" do
    sleep_fun = fn _ms -> flunk("sleep should not be called") end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)
    SparkEx.RetryPolicyRegistry.set_policies(retry: %{sleep_fun: sleep_fun})

    result =
      Client.retry_with_backoff(fn ->
        {:error, %Remote{message: "internal", grpc_status: 13, error_class: "OTHER"}}
      end)

    assert {:error, %Remote{grpc_status: 13}} = result
  end

  test "retries when RetryInfo retry_delay_ms is set even on non-default status" do
    parent = self()
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
        sleep_fun: sleep_fun
      }
    )

    result =
      Client.retry_with_backoff(fn ->
        {:error,
         %Remote{
           message: "resource exhausted",
           grpc_status: 8,
           retry_delay_ms: 25
         }}
      end)

    assert {:error, %Remote{grpc_status: 8}} = result
    assert_received {:slept, 25}
  end

  test "normalize_session_policies! rejects unknown policy types and keys" do
    assert_raise ArgumentError, ~r/unknown retry policy type/, fn ->
      SparkEx.RetryPolicyRegistry.normalize_session_policies!(%{bogus: %{max_retries: 1}})
    end

    assert_raise ArgumentError, ~r/unknown retry policy key/, fn ->
      SparkEx.RetryPolicyRegistry.normalize_session_policies!(%{retry: %{not_a_key: 1}})
    end
  end

  test "uses PySpark DefaultPolicy fields by default" do
    policy = SparkEx.RetryPolicyRegistry.default_policy_template()
    assert policy.max_retries == 15
    assert policy.initial_backoff_ms == 50
    assert policy.max_backoff_ms == 60_000
    assert policy.backoff_multiplier == 4.0
    assert policy.jitter_ms == 500
    assert policy.min_jitter_threshold_ms == 2_000
  end

  test "per-session retry_policies override the global default" do
    # Avoid mutating the shared ETS registry — async tests in other
    # modules rely on the module-level defaults staying put. The session
    # struct's own override is enough to prove the precedence chain
    # because the registry default of 15 retries would otherwise
    # dominate.
    parent = self()
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    session = %SparkEx.Session{
      session_id: "s",
      user_id: "u",
      client_type: "c",
      retry_policies: %{
        retry: %{
          max_retries: 1,
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
          sleep_fun: sleep_fun
        }
      }
    }

    counter = :erlang.make_ref()
    Process.put(counter, 0)

    result =
      SparkEx.Connect.Client.retry_with_backoff(
        fn ->
          Process.put(counter, Process.get(counter, 0) + 1)
          {:error, %SparkEx.Error.Remote{message: "unavailable", grpc_status: 14}}
        end,
        session: session
      )

    assert {:error, %SparkEx.Error.Remote{grpc_status: 14}} = result
    # max_retries: 1 → 2 attempts total. If the session override were
    # ignored, the registry default (15) would attempt many more.
    assert Process.get(counter) == 2
  end

  test "per-session retry_policies are partial overrides; unrelated keys are inherited" do
    # Verify the merge contract via `policy_for/2` directly so the test
    # doesn't depend on (or perturb) the shared ETS-backed registry's
    # current contents. We only assert that the overridden key wins
    # AND every other key matches whatever the global lookup returns
    # for that same field — proving the merge is a partial overlay
    # rather than a default-reset.
    base = SparkEx.RetryPolicyRegistry.policy_for(nil, :retry)

    session = %SparkEx.Session{
      session_id: "s",
      user_id: "u",
      client_type: "c",
      retry_policies: %{retry: %{max_retries: 1}}
    }

    merged = SparkEx.RetryPolicyRegistry.policy_for(session, :retry)

    assert merged.max_retries == 1

    for key <- [
          :initial_backoff_ms,
          :max_backoff_ms,
          :backoff_multiplier,
          :jitter_ms,
          :min_jitter_threshold_ms,
          :max_server_retry_delay,
          :jitter_fun,
          :sleep_fun
        ] do
      assert Map.get(merged, key) == Map.get(base, key),
             "expected partial override to leave #{inspect(key)} untouched"
    end
  end

  test "caps retry_delay_ms using max_server_retry_delay" do
    parent = self()
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

    SparkEx.RetryPolicyRegistry.set_policies(
      retry: %{
        max_retries: 2,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
        max_server_retry_delay: 1_000,
        sleep_fun: sleep_fun
      }
    )

    result =
      Client.retry_with_backoff(fn ->
        {:error, %Remote{message: "unavailable", grpc_status: 14, retry_delay_ms: 70_000}}
      end)

    assert {:error, %Remote{grpc_status: 14}} = result
    assert_received {:slept, 1_000}
    assert_received {:slept, 1_000}
  end
end
