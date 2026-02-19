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
