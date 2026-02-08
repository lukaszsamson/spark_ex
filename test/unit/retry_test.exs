defmodule SparkEx.Connect.RetryTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias SparkEx.Error.Remote

  test "retries transient failures and succeeds" do
    parent = self()

    jitter_fun = fn capped -> capped end
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    attempt_counter = :erlang.make_ref()
    Process.put(attempt_counter, 0)

    result =
      Client.retry_with_backoff(
        fn ->
          attempt = Process.get(attempt_counter, 0) + 1
          Process.put(attempt_counter, attempt)

          if attempt < 3 do
            {:error, %Remote{message: "temporary", grpc_status: 14}}
          else
            {:ok, :done}
          end
        end,
        max_retries: 3,
        initial_backoff_ms: 10,
        max_backoff_ms: 40,
        jitter_fun: jitter_fun,
        sleep_fun: sleep_fun
      )

    assert result == {:ok, :done}
    assert Process.get(attempt_counter) == 3
    assert_received {:slept, 10}
    assert_received {:slept, 20}
    refute_received {:slept, 40}
  end

  test "does not retry non-transient failures" do
    sleep_fun = fn _ms -> flunk("sleep should not be called") end

    result =
      Client.retry_with_backoff(
        fn -> {:error, %Remote{message: "bad request", grpc_status: 3}} end,
        max_retries: 3,
        sleep_fun: sleep_fun
      )

    assert {:error, %Remote{grpc_status: 3}} = result
  end

  test "returns last transient error after max retries" do
    parent = self()
    jitter_fun = fn capped -> capped end
    sleep_fun = fn ms -> send(parent, {:slept, ms}) end

    attempt_counter = :erlang.make_ref()
    Process.put(attempt_counter, 0)

    result =
      Client.retry_with_backoff(
        fn ->
          attempt = Process.get(attempt_counter, 0) + 1
          Process.put(attempt_counter, attempt)
          {:error, %Remote{message: "still unavailable", grpc_status: 14}}
        end,
        max_retries: 2,
        initial_backoff_ms: 5,
        max_backoff_ms: 20,
        jitter_fun: jitter_fun,
        sleep_fun: sleep_fun
      )

    assert {:error, %Remote{grpc_status: 14}} = result
    assert Process.get(attempt_counter) == 3
    assert_received {:slept, 5}
    assert_received {:slept, 10}
    refute_received {:slept, 20}
  end
end
