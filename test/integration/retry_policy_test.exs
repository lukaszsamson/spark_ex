defmodule SparkEx.Integration.RetryPolicyTest do
  use ExUnit.Case

  @moduletag :integration

  test "set_retry_policies updates configuration" do
    on_exit(fn -> SparkEx.set_retry_policies(%{}) end)

    SparkEx.set_retry_policies(retry: %{max_retries: 0})
    policies = SparkEx.get_retry_policies()

    assert policies.retry.max_retries == 0
  end
end
