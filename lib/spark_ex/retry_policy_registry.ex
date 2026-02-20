defmodule SparkEx.RetryPolicyRegistry do
  @moduledoc """
  Stores retry policies for Spark Connect client operations.
  """

  @table :spark_ex_retry_policies

  @type policy_type :: :retry | :reattach

  @spec set_policies(map() | keyword()) :: :ok
  def set_policies(policies) when is_list(policies) or is_map(policies) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)

    policies
    |> normalize_policies()
    |> Enum.each(fn {type, policy} ->
      :ets.insert(@table, {type, policy})
    end)

    :ok
  end

  @spec get_policies() :: %{policy_type() => map()}
  def get_policies() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)

    stored =
      @table
      |> :ets.tab2list()
      |> Map.new(fn {type, policy} -> {type, policy} end)

    Map.merge(default_policies(), stored)
  end

  @spec policy(policy_type()) :: map()
  def policy(type) when type in [:retry, :reattach] do
    Map.get(get_policies(), type)
  end

  defp default_policies() do
    %{
      retry: default_policy(),
      reattach: default_policy()
    }
  end

  defp default_policy() do
    %{
      max_retries: 3,
      initial_backoff_ms: 100,
      max_backoff_ms: 5_000,
      max_server_retry_delay: 10 * 60 * 1000,
      jitter_fun: &default_jitter/1,
      sleep_fun: &Process.sleep/1
    }
  end

  defp normalize_policies(policies) do
    map = if is_list(policies), do: Map.new(policies), else: policies

    Enum.reduce(map, %{}, fn
      {type, policy}, acc when type in [:retry, :reattach] ->
        Map.put(acc, type, normalize_policy(policy))

      {_type, _policy}, acc ->
        acc
    end)
  end

  defp normalize_policy(policy) when is_list(policy), do: normalize_policy(Map.new(policy))

  defp normalize_policy(policy) when is_map(policy) do
    allowed = [
      :max_retries,
      :initial_backoff_ms,
      :max_backoff_ms,
      :max_server_retry_delay,
      :jitter_fun,
      :sleep_fun
    ]

    normalized = Map.take(policy, allowed)
    validate_policy!(normalized)
    Map.merge(default_policy(), normalized)
  end

  defp validate_policy!(policy) do
    validate_nonneg_int!(policy, :max_retries)
    validate_nonneg_int!(policy, :initial_backoff_ms)
    validate_nonneg_int!(policy, :max_backoff_ms)
    validate_nonneg_int!(policy, :max_server_retry_delay)
    validate_fun!(policy, :jitter_fun, 1)
    validate_fun!(policy, :sleep_fun, 1)
    :ok
  end

  defp validate_nonneg_int!(policy, key) do
    case Map.get(policy, key) do
      nil ->
        :ok

      value when is_integer(value) and value >= 0 ->
        :ok

      other ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a non-negative integer, got: #{inspect(other)}"
    end
  end

  defp validate_fun!(policy, key, arity) do
    case Map.get(policy, key) do
      nil ->
        :ok

      fun when is_function(fun, arity) ->
        :ok

      other ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a function with arity #{arity}, got: #{inspect(other)}"
    end
  end

  defp default_jitter(capped) do
    :rand.uniform(capped + 1) - 1
  end
end
