defmodule SparkEx.RetryPolicyRegistry do
  @moduledoc """
  Stores retry policies for Spark Connect client operations.
  """

  @table :spark_ex_retry_policies

  @type policy_type :: :retry | :reattach | :streaming

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
  def policy(type) when type in [:retry, :reattach, :streaming] do
    Map.get(get_policies(), type)
  end

  @doc """
  Look up a policy with a session-first preference.

  When `session.retry_policies` carries a per-session override for `type`,
  it wins; otherwise we fall back to the global ETS-backed registry. Pass
  `nil` for callers that don't have a session in hand.
  """
  @spec policy_for(SparkEx.Session.t() | nil, policy_type()) :: map()
  def policy_for(%{retry_policies: %{} = policies}, type)
      when type in [:retry, :reattach, :streaming] do
    case Map.get(policies, type) do
      nil -> policy(type)
      override -> Map.merge(policy(type), override)
    end
  end

  def policy_for(_session, type) when type in [:retry, :reattach, :streaming] do
    policy(type)
  end

  @doc """
  Normalize and validate per-session policy overrides.

  Returns a map keyed by `:retry | :reattach | :streaming` whose values
  contain only the user-supplied keys (validated, but **not** merged
  against the global defaults). The session-aware lookup
  `policy_for/2` merges these partial maps onto the active global
  policy, so callers who specify a single key (e.g. `max_retries: 3`)
  do not implicitly reset every other knob (`sleep_fun`, jitter
  config, etc.) back to defaults.

  Raises `ArgumentError` on:

    * unknown policy types — anything outside
      `:retry | :reattach | :streaming`
    * unknown keys inside a per-type policy map
    * key values that fail per-key validation
      (non-negative integer / positive number / function arity)

  Intended for `SparkEx.Session` user input. The global registry's
  `set_policies/1` keeps its more permissive, default-merging behavior
  for backward compatibility.
  """
  @spec normalize_session_policies!(map() | keyword()) :: %{policy_type() => map()}
  def normalize_session_policies!(policies) when is_map(policies) or is_list(policies) do
    map = if is_list(policies), do: Map.new(policies), else: policies

    Enum.into(map, %{}, fn {type, policy} ->
      unless type in [:retry, :reattach, :streaming] do
        raise ArgumentError,
              "unknown retry policy type #{inspect(type)}; expected one of " <>
                ":retry, :reattach, :streaming"
      end

      {type, normalize_partial_policy!(policy)}
    end)
  end

  defp default_policies() do
    %{
      retry: default_policy(),
      reattach: default_policy(),
      streaming: streaming_policy()
    }
  end

  @doc false
  @spec default_policy_template() :: map()
  def default_policy_template, do: default_policy()

  # Matches PySpark's DefaultPolicy: 15 retries, 50ms initial backoff,
  # 4x multiplier, 60s max backoff, 500ms jitter, 2000ms min-delay threshold.
  defp default_policy() do
    %{
      max_retries: 15,
      initial_backoff_ms: 50,
      max_backoff_ms: 60_000,
      backoff_multiplier: 4.0,
      jitter_ms: 500,
      min_jitter_threshold_ms: 2_000,
      max_server_retry_delay: 10 * 60 * 1000,
      jitter_fun: &default_jitter/1,
      sleep_fun: &Process.sleep/1
    }
  end

  defp streaming_policy(), do: default_policy()

  defp normalize_policies(policies) do
    map = if is_list(policies), do: Map.new(policies), else: policies

    Enum.reduce(map, %{}, fn
      {type, policy}, acc when type in [:retry, :reattach, :streaming] ->
        Map.put(acc, type, normalize_policy(policy))

      {_type, _policy}, acc ->
        acc
    end)
  end

  defp normalize_policy(policy) when is_list(policy), do: normalize_policy(Map.new(policy))

  defp normalize_policy(policy) when is_map(policy) do
    normalized = Map.take(policy, allowed_policy_keys())
    validate_policy!(normalized)
    Map.merge(default_policy(), normalized)
  end

  # Per-session overrides keep ONLY the user-provided keys (no default
  # merge), and surface unknown keys as an error rather than silently
  # dropping them. The global `set_policies/1` path stays permissive
  # for backward compatibility.
  defp normalize_partial_policy!(policy) when is_list(policy),
    do: normalize_partial_policy!(Map.new(policy))

  defp normalize_partial_policy!(policy) when is_map(policy) do
    allowed = allowed_policy_keys()

    case Map.keys(policy) -- allowed do
      [] ->
        validate_policy!(policy)
        policy

      extras ->
        raise ArgumentError,
              "unknown retry policy key(s) #{inspect(extras)}; expected any of #{inspect(allowed)}"
    end
  end

  defp normalize_partial_policy!(other) do
    raise ArgumentError,
          "expected a map or keyword list of policy options, got: #{inspect(other)}"
  end

  defp allowed_policy_keys do
    [
      :max_retries,
      :initial_backoff_ms,
      :max_backoff_ms,
      :backoff_multiplier,
      :jitter_ms,
      :min_jitter_threshold_ms,
      :max_server_retry_delay,
      :jitter_fun,
      :sleep_fun
    ]
  end

  defp validate_policy!(policy) do
    validate_nonneg_int!(policy, :max_retries)
    validate_nonneg_int!(policy, :initial_backoff_ms)
    validate_nonneg_int!(policy, :max_backoff_ms)
    validate_nonneg_int!(policy, :jitter_ms)
    validate_nonneg_int!(policy, :min_jitter_threshold_ms)
    validate_nonneg_int!(policy, :max_server_retry_delay)
    validate_pos_number!(policy, :backoff_multiplier)
    validate_fun!(policy, :jitter_fun, 1)
    validate_fun!(policy, :sleep_fun, 1)
    :ok
  end

  defp validate_pos_number!(policy, key) do
    case Map.get(policy, key) do
      nil ->
        :ok

      value when is_number(value) and value > 0 ->
        :ok

      other ->
        raise ArgumentError,
              "expected #{inspect(key)} to be a positive number, got: #{inspect(other)}"
    end
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
