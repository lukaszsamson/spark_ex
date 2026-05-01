defmodule SparkEx.Internal.StreamingTimeout do
  @moduledoc false

  # Shared helper for `await_termination` / `await_any_termination` timeout
  # handling. Mirrors PySpark's `awaitTermination(timeout=None)` semantics:
  # `nil` means "wait forever", anything else is a positive number of
  # seconds and is converted to milliseconds for the proto and the
  # GenServer call timeout.

  @doc """
  Resolves the user-supplied opts into `{timeout_ms, opts_for_call}`.

  - `timeout_ms` is the value to put in the proto's `timeout_ms` field
    (`nil` for "no timeout").
  - `opts_for_call` is the opts list with `:timeout` rewritten to the
    millisecond value so that the downstream GenServer call timeout
    matches the gRPC timeout instead of being interpreted as raw ms.
  """
  @spec resolve(keyword()) :: {non_neg_integer() | nil, keyword()}
  def resolve(opts) when is_list(opts) do
    timeout_ms = normalize(Keyword.get(opts, :timeout, nil))
    {timeout_ms, Keyword.put(opts, :timeout, timeout_ms)}
  end

  defp normalize(nil), do: nil

  defp normalize(seconds) when is_number(seconds) and seconds > 0 do
    trunc(seconds * 1000)
  end

  defp normalize(other) do
    raise ArgumentError,
          "expected :timeout to be a positive number of seconds or nil, got: #{inspect(other)}"
  end
end
