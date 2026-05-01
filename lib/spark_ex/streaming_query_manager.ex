defmodule SparkEx.StreamingQueryManager do
  @moduledoc """
  Manages all active streaming queries for a Spark session.

  Mirrors PySpark's `StreamingQueryManager`.

  ## Examples

      {:ok, queries} = SparkEx.StreamingQueryManager.active(session)
      {:ok, query} = SparkEx.StreamingQueryManager.get(session, "some-query-id")
  """

  alias SparkEx.StreamingQuery

  @doc """
  Returns a list of all active streaming queries.
  """
  @spec active(GenServer.server()) :: {:ok, [StreamingQuery.t()]} | {:error, term()}
  def active(session) do
    case execute_command(session, {:active}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:active, active_result} ->
            build_streaming_queries(session, active_result.active_queries)

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc "Alias for `active/1`."
  @spec list_active(GenServer.server()) :: {:ok, [StreamingQuery.t()]} | {:error, term()}
  def list_active(session), do: active(session)

  @doc """
  Returns the streaming query with the given ID, or nil if not found.
  """
  @spec get(GenServer.server(), String.t()) :: {:ok, StreamingQuery.t() | nil} | {:error, term()}
  def get(session, query_id) when is_binary(query_id) do
    case execute_command(session, {:get_query, query_id}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:query, instance} ->
            build_streaming_query(session, instance)

          nil ->
            {:ok, nil}

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Waits until any of the active queries terminates.

  ## Options

    * `:timeout` — timeout in seconds (default: no timeout). Must be a
      positive number when set; `nil` means wait forever. Mirrors
      PySpark's `awaitAnyTermination(timeout=None)` semantics.

  Returns `{:ok, nil}` for the no-timeout form. Returns `{:ok,
  terminated?}` for the timeout form.
  """
  @spec await_any_termination(GenServer.server(), keyword()) ::
          {:ok, boolean() | nil} | {:error, term()}
  def await_any_termination(session, opts \\ []) do
    {timeout_ms, opts} = SparkEx.Internal.StreamingTimeout.resolve(opts)
    opts = Keyword.put_new(opts, :reattach_policy, :streaming)

    case execute_command(session, {:await_any_termination, timeout_ms}, opts) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:await_any_termination, at} ->
            if timeout_ms == nil, do: {:ok, nil}, else: {:ok, at.terminated}

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Forgets about past terminated queries so that `active/1` only returns current queries.
  """
  @spec reset_terminated(GenServer.server()) :: :ok | {:error, term()}
  def reset_terminated(session) do
    case execute_command(session, {:reset_terminated}) do
      {:ok, _result} -> :ok
      {:error, _} = error -> error
    end
  end

  # ── Listener Management ──
  #
  # PySpark's `StreamingQueryManager.addListener` cloudpickles a Python
  # `StreamingQueryListener` and ships the bytes to the server, where they
  # are deserialized and run inside a Python worker. There is no portable
  # way to produce that payload from Elixir, so the
  # `add_listener`/`remove_listener` pair below is intentionally limited
  # to expert callers that have constructed the payload externally
  # (typically a Java listener jar or a pre-pickled Python listener).
  #
  # For the normal Elixir flow — receiving streaming-query events on the
  # client and dispatching them to Elixir callback modules — use
  # `SparkEx.StreamingQueryListenerBus`, which speaks the
  # `addListenerBusListener` RPC.

  @doc """
  Registers a pre-serialized streaming-query listener on the server.

  > #### Unsupported from Elixir {: .warning}
  >
  > This wraps PySpark's `addListener` RPC, which expects a
  > cloudpickled Python listener (or a serialized Java listener) as its
  > payload. There is no portable way to produce such a payload from
  > Elixir. Use `SparkEx.StreamingQueryListenerBus.add_listener/2` for
  > the supported Elixir-callback flow.

  ## Parameters

    * `listener_id` — unique identifier for the listener
    * `listener_payload` — pre-serialized listener payload bytes
  """
  @spec add_listener(GenServer.server(), String.t(), binary()) ::
          {:ok, boolean()} | {:error, term()}
  def add_listener(session, listener_id, listener_payload)
      when is_binary(listener_id) and is_binary(listener_payload) do
    case execute_command(session, {:add_listener, listener_id, listener_payload}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:add_listener, added} -> {:ok, added}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Removes a pre-serialized streaming-query listener from the server.

  > #### Unsupported from Elixir {: .warning}
  >
  > Companion to `add_listener/3`; same caveats apply. Use
  > `SparkEx.StreamingQueryListenerBus.remove_listener/2` for the
  > supported Elixir-callback flow.

  ## Parameters

    * `listener_id` — unique identifier of the listener to remove
    * `listener_payload` — pre-serialized listener payload bytes
  """
  @spec remove_listener(GenServer.server(), String.t(), binary()) ::
          {:ok, boolean()} | {:error, term()}
  def remove_listener(session, listener_id, listener_payload)
      when is_binary(listener_id) and is_binary(listener_payload) do
    case execute_command(session, {:remove_listener, listener_id, listener_payload}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:remove_listener, removed} -> {:ok, removed}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Lists all registered streaming-query listener IDs on the server.

  Returns IDs registered via `add_listener/3` only. Listeners registered
  through `SparkEx.StreamingQueryListenerBus` use the separate
  `addListenerBusListener` RPC and are not reflected here.
  """
  @spec list_listeners(GenServer.server()) :: {:ok, [String.t()]} | {:error, term()}
  def list_listeners(session) do
    case execute_command(session, {:list_listeners}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:list_listeners, list_result} -> {:ok, list_result.listener_ids}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  # --- Private ---

  defp execute_command(session, cmd_type, opts \\ []) do
    SparkEx.Session.execute_command_with_result(
      session,
      {:streaming_query_manager_command, cmd_type},
      opts
    )
  end

  defp build_streaming_queries(session, instances) do
    Enum.reduce_while(instances, {:ok, []}, fn instance, {:ok, acc} ->
      case build_streaming_query(session, instance) do
        {:ok, query} -> {:cont, {:ok, [query | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, queries} -> {:ok, Enum.reverse(queries)}
      {:error, _} = error -> error
    end
  end

  defp build_streaming_query(session, %{id: %{id: query_id, run_id: run_id}} = instance)
       when is_binary(query_id) and query_id != "" and is_binary(run_id) and run_id != "" do
    {:ok,
     %StreamingQuery{
       session: session,
       query_id: query_id,
       run_id: run_id,
       name: if(instance.name == "", do: nil, else: instance.name)
     }}
  end

  defp build_streaming_query(_session, nil) do
    {:ok, nil}
  end

  defp build_streaming_query(_session, instance) do
    {:error, {:unexpected_result, {:missing_query_ids, instance}}}
  end
end
