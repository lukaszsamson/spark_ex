defmodule SparkEx.EtsTableOwner do
  @moduledoc false

  use GenServer

  @tables [
    {:spark_ex_retry_policies, :set},
    {:spark_ex_user_context_extensions, :set},
    {:spark_ex_observations, :set},
    {:spark_ex_udt_deserializers, :set},
    {:spark_ex_progress_handlers, :bag},
    {:spark_ex_streaming_listener_buses, :bag},
    # Session-pid -> :atomics ref for SparkEx.Internal.PlanIds. The owner
    # process must create this so its lifetime spans every Session; if
    # the first registering session were the creator, that session's
    # exit would tear down the table and orphan other live sessions'
    # allocator refs.
    {:spark_ex_plan_id_counters, :set},
    # Session-pid -> connection snapshot for SparkEx.Internal.SessionSnapshot
    # (out-of-band Interrupt RPCs that must not queue behind a running
    # execute on the Session GenServer).
    {:spark_ex_session_snapshots, :set}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec ensure_table!(atom(), :set | :bag) :: :ok
  def ensure_table!(table, type) when type in [:set, :bag] do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [:named_table, :public, type])
          :ok
        rescue
          ArgumentError ->
            # Race condition: another process may have created the table.
            # Verify it now exists; if not, re-raise.
            case :ets.whereis(table) do
              :undefined ->
                reraise ArgumentError,
                        [
                          message:
                            "failed to create ETS table #{inspect(table)} and it does not exist"
                        ],
                        __STACKTRACE__

              _tid ->
                :ok
            end
        end

      _tid ->
        :ok
    end
  end

  @typedoc """
  A sweep instruction applied to an owned ETS table when a monitored
  process dies:

    * `{:delete_key, table, key}` — `:ets.delete(table, key)`
    * `{:match_delete, table, pattern}` — `:ets.match_delete(table, pattern)`
  """
  @type sweep_action ::
          {:delete_key, atom(), term()}
          | {:match_delete, atom(), term()}

  @doc """
  Monitors `pid` and, when it dies, applies each sweep action to reclaim
  any ETS rows the process owns. Used to bound the lifetime of per-process
  rows (plan-id allocator refs, thread-local user-context extensions) to
  the registering process even when it exits abnormally (no `terminate/2`).

  Safe to call multiple times for the same pid; actions accumulate and are
  all applied on the single `:DOWN`.
  """
  @spec monitor(pid(), sweep_action() | [sweep_action()]) :: :ok
  def monitor(pid, actions) when is_pid(pid) do
    actions = List.wrap(actions)
    GenServer.cast(__MODULE__, {:monitor, pid, actions})
  end

  @impl true
  def init(:ok) do
    Enum.each(@tables, fn {table, type} ->
      ensure_table!(table, type)
    end)

    # %{monitor_ref => {pid, [sweep_action]}}
    {:ok, %{monitors: %{}, by_pid: %{}}}
  end

  @impl true
  def handle_cast({:monitor, pid, actions}, state) do
    case Map.get(state.by_pid, pid) do
      nil ->
        ref = Process.monitor(pid)

        {:noreply,
         %{
           state
           | monitors: Map.put(state.monitors, ref, {pid, actions}),
             by_pid: Map.put(state.by_pid, pid, ref)
         }}

      ref ->
        {pid, existing} = Map.fetch!(state.monitors, ref)

        {:noreply, %{state | monitors: Map.put(state.monitors, ref, {pid, existing ++ actions})}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {{^pid, actions}, monitors} ->
        Enum.each(actions, &apply_sweep/1)
        {:noreply, %{state | monitors: monitors, by_pid: Map.delete(state.by_pid, pid)}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp apply_sweep({:delete_key, table, key}) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _ -> :ets.delete(table, key)
    end
  end

  defp apply_sweep({:match_delete, table, pattern}) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _ -> :ets.match_delete(table, pattern)
    end
  end
end
