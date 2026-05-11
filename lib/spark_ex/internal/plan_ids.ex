defmodule SparkEx.Internal.PlanIds do
  @moduledoc false

  # Plan_id allocator.
  #
  # PySpark assigns a stable plan_id to each LogicalPlan at construction
  # time using a session-scoped counter shared across the public API and
  # the proto encoder. SparkEx must do the same: when a DataFrame is built
  # in process A (test process or any caller) and later encoded in process
  # B (the Session GenServer), both processes have to draw plan_ids from
  # one shared counter — otherwise the encoded plan can contain two
  # different relations with the same plan_id (caller's stamped id == an
  # encoder-allocated synthetic id), which produces an ambiguous /
  # invalid Spark Connect plan that the Spark 3.5.x analyzer rejects.
  #
  # Architecture:
  #
  #   * The `:spark_ex_plan_id_counters` ETS table is owned by
  #     `SparkEx.EtsTableOwner` (declared statically in its `@tables`),
  #     so the table lifetime spans every Session and outlives any
  #     individual session's termination.
  #   * Each `SparkEx.Session` GenServer creates an `:atomics` counter
  #     ref in its `init/1` via `register_session/1` and inserts
  #     `{pid, ref}` into the ETS table. `terminate/1` removes it.
  #   * `DataFrame.new(session, plan)` (running in the caller's process)
  #     looks up the session's counter ref and reserves the next id via
  #     `:atomics.add_get/3` directly — one ETS read, no GenServer
  #     round-trip.
  #   * `PlanEncoder.next_id/1` (running in the session's process during
  #     encode) reserves each synthetic id from the *same* atomic via
  #     `next(self())`. The threaded `counter` parameter is preserved
  #     for API compatibility but no longer participates in uniqueness,
  #     so encode-vs-caller races on the shared counter are atomic.
  #   * If `session` is not a registered SparkEx session (test fixtures
  #     using `self()`, plan_encoder_test plans built standalone), the
  #     allocator falls back to a per-process `:counters` ref stashed in
  #     the process dictionary. Single-process tests cannot collide with
  #     themselves and don't encode across processes, so the fallback is
  #     safe.
  #
  # Counter values are 0-based (matching PySpark's `_fresh_plan_id`):
  # `:atomics.add_get(ref, 1, 1)` returns the post-increment value, then
  # we subtract 1 to get the pre-increment id. `:counters` is read-then-
  # increment in the fallback for the same reason.

  @ets_table :spark_ex_plan_id_counters

  @doc false
  # The ETS table is owned by `SparkEx.EtsTableOwner` (declared in its
  # `@tables`). This is a no-op kept for back-compat with the
  # `SparkEx.Application.start/2` hook.
  def init, do: :ok

  @doc """
  Registers a session-scoped allocator.

  Called from `SparkEx.Session.init/1`. The returned `:atomics` ref must be
  kept on session state so it can be read/synced with the encoder counter.
  """
  @spec register_session(pid()) :: :atomics.atomics_ref()
  def register_session(session_pid) when is_pid(session_pid) do
    ref = :atomics.new(1, [])
    :ets.insert(@ets_table, {session_pid, ref})
    ref
  end

  @doc """
  Removes a session's allocator (called from `Session.terminate/2`).
  """
  @spec unregister_session(pid()) :: :ok
  def unregister_session(session_pid) when is_pid(session_pid) do
    case :ets.whereis(@ets_table) do
      :undefined -> :ok
      _ -> :ets.delete(@ets_table, session_pid)
    end

    :ok
  end

  @doc """
  Returns the next plan_id for the given session.

  If `session` is a registered SparkEx.Session pid, draws from its shared
  atomic counter. Otherwise falls back to a per-process counter (safe for
  test fixtures that never encode cross-process).
  """
  @spec next(GenServer.server()) :: non_neg_integer()
  def next(session) when is_pid(session) do
    case :ets.whereis(@ets_table) do
      :undefined ->
        next_fallback()

      _ ->
        case :ets.lookup(@ets_table, session) do
          [{_pid, ref}] -> :atomics.add_get(ref, 1, 1) - 1
          [] -> next_fallback()
        end
    end
  end

  def next(_session), do: next_fallback()

  @doc """
  Reads the current counter value (next id that *would* be allocated).

  Used by `SparkEx.Session` to sync `state.plan_id_counter` with the
  session's atomic before encoding so the encoder's synthetic-id
  allocations live in the same namespace as stamped DataFrame ids.
  """
  @spec peek(pid()) :: non_neg_integer()
  def peek(session) when is_pid(session) do
    case :ets.whereis(@ets_table) do
      :undefined ->
        peek_process_fallback()

      _ ->
        case :ets.lookup(@ets_table, session) do
          [{_pid, ref}] -> :atomics.get(ref, 1)
          [] -> peek_process_fallback()
        end
    end
  end

  defp peek_process_fallback do
    case Process.get(__MODULE__) do
      nil -> 0
      ref -> :counters.get(ref, 1)
    end
  end

  @doc """
  Wraps a raw plan tuple with a stable id allocated from `session`.

  Idempotent: a plan that is already wrapped is returned as-is.
  """
  @spec wrap(GenServer.server(), term()) :: {:plan_id, non_neg_integer(), term()}
  def wrap(_session, {:plan_id, _, _} = plan), do: plan
  def wrap(session, plan), do: {:plan_id, next(session), plan}

  @doc "Extracts the plan_id from a wrapped plan."
  @spec id_of(term()) :: non_neg_integer()
  def id_of({:plan_id, id, _}) when is_integer(id), do: id

  def id_of(plan) do
    raise ArgumentError,
          "plan is not stamped with a stable plan_id (refactor expected " <>
            "every DataFrame plan to be wrapped via SparkEx.Internal.PlanIds.wrap/2). " <>
            "Got: #{inspect(plan)}"
  end

  # ── Private ──

  defp next_fallback do
    ref =
      case Process.get(__MODULE__) do
        nil ->
          ref = :counters.new(1, [])
          Process.put(__MODULE__, ref)
          ref

        ref ->
          ref
      end

    id = :counters.get(ref, 1)
    :counters.add(ref, 1, 1)
    id
  end
end
