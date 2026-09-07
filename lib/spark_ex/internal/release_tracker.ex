defmodule SparkEx.Internal.ReleaseTracker do
  @moduledoc false

  # Session-scoped bookkeeping for the fire-and-forget `ReleaseExecute` tasks
  # started on `SparkEx.TaskSupervisor` (reattach checkpoints, terminal
  # release_all, ManagedStream close).
  #
  # PySpark keeps those releases on a dedicated executor and, in
  # `SparkConnectClient.close()`, waits up to 10s for the pending release
  # futures *before* sending `ReleaseSession` (SPARK-55406 / SPARK-55362,
  # python/pyspark/sql/connect/client/reattach.py). Otherwise the server can
  # observe `ReleaseSession` first and be left with orphaned executions whose
  # buffers only a later GC reclaims.
  #
  # We keep the existing global `Task.Supervisor` (no new supervision
  # machinery) and only record which in-flight release task belongs to which
  # session id, so `SparkEx.Session.terminate/2` can drain *its own* pending
  # releases without ever blocking on another session's.
  #
  # Rows are `{{session_id, ref}, pid | nil}`. The row is inserted by the
  # *calling* process before the task is spawned (so a drain can never miss a
  # release that is about to start) and removed by the task itself when it
  # finishes. `pid` stays `nil` for the short window between insert and
  # `start_child/2` returning; a drain treats such a row as pending.

  @table :spark_ex_release_tasks
  @poll_interval_ms 5

  @doc """
  Starts `fun` as a supervised task tracked as a pending release for
  `session_id`.

  Falls back to an untracked supervised task when no session id is known
  (`nil`), which keeps the behaviour identical to the pre-tracking code for
  call sites that have no session context.
  """
  @spec start_tracked(String.t() | nil, (-> term())) :: {:ok, pid()} | {:error, term()}
  def start_tracked(session_id, fun) when is_function(fun, 0) do
    case session_id do
      id when is_binary(id) and id != "" -> do_start_tracked(id, fun)
      _ -> SparkEx.Connect.Client.start_supervised_task(fun)
    end
  end

  defp do_start_tracked(session_id, fun) do
    ensure_table()
    key = {session_id, make_ref()}
    :ets.insert(@table, {key, nil})

    case SparkEx.Connect.Client.start_supervised_task(fn ->
           try do
             fun.()
           after
             untrack(key)
           end
         end) do
      {:ok, pid} = ok ->
        # No-op when the task already finished and deleted its row.
        _ = :ets.update_element(@table, key, {2, pid})
        ok

      {:error, _} = error ->
        untrack(key)
        error
    end
  end

  @doc """
  Blocks until every pending release task for `session_id` has finished, or
  until `timeout_ms` elapses.

  Returns `:ok` when nothing is (or is left) pending, and `{:timeout, n}` when
  `n` releases were still in flight at the deadline. Returns `:ok` immediately
  when `session_id` is unknown or nothing was ever tracked.
  """
  @spec await_pending(String.t() | nil, timeout()) :: :ok | {:timeout, pos_integer()}
  def await_pending(session_id, timeout_ms)
      when is_binary(session_id) and is_integer(timeout_ms) and timeout_ms >= 0 do
    if :ets.whereis(@table) == :undefined do
      :ok
    else
      do_await(session_id, System.monotonic_time(:millisecond) + timeout_ms)
    end
  end

  def await_pending(_session_id, _timeout_ms), do: :ok

  defp do_await(session_id, deadline) do
    case pending_count(session_id) do
      0 ->
        :ok

      pending ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:timeout, pending}
        else
          Process.sleep(@poll_interval_ms)
          do_await(session_id, deadline)
        end
    end
  end

  @doc """
  Number of release tasks currently tracked for `session_id`.

  Prunes rows whose task died without running its cleanup (e.g. a brutal kill
  during application shutdown) so a drain can never wedge on a dead task.
  """
  @spec pending_count(String.t()) :: non_neg_integer()
  def pending_count(session_id) when is_binary(session_id) do
    case :ets.whereis(@table) do
      :undefined ->
        0

      _ ->
        @table
        |> :ets.match_object({{session_id, :_}, :_})
        |> Enum.count(fn {key, pid} ->
          if is_pid(pid) and not Process.alive?(pid) do
            untrack(key)
            false
          else
            true
          end
        end)
    end
  end

  @doc """
  Drops every row for `session_id` (session teardown backstop).
  """
  @spec clear(String.t() | nil) :: :ok
  def clear(session_id) when is_binary(session_id) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      _ -> :ets.match_delete(@table, {{session_id, :_}, :_})
    end

    :ok
  end

  def clear(_session_id), do: :ok

  defp untrack(key) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      _ -> :ets.delete(@table, key)
    end

    :ok
  end

  defp ensure_table do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
  end
end
