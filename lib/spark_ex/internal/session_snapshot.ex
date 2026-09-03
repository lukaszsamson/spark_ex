defmodule SparkEx.Internal.SessionSnapshot do
  @moduledoc false

  # Read-only snapshot of the connection fields that control-plane RPCs need
  # (channel, session ids, user context, retry policies), published to ETS by
  # `SparkEx.Session` so out-of-band RPCs — currently Interrupt — can run from
  # the *caller's* process while the Session GenServer is busy executing a
  # plan. Without this, `interrupt_all/tag/operation` would queue behind the
  # very operation they are meant to cancel (see NO_SERIALIZED_RPC.md);
  # PySpark issues interrupts lock-free from any thread.
  #
  # The table is owned by `SparkEx.EtsTableOwner` (declared in its `@tables`)
  # so its lifetime spans every Session. The Session deletes its row when it
  # is released/closed (so the fast path falls back to the GenServer, which
  # replies with the proper lifecycle error) and on terminate; the owner
  # sweeps the row on the session's `:DOWN` as a backstop.

  @ets_table :spark_ex_session_snapshots

  @type snapshot :: %{
          channel: GRPC.Channel.t(),
          session_id: String.t(),
          server_side_session_id: String.t() | nil,
          user_id: String.t() | nil,
          client_type: String.t() | nil,
          retry_policies: map() | nil
        }

  @spec put(pid(), snapshot()) :: :ok
  def put(session_pid, snapshot) when is_pid(session_pid) and is_map(snapshot) do
    already_registered? =
      :ets.whereis(@ets_table) != :undefined and :ets.member(@ets_table, session_pid)

    :ets.insert(@ets_table, {session_pid, snapshot})

    unless already_registered? do
      SparkEx.EtsTableOwner.monitor(session_pid, {:delete_key, @ets_table, session_pid})
    end

    :ok
  end

  @spec delete(pid()) :: :ok
  def delete(session_pid) when is_pid(session_pid) do
    case :ets.whereis(@ets_table) do
      :undefined -> :ok
      _ -> :ets.delete(@ets_table, session_pid)
    end

    :ok
  end

  @doc """
  Fetches the snapshot for a session ref (pid, name, or via tuple).

  Returns `:error` for unregistered sessions (fake test sessions, released or
  closed sessions, remote pids) — callers fall back to the GenServer call.
  """
  @spec fetch(GenServer.server()) :: {:ok, snapshot()} | :error
  def fetch(session) when is_pid(session) do
    with true <- node(session) == node(),
         table when table != :undefined <- :ets.whereis(@ets_table),
         [{_pid, snapshot}] <- :ets.lookup(@ets_table, session) do
      {:ok, snapshot}
    else
      _ -> :error
    end
  end

  def fetch(session) do
    case GenServer.whereis(session) do
      pid when is_pid(pid) -> fetch(pid)
      _ -> :error
    end
  end
end
