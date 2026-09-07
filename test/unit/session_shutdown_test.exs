defmodule SparkEx.Unit.SessionShutdownTest do
  # Starts real (unreachable) sessions and supervisors; not safe to run async.
  use ExUnit.Case, async: false

  alias SparkEx.Internal.ReleaseTracker
  alias SparkEx.Test.BlackholeServer

  # Worst case for `SparkEx.Session.terminate/2` against an unresponsive
  # server: the release drain (`:release_drain_timeout_ms`, 0 here) plus the
  # ReleaseSession `Task.yield/2` (5s) plus `Task.shutdown/1`'s grace (5s).
  # Everything below must stay well inside this bound.
  @stop_bound_ms 15_000

  defp connect_to_blackhole(opts \\ []) do
    {:ok, port, _acceptor} = BlackholeServer.start_link()

    SparkEx.connect(
      Keyword.merge([url: "sc://127.0.0.1:#{port}", release_drain_timeout_ms: 0], opts)
    )
  end

  describe "unreachable-but-accepting server" do
    test "Session.stop/1 returns within a bounded time" do
      {:ok, session} = connect_to_blackhole()

      {elapsed, result} = :timer.tc(fn -> SparkEx.Session.stop(session) end, :millisecond)

      assert result == :ok
      assert elapsed < @stop_bound_ms
      refute Process.alive?(session)
    end

    test "a pending release cannot extend stop beyond the drain timeout" do
      {:ok, session} = connect_to_blackhole(release_drain_timeout_ms: 200)
      session_id = SparkEx.Session.get_state(session).session_id
      parent = self()

      {:ok, _pid} =
        ReleaseTracker.start_tracked(session_id, fn ->
          send(parent, :hanging_release_started)

          receive do
            :never -> :ok
          after
            60_000 -> :ok
          end
        end)

      assert_receive :hanging_release_started, 2_000

      {elapsed, :ok} = :timer.tc(fn -> SparkEx.Session.stop(session) end, :millisecond)

      assert elapsed >= 200
      assert elapsed < @stop_bound_ms

      ReleaseTracker.clear(session_id)
    end

    test "supervised shutdown terminates the session within a bounded time" do
      {:ok, port, _acceptor} = BlackholeServer.start_link()

      children = [
        %{
          id: :blackhole_session,
          start:
            {SparkEx.Session, :start_link,
             [[url: "sc://127.0.0.1:#{port}", release_drain_timeout_ms: 0]]},
          shutdown: @stop_bound_ms
        }
      ]

      {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)
      [{_id, session, _type, _mods}] = Supervisor.which_children(sup)
      ref = Process.monitor(session)

      {elapsed, :ok} = :timer.tc(fn -> Supervisor.stop(sup) end, :millisecond)

      assert_receive {:DOWN, ^ref, :process, ^session, _reason}, 1_000
      assert elapsed < @stop_bound_ms
    end

    test "Process.exit(pid, :shutdown) terminates the session within a bounded time" do
      # start_link links the session to this test process; trap the exit so
      # the session's :shutdown does not take the test down with it.
      Process.flag(:trap_exit, true)
      {:ok, session} = connect_to_blackhole()
      ref = Process.monitor(session)

      start = System.monotonic_time(:millisecond)
      Process.exit(session, :shutdown)

      assert_receive {:DOWN, ^ref, :process, ^session, :shutdown}, @stop_bound_ms
      assert System.monotonic_time(:millisecond) - start < @stop_bound_ms
    after
      Process.flag(:trap_exit, false)
    end
  end

  describe "closed port" do
    test "session creation fails fast instead of hanging" do
      port = BlackholeServer.closed_port()

      # Session.start_link/1 links the caller: an init failure exits the
      # caller with the init reason unless it traps exits.
      Process.flag(:trap_exit, true)

      {elapsed, result} =
        :timer.tc(fn -> SparkEx.connect(url: "sc://127.0.0.1:#{port}") end, :millisecond)

      assert {:error, reason} = result

      assert reason in [:timeout, :econnrefused, :closed],
             "expected a fail-fast connect error, got: #{inspect(reason)}"

      assert elapsed < @stop_bound_ms
    after
      Process.flag(:trap_exit, false)
    end
  end
end
