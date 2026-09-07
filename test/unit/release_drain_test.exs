defmodule SparkEx.Unit.ReleaseDrainTest do
  # Touches the process-global release-task table and telemetry handlers.
  use ExUnit.Case, async: false

  alias Spark.Connect.{ExecutePlanResponse, Plan}
  alias SparkEx.Connect.Client
  alias SparkEx.Internal.ReleaseTracker
  alias SparkEx.ManagedStream
  alias SparkEx.Test.BlackholeServer

  defp unique_session_id, do: "sess-" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

  defp gate do
    parent = self()

    fn ->
      send(parent, :release_started)

      receive do
        :release_go -> :ok
      after
        30_000 -> :ok
      end
    end
  end

  describe "ReleaseTracker" do
    test "await_pending returns only after the pending release finished" do
      session_id = unique_session_id()
      parent = self()

      {:ok, _pid} =
        ReleaseTracker.start_tracked(session_id, fn ->
          Process.sleep(150)
          send(parent, :release_done)
          :ok
        end)

      assert ReleaseTracker.pending_count(session_id) == 1
      assert ReleaseTracker.await_pending(session_id, 5_000) == :ok
      # Ordering: the release completed *before* await returned.
      assert_received :release_done
      assert ReleaseTracker.pending_count(session_id) == 0
    end

    test "await_pending is bounded when a release hangs" do
      session_id = unique_session_id()
      hang = gate()

      {:ok, _pid} = ReleaseTracker.start_tracked(session_id, hang)
      assert_receive :release_started, 1_000

      {elapsed, result} =
        :timer.tc(fn -> ReleaseTracker.await_pending(session_id, 200) end, :millisecond)

      assert result == {:timeout, 1}
      assert elapsed >= 190
      assert elapsed < 2_000

      send_release_go()
      ReleaseTracker.clear(session_id)
    end

    test "another session's pending release is not awaited" do
      mine = unique_session_id()
      other = unique_session_id()
      hang = gate()

      {:ok, _pid} = ReleaseTracker.start_tracked(other, hang)
      assert_receive :release_started, 1_000

      {elapsed, result} =
        :timer.tc(fn -> ReleaseTracker.await_pending(mine, 5_000) end, :millisecond)

      assert result == :ok
      assert elapsed < 500
      assert ReleaseTracker.pending_count(other) == 1

      send_release_go()
      ReleaseTracker.clear(other)
    end

    test "a release task that dies without cleanup is pruned instead of wedging the drain" do
      session_id = unique_session_id()

      {:ok, pid} =
        ReleaseTracker.start_tracked(session_id, fn ->
          receive do
            :never -> :ok
          end
        end)

      assert ReleaseTracker.pending_count(session_id) == 1
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

      assert ReleaseTracker.await_pending(session_id, 1_000) == :ok
    end
  end

  describe "release tasks are tracked per session" do
    test "reattach stream release_all is tracked against the session" do
      session_id = unique_session_id()

      session = %SparkEx.Session{
        channel: nil,
        session_id: session_id,
        server_side_session_id: "server-side",
        user_id: "test_user",
        client_type: "elixir/test"
      }

      execute_stream_fun = fn _request, _timeout ->
        {:ok,
         [
           {:ok,
            %ExecutePlanResponse{
              response_id: "r1",
              response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
            }}
         ]}
      end

      hang = gate()

      # Run the enumeration elsewhere: the tracked release_all task must be
      # observable as pending *while* it is in flight.
      task =
        Task.async(fn ->
          Client.execute_plan(session, %Plan{},
            execute_stream_fun: execute_stream_fun,
            reattach_stream_fun: fn _ -> {:ok, []} end,
            release_execute_fun: fn _opts -> hang.() end
          )
        end)

      assert_receive :release_started, 2_000
      assert ReleaseTracker.pending_count(session_id) == 1

      send_release_go()
      assert {:ok, _} = Task.await(task, 20_000)
      ReleaseTracker.clear(session_id)
    end

    test "managed stream release is tracked against the session" do
      session_id = unique_session_id()
      hang = gate()

      {:ok, stream} =
        ManagedStream.new(Stream.repeatedly(fn -> :row end),
          session_id: session_id,
          release_fun: fn _opts -> hang.() end
        )

      :ok = ManagedStream.close(stream)

      assert_receive :release_started, 2_000
      assert ReleaseTracker.pending_count(session_id) == 1

      send_release_go()
      ReleaseTracker.clear(session_id)
    end
  end

  describe "Session.stop/1 drains releases before ReleaseSession" do
    setup do
      {:ok, port, _acceptor} = BlackholeServer.start_link()
      {:ok, port: port}
    end

    test "pending release completes before ReleaseSession is sent", %{port: port} do
      {:ok, session} = SparkEx.connect(url: "sc://127.0.0.1:#{port}")
      session_id = SparkEx.Session.get_state(session).session_id

      attach_release_session_telemetry(session_id)
      parent = self()

      {:ok, _pid} =
        ReleaseTracker.start_tracked(session_id, fn ->
          Process.sleep(200)
          send(parent, :release_execute_done)
          :ok
        end)

      :ok = SparkEx.Session.stop(session)

      assert receive_order(session_id, 2) == [
               :release_execute_done,
               {:release_session_rpc, session_id}
             ]
    end

    test "explicit Session.release/1 also drains pending releases first", %{port: port} do
      {:ok, session} = SparkEx.connect(url: "sc://127.0.0.1:#{port}")
      session_id = SparkEx.Session.get_state(session).session_id

      attach_release_session_telemetry(session_id)
      parent = self()

      {:ok, _pid} =
        ReleaseTracker.start_tracked(session_id, fn ->
          Process.sleep(200)
          send(parent, :release_execute_done)
          :ok
        end)

      # The ReleaseSession RPC itself hangs against the black hole, so run the
      # call off-test and only observe the ordering of the two events.
      spawn(fn -> catch_exit(SparkEx.Session.release(session)) end)

      assert receive_order(session_id, 2) == [
               :release_execute_done,
               {:release_session_rpc, session_id}
             ]

      :ok = SparkEx.Session.stop(session)
    end

    test "child_spec sizes the supervisor shutdown to the drain budget" do
      default = SparkEx.Session.child_spec(url: "sc://127.0.0.1:1")
      assert default.shutdown > 10_000
      assert default.restart == :temporary

      custom = SparkEx.Session.child_spec(url: "sc://127.0.0.1:1", release_drain_timeout_ms: 300)
      assert custom.shutdown == 300 + 10_000 + 1_000

      assert_raise ArgumentError, ~r/release_drain_timeout_ms/, fn ->
        SparkEx.Session.child_spec(url: "sc://127.0.0.1:1", release_drain_timeout_ms: -1)
      end
    end

    test "a hanging release does not block stop past the drain timeout", %{port: port} do
      {:ok, session} =
        SparkEx.connect(url: "sc://127.0.0.1:#{port}", release_drain_timeout_ms: 300)

      session_id = SparkEx.Session.get_state(session).session_id
      hang = gate()
      {:ok, _pid} = ReleaseTracker.start_tracked(session_id, hang)
      assert_receive :release_started, 2_000

      {elapsed, :ok} = :timer.tc(fn -> SparkEx.Session.stop(session) end, :millisecond)

      # 300ms drain + the bounded ReleaseSession attempt against a black hole.
      assert elapsed >= 300
      assert elapsed < 15_000

      send_release_go()
      ReleaseTracker.clear(session_id)
    end

    test "stop is idempotent and sends no RPC after the channel is closed", %{port: port} do
      {:ok, session} = SparkEx.connect(url: "sc://127.0.0.1:#{port}")
      session_id = SparkEx.Session.get_state(session).session_id
      attach_release_session_telemetry(session_id)

      assert :ok = SparkEx.Session.stop(session)
      assert_receive {:release_session_rpc, ^session_id}, 5_000
      # A retried ReleaseSession attempt may still be queued; drop those so
      # the refute below only sees RPCs caused by the *second* stop.
      flush_release_session_rpcs(session_id)

      assert :ok = SparkEx.Session.stop(session)
      refute_receive {:release_session_rpc, ^session_id}, 200
      assert ReleaseTracker.pending_count(session_id) == 0
    end
  end

  defp send_release_go do
    # The gate closure captured the test pid; releasing it lets the tracked
    # task finish so it does not linger past the test.
    for pid <- Task.Supervisor.children(SparkEx.TaskSupervisor), do: send(pid, :release_go)
    :ok
  end

  defp attach_release_session_telemetry(session_id) do
    parent = self()
    handler_id = {__MODULE__, session_id, System.unique_integer()}

    :telemetry.attach(
      handler_id,
      [:spark_ex, :rpc, :start],
      fn _event, _measurements, metadata, _config ->
        if metadata[:rpc] == :release_session and metadata[:session_id] == session_id do
          send(parent, {:release_session_rpc, session_id})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  # Collects the next `count` release-lifecycle messages in arrival order.
  defp receive_order(_session_id, 0), do: []

  defp receive_order(session_id, count) do
    receive do
      :release_execute_done = msg ->
        [msg | receive_order(session_id, count - 1)]

      {:release_session_rpc, ^session_id} = msg ->
        [msg | receive_order(session_id, count - 1)]
    after
      10_000 -> []
    end
  end

  defp flush_release_session_rpcs(session_id) do
    receive do
      {:release_session_rpc, ^session_id} -> flush_release_session_rpcs(session_id)
    after
      300 -> :ok
    end
  end
end
