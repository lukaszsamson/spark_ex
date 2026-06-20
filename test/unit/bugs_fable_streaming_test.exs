defmodule SparkEx.BugsFableStreamingTest do
  use ExUnit.Case, async: true

  alias SparkEx.Internal.StreamingTimeout
  alias SparkEx.StreamingQuery
  alias SparkEx.StreamingQueryManager
  alias SparkEx.StreamingQueryListenerBus

  # A fake session GenServer that records the opts passed to
  # execute_command_with_result and replies with a canned streaming result.
  defmodule RecordingSession do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts), do: {:ok, %{parent: Keyword.fetch!(opts, :parent)}}

    @impl true
    def handle_call({:execute_command_with_result, command, opts}, _from, state) do
      send(state.parent, {:recorded, command, opts})
      {:reply, reply_for(command), state}
    end

    defp reply_for({:streaming_query_command, _q, _r, {:await_termination, _ms}}) do
      {:ok,
       {:streaming_query,
        %Spark.Connect.StreamingQueryCommandResult{
          result_type:
            {:await_termination,
             %Spark.Connect.StreamingQueryCommandResult.AwaitTerminationResult{terminated: true}}
        }}}
    end

    defp reply_for({:streaming_query_command, _q, _r, {:process_all_available}}) do
      {:ok, {:streaming_query, %Spark.Connect.StreamingQueryCommandResult{result_type: nil}}}
    end

    defp reply_for({:streaming_query_manager_command, {:await_any_termination, _ms}}) do
      {:ok,
       {:streaming_query_manager,
        %Spark.Connect.StreamingQueryManagerCommandResult{
          result_type:
            {:await_any_termination,
             %Spark.Connect.StreamingQueryManagerCommandResult.AwaitAnyTerminationResult{
               terminated: true
             }}
        }}}
    end
  end

  # A fake session that serves a never-ending listener event stream, so the
  # listener bus's reader task stays alive (giving us a stable stream_task to
  # assert against) and registers successfully.
  defmodule StreamingFakeSession do
    use GenServer

    def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, %{}}

    @impl true
    def handle_call({:execute_command_stream, _command, _opts}, _from, state) do
      stream =
        Stream.resource(
          fn -> :added end,
          fn
            :added ->
              response = %Spark.Connect.ExecutePlanResponse{
                response_type:
                  {:streaming_query_listener_events_result,
                   %Spark.Connect.StreamingQueryListenerEventsResult{
                     listener_bus_listener_added: true,
                     events: []
                   }}
              }

              {[{:ok, response}], :idle}

            :idle ->
              Process.sleep(:infinity)
              {:halt, :ok}
          end,
          fn _ -> :ok end
        )

      {:reply, {:ok, stream}, state}
    end

    @impl true
    def handle_call({:execute_command_with_result, _command, _opts}, _from, state) do
      {:reply, {:ok, {:streaming_query_manager, %{}}}, state}
    end
  end

  defp query(session), do: %StreamingQuery{session: session, query_id: "q1", run_id: "r1"}

  # ── FABLE-03: no-timeout await forms must not inject a finite/invalid timeout ──

  describe "FABLE-03 await no-timeout timeout propagation" do
    test "StreamingTimeout.resolve maps nil to nil (→ :infinity at gRPC + GenServer)" do
      assert {nil, opts} = StreamingTimeout.resolve([])
      # The opts list carries :timeout = nil, which Session.call_timeout maps
      # to :infinity and the gRPC client maps to an infinite gRPC deadline.
      assert Keyword.fetch!(opts, :timeout) == nil
    end

    test "StreamingTimeout.resolve converts positive seconds to ms" do
      assert {5_000, opts} = StreamingTimeout.resolve(timeout: 5)
      assert Keyword.fetch!(opts, :timeout) == 5_000
    end

    test "await_termination with no timeout passes :timeout = nil (not finite, not absent)" do
      {:ok, session} = RecordingSession.start_link(parent: self())

      assert {:ok, nil} = StreamingQuery.await_termination(query(session))

      assert_receive {:recorded,
                      {:streaming_query_command, "q1", "r1", {:await_termination, nil}}, opts}

      # Key is present and nil — NOT a finite ms value and NOT absent (which
      # would inherit the 60s/65s defaults and ultimately crash on `after nil`).
      assert Keyword.has_key?(opts, :timeout)
      assert Keyword.fetch!(opts, :timeout) == nil
    end

    test "await_termination with a timeout passes the ms value" do
      {:ok, session} = RecordingSession.start_link(parent: self())

      assert {:ok, true} = StreamingQuery.await_termination(query(session), timeout: 5)

      assert_receive {:recorded,
                      {:streaming_query_command, "q1", "r1", {:await_termination, 5_000}}, opts}

      assert Keyword.fetch!(opts, :timeout) == 5_000
    end

    test "await_any_termination with no timeout passes :timeout = nil" do
      {:ok, session} = RecordingSession.start_link(parent: self())

      assert {:ok, nil} = StreamingQueryManager.await_any_termination(session)

      assert_receive {:recorded,
                      {:streaming_query_manager_command, {:await_any_termination, nil}}, opts}

      assert Keyword.fetch!(opts, :timeout) == nil
    end
  end

  # ── FABLE-25: process_all_available blocks indefinitely (infinite timeout) ──

  describe "FABLE-25 process_all_available" do
    test "passes :timeout = nil so both gRPC and GenServer calls are infinite" do
      {:ok, session} = RecordingSession.start_link(parent: self())

      assert :ok = StreamingQuery.process_all_available(query(session))

      assert_receive {:recorded, {:streaming_query_command, "q1", "r1", {:process_all_available}},
                      opts}

      assert Keyword.fetch!(opts, :timeout) == nil
      assert Keyword.get(opts, :reattach_policy) == :streaming
    end
  end

  # ── FABLE-49 + FABLE-50: synchronous started dispatch + lazy module loading ──

  defmodule SyncStartedListener do
    @behaviour SparkEx.StreamingQueryListener
    @pid_key {__MODULE__, :pid}

    def on_query_started(event) do
      pid = :persistent_term.get(@pid_key, nil)
      # Simulate non-trivial callback work; the caller must block until done.
      if pid, do: send(pid, {:on_started_ran, event})
    end

    def on_query_progress(_event), do: :ok
    def on_query_terminated(_event), do: :ok
    def on_query_idle(_event), do: :ok

    def set_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_pid, do: :persistent_term.erase(@pid_key)
  end

  describe "FABLE-49 synchronous QueryStarted dispatch" do
    test "post_query_started returns only after callbacks have run" do
      SyncStartedListener.set_pid(self())
      on_exit(fn -> SyncStartedListener.clear_pid() end)

      {:ok, session} = StreamingFakeSession.start_link()
      {:ok, bus} = StreamingQueryListenerBus.start_link(session)
      on_exit(fn -> if Process.alive?(bus), do: StreamingQueryListenerBus.stop(bus) end)

      :ok = StreamingQueryListenerBus.add_listener(bus, SyncStartedListener)
      :ok = StreamingQueryListenerBus.post_query_started(session, ~s({"id":"q1"}))

      # Because the dispatch is synchronous, the message is already in our
      # mailbox by the time post_query_started returns — assert without timeout.
      assert_received {:on_started_ran, %{type: :started, data: %{"id" => "q1"}}}
    end

    test "post_query_started tolerates a dead bus" do
      {:ok, session} = StreamingFakeSession.start_link()
      {:ok, bus} = StreamingQueryListenerBus.start_link(session)
      :ok = StreamingQueryListenerBus.stop(bus)
      # Registry may still hold a stale entry momentarily; the call must not raise.
      assert :ok = StreamingQueryListenerBus.post_query_started(session, ~s({"id":"q1"}))
    end
  end

  # ── FABLE-26: stale DOWN must not clobber the current stream task ──

  describe "FABLE-26 listener-bus stale DOWN handling" do
    test "DOWN from a non-current stream_task pid is ignored" do
      {:ok, session} = StreamingFakeSession.start_link()
      {:ok, bus} = StreamingQueryListenerBus.start_link(session)
      on_exit(fn -> if Process.alive?(bus), do: StreamingQueryListenerBus.stop(bus) end)

      # Force a current stream_task into place by adding a listener (the bus
      # spawns a reader task). We then fabricate a DOWN from a *different* pid.
      :ok = StreamingQueryListenerBus.add_listener(bus, SyncStartedListener)

      state_before = :sys.get_state(bus)
      current_task = state_before.stream_task
      assert is_pid(current_task)

      stale_pid = spawn(fn -> :ok end)
      # Ensure stale_pid != current_task.
      refute stale_pid == current_task

      # Deliver a fabricated stale DOWN (as if an old killed task died late).
      send(bus, {:DOWN, make_ref(), :process, stale_pid, :shutdown})

      # Give the bus a moment to process the message.
      :sys.get_state(bus)
      state_after = :sys.get_state(bus)

      # The current stream_task must be untouched — not clobbered to nil.
      assert state_after.stream_task == current_task
    end
  end

  # ── FABLE-27: ManagedStream controller stops after close (no process leak) ──

  describe "FABLE-27 ManagedStream controller termination" do
    test "controller process stops after explicit close" do
      {:ok, stream} =
        SparkEx.ManagedStream.new(Stream.repeatedly(fn -> :row end),
          release_fun: fn _opts -> {:ok, :released} end
        )

      controller = stream.controller
      assert Process.alive?(controller)

      :ok = SparkEx.ManagedStream.close(stream)

      assert wait_until_dead(controller, 1_000)
    end

    test "controller process stops after stream enumeration finishes" do
      {:ok, stream} =
        SparkEx.ManagedStream.new([1, 2, 3], release_fun: fn _opts -> {:ok, :released} end)

      controller = stream.controller
      assert Enum.to_list(stream) == [1, 2, 3]

      assert wait_until_dead(controller, 1_000)
    end

    test "controller process stops after idle timeout" do
      {:ok, stream} =
        SparkEx.ManagedStream.new(Stream.repeatedly(fn -> :row end),
          idle_timeout: 30,
          release_fun: fn _opts -> {:ok, :released} end
        )

      assert wait_until_dead(stream.controller, 1_000)
    end

    test "controller process stops after owner exit" do
      owner = spawn(fn -> Process.sleep(:infinity) end)

      {:ok, stream} =
        SparkEx.ManagedStream.new(Stream.repeatedly(fn -> :row end),
          owner: owner,
          release_fun: fn _opts -> {:ok, :released} end
        )

      Process.exit(owner, :kill)
      assert wait_until_dead(stream.controller, 1_000)
    end
  end

  defp wait_until_dead(pid, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until_dead(pid, deadline)
  end

  defp do_wait_until_dead(pid, deadline) do
    cond do
      not Process.alive?(pid) ->
        true

      System.monotonic_time(:millisecond) > deadline ->
        false

      true ->
        Process.sleep(10)
        do_wait_until_dead(pid, deadline)
    end
  end
end
