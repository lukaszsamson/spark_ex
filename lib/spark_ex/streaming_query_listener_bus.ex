defmodule SparkEx.StreamingQueryListenerBus do
  @moduledoc """
  Manages a persistent gRPC stream for receiving streaming query listener events.

  This process opens a server-side listener bus connection and dispatches
  events (started, progress, terminated, idle) to registered listener callbacks.

  ## Listener Behaviour

  Listeners must implement the `SparkEx.StreamingQueryListener` behaviour:

      defmodule MyListener do
        @behaviour SparkEx.StreamingQueryListener

        @impl true
        def on_query_progress(event), do: IO.inspect(event, label: "progress")

        @impl true
        def on_query_terminated(event), do: IO.inspect(event, label: "terminated")

        @impl true
        def on_query_idle(event), do: IO.inspect(event, label: "idle")
      end

  ## Usage

      {:ok, bus} = SparkEx.StreamingQueryListenerBus.start_link(session)
      :ok = SparkEx.StreamingQueryListenerBus.add_listener(bus, MyListener)
      # ... events will be dispatched to MyListener as they arrive ...
      :ok = SparkEx.StreamingQueryListenerBus.stop(bus)
  """

  use GenServer

  alias Spark.Connect.{ExecutePlanResponse, StreamingQueryListenerEvent}
  import Bitwise, only: [bsl: 2]
  require Logger

  defmodule Registry do
    @moduledoc false
    use GenServer

    @registry_table :spark_ex_streaming_listener_buses

    @spec ensure_started() :: :ok
    def ensure_started do
      case Process.whereis(__MODULE__) do
        nil ->
          case GenServer.start(__MODULE__, :ok, name: __MODULE__) do
            {:ok, _pid} -> :ok
            {:error, {:already_started, _pid}} -> :ok
          end

        _pid ->
          :ok
      end
    end

    @spec register(term(), pid()) :: :ok
    def register(session, pid) when is_pid(pid) do
      ensure_started()
      GenServer.call(__MODULE__, {:register, session, pid})
    end

    @spec unregister(term(), pid()) :: :ok
    def unregister(session, pid) when is_pid(pid) do
      ensure_started()
      GenServer.call(__MODULE__, {:unregister, session, pid})
    end

    @spec buses_for_session(term()) :: [pid()]
    def buses_for_session(session) do
      ensure_started()
      GenServer.call(__MODULE__, {:buses_for_session, session})
    end

    @impl true
    def init(:ok) do
      SparkEx.EtsTableOwner.ensure_table!(@registry_table, :bag)
      {:ok, %{refs: %{}}}
    end

    @impl true
    def handle_call({:register, session, pid}, _from, state) do
      existing =
        @registry_table
        |> :ets.lookup(session)
        |> Enum.find(fn
          {^session, ^pid, _ref} -> true
          _ -> false
        end)

      case existing do
        nil ->
          ref = Process.monitor(pid)
          true = :ets.insert(@registry_table, {session, pid, ref})
          {:reply, :ok, put_in(state.refs[ref], {session, pid})}

        {_session, _pid, _ref} ->
          {:reply, :ok, state}
      end
    end

    def handle_call({:unregister, session, pid}, _from, state) do
      entries =
        @registry_table
        |> :ets.lookup(session)
        |> Enum.filter(fn
          {^session, ^pid, _ref} -> true
          _ -> false
        end)

      next_state =
        Enum.reduce(entries, state, fn {^session, ^pid, ref}, acc ->
          Process.demonitor(ref, [:flush])
          true = :ets.delete_object(@registry_table, {session, pid, ref})
          %{acc | refs: Map.delete(acc.refs, ref)}
        end)

      {:reply, :ok, next_state}
    end

    def handle_call({:buses_for_session, session}, _from, state) do
      buses =
        @registry_table
        |> :ets.lookup(session)
        |> Enum.map(fn {^session, pid, _ref} -> pid end)

      {:reply, buses, state}
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
      case Map.pop(state.refs, ref) do
        {{session, pid}, refs} ->
          true = :ets.delete_object(@registry_table, {session, pid, ref})
          {:noreply, %{state | refs: refs}}

        {nil, _refs} ->
          {:noreply, state}
      end
    end
  end

  # Reconnect strategy: exponential backoff with a hard cap on the delay,
  # and a generous attempt ceiling so that long-lived buses survive transient
  # server-side disconnects without silently giving up. Persistent failures
  # are reported via `[:spark_ex, :streaming, :listener_bus, :reconnect]`
  # telemetry so operators can observe sustained reconnect storms.
  @max_reconnect_attempts 100
  @reconnect_backoff_base_ms 200
  @reconnect_backoff_cap_ms 30_000

  defstruct [
    :session,
    :stream_task,
    listeners: [],
    registered?: false,
    pending_register_waiters: [],
    closing_stream?: false,
    reconnect_attempts: 0
  ]

  # --- Public API ---

  @doc """
  Starts the listener bus process.

  The server-side event stream is opened lazily when the first listener is added.
  """
  @spec start_link(GenServer.server(), keyword()) :: GenServer.on_start()
  def start_link(session, opts \\ []) do
    GenServer.start_link(__MODULE__, {session, opts}, Keyword.take(opts, [:name]))
  end

  @doc """
  Adds a listener module to receive event callbacks.
  """
  @spec add_listener(GenServer.server(), module()) :: :ok | {:error, term()}
  def add_listener(bus, listener_module) when is_atom(listener_module) do
    GenServer.call(bus, {:add_listener, listener_module})
  end

  @doc """
  Removes a listener module.
  """
  @spec remove_listener(GenServer.server(), module()) :: :ok
  def remove_listener(bus, listener_module) when is_atom(listener_module) do
    GenServer.call(bus, {:remove_listener, listener_module})
  end

  @doc """
  Returns the list of registered listener modules.
  """
  @spec list_listeners(GenServer.server()) :: [module()]
  def list_listeners(bus) do
    GenServer.call(bus, :list_listeners)
  end

  @doc """
  Stops the listener bus and closes the event stream.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(bus) do
    GenServer.stop(bus, :normal)
  end

  @doc """
  Dispatches a QueryStarted-like event JSON to listener buses for the given session.

  Delivery is synchronous: this returns only after every bus for the session
  has finished invoking its listeners' `on_query_started` callbacks. PySpark
  guarantees `onQueryStart` runs on all listeners before
  `DataStreamWriter.start()` returns (sql/streaming/listener.py:88-95), so
  `StreamWriter.start/2` must not return while the started event is still
  queued (FABLE-49).
  """
  @spec post_query_started(GenServer.server(), String.t()) :: :ok
  def post_query_started(session, event_json) when is_binary(event_json) do
    event = %{
      type: :started,
      raw_json: event_json,
      data:
        case Jason.decode(event_json) do
          {:ok, parsed} -> parsed
          {:error, _} -> event_json
        end
    }

    buses_for_session(session)
    |> Enum.each(fn bus ->
      # Synchronous call so the callbacks run before we return. :infinity
      # because the synchronous-delivery guarantee must hold however long a
      # listener's on_query_started takes (the default 5s call timeout would
      # silently drop the event). Swallow a dead/stopping bus the same way a
      # cast would have been dropped.
      try do
        GenServer.call(bus, {:dispatch_event_sync, event}, :infinity)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  # --- GenServer callbacks ---

  @impl true
  def init({session, _opts}) do
    register_bus(session, self())
    {:ok, %__MODULE__{session: session}}
  end

  @impl true
  def handle_call({:add_listener, module}, from, state) do
    if module in state.listeners and state.registered? do
      {:reply, :ok, state}
    else
      listeners =
        if module in state.listeners, do: state.listeners, else: [module | state.listeners]

      cond do
        state.registered? ->
          {:reply, :ok, %{state | listeners: listeners}}

        state.stream_task != nil ->
          {:noreply,
           %{
             state
             | listeners: listeners,
               pending_register_waiters: [from | state.pending_register_waiters]
           }}

        true ->
          task = start_reader_task(state.session)

          {:noreply,
           %{
             state
             | stream_task: task,
               listeners: listeners,
               pending_register_waiters: [from | state.pending_register_waiters]
           }}
      end
    end
  end

  def handle_call({:remove_listener, module}, _from, state) do
    listeners = List.delete(state.listeners, module)

    if listeners == [] and state.stream_task != nil do
      {:reply, :ok, stop_event_stream(%{state | listeners: listeners})}
    else
      {:reply, :ok, %{state | listeners: listeners}}
    end
  end

  def handle_call(:list_listeners, _from, state) do
    {:reply, state.listeners, state}
  end

  # Synchronous dispatch: used for QueryStarted events so the caller (e.g.
  # StreamWriter.start/2) only returns after on_query_started callbacks have
  # run (FABLE-49).
  def handle_call({:dispatch_event_sync, event}, _from, state) do
    dispatch_event(state.listeners, event)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:dispatch_event, event}, state) do
    dispatch_event(state.listeners, event)
    {:noreply, state}
  end

  @impl true
  def handle_info({:listener_event, event}, state) do
    dispatch_event(state.listeners, event)
    {:noreply, state}
  end

  def handle_info(:listener_bus_registered, state) do
    Enum.each(state.pending_register_waiters, &GenServer.reply(&1, :ok))

    {:noreply,
     %{
       state
       | registered?: true,
         pending_register_waiters: [],
         closing_stream?: false,
         reconnect_attempts: 0
     }}
  end

  def handle_info({:listener_stream_ended, reason}, state) do
    case reason do
      :normal ->
        maybe_reconnect(state, "graceful EOF")

      {:error, _} = error ->
        Logger.warning("StreamingQueryListenerBus stream ended with error: #{inspect(error)}")
        maybe_reconnect(state, "transport error")

      other ->
        Enum.each(state.pending_register_waiters, &GenServer.reply(&1, {:error, other}))

        Logger.warning(
          "StreamingQueryListenerBus stream ended with unexpected reason: #{inspect(other)}"
        )

        {:noreply,
         %{
           state
           | stream_task: nil,
             registered?: false,
             pending_register_waiters: [],
             closing_stream?: false
         }}
    end
  end

  def handle_info(:reconnect_listener_stream, state) do
    cond do
      state.listeners == [] ->
        {:noreply, state}

      state.stream_task != nil ->
        {:noreply, state}

      true ->
        task = start_reader_task(state.session)
        {:noreply, %{state | stream_task: task}}
    end
  end

  # A DOWN message from a pid that is not the CURRENT `stream_task` is stale
  # — e.g. a task we already killed during a remove/re-add race, whose DOWN
  # arrives after a replacement task2 was started (or after the stream-ended
  # path already cleared `stream_task`). Acting on it would clobber
  # `stream_task` back to `nil` and reset `registered?`, leaking task2's gRPC
  # stream and the server-side listener forever (FABLE-26). PySpark serializes
  # its single handler thread under a lock; we serialize via this pid check.
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{stream_task: task} = state)
      when pid != task do
    {:noreply, state}
  end

  # From here on, `pid == state.stream_task`: this DOWN is authoritative.
  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
    {:noreply, %{state | stream_task: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, %{closing_stream?: true} = state) do
    {:noreply, %{state | stream_task: nil, closing_stream?: false}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Enum.each(state.pending_register_waiters, &GenServer.reply(&1, {:error, reason}))

    Logger.warning("StreamingQueryListenerBus stream task crashed: #{inspect(reason)}")

    {:noreply,
     %{
       state
       | stream_task: nil,
         registered?: false,
         pending_register_waiters: [],
         closing_stream?: false
     }}
  end

  @impl true
  def terminate(_reason, state) do
    unregister_bus(state.session, self())
    _ = stop_event_stream(state)

    :ok
  end

  # --- Private ---

  defp maybe_reconnect(state, context) do
    if state.listeners != [] and not state.closing_stream? and
         state.reconnect_attempts < @max_reconnect_attempts do
      attempts = state.reconnect_attempts + 1
      delay = reconnect_delay_ms(attempts)

      :telemetry.execute(
        [:spark_ex, :streaming, :listener_bus, :reconnect],
        %{attempt: attempts, delay_ms: delay},
        %{session: state.session, max_attempts: @max_reconnect_attempts}
      )

      Logger.debug(
        "StreamingQueryListenerBus reconnecting after #{context} (attempt #{attempts}/#{@max_reconnect_attempts}, delay #{delay}ms)"
      )

      Process.send_after(self(), :reconnect_listener_stream, delay)

      {:noreply,
       %{
         state
         | stream_task: nil,
           registered?: false,
           reconnect_attempts: attempts
       }}
    else
      if state.listeners != [] and not state.closing_stream? do
        :telemetry.execute(
          [:spark_ex, :streaming, :listener_bus, :reconnect_exhausted],
          %{attempts: state.reconnect_attempts},
          %{session: state.session, max_attempts: @max_reconnect_attempts}
        )

        Logger.warning(
          "StreamingQueryListenerBus reconnect attempts exhausted after #{state.reconnect_attempts} tries; bus is now inert (still alive, listeners retained)"
        )
      end

      Enum.each(
        state.pending_register_waiters,
        &GenServer.reply(&1, {:error, :stream_closed})
      )

      {:noreply,
       %{
         state
         | stream_task: nil,
           registered?: false,
           pending_register_waiters: [],
           closing_stream?: false
       }}
    end
  end

  @doc false
  # Exponential backoff: base * 2^(attempt-1), capped at the configured ceiling.
  # Exposed (module-private) for the reconnect path; pure function for tests.
  @spec reconnect_delay_ms(pos_integer()) :: pos_integer()
  def reconnect_delay_ms(attempt) when is_integer(attempt) and attempt >= 1 do
    shift = min(attempt - 1, 30)
    raw = @reconnect_backoff_base_ms * bsl(1, shift)
    min(raw, @reconnect_backoff_cap_ms)
  end

  defp start_reader_task(session) do
    parent = self()

    {pid, _ref} =
      spawn_monitor(fn ->
        case SparkEx.Session.execute_command_stream(
               session,
               {:streaming_query_listener_bus_command, :add}
             ) do
          {:ok, stream} ->
            read_events(stream, parent)

          {:error, reason} ->
            send(parent, {:listener_stream_ended, reason})
        end
      end)

    pid
  end

  defp read_events(stream, parent) do
    result =
      Enum.reduce_while(stream, :normal, fn
        {:ok, %ExecutePlanResponse{} = resp}, _acc ->
          case resp.response_type do
            {:streaming_query_listener_events_result, result} ->
              if result.listener_bus_listener_added == true do
                send(parent, :listener_bus_registered)
              end

              Enum.each(result.events, fn event ->
                send(parent, {:listener_event, parse_event(event)})
              end)

            _ ->
              :ok
          end

          {:cont, :normal}

        {:error, reason}, _acc ->
          Logger.warning("StreamingQueryListenerBus stream error: #{inspect(reason)}")
          {:halt, {:error, reason}}
      end)

    send(parent, {:listener_stream_ended, result})
  end

  defp stop_event_stream(%__MODULE__{stream_task: nil} = state), do: state

  defp stop_event_stream(state) do
    # Match PySpark listener bus lifecycle: when the last listener is removed,
    # explicitly request server-side listener removal.
    try do
      SparkEx.Session.execute_command_with_result(
        state.session,
        {:streaming_query_listener_bus_command, :remove}
      )
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    if state.stream_task do
      Process.exit(state.stream_task, :shutdown)
    end

    Enum.each(state.pending_register_waiters, &GenServer.reply(&1, {:error, :stream_closed}))

    %{
      state
      | stream_task: nil,
        registered?: false,
        pending_register_waiters: [],
        closing_stream?: true
    }
  end

  defp parse_event(%StreamingQueryListenerEvent{} = event) do
    event_type =
      case event.event_type do
        :QUERY_PROGRESS_EVENT -> :progress
        :QUERY_TERMINATED_EVENT -> :terminated
        :QUERY_IDLE_EVENT -> :idle
        _ -> :unknown
      end

    json =
      case Jason.decode(event.event_json) do
        {:ok, parsed} -> parsed
        {:error, _} -> event.event_json
      end

    %{type: event_type, data: json, raw_json: event.event_json}
  end

  defp dispatch_event(listeners, %{type: :progress} = event) do
    Enum.each(listeners, fn module ->
      safe_call(module, :on_query_progress, [event])
    end)
  end

  defp dispatch_event(listeners, %{type: :started} = event) do
    Enum.each(listeners, fn module ->
      # `on_query_started/1` is an optional callback, so we probe with
      # `function_exported?/3`. That returns false for a not-yet-loaded module
      # (common in dev/iex), which would silently skip the callback — so force
      # the module to load first (FABLE-50). The other callbacks are required
      # and dispatched via `apply/3`, which loads the module on demand.
      if Code.ensure_loaded?(module) and function_exported?(module, :on_query_started, 1) do
        safe_call(module, :on_query_started, [event])
      end
    end)
  end

  defp dispatch_event(listeners, %{type: :terminated} = event) do
    Enum.each(listeners, fn module ->
      safe_call(module, :on_query_terminated, [event])
    end)
  end

  defp dispatch_event(listeners, %{type: :idle} = event) do
    Enum.each(listeners, fn module ->
      safe_call(module, :on_query_idle, [event])
    end)
  end

  defp dispatch_event(_listeners, _event), do: :ok

  defp register_bus(session, pid) do
    Registry.register(session, pid)
  end

  defp unregister_bus(session, pid) do
    Registry.unregister(session, pid)
  end

  defp buses_for_session(session) do
    Registry.buses_for_session(session)
  end

  defp safe_call(module, function, args) do
    apply(module, function, args)
  rescue
    e ->
      Logger.warning(
        "Streaming listener #{inspect(module)}.#{function} failed: #{Exception.format(:error, e, __STACKTRACE__)}"
      )

      :ok
  catch
    kind, reason ->
      Logger.warning(
        "Streaming listener #{inspect(module)}.#{function} failed: #{inspect({kind, reason})}"
      )

      :ok
  end
end
