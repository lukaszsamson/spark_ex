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
  require Logger

  defmodule Registry do
    @moduledoc false
    use GenServer

    @registry_table :spark_ex_streaming_listener_buses

    @spec ensure_started() :: :ok
    def ensure_started do
      case Process.whereis(__MODULE__) do
        nil ->
          case GenServer.start_link(__MODULE__, :ok, name: __MODULE__) do
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

  defstruct [
    :session,
    :stream_task,
    listeners: []
  ]

  # --- Public API ---

  @doc """
  Starts the listener bus process and opens the server-side event stream.
  """
  @spec start_link(GenServer.server(), keyword()) :: GenServer.on_start()
  def start_link(session, opts \\ []) do
    GenServer.start_link(__MODULE__, {session, opts}, Keyword.take(opts, [:name]))
  end

  @doc """
  Adds a listener module to receive event callbacks.
  """
  @spec add_listener(GenServer.server(), module()) :: :ok
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
      GenServer.cast(bus, {:dispatch_event, event})
    end)

    :ok
  end

  # --- GenServer callbacks ---

  @impl true
  def init({session, _opts}) do
    state = %__MODULE__{session: session}

    case start_event_stream(session) do
      {:ok, stream} ->
        register_bus(session, self())
        task = start_reader_task(stream)
        {:ok, %{state | stream_task: task}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:add_listener, module}, _from, state) do
    if module in state.listeners do
      {:reply, :ok, state}
    else
      {:reply, :ok, %{state | listeners: [module | state.listeners]}}
    end
  end

  def handle_call({:remove_listener, module}, _from, state) do
    {:reply, :ok, %{state | listeners: List.delete(state.listeners, module)}}
  end

  def handle_call(:list_listeners, _from, state) do
    {:reply, state.listeners, state}
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

  def handle_info({:listener_stream_ended, reason}, state) do
    case reason do
      :normal -> {:stop, :normal, state}
      error -> {:stop, {:stream_error, error}, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    {:stop, {:stream_task_crash, reason}, state}
  end

  @impl true
  def terminate(_reason, state) do
    unregister_bus(state.session, self())

    if state.stream_task do
      Process.exit(state.stream_task, :shutdown)
    end

    # Best-effort: send remove command to close server-side listener
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

    :ok
  end

  # --- Private ---

  defp start_event_stream(session) do
    SparkEx.Session.execute_command_stream(
      session,
      {:streaming_query_listener_bus_command, :add}
    )
  end

  defp start_reader_task(stream) do
    parent = self()

    {pid, _ref} =
      spawn_monitor(fn ->
        read_events(stream, parent)
      end)

    pid
  end

  defp read_events(stream, parent) do
    result =
      Enum.reduce_while(stream, :normal, fn
        {:ok, %ExecutePlanResponse{} = resp}, _acc ->
          case resp.response_type do
            {:streaming_query_listener_events_result, result} ->
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
      if function_exported?(module, :on_query_started, 1) do
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
