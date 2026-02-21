defmodule SparkEx.ManagedStream do
  @moduledoc """
  Managed wrapper around response streams with explicit lifecycle controls.

  Features:
  - owner-process monitoring (auto-close on owner exit)
  - explicit `close/1`
  - optional idle timeout auto-close
  """

  @type release_fun :: (keyword() -> {:ok, term()} | {:error, term()} | term())

  @type t :: %__MODULE__{
          stream: Enumerable.t(),
          controller: pid()
        }

  defstruct [:stream, :controller]

  @doc false
  @spec new(Enumerable.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(stream, opts) do
    owner = Keyword.get(opts, :owner, self())
    idle_timeout = Keyword.get(opts, :idle_timeout)
    release_fun = Keyword.fetch!(opts, :release_fun)

    with {:ok, controller} <-
           SparkEx.ManagedStream.Controller.start_link(
             owner: owner,
             idle_timeout: idle_timeout,
             release_fun: release_fun
           ) do
      wrapped =
        Stream.transform(
          stream,
          fn -> :ok end,
          fn item, state ->
            SparkEx.ManagedStream.Controller.touch(controller)
            {[item], state}
          end,
          fn _ ->
            SparkEx.ManagedStream.Controller.close(controller, :stream_finished)
            :ok
          end
        )

      {:ok, %__MODULE__{stream: wrapped, controller: controller}}
    end
  end

  @doc """
  Closes the stream and releases server-side resources.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{controller: controller}) when is_pid(controller) do
    SparkEx.ManagedStream.Controller.close(controller, :explicit_close)
  end
end

defmodule SparkEx.ManagedStream.Controller do
  @moduledoc false

  use GenServer
  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec touch(pid()) :: :ok
  def touch(controller) when is_pid(controller) do
    GenServer.cast(controller, :touch)
  end

  @spec close(pid(), atom()) :: :ok
  def close(controller, reason) when is_pid(controller) and is_atom(reason) do
    GenServer.call(controller, {:close, reason})
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(opts) do
    owner = Keyword.get(opts, :owner, self())
    idle_timeout = Keyword.get(opts, :idle_timeout, nil)
    release_fun = Keyword.fetch!(opts, :release_fun)

    owner_ref = Process.monitor(owner)
    timer_ref = arm_idle_timer(idle_timeout)

    {:ok,
     %{
       owner: owner,
       owner_ref: owner_ref,
       idle_timeout: idle_timeout,
       timer_ref: timer_ref,
       release_fun: release_fun,
       closed?: false
     }}
  end

  @impl true
  def handle_cast(:touch, %{closed?: false} = state) do
    {:noreply, reset_idle_timer(state)}
  end

  def handle_cast(:touch, state), do: {:noreply, state}

  @impl true
  def handle_call({:close, reason}, _from, state) do
    {:ok, next_state} = do_close(reason, state)
    {:reply, :ok, next_state}
  end

  @impl true
  def handle_info(:idle_timeout, state) do
    {:ok, next_state} = do_close(:idle_timeout, state)
    {:noreply, next_state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_ref: ref} = state) do
    {:ok, next_state} = do_close(:owner_down, state)
    {:noreply, next_state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp do_close(_reason, %{closed?: true} = state), do: {:ok, state}

  defp do_close(_reason, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    case state.release_fun.([]) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.debug("managed stream release failed: #{inspect(reason)}")

      other ->
        Logger.debug("managed stream release returned: #{inspect(other)}")
    end

    Process.demonitor(state.owner_ref, [:flush])
    {:ok, %{state | closed?: true, timer_ref: nil}}
  end

  defp reset_idle_timer(%{idle_timeout: timeout, timer_ref: timer_ref} = state)
       when is_integer(timeout) and timeout > 0 do
    if timer_ref, do: Process.cancel_timer(timer_ref)
    %{state | timer_ref: Process.send_after(self(), :idle_timeout, timeout)}
  end

  defp reset_idle_timer(state), do: state

  defp arm_idle_timer(timeout) when is_integer(timeout) and timeout > 0 do
    Process.send_after(self(), :idle_timeout, timeout)
  end

  defp arm_idle_timer(_), do: nil
end

defimpl Enumerable, for: SparkEx.ManagedStream do
  def reduce(%SparkEx.ManagedStream{stream: stream}, acc, fun) do
    Enumerable.reduce(stream, acc, fun)
  end

  def member?(_managed, _value), do: {:error, SparkEx.ManagedStream}
  def count(_managed), do: {:error, SparkEx.ManagedStream}
  def slice(_managed), do: {:error, SparkEx.ManagedStream}
end
