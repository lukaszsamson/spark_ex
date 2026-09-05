defmodule SparkEx.ManagedStream do
  @moduledoc """
  Managed wrapper around response streams with explicit lifecycle controls.

  Features:
  - owner-process monitoring (auto-close on owner exit)
  - explicit `close/1`
  - optional idle timeout auto-close
  """

  @default_release_timeout 5_000

  @type release_fun :: (keyword() -> {:ok, term()} | {:error, term()} | term())

  @type t :: %__MODULE__{
          stream: Enumerable.t(),
          controller: pid()
        }

  defstruct [:stream, :controller]

  @doc false
  @spec new_closed_flag() :: :atomics.atomics_ref()
  def new_closed_flag, do: :atomics.new(1, signed: false)

  @doc false
  @spec closed?(:atomics.atomics_ref()) :: boolean()
  def closed?(flag), do: :atomics.get(flag, 1) == 1

  @doc false
  @spec new(Enumerable.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(stream, opts) do
    owner = Keyword.get(opts, :owner, self())
    idle_timeout = Keyword.get(opts, :idle_timeout)
    release_fun = Keyword.fetch!(opts, :release_fun)
    release_timeout = Keyword.get(opts, :release_timeout, @default_release_timeout)
    closed_flag = Keyword.get_lazy(opts, :closed_flag, &new_closed_flag/0)

    # T-03: the controller must NOT be linked to the caller. The caller is
    # almost always the owner, and an abnormal owner exit would take a linked
    # controller down before it can act on the owner-monitor :DOWN — leaking
    # the remote execution on exactly the failure path the monitor exists to
    # cover. The controller is short-lived, stops itself after release, and is
    # cleaned up by the owner monitor, so an unlinked start is safe.
    with {:ok, controller} <-
           SparkEx.ManagedStream.Controller.start(
             owner: owner,
             idle_timeout: idle_timeout,
             release_fun: release_fun,
             release_timeout: release_timeout,
             closed_flag: closed_flag
           ) do
      wrapped =
        Stream.transform(
          stream,
          fn -> :ok end,
          fn item, state ->
            # A close from another process (owner/explicit) while this one is
            # enumerating: stop yielding instead of continuing to consume an
            # operation whose server-side state has been released.
            if closed?(closed_flag) do
              {:halt, state}
            else
              SparkEx.ManagedStream.Controller.touch(controller)
              {[item], state}
            end
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

  @spec start(keyword()) :: GenServer.on_start()
  def start(opts) do
    GenServer.start(__MODULE__, opts)
  end

  @doc false
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
    release_timeout = Keyword.get(opts, :release_timeout, 5_000)
    closed_flag = Keyword.get_lazy(opts, :closed_flag, &SparkEx.ManagedStream.new_closed_flag/0)

    owner_ref = Process.monitor(owner)
    timer_ref = arm_idle_timer(idle_timeout)

    {:ok,
     %{
       owner: owner,
       owner_ref: owner_ref,
       idle_timeout: idle_timeout,
       timer_ref: timer_ref,
       release_fun: release_fun,
       release_timeout: release_timeout,
       closed_flag: closed_flag,
       closed?: false
     }}
  end

  @impl true
  def handle_cast(:touch, %{closed?: false} = state) do
    {:noreply, reset_idle_timer(state)}
  end

  def handle_cast(:touch, state), do: {:noreply, state}

  # Once the stream is closed and the release task has been fired, the
  # controller has no further work to do. Stop it so we don't leak one
  # inert GenServer per finished/closed managed stream (FABLE-27). The
  # already-closed branch of `do_close` returns the unchanged state without
  # re-firing release; a redundant close on a stopped controller is harmless
  # because `Controller.close/2` swallows the `:exit`.
  @impl true
  def handle_call({:close, reason}, _from, state) do
    {:ok, next_state} = do_close(reason, state)
    {:stop, :normal, :ok, next_state}
  end

  @impl true
  def handle_info(:idle_timeout, state) do
    {:ok, next_state} = do_close(:idle_timeout, state)
    {:stop, :normal, next_state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_ref: ref} = state) do
    {:ok, next_state} = do_close(:owner_down, state)
    {:stop, :normal, next_state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp do_close(_reason, %{closed?: true} = state), do: {:ok, state}

  defp do_close(_reason, state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    Process.demonitor(state.owner_ref, [:flush])
    :atomics.put(state.closed_flag, 1, 1)
    start_async_release(state.release_fun, state.release_timeout)

    {:ok, %{state | closed?: true, timer_ref: nil}}
  end

  defp start_async_release(release_fun, timeout_ms) do
    # T-19: during application shutdown the TaskSupervisor may already be
    # gone; start_child then exits with :noproc instead of returning an error
    # tuple, which is what the helper normalises.
    case SparkEx.Connect.Client.start_supervised_task(fn ->
           run_release_fun(release_fun, timeout_ms)
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        log_release_failure({:task_start_failed, reason}, timeout_ms)
    end
  end

  # If the release RPC does not finish within `timeout_ms`, the local task
  # is given a short graceful-shutdown grace period (via `Task.shutdown/2`)
  # before being killed. This lets the gRPC client cancel the in-flight
  # call cleanly rather than leaking a half-open stream the way the prior
  # `:brutal_kill` could. The server retains buffer state for the operation
  # until its own GC reclaims it (typically after the operation's
  # idle/retention timeout); increase `release_timeout` if you need a
  # stricter guarantee that the release was acknowledged before the
  # controller exits.
  @release_shutdown_grace_ms 1_000

  defp run_release_fun(release_fun, timeout_ms) do
    case SparkEx.Connect.Client.async_nolink_supervised(fn ->
           release_fun.(timeout: timeout_ms)
         end) do
      {:ok, task} -> await_release(task, timeout_ms)
      {:error, :noproc} -> log_release_failure({:task_start_failed, :noproc}, timeout_ms)
    end
  end

  defp await_release(task, timeout_ms) do
    case Task.yield(task, timeout_ms) || Task.shutdown(task, @release_shutdown_grace_ms) do
      {:ok, {:ok, _}} ->
        :ok

      {:ok, {:error, reason}} ->
        log_release_failure(reason, timeout_ms)

      {:ok, other} ->
        log_release_failure({:unexpected_release_result, other}, timeout_ms)

      {:exit, reason} ->
        log_release_failure({:task_exit, reason}, timeout_ms)

      nil ->
        log_release_failure(:timeout, timeout_ms)
    end
  end

  defp log_release_failure(reason, timeout_ms) do
    Logger.debug("managed stream release failed: #{inspect(reason)}")

    :telemetry.execute(
      [:spark_ex, :managed_stream, :release_failed],
      %{},
      %{reason: reason, timeout_ms: timeout_ms}
    )
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
