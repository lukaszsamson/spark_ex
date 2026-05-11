defmodule SparkEx.Test.TestSession do
  @moduledoc """
  Minimal `GenServer` that registers a `SparkEx.Internal.PlanIds` allocator
  in its `init/1` and exposes `encode/2` so unit tests can run the
  `PlanEncoder` *inside* the session process — matching the production
  flow where `Session.safe_encode_with/3` invokes the encoder from the
  GenServer that owns the atomic. Without this, encoder calls in test
  code would see `self()` = test process and fall back to a per-process
  counter that doesn't share the session's id namespace.
  """

  use GenServer

  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.Internal.PlanIds

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  @spec encode(GenServer.server(), term()) :: Spark.Connect.Plan.t()
  def encode(server, plan_term) do
    GenServer.call(server, {:encode, plan_term}, 60_000)
  end

  @impl true
  def init(:ok) do
    PlanIds.register_session(self())
    {:ok, %{}}
  end

  @impl true
  def handle_call({:encode, plan_term}, _from, state) do
    counter = PlanIds.peek(self())
    {encoded, _} = PlanEncoder.encode(plan_term, counter)
    {:reply, encoded, state}
  end

  @impl true
  def terminate(_reason, _state) do
    PlanIds.unregister_session(self())
    :ok
  end
end
