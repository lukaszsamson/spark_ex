defmodule SparkEx.Internal.PlanIds do
  @moduledoc false

  # Per-process monotonic plan_id allocator.
  #
  # PySpark assigns a stable plan_id to each LogicalPlan at construction time.
  # SparkEx mirrors that, but uses a *per-process* `:counters` ref (stashed in
  # the process dictionary) instead of a global atomic. Within a single
  # process — typically one test, one caller of the SparkEx public API, or
  # one Session GenServer — plan_ids are sequential and contiguous starting
  # at 1. That matches PySpark's per-session sequential allocation, which
  # Spark Connect's analyzer relies on (some 3.5.x paths use plan_id as an
  # array index and reject sparse / large values).
  #
  # Cross-process plan composition is supported: when a DataFrame built in
  # process A is referenced from process B, A's plan_ids may equal ids B
  # later allocates, but the encoded plan tree is built and serialized in a
  # single process — collisions only matter when multiple processes feed the
  # same encode pass, which is not the SparkEx flow (the Session GenServer
  # owns the encode for its own DataFrames).

  @key __MODULE__

  @doc "Returns the next plan_id (sequential per calling process, starting at 1)."
  @spec next() :: pos_integer()
  def next do
    ref = process_counter()
    :counters.add(ref, 1, 1)
    :counters.get(ref, 1)
  end

  @doc """
  Wraps a raw plan tuple with a stable id.

  Idempotent: a plan that is already wrapped with `{:plan_id, _, _}` is
  returned as-is so chained DataFrame ops do not pile wrappers.
  """
  @spec wrap(term()) :: {:plan_id, pos_integer(), term()}
  def wrap({:plan_id, _, _} = plan), do: plan
  def wrap(plan), do: {:plan_id, next(), plan}

  @doc "Extracts the plan_id from a wrapped plan."
  @spec id_of(term()) :: pos_integer()
  def id_of({:plan_id, id, _}) when is_integer(id), do: id

  def id_of(plan) do
    raise ArgumentError,
          "plan is not stamped with a stable plan_id (refactor expected " <>
            "every DataFrame plan to be wrapped via SparkEx.Internal.PlanIds.wrap/1). " <>
            "Got: #{inspect(plan)}"
  end

  # Application start hook (no-op now — kept for backward compatibility with
  # callers that were initializing the old persistent_term-based allocator).
  @doc false
  def init, do: :ok

  defp process_counter do
    case Process.get(@key) do
      nil ->
        ref = :counters.new(1, [])
        Process.put(@key, ref)
        ref

      ref ->
        ref
    end
  end
end
