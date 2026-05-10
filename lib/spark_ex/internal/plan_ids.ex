defmodule SparkEx.Internal.PlanIds do
  @moduledoc false

  # Per-process monotonic plan_id allocator.
  #
  # PySpark assigns a stable plan_id to each LogicalPlan at construction
  # time. SparkEx mirrors that exactly, including PySpark's read-then-
  # increment ordering: `_fresh_plan_id` returns the value before bumping
  # the counter, so the first DataFrame in a session gets `plan_id = 0`.
  # We use a *per-process* `:counters` ref (stashed in the process
  # dictionary) instead of a global atomic so plan_ids within one process
  # — typically one test, one caller of the public API, or one Session
  # GenServer — are sequential and contiguous starting at 0.
  #
  # Why 0-based matters: Spark Connect 3.5.x's analyzer treats `plan_id =
  # 0` as a fallback path for some rel_types whose proto field tags
  # (`UnresolvedTableValuedFunction` field 43, `Transpose`,
  # `SubqueryExpression` field 21) are absent from 3.5's proto surface.
  # Starting at any other value sends the same `oneof rel_type` field but
  # with a non-zero plan_id; the 3.5 analyzer then takes a strict-decode
  # path that fails with IndexOutOfBoundsException ("Expected Relation to
  # be set, but is empty") on those features. PySpark's choice of 0-based
  # IDs preserves the fallback path; we follow.
  #
  # Cross-process plan composition is supported: a DataFrame built in
  # process A may have plan_ids that equal ids B later allocates, but the
  # encoded plan tree is built and serialized in a single process —
  # collisions only matter when multiple processes feed the same encode
  # pass, which is not the SparkEx flow (the Session GenServer owns the
  # encode for its own DataFrames).

  @key __MODULE__

  @doc """
  Returns the next plan_id (sequential per calling process, starting at 0).

  PySpark allocates `_nextPlanId = 0` at session creation and `_fresh_plan_id`
  returns the value *before* incrementing. We mirror that read-then-increment
  ordering so the first DataFrame in a process gets `plan_id = 0`. Spark
  Connect 3.5.x's analyzer treats `plan_id = 0` as a fallback path for some
  rel_types whose proto field tags (e.g. `UnresolvedTableValuedFunction`,
  `Transpose`) are absent in 3.5's proto surface — starting at 1 instead
  caused IndexOutOfBoundsException on those tests.
  """
  @spec next() :: non_neg_integer()
  def next do
    ref = process_counter()
    id = :counters.get(ref, 1)
    :counters.add(ref, 1, 1)
    id
  end

  @doc """
  Wraps a raw plan tuple with a stable id.

  Idempotent: a plan that is already wrapped with `{:plan_id, _, _}` is
  returned as-is so chained DataFrame ops do not pile wrappers.
  """
  @spec wrap(term()) :: {:plan_id, non_neg_integer(), term()}
  def wrap({:plan_id, _, _} = plan), do: plan
  def wrap(plan), do: {:plan_id, next(), plan}

  @doc "Extracts the plan_id from a wrapped plan."
  @spec id_of(term()) :: non_neg_integer()
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
