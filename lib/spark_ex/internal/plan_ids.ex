defmodule SparkEx.Internal.PlanIds do
  @moduledoc false

  # Global, monotonically-increasing allocator for DataFrame plan_ids.
  #
  # PySpark assigns a stable plan_id to each LogicalPlan at construction time;
  # SparkEx mirrors that with a global :atomics counter. Stable ids let the
  # encoder skip the counter+remap heuristic — every column reference can
  # carry the source DataFrame's id directly, and `RelationCommon.plan_id` is
  # set from the same id when the plan is encoded.
  #
  # Counter starts at 1 and increments forever. The encoder's per-pass
  # counter (used for synthetic ids like `with_relations` containers) also
  # draws from this allocator via `next_id/1`, so all ids are unique across
  # the entire process lifetime — no collisions between stable and synthetic
  # ids regardless of plan-tree composition.
  #
  # Spark Connect's `RelationCommon.plan_id` is `int64` on the wire, but
  # internal Scala code uses `Int` in some paths; ids stay safely within
  # signed-int32 range for any reasonable process lifetime.

  @key __MODULE__

  @doc false
  def init do
    case :persistent_term.get(@key, :undefined) do
      :undefined ->
        ref = :atomics.new(1, [])
        :persistent_term.put(@key, ref)
        ref

      ref ->
        ref
    end
  end

  @doc "Returns the next plan_id (monotonic, ≥ 1)."
  @spec next() :: pos_integer()
  def next do
    ref =
      case :persistent_term.get(@key, :undefined) do
        :undefined -> init()
        ref -> ref
      end

    :atomics.add_get(ref, 1, 1)
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
end
