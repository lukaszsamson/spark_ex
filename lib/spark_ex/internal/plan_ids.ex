defmodule SparkEx.Internal.PlanIds do
  @moduledoc false

  # Global, monotonically-increasing allocator for DataFrame plan_ids.
  #
  # PySpark assigns a stable plan_id to each LogicalPlan at construction time;
  # SparkEx mirrors that with a global atomic counter. Stable ids let the
  # encoder skip the counter+remap heuristic — every column reference can carry
  # the source DataFrame's id directly, and `RelationCommon.plan_id` is set
  # from the same id when the plan is encoded.
  #
  # Started at a high base (2^31) so stable ids cannot collide with encoder
  # counter ids (which start near 0 inside `PlanEncoder.encode/2` for synthetic
  # wrappers like `with_relations`).

  import Bitwise

  @key __MODULE__
  @base 1 <<< 31

  @doc false
  def init do
    case :persistent_term.get(@key, :undefined) do
      :undefined ->
        ref = :atomics.new(1, [])
        :atomics.put(ref, 1, @base)
        :persistent_term.put(@key, ref)
        ref

      ref ->
        ref
    end
  end

  @doc "Returns the next plan_id (monotonic, ≥ 2^31)."
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
