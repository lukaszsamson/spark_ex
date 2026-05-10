defmodule SparkEx.Test.PlanHelpers do
  @moduledoc """
  Helpers for unit tests that pattern-match on internal DataFrame plan tuples.

  After the stable plan_id refactor, every plan tuple is wrapped with
  `{:plan_id, integer_id, raw_tuple}` at construction. Tests that assert on
  the raw tuple shape can either:

  1. Pattern-match through the wrapper explicitly:
     `assert {:plan_id, _, {:project, _, _}} = df.plan`

  2. Strip wrappers via `unwrap/1` and assert on the bare shape:
     `assert {:project, _, _} = unwrap(df.plan)`

  `unwrap/1` recursively removes plan_id wrappers from a plan term so that
  the inner tuple matches what tests had before the refactor.
  """

  @doc "Recursively strips `{:plan_id, _, inner}` wrappers from a plan term."
  def unwrap({:plan_id, _, inner}), do: unwrap(inner)

  def unwrap(plan) when is_tuple(plan) do
    plan
    |> Tuple.to_list()
    |> Enum.map(&unwrap/1)
    |> List.to_tuple()
  end

  def unwrap(list) when is_list(list), do: Enum.map(list, &unwrap/1)
  def unwrap(other), do: other

  @doc "Convenience for pattern-friendly access: `unwrap_plan(df)` == `unwrap(df.plan)`."
  def unwrap_plan(%SparkEx.DataFrame{plan: plan}), do: unwrap(plan)
end
