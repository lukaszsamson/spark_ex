defmodule SparkEx.Spark42NearestByJoinTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{Expression, Plan}
  alias SparkEx.{Column, DataFrame}
  alias SparkEx.Connect.PlanEncoder

  defp inputs(session \\ self()) do
    left = DataFrame.new(session, {:sql, "SELECT 1 AS l", []})
    right = DataFrame.new(session, {:sql, "SELECT 2 AS r", []})
    {left, right}
  end

  test "builds and encodes the complete nearest-by relation with bound references" do
    {left, right} = inputs()

    ranking = %Column{
      expr:
        {:fn, "subtract", [DataFrame.col(left, "l").expr, DataFrame.col(right, "r").expr], false}
    }

    result =
      DataFrame.nearest_by_join(left, right, ranking, 7,
        mode: :exact,
        direction: :distance,
        join_type: :left_outer
      )

    %DataFrame{plan: {:plan_id, result_id, nearest_plan}} = result
    {%Plan{op_type: {:root, encoded}}, _} = PlanEncoder.encode(result.plan, 0)

    assert encoded.common.plan_id == result_id

    assert {:nearest_by_join, nearest} = encoded.rel_type
    assert nearest.num_results == 7
    assert nearest.join_type == "leftouter"
    assert nearest.mode == "exact"
    assert nearest.direction == "distance"
    assert nearest.left.common.plan_id == elem(left.plan, 1)
    assert nearest.right.common.plan_id == elem(right.plan, 1)

    assert %Expression{expr_type: {:unresolved_function, function}} = nearest.ranking_expression
    assert function.function_name == "subtract"

    assert Enum.map(function.arguments, fn
             %Expression{expr_type: {:unresolved_attribute, attribute}} -> attribute.plan_id
           end) == [elem(left.plan, 1), elem(right.plan, 1)]

    assert match?({:nearest_by_join, _, _, _, 7, "leftouter", "exact", "distance"}, nearest_plan)
  end

  test "preserves observations on both inline inputs" do
    {left, right} = inputs()

    left =
      DataFrame.update_plan(
        left,
        {:collect_metrics, left.plan, "left_obs", [{:fn, "count", [{:star}], false}]}
      )

    right =
      DataFrame.update_plan(
        right,
        {:collect_metrics, right.plan, "right_obs", [{:fn, "count", [{:star}], false}]}
      )

    result =
      DataFrame.nearest_by_join(left, right, %Column{expr: {:lit, 1}}, 1,
        mode: "approx",
        direction: "similarity"
      )

    {%Plan{op_type: {:root, root}}, _} = PlanEncoder.encode(result.plan, 0)
    {:nearest_by_join, nearest} = root.rel_type
    assert {:collect_metrics, %{name: "left_obs"}} = nearest.left.rel_type
    assert {:collect_metrics, %{name: "right_obs"}} = nearest.right.rel_type
  end

  test "normalizes documented aliases and case" do
    {left, right} = inputs()

    for {join_type, expected} <- [
          inner: "inner",
          left: "leftouter",
          left_outer: "leftouter",
          "LEFT OUTER": "leftouter"
        ] do
      result =
        DataFrame.nearest_by_join(left, right, %Column{expr: {:lit, 1}}, 1,
          mode: "APPROX",
          direction: "SIMILARITY",
          join_type: join_type
        )

      assert {:plan_id, _, {:nearest_by_join, _, _, _, 1, ^expected, "approx", "similarity"}} =
               result.plan
    end
  end

  test "rejects invalid arguments before encoding" do
    {left, right} = inputs()
    ranking = %Column{expr: {:lit, 1}}

    for k <- [0, 100_001, 1.0, nil] do
      assert_raise ArgumentError, ~r/num_results/, fn ->
        DataFrame.nearest_by_join(left, right, ranking, k,
          mode: :exact,
          direction: :distance
        )
      end
    end

    assert_raise ArgumentError, ~r/requires the :mode option/, fn ->
      DataFrame.nearest_by_join(left, right, ranking, 1, direction: :distance)
    end

    assert_raise ArgumentError, ~r/requires the :direction option/, fn ->
      DataFrame.nearest_by_join(left, right, ranking, 1, mode: :exact)
    end

    for {opts, message} <- [
          {[mode: :bogus, direction: :distance], ~r/:mode/},
          {[mode: :exact, direction: :bogus], ~r/:direction/},
          {[mode: :exact, direction: :distance, join_type: :outer], ~r/:join_type/},
          {[mode: :exact, direction: :distance, extra: true], ~r/unknown/}
        ] do
      assert_raise ArgumentError, message, fn ->
        DataFrame.nearest_by_join(left, right, ranking, 1, opts)
      end
    end
  end

  test "requires a Column, keyword options, and the same session" do
    {left, right} = inputs()
    ranking = %Column{expr: {:lit, 1}}

    assert_raise ArgumentError, ~r/SparkEx.Column/, fn ->
      DataFrame.nearest_by_join(left, right, "l", 1, mode: :exact, direction: :distance)
    end

    assert_raise ArgumentError, ~r/keyword list/, fn ->
      DataFrame.nearest_by_join(left, right, ranking, 1, %{})
    end

    other_session = DataFrame.new(:another_session, {:sql, "SELECT 2", []})

    assert_raise ArgumentError, ~r/different sessions/, fn ->
      DataFrame.nearest_by_join(left, other_session, ranking, 1,
        mode: :exact,
        direction: :distance
      )
    end
  end
end
