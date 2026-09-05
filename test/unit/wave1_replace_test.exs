defmodule SparkEx.Wave1ReplaceTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame

  # T-24: DataFrame.replace must not turn an omitted `value` into an explicit
  # nil replacement; the NA.replace guard has to fire through the delegation.

  defp df, do: %DataFrame{session: :fake, plan: {:plan_id, 1, {:range, 0, 1, 1, nil}}}

  test "replace/2 with a scalar raises because value is required" do
    assert_raise ArgumentError, ~r/requires a `value`/, fn ->
      DataFrame.replace(df(), "N/A")
    end
  end

  test "replace/3 with a scalar and only opts raises" do
    assert_raise ArgumentError, ~r/requires a `value`/, fn ->
      DataFrame.replace(df(), "N/A", subset: ["a"])
    end
  end

  test "replace/2 with a map does not require value" do
    assert %DataFrame{plan: {:plan_id, _, {:na_replace, _, _, _}}} =
             DataFrame.replace(df(), %{"N/A" => "missing"})
  end

  test "replace/3 with an explicit nil value treats nil as a real value" do
    assert %DataFrame{plan: {:plan_id, _, {:na_replace, _, _, _}}} =
             DataFrame.replace(df(), "N/A", nil)
  end

  test "replace/4 forwards value and opts" do
    assert %DataFrame{plan: {:plan_id, _, {:na_replace, _, ["a"], _}}} =
             DataFrame.replace(df(), "N/A", "x", subset: ["a"])
  end
end
