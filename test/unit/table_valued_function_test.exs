defmodule SparkEx.Unit.TableValuedFunctionTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.TableValuedFunction
  alias SparkEx.DataFrame
  alias SparkEx.Column

  test "call/3 builds a table valued function plan" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.call(tvf, "range", [1, 10])

    assert {:table_valued_function, "range", [{:lit, 1}, {:lit, 10}]} = unwrap_plan(df)
  end

  test "SparkEx.tvf/1 returns accessor" do
    tvf = SparkEx.tvf(self())

    assert %TableValuedFunction{} = tvf
    assert tvf.session == self()
  end

  test "explode/2 uses explode tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.explode(tvf, %Column{expr: {:col, "items"}})

    assert {:table_valued_function, "explode", [{:col, "items"}]} = unwrap_plan(df)
  end

  test "inline/2 uses inline tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.inline(tvf, %Column{expr: {:col, "records"}})

    assert {:table_valued_function, "inline", [{:col, "records"}]} = unwrap_plan(df)
  end

  test "stack/3 uses stack tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.stack(tvf, 2, [1, 2, 3, 4])

    assert {:table_valued_function, "stack",
            [{:lit, 2}, {:lit, 1}, {:lit, 2}, {:lit, 3}, {:lit, 4}]} = unwrap_plan(df)
  end

  test "stack/3 raises when num_rows is non-positive" do
    tvf = TableValuedFunction.new(self())

    assert_raise ArgumentError, ~r/positive integer/, fn ->
      TableValuedFunction.stack(tvf, 0, [1, 2])
    end
  end
end
