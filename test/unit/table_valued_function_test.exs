defmodule SparkEx.Unit.TableValuedFunctionTest do
  use ExUnit.Case, async: true

  alias SparkEx.TableValuedFunction
  alias SparkEx.DataFrame
  alias SparkEx.Column

  test "call/3 builds a table valued function plan" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.call(tvf, "range", [1, 10])

    assert %DataFrame{plan: {:table_valued_function, "range", [{:lit, 1}, {:lit, 10}]}} = df
  end

  test "SparkEx.tvf/1 returns accessor" do
    tvf = SparkEx.tvf(self())

    assert %TableValuedFunction{} = tvf
    assert tvf.session == self()
  end

  test "explode/2 uses explode tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.explode(tvf, %Column{expr: {:col, "items"}})

    assert %DataFrame{plan: {:table_valued_function, "explode", [{:col, "items"}]}} = df
  end

  test "inline/2 uses inline tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.inline(tvf, %Column{expr: {:col, "records"}})

    assert %DataFrame{plan: {:table_valued_function, "inline", [{:col, "records"}]}} = df
  end

  test "stack/3 uses stack tvf" do
    tvf = TableValuedFunction.new(self())
    df = TableValuedFunction.stack(tvf, 2, [1, 2, 3, 4])

    assert %DataFrame{
             plan:
               {:table_valued_function, "stack",
                [{:lit, 2}, {:lit, 1}, {:lit, 2}, {:lit, 3}, {:lit, 4}]}
           } = df
  end
end
