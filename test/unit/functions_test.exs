defmodule SparkEx.FunctionsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  describe "constructors" do
    test "col/1 creates column reference" do
      assert %Column{expr: {:col, "name"}} = Functions.col("name")
    end

    test "lit/1 creates literal for various types" do
      assert %Column{expr: {:lit, 42}} = Functions.lit(42)
      assert %Column{expr: {:lit, "hello"}} = Functions.lit("hello")
      assert %Column{expr: {:lit, true}} = Functions.lit(true)
      assert %Column{expr: {:lit, nil}} = Functions.lit(nil)
      assert %Column{expr: {:lit, 3.14}} = Functions.lit(3.14)
    end

    test "expr/1 creates expression string" do
      assert %Column{expr: {:expr, "age + 1"}} = Functions.expr("age + 1")
    end

    test "star/0 creates unresolved star" do
      assert %Column{expr: {:star}} = Functions.star()
    end
  end

  describe "aggregate functions" do
    test "count/1" do
      result = Functions.count(Functions.col("x"))
      assert %Column{expr: {:fn, "count", [{:col, "x"}], false}} = result
    end

    test "sum/1" do
      result = Functions.sum(Functions.col("x"))
      assert %Column{expr: {:fn, "sum", [{:col, "x"}], false}} = result
    end

    test "avg/1" do
      result = Functions.avg(Functions.col("x"))
      assert %Column{expr: {:fn, "avg", [{:col, "x"}], false}} = result
    end

    test "min/1" do
      result = Functions.min(Functions.col("x"))
      assert %Column{expr: {:fn, "min", [{:col, "x"}], false}} = result
    end

    test "max/1" do
      result = Functions.max(Functions.col("x"))
      assert %Column{expr: {:fn, "max", [{:col, "x"}], false}} = result
    end

    test "count_distinct/1 sets is_distinct flag" do
      result = Functions.count_distinct(Functions.col("x"))
      assert %Column{expr: {:fn, "count", [{:col, "x"}], true}} = result
    end
  end

  describe "sort helpers" do
    test "asc/1 delegates to Column" do
      result = Functions.asc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, nil}} = result
    end

    test "desc/1 delegates to Column" do
      result = Functions.desc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, nil}} = result
    end
  end
end
