defmodule SparkEx.ColumnTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  describe "comparisons" do
    test "eq/2 creates equality function expression" do
      result = Column.eq(Functions.col("a"), Functions.lit(1))
      assert %Column{expr: {:fn, "==", [{:col, "a"}, {:lit, 1}], false}} = result
    end

    test "neq/2" do
      result = Column.neq(Functions.col("a"), Functions.lit(1))
      assert %Column{expr: {:fn, "!=", [{:col, "a"}, {:lit, 1}], false}} = result
    end

    test "gt/2" do
      result = Column.gt(Functions.col("a"), Functions.lit(10))
      assert %Column{expr: {:fn, ">", [{:col, "a"}, {:lit, 10}], false}} = result
    end

    test "gte/2" do
      result = Column.gte(Functions.col("a"), Functions.lit(10))
      assert %Column{expr: {:fn, ">=", [{:col, "a"}, {:lit, 10}], false}} = result
    end

    test "lt/2" do
      result = Column.lt(Functions.col("a"), Functions.lit(10))
      assert %Column{expr: {:fn, "<", [{:col, "a"}, {:lit, 10}], false}} = result
    end

    test "lte/2" do
      result = Column.lte(Functions.col("a"), Functions.lit(10))
      assert %Column{expr: {:fn, "<=", [{:col, "a"}, {:lit, 10}], false}} = result
    end
  end

  describe "boolean operations" do
    test "and_/2" do
      left = Column.gt(Functions.col("a"), Functions.lit(0))
      right = Column.lt(Functions.col("a"), Functions.lit(100))
      result = Column.and_(left, right)
      assert %Column{expr: {:fn, "and", [_, _], false}} = result
    end

    test "or_/2" do
      left = Column.eq(Functions.col("a"), Functions.lit(1))
      right = Column.eq(Functions.col("a"), Functions.lit(2))
      result = Column.or_(left, right)
      assert %Column{expr: {:fn, "or", [_, _], false}} = result
    end

    test "not_/1" do
      col = Column.eq(Functions.col("a"), Functions.lit(1))
      result = Column.not_(col)
      assert %Column{expr: {:fn, "not", [_], false}} = result
    end
  end

  describe "null checks" do
    test "is_null/1" do
      result = Column.is_null(Functions.col("x"))
      assert %Column{expr: {:fn, "isnull", [{:col, "x"}], false}} = result
    end

    test "is_not_null/1" do
      result = Column.is_not_null(Functions.col("x"))
      assert %Column{expr: {:fn, "isnotnull", [{:col, "x"}], false}} = result
    end
  end

  describe "arithmetic" do
    test "plus/2" do
      result = Column.plus(Functions.col("a"), Functions.lit(1))
      assert %Column{expr: {:fn, "+", [{:col, "a"}, {:lit, 1}], false}} = result
    end

    test "minus/2" do
      result = Column.minus(Functions.col("a"), Functions.lit(1))
      assert %Column{expr: {:fn, "-", [{:col, "a"}, {:lit, 1}], false}} = result
    end

    test "multiply/2" do
      result = Column.multiply(Functions.col("a"), Functions.lit(2))
      assert %Column{expr: {:fn, "*", [{:col, "a"}, {:lit, 2}], false}} = result
    end

    test "divide/2" do
      result = Column.divide(Functions.col("a"), Functions.lit(2))
      assert %Column{expr: {:fn, "/", [{:col, "a"}, {:lit, 2}], false}} = result
    end
  end

  describe "cast" do
    test "cast/2 wraps expression with type string" do
      result = Column.cast(Functions.col("x"), "int")
      assert %Column{expr: {:cast, {:col, "x"}, "int"}} = result
    end
  end

  describe "sort ordering" do
    test "asc/1" do
      result = Column.asc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, nil}} = result
    end

    test "desc/1" do
      result = Column.desc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, nil}} = result
    end

    test "asc_nulls_first/1" do
      result = Column.asc_nulls_first(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_first}} = result
    end

    test "desc_nulls_last/1" do
      result = Column.desc_nulls_last(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_last}} = result
    end
  end

  describe "alias" do
    test "alias_/2 wraps expression with name" do
      result = Column.alias_(Functions.col("x"), "renamed")
      assert %Column{expr: {:alias, {:col, "x"}, "renamed"}} = result
    end
  end

  describe "string operations" do
    test "contains/2" do
      result = Column.contains(Functions.col("name"), Functions.lit("test"))
      assert %Column{expr: {:fn, "contains", [{:col, "name"}, {:lit, "test"}], false}} = result
    end

    test "starts_with/2" do
      result = Column.starts_with(Functions.col("name"), Functions.lit("pre"))
      assert %Column{expr: {:fn, "startswith", [{:col, "name"}, {:lit, "pre"}], false}} = result
    end
  end
end
