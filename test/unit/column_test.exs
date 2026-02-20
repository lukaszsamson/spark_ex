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

    test "eq_null_safe/2 supports date literals" do
      result = Column.eq_null_safe(Functions.col("d"), ~D[2024-01-01])
      assert %Column{expr: {:fn, "<=>", [{:col, "d"}, {:lit, ~D[2024-01-01]}], false}} = result
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
      assert %Column{expr: {:fn, "isNull", [{:col, "x"}], false}} = result
    end

    test "is_not_null/1" do
      result = Column.is_not_null(Functions.col("x"))
      assert %Column{expr: {:fn, "isNotNull", [{:col, "x"}], false}} = result
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

    test "supports numeric reverse operators" do
      result = Column.plus(Functions.col("a"), 1)
      assert %Column{expr: {:fn, "+", [{:col, "a"}, {:lit, 1}], false}} = result
    end

    test "pow/2 builds power expression" do
      result = Column.pow(Functions.col("a"), 2)
      assert %Column{expr: {:fn, "power", [{:col, "a"}, {:lit, 2}], false}} = result
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
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_first}} = result
    end

    test "desc/1" do
      result = Column.desc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_last}} = result
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
      assert %Column{expr: {:fn, "startsWith", [{:col, "name"}, {:lit, "pre"}], false}} = result
    end

    test "like/2" do
      result = Column.like(Functions.col("name"), Functions.lit("%test%"))
      assert %Column{expr: {:fn, "like", [{:col, "name"}, {:lit, "%test%"}], false}} = result
    end

    test "rlike/2" do
      result = Column.rlike(Functions.col("name"), "%test%")
      assert %Column{expr: {:fn, "rlike", [{:col, "name"}, {:lit, "%test%"}], false}} = result
    end

    test "ilike/2" do
      result = Column.ilike(Functions.col("name"), "%test%")
      assert %Column{expr: {:fn, "ilike", [{:col, "name"}, {:lit, "%test%"}], false}} = result
    end
  end

  describe "membership / range" do
    test "isin/2 accepts tagged decimal literals" do
      result = Column.isin(Functions.col("a"), [{:decimal, "1.20", 3, 2}])

      assert %Column{expr: {:fn, "in", [{:col, "a"}, {:lit, {:decimal, "1.20", 3, 2}}], false}} =
               result
    end

    test "between/3 accepts tagged intervals" do
      result =
        Column.between(
          Functions.col("a"),
          {:day_time_interval, 100},
          {:day_time_interval, 200}
        )

      assert %Column{expr: {:fn, "and", [_, _], false}} = result
    end
  end

  describe "struct field helpers" do
    test "with_field/3 creates update_fields expression" do
      result = Column.with_field(Functions.col("s"), "name", Functions.lit("bob"))

      assert %Column{expr: {:update_fields, {:col, "s"}, "name", {:lit, "bob"}}} = result
    end

    test "drop_fields/2 chains update_fields" do
      result = Column.drop_fields(Functions.col("s"), ["a", "b"])

      assert %Column{expr: {:update_fields, {:update_fields, {:col, "s"}, "a", nil}, "b", nil}} =
               result
    end
  end

  describe "when_/2 and otherwise/2" do
    test "builds when_/otherwise expression chain" do
      condition = Column.gt(Functions.col("score"), Functions.lit(90))
      result = Column.when_(condition, Functions.lit("A")) |> Column.otherwise("B")

      assert %Column{
               expr:
                 {:fn, "when",
                  [
                    {:fn, ">", [{:col, "score"}, {:lit, 90}], false},
                    {:lit, "A"},
                    {:lit, "B"}
                  ], false}
             } = result
    end
  end
end
