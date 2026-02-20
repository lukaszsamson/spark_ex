defmodule SparkEx.M11.ColumnTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  # ── New binary operators with auto-coercion ──

  describe "auto-coercion on binary operators" do
    test "eq/2 auto-coerces right operand to literal" do
      result = Column.eq(Functions.col("a"), 42)
      assert %Column{expr: {:fn, "==", [{:col, "a"}, {:lit, 42}], false}} = result
    end

    test "gt/2 auto-coerces string right operand" do
      result = Column.gt(Functions.col("a"), "hello")
      assert %Column{expr: {:fn, ">", [{:col, "a"}, {:lit, "hello"}], false}} = result
    end

    test "plus/2 auto-coerces integer right operand" do
      result = Column.plus(Functions.col("a"), 10)
      assert %Column{expr: {:fn, "+", [{:col, "a"}, {:lit, 10}], false}} = result
    end

    test "minus/2 auto-coerces float right operand" do
      result = Column.minus(Functions.col("a"), 1.5)
      assert %Column{expr: {:fn, "-", [{:col, "a"}, {:lit, 1.5}], false}} = result
    end

    test "Column-to-Column binary ops still work" do
      result = Column.eq(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "==", [{:col, "a"}, {:col, "b"}], false}} = result
    end
  end

  # ── New binary operators ──

  describe "eq_null_safe/2" do
    test "creates null-safe equality expression" do
      result = Column.eq_null_safe(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "<=>", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "auto-coerces right operand" do
      result = Column.eq_null_safe(Functions.col("a"), nil)
      assert %Column{expr: {:fn, "<=>", [{:col, "a"}, {:lit, nil}], false}} = result
    end
  end

  describe "mod/2" do
    test "creates modulo expression" do
      result = Column.mod(Functions.col("a"), 3)
      assert %Column{expr: {:fn, "%", [{:col, "a"}, {:lit, 3}], false}} = result
    end
  end

  describe "bitwise operators" do
    test "bitwise_and/2" do
      result = Column.bitwise_and(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "&", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "bitwise_or/2" do
      result = Column.bitwise_or(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "|", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "bitwise_xor/2" do
      result = Column.bitwise_xor(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "^", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "bitwise_not/1" do
      result = Column.bitwise_not(Functions.col("a"))
      assert %Column{expr: {:fn, "~", [{:col, "a"}], false}} = result
    end
  end

  describe "string binary operators" do
    test "ends_with/2" do
      result = Column.ends_with(Functions.col("name"), Functions.lit(".csv"))
      assert %Column{expr: {:fn, "endsWith", [{:col, "name"}, {:lit, ".csv"}], false}} = result
    end

    test "rlike/2" do
      result = Column.rlike(Functions.col("name"), Functions.lit("^test"))
      assert %Column{expr: {:fn, "rlike", [{:col, "name"}, {:lit, "^test"}], false}} = result
    end

    test "ilike/2" do
      result = Column.ilike(Functions.col("name"), Functions.lit("%test%"))
      assert %Column{expr: {:fn, "ilike", [{:col, "name"}, {:lit, "%test%"}], false}} = result
    end
  end

  # ── New unary operators ──

  describe "new unary operators" do
    test "is_nan/1" do
      result = Column.is_nan(Functions.col("x"))
      assert %Column{expr: {:fn, "isNaN", [{:col, "x"}], false}} = result
    end

    test "negate/1" do
      result = Column.negate(Functions.col("x"))
      assert %Column{expr: {:fn, "negative", [{:col, "x"}], false}} = result
    end
  end

  # ── try_cast ──

  describe "try_cast/2" do
    test "creates try-cast expression" do
      result = Column.try_cast(Functions.col("x"), "int")
      assert %Column{expr: {:cast, {:col, "x"}, "int", :try}} = result
    end
  end

  # ── isin ──

  describe "isin/2" do
    test "creates in expression with literal values" do
      result = Column.isin(Functions.col("status"), ["active", "pending"])

      assert %Column{
               expr: {:fn, "in", [{:col, "status"}, {:lit, "active"}, {:lit, "pending"}], false}
             } = result
    end

    test "creates in expression with Column values" do
      result = Column.isin(Functions.col("a"), [Functions.col("b"), Functions.lit(1)])
      assert %Column{expr: {:fn, "in", [{:col, "a"}, {:col, "b"}, {:lit, 1}], false}} = result
    end

    test "creates in expression with mixed values" do
      result = Column.isin(Functions.col("a"), [1, "two", Functions.col("c")])

      assert %Column{
               expr: {:fn, "in", [{:col, "a"}, {:lit, 1}, {:lit, "two"}, {:col, "c"}], false}
             } = result
    end
  end

  # ── between ──

  describe "between/3" do
    test "creates between expression as AND of >= and <=" do
      result = Column.between(Functions.col("age"), 18, 65)

      assert %Column{
               expr:
                 {:fn, "and",
                  [
                    {:fn, ">=", [{:col, "age"}, {:lit, 18}], false},
                    {:fn, "<=", [{:col, "age"}, {:lit, 65}], false}
                  ], false}
             } = result
    end

    test "between with Column bounds" do
      result =
        Column.between(Functions.col("x"), Functions.col("lower"), Functions.col("upper"))

      assert %Column{
               expr:
                 {:fn, "and",
                  [
                    {:fn, ">=", [{:col, "x"}, {:col, "lower"}], false},
                    {:fn, "<=", [{:col, "x"}, {:col, "upper"}], false}
                  ], false}
             } = result
    end
  end

  # ── get_item / get_field ──

  describe "get_item/2" do
    test "creates extract value for array index" do
      result = Column.get_item(Functions.col("arr"), 0)
      assert %Column{expr: {:unresolved_extract_value, {:col, "arr"}, {:lit, 0}}} = result
    end

    test "creates extract value for map key" do
      result = Column.get_item(Functions.col("m"), "key")
      assert %Column{expr: {:unresolved_extract_value, {:col, "m"}, {:lit, "key"}}} = result
    end

    test "creates extract value with Column key" do
      result = Column.get_item(Functions.col("m"), Functions.col("k"))
      assert %Column{expr: {:unresolved_extract_value, {:col, "m"}, {:col, "k"}}} = result
    end
  end

  describe "get_field/2" do
    test "creates extract value for struct field" do
      result = Column.get_field(Functions.col("s"), "name")
      assert %Column{expr: {:unresolved_extract_value, {:col, "s"}, {:lit, "name"}}} = result
    end
  end

  # ── substr ──

  describe "substr/3" do
    test "creates substr expression" do
      result = Column.substr(Functions.col("s"), 1, 5)
      assert %Column{expr: {:fn, "substr", [{:col, "s"}, {:lit, 1}, {:lit, 5}], false}} = result
    end

    test "accepts Column arguments for pos and len" do
      result = Column.substr(Functions.col("s"), Functions.col("p"), Functions.col("l"))

      assert %Column{expr: {:fn, "substr", [{:col, "s"}, {:col, "p"}, {:col, "l"}], false}} =
               result
    end
  end

  # ── over (window binding) ──

  describe "over/2" do
    test "creates window expression" do
      w = SparkEx.Window.partition_by(["dept"])
      result = Column.over(Functions.col("salary"), w)

      assert %Column{
               expr: {:window, {:col, "salary"}, [{:col, "dept"}], [], nil}
             } = result
    end

    test "creates window expression with order" do
      w =
        SparkEx.Window.partition_by(["dept"])
        |> SparkEx.WindowSpec.order_by(["salary"])

      result = Column.over(Functions.col("salary"), w)

      assert %Column{
               expr:
                 {:window, {:col, "salary"}, [{:col, "dept"}],
                  [{:sort_order, {:col, "salary"}, :asc, :nulls_first}], nil}
             } = result
    end

    test "creates window expression with frame" do
      w =
        SparkEx.Window.partition_by(["dept"])
        |> SparkEx.WindowSpec.order_by(["salary"])
        |> SparkEx.WindowSpec.rows_between(-1, 1)

      result = Column.over(Functions.col("salary"), w)

      assert %Column{
               expr: {:window, {:col, "salary"}, [{:col, "dept"}], _order, {:rows, -1, 1}}
             } = result
    end
  end
end
