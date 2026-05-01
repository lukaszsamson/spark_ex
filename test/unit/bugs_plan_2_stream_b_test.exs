defmodule SparkEx.BugsPlan2.StreamBTest do
  use ExUnit.Case, async: true

  alias SparkEx.{Column, Functions, Window, WindowSpec}
  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{DataType, Expression}

  # ── B1: Window frame boundaries ──

  describe "B1 — Window.unbounded_preceding/0 and unbounded_following/0" do
    test "emits distinct preceding/following atoms" do
      assert Window.unbounded_preceding() == :unbounded_preceding
      assert Window.unbounded_following() == :unbounded_following
    end

    test "WindowSpec.rows_between/3 with explicit atoms" do
      spec =
        WindowSpec.rows_between(%WindowSpec{}, :unbounded_preceding, :unbounded_following)

      assert %WindowSpec{frame_spec: {:rows, :unbounded_preceding, :unbounded_following}} = spec
    end

    test "WindowSpec.rows_between/3 :unbounded resolves by lower/upper position" do
      spec = WindowSpec.rows_between(%WindowSpec{}, :unbounded, :unbounded)
      assert %WindowSpec{frame_spec: {:rows, :unbounded_preceding, :unbounded_following}} = spec
    end

    test "Long.MIN/MAX integers clamp by position" do
      spec =
        WindowSpec.rows_between(
          %WindowSpec{},
          -9_223_372_036_854_775_808,
          9_223_372_036_854_775_807
        )

      assert %WindowSpec{frame_spec: {:rows, :unbounded_preceding, :unbounded_following}} = spec
    end

    test "Int.MinValue/MaxValue stay finite (ROW frames support 32-bit literals)" do
      spec = WindowSpec.rows_between(%WindowSpec{}, -2_147_483_648, 2_147_483_647)
      assert %WindowSpec{frame_spec: {:rows, -2_147_483_648, 2_147_483_647}} = spec
    end

    test "encoder maps Int.MinValue/MaxValue as integer literals, not unbounded" do
      win =
        {:window, {:fn, "sum", [{:col, "x"}], false}, [], [{:sort_order, {:col, "x"}, :asc, nil}],
         {:rows, -2_147_483_648, 2_147_483_647}}

      expr = PlanEncoder.encode_expression(win)
      assert %Expression{expr_type: {:window, window}} = expr
      assert {:value, lower_expr} = window.frame_spec.lower.boundary
      assert {:value, upper_expr} = window.frame_spec.upper.boundary
      assert %Expression{expr_type: {:literal, lower_lit}} = lower_expr
      assert %Expression{expr_type: {:literal, upper_lit}} = upper_expr
      assert {:integer, -2_147_483_648} = lower_lit.literal_type
      assert {:integer, 2_147_483_647} = upper_lit.literal_type
    end

    test "encoder maps :unbounded_preceding and :unbounded_following to unbounded boundary" do
      win =
        {:window, {:fn, "sum", [{:col, "x"}], false}, [], [{:sort_order, {:col, "x"}, :asc, nil}],
         {:rows, :unbounded_preceding, :unbounded_following}}

      expr = PlanEncoder.encode_expression(win)
      assert %Expression{expr_type: {:window, window}} = expr
      assert {:unbounded, true} = window.frame_spec.lower.boundary
      assert {:unbounded, true} = window.frame_spec.upper.boundary
    end

    test "encoder still maps Long.MIN/MAX literals to unbounded as a backstop" do
      win =
        {:window, {:fn, "sum", [{:col, "x"}], false}, [], [{:sort_order, {:col, "x"}, :asc, nil}],
         {:range, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807}}

      expr = PlanEncoder.encode_expression(win)
      assert %Expression{expr_type: {:window, window}} = expr
      assert {:unbounded, true} = window.frame_spec.lower.boundary
      assert {:unbounded, true} = window.frame_spec.upper.boundary
    end
  end

  # ── B2: Struct literal named fields + contains_null ──

  describe "B2 — struct literal accepts named fields" do
    test "encodes {:struct, [{name, value}, ...]} with declared names" do
      lit =
        PlanEncoder.encode_expression({:lit, {:struct, [{"name", "Alice"}, {"age", 30}]}})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:struct, s}}}} =
               lit

      assert %DataType{kind: {:struct, %DataType.Struct{fields: fields}}} = s.struct_type

      assert [
               %DataType.StructField{name: "name"},
               %DataType.StructField{name: "age"}
             ] = fields
    end

    test "atom keys are stringified" do
      lit = PlanEncoder.encode_expression({:lit, {:struct, [{:name, "Alice"}]}})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:struct, s}}}} =
               lit

      assert %DataType{kind: {:struct, %DataType.Struct{fields: [field]}}} = s.struct_type
      assert field.name == "name"
    end

    test "positional fallback still works" do
      lit = PlanEncoder.encode_expression({:lit, {:struct, ["x", 1]}})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:struct, s}}}} =
               lit

      assert %DataType{kind: {:struct, %DataType.Struct{fields: [f1, f2]}}} = s.struct_type
      assert f1.name == "col1"
      assert f2.name == "col2"
    end
  end

  describe "B2 — array contains_null defaults to true" do
    test "inferred array data type sets contains_null: true" do
      # Trigger inference via a struct field (which calls infer_literal_data_type).
      lit = PlanEncoder.encode_expression({:lit, {:struct, [{"items", [1, 2, 3]}]}})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:struct, s}}}} =
               lit

      assert %DataType{kind: {:struct, %DataType.Struct{fields: [field]}}} = s.struct_type

      assert %DataType{
               kind: {:array, %DataType.Array{contains_null: true}}
             } = field.data_type
    end

    test "inferred map value_contains_null defaults to true" do
      lit = PlanEncoder.encode_expression({:lit, {:struct, [{"by_id", %{"a" => 1}}]}})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:struct, s}}}} =
               lit

      assert %DataType{kind: {:struct, %DataType.Struct{fields: [field]}}} = s.struct_type

      assert %DataType{
               kind: {:map, %DataType.Map{value_contains_null: true}}
             } = field.data_type
    end
  end

  describe "B2 — element_type promotion reconciled with child literals" do
    test "[int, long] yields long element_type and long-typed children" do
      lit = PlanEncoder.encode_expression({:lit, [1, 2_147_483_649]})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:array, arr}}}} =
               lit

      assert %DataType{kind: {:long, _}} = arr.element_type

      Enum.each(arr.elements, fn child ->
        assert {:long, _} = child.literal_type
      end)
    end

    test "[int, double] yields double element_type and double children" do
      lit = PlanEncoder.encode_expression({:lit, [1, 2.5]})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:array, arr}}}} =
               lit

      assert %DataType{kind: {:double, _}} = arr.element_type

      Enum.each(arr.elements, fn child ->
        assert {:double, _} = child.literal_type
      end)
    end
  end

  # ── B3: Qualified star references ──

  describe "B3 — col/1 detects \".*\" suffix" do
    test "bare \"*\" is unqualified star" do
      assert %Column{expr: {:star}} = Functions.col("*")
    end

    test "\"foo.*\" produces targeted star with target \"foo\"" do
      assert %Column{expr: {:star, "foo"}} = Functions.col("foo.*")
    end

    test "\"a.b.*\" preserves multi-segment target" do
      assert %Column{expr: {:star, "a.b"}} = Functions.col("a.b.*")
    end

    test "\"foo\" without star suffix stays a col reference" do
      assert %Column{expr: {:col, "foo"}} = Functions.col("foo")
    end

    test "\"foo.bar\" without star suffix stays a col reference (struct field path)" do
      assert %Column{expr: {:col, "foo.bar"}} = Functions.col("foo.bar")
    end

    test "encoder wires {:star, target} into UnresolvedStar.unparsed_target" do
      expr = PlanEncoder.encode_expression({:star, "foo"})
      assert %Expression{expr_type: {:unresolved_star, star}} = expr
      assert star.unparsed_target == "foo"
    end
  end

  # ── B4: Cast eval-mode public API ──

  describe "B4 — Column.cast/3 with eval mode" do
    test "default mode falls through to plain cast/2" do
      result = Column.cast(Functions.col("x"), "int", :default)
      assert %Column{expr: {:cast, {:col, "x"}, "int"}} = result
    end

    test ":try mode is equivalent to try_cast" do
      result = Column.cast(Functions.col("x"), "int", :try)
      assert %Column{expr: {:cast, {:col, "x"}, "int", :try}} = result
    end

    test ":legacy mode" do
      result = Column.cast(Functions.col("x"), "int", :legacy)
      assert %Column{expr: {:cast, {:col, "x"}, "int", :legacy}} = result
    end

    test ":ansi mode" do
      result = Column.cast(Functions.col("x"), "int", :ansi)
      assert %Column{expr: {:cast, {:col, "x"}, "int", :ansi}} = result
    end

    test "try_cast/2 still works (wrapper)" do
      assert %Column{expr: {:cast, {:col, "x"}, "int", :try}} =
               Column.try_cast(Functions.col("x"), "int")
    end

    test "encoder maps :legacy and :ansi to EVAL_MODE_LEGACY / EVAL_MODE_ANSI" do
      legacy = PlanEncoder.encode_expression({:cast, {:col, "x"}, "int", :legacy})
      ansi = PlanEncoder.encode_expression({:cast, {:col, "x"}, "int", :ansi})

      assert %Expression{expr_type: {:cast, %Expression.Cast{eval_mode: :EVAL_MODE_LEGACY}}} =
               legacy

      assert %Expression{expr_type: {:cast, %Expression.Cast{eval_mode: :EVAL_MODE_ANSI}}} = ansi
    end
  end

  # ── B5: Set-op :minus alias ──

  describe "B5 — :minus aliases :except in set op encoding" do
    test ":except encodes to SET_OP_TYPE_EXCEPT" do
      {plan, _} =
        PlanEncoder.encode_relation(
          {:set_operation, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, :except, false},
          0
        )

      assert %{rel_type: {:set_op, set_op}} = plan
      assert set_op.set_op_type == :SET_OP_TYPE_EXCEPT
    end

    test ":minus also encodes to SET_OP_TYPE_EXCEPT" do
      {plan, _} =
        PlanEncoder.encode_relation(
          {:set_operation, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, :minus, false},
          0
        )

      assert %{rel_type: {:set_op, set_op}} = plan
      assert set_op.set_op_type == :SET_OP_TYPE_EXCEPT
    end
  end
end
