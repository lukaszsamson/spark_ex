defmodule SparkEx.Connect.BugsFableEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.WindowSpec
  alias Spark.Connect.{Expression, Plan, Relation, SQL}

  # ── FABLE-07: window frame boundary 0 must encode as current_row ──
  describe "window frame boundary 0 → current_row (FABLE-07)" do
    defp frame(frame_type, lower, upper) do
      expr =
        PlanEncoder.encode_expression(
          {:window, {:fn, "sum", [{:col, "x"}], false}, [], [], {frame_type, lower, upper}}
        )

      %Expression{expr_type: {:window, %Expression.Window{frame_spec: frame_spec}}} = expr
      frame_spec
    end

    test "ROW frame: 0 lower and upper become current_row, not a literal" do
      fs = frame(:rows, 0, 0)
      assert fs.lower.boundary == {:current_row, true}
      assert fs.upper.boundary == {:current_row, true}
    end

    test "RANGE frame: 0 lower and upper become current_row, not a long literal" do
      fs = frame(:range, 0, 0)
      assert fs.lower.boundary == {:current_row, true}
      assert fs.upper.boundary == {:current_row, true}
    end

    test "RANGE frame: range_between(0, :unbounded_following) — lower is current_row" do
      fs = frame(:range, 0, :unbounded_following)
      assert fs.lower.boundary == {:current_row, true}
      assert fs.upper.boundary == {:unbounded, true}
    end

    test "non-zero offsets still encode as literals" do
      fs = frame(:rows, -1, 1)
      assert {:value, _} = fs.lower.boundary
      assert {:value, _} = fs.upper.boundary
    end
  end

  # ── FABLE-55: window boundary clamp threshold off-by-one vs PySpark ──
  describe "WindowSpec clamp threshold (FABLE-55)" do
    @jvm_long_min -9_223_372_036_854_775_808
    @preceding_threshold -9_223_372_036_854_775_807

    test "lower bound at -(2^63 - 1) clamps to :unbounded_preceding" do
      spec = WindowSpec.rows_between(%WindowSpec{}, @preceding_threshold, 0)
      assert {:rows, :unbounded_preceding, _} = spec.frame_spec
    end

    test "lower bound at JVM_LONG_MIN still clamps to :unbounded_preceding" do
      spec = WindowSpec.rows_between(%WindowSpec{}, @jvm_long_min, 0)
      assert {:rows, :unbounded_preceding, _} = spec.frame_spec
    end

    test "lower bound just above threshold (-(2^63 - 2)) is NOT clamped" do
      v = -(9_223_372_036_854_775_808 - 2)
      spec = WindowSpec.range_between(%WindowSpec{}, v, 0)
      assert {:range, ^v, _} = spec.frame_spec
    end

    test "upper bound at 2^63 - 1 clamps to :unbounded_following" do
      v = 9_223_372_036_854_775_807
      spec = WindowSpec.rows_between(%WindowSpec{}, 0, v)
      assert {:rows, _, :unbounded_following} = spec.frame_spec
    end
  end

  # ── FABLE-33: Decimal literals with positive exponent ──
  describe "Decimal literal encoding (FABLE-33)" do
    test "positive-exponent decimal encodes in plain notation, not scientific" do
      expr = PlanEncoder.encode_expression({:lit, Decimal.new("1.5e10")})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:decimal, dec}}}
             } = expr

      assert dec.value == "15000000000"
      # 11 integer digits, no fractional part → precision 11, scale 0
      assert dec.scale == 0
      assert dec.precision == 11
    end

    test "fractional decimal keeps its scale" do
      expr = PlanEncoder.encode_expression({:lit, Decimal.new("1.5")})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:decimal, dec}}}
             } = expr

      assert dec.value == "1.5"
      assert dec.scale == 1
    end
  end

  # ── FABLE-35: SQL positional args never reinterpreted as named ──
  describe "SQL args positional vs named (FABLE-35)" do
    test "a keyword-looking list is positional, not named" do
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", [{:lit, 1}]}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, %SQL{} = sql}}}} = plan
      assert map_size(sql.named_arguments) == 0
      assert length(sql.pos_arguments) == 1
    end

    test "a list of expr-tuple elements is positional, not named" do
      # Each element is a standalone positional expression; the {atom, term}
      # shape must NOT be reinterpreted as the named arg atom => term.
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?, ?", [{:lit, 42}, {:expr, "x"}]}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, %SQL{} = sql}}}} = plan
      assert map_size(sql.named_arguments) == 0
      assert length(sql.pos_arguments) == 2
    end

    test "a map is named" do
      {plan, _} = PlanEncoder.encode({:sql, "SELECT :id", %{id: 42}}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, %SQL{} = sql}}}} = plan
      assert map_size(sql.named_arguments) == 1
      assert sql.pos_arguments == []
    end
  end

  # ── FABLE-08: rewrite_expr/4 handles the 4-tuple alias with metadata ──
  describe "metadata alias hoists nested subquery references (FABLE-08)" do
    setup do
      child = {:plan_id, 100, {:sql, "SELECT * FROM t", nil}}
      referenced = {:plan_id, 200, {:sql, "SELECT 1", nil}}
      %{child: child, referenced: referenced}
    end

    test "subquery under 3-tuple alias is hoisted into with_relations.references", ctx do
      col = {:alias, {:subquery, :scalar, ctx.referenced, []}, "n"}
      {plan, _} = PlanEncoder.encode({:project, ctx.child, [col]}, 0)
      assert %Plan{op_type: {:root, root}} = plan
      assert {:with_relations, wr} = root.rel_type
      assert wr.references != []
    end

    test "subquery under 4-tuple metadata alias is ALSO hoisted", ctx do
      meta = ~s({"comment":"c"})
      col = {:alias, {:subquery, :scalar, ctx.referenced, []}, "n", meta}
      {plan, _} = PlanEncoder.encode({:project, ctx.child, [col]}, 0)
      assert %Plan{op_type: {:root, root}} = plan

      assert {:with_relations, wr} = root.rel_type,
             "metadata alias must still hoist the subquery into with_relations"

      assert wr.references != []
    end
  end

  # ── FABLE-34: rewrite_plan supports map_partitions/group_map/co_group_map ──
  describe "rewrite_plan supports map/group relations (FABLE-34)" do
    @py_udf %Spark.Connect.PythonUDF{
      output_type: nil,
      eval_type: 200,
      command: <<>>,
      python_ver: "3.11"
    }

    defp scalar_fn do
      %Spark.Connect.CommonInlineUserDefinedFunction{
        function_name: "f",
        deterministic: true,
        function: {:python_udf, @py_udf}
      }
    end

    test "map_partitions does not raise 'unsupported plan tuple'" do
      child = {:sql, "SELECT * FROM t", nil}
      plan = {:map_partitions, child, scalar_fn(), []}
      assert {%Plan{}, _} = PlanEncoder.encode(plan, 0)
    end

    test "group_map does not raise and rewrites grouping exprs" do
      child = {:sql, "SELECT * FROM t", nil}
      plan = {:group_map, child, [{:col, "k"}], scalar_fn(), []}
      assert {%Plan{}, _} = PlanEncoder.encode(plan, 0)
    end

    test "co_group_map does not raise" do
      left = {:sql, "SELECT * FROM a", nil}
      right = {:sql, "SELECT * FROM b", nil}
      plan = {:co_group_map, left, [{:col, "k"}], right, [{:col, "k"}], scalar_fn(), []}
      assert {%Plan{}, _} = PlanEncoder.encode(plan, 0)
    end

    test "inline_udtf rewrites its argument expressions" do
      ref = {:plan_id, 300, {:sql, "SELECT 1", nil}}
      arg = {:subquery, :scalar, ref, []}

      plan =
        {:inline_udtf, "f", [arg],
         %Spark.Connect.PythonUDTF{eval_type: 300, command: <<>>, python_ver: "3.11"}, nil, 300,
         "3.11", true}

      # Wrap in a project so attach_with_relations runs over the whole tree and
      # the rewritten subquery reference gets hoisted.
      child = {:plan_id, 301, {:sql, "SELECT * FROM t", nil}}
      {plan, _} = PlanEncoder.encode({:lateral_join, child, plan, nil, :inner}, 0)
      assert %Plan{op_type: {:root, root}} = plan
      assert {:with_relations, wr} = root.rel_type
      assert wr.references != []
    end
  end
end
