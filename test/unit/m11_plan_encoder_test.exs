defmodule SparkEx.M11.PlanEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Expression, Plan, Relation}

  # ── try_cast expression ──

  describe "encode_expression — try_cast" do
    test "encodes try_cast with EVAL_MODE_TRY" do
      expr = PlanEncoder.encode_expression({:cast, {:col, "x"}, "int", :try})

      assert %Expression{expr_type: {:cast, cast}} = expr
      assert {:type_str, "int"} = cast.cast_to_type
      assert cast.eval_mode == :EVAL_MODE_TRY
      assert {:unresolved_attribute, _} = cast.expr.expr_type
    end

    test "regular cast has no eval_mode" do
      expr = PlanEncoder.encode_expression({:cast, {:col, "x"}, "int"})

      assert %Expression{expr_type: {:cast, cast}} = expr
      assert {:type_str, "int"} = cast.cast_to_type
      # default eval_mode
      refute cast.eval_mode == :EVAL_MODE_TRY
    end
  end

  # ── Window expression ──

  describe "encode_expression — window" do
    test "encodes window with partition and order" do
      win_expr =
        {:window, {:fn, "row_number", [], false}, [{:col, "dept"}],
         [{:sort_order, {:col, "salary"}, :asc, nil}], nil}

      expr = PlanEncoder.encode_expression(win_expr)

      assert %Expression{expr_type: {:window, window}} = expr
      assert %Expression.Window{} = window
      assert {:unresolved_function, func} = window.window_function.expr_type
      assert func.function_name == "row_number"
      assert length(window.partition_spec) == 1
      assert length(window.order_spec) == 1
      assert window.frame_spec == nil
    end

    test "encodes window with frame spec (rows)" do
      win_expr =
        {:window, {:fn, "sum", [{:col, "amount"}], false}, [{:col, "dept"}],
         [{:sort_order, {:col, "date"}, :asc, nil}], {:rows, -1, 1}}

      expr = PlanEncoder.encode_expression(win_expr)

      assert %Expression{expr_type: {:window, window}} = expr
      assert %Expression.Window.WindowFrame{} = window.frame_spec
      assert window.frame_spec.frame_type == :FRAME_TYPE_ROW
      assert {:value, _} = window.frame_spec.lower.boundary
      assert {:value, _} = window.frame_spec.upper.boundary
    end

    test "encodes window with frame spec (range, unbounded)" do
      win_expr =
        {:window, {:fn, "sum", [{:col, "x"}], false}, [], [{:sort_order, {:col, "x"}, :asc, nil}],
         {:range, :unbounded, :current_row}}

      expr = PlanEncoder.encode_expression(win_expr)

      assert %Expression{expr_type: {:window, window}} = expr
      assert window.frame_spec.frame_type == :FRAME_TYPE_RANGE
      assert {:unbounded, true} = window.frame_spec.lower.boundary
      assert {:current_row, true} = window.frame_spec.upper.boundary
    end
  end

  # ── Unresolved extract value ──

  describe "encode_expression — unresolved_extract_value" do
    test "encodes array access" do
      expr = PlanEncoder.encode_expression({:unresolved_extract_value, {:col, "arr"}, {:lit, 0}})

      assert %Expression{expr_type: {:unresolved_extract_value, ev}} = expr
      assert {:unresolved_attribute, _} = ev.child.expr_type

      assert {:literal, %Expression.Literal{literal_type: {:integer, 0}}} =
               ev.extraction.expr_type
    end

    test "encodes map access" do
      expr =
        PlanEncoder.encode_expression({:unresolved_extract_value, {:col, "m"}, {:lit, "key"}})

      assert %Expression{expr_type: {:unresolved_extract_value, ev}} = expr

      assert {:literal, %Expression.Literal{literal_type: {:string, "key"}}} =
               ev.extraction.expr_type
    end
  end

  # ── Lambda expression ──

  describe "encode_expression — lambda" do
    test "encodes single-variable lambda" do
      lambda_expr =
        {:lambda, {:fn, "+", [{:lambda_var, "x"}, {:lit, 1}], false}, [{:lambda_var, "x"}]}

      expr = PlanEncoder.encode_expression(lambda_expr)

      assert %Expression{expr_type: {:lambda_function, lambda}} = expr
      assert %Expression.LambdaFunction{} = lambda
      assert {:unresolved_function, func} = lambda.function.expr_type
      assert func.function_name == "+"
      assert length(lambda.arguments) == 1
      [arg] = lambda.arguments
      assert %Expression.UnresolvedNamedLambdaVariable{name_parts: ["x"]} = arg
    end

    test "encodes two-variable lambda" do
      lambda_expr =
        {:lambda, {:fn, "+", [{:lambda_var, "acc"}, {:lambda_var, "x"}], false},
         [{:lambda_var, "acc"}, {:lambda_var, "x"}]}

      expr = PlanEncoder.encode_expression(lambda_expr)

      assert %Expression{expr_type: {:lambda_function, lambda}} = expr
      assert length(lambda.arguments) == 2
      [arg1, arg2] = lambda.arguments
      assert arg1.name_parts == ["acc"]
      assert arg2.name_parts == ["x"]
    end
  end

  describe "encode_expression — lambda_var" do
    test "encodes standalone lambda variable" do
      expr = PlanEncoder.encode_expression({:lambda_var, "x"})

      assert %Expression{expr_type: {:unresolved_named_lambda_variable, var}} = expr
      assert %Expression.UnresolvedNamedLambdaVariable{name_parts: ["x"]} = var
    end
  end

  # ── Subquery expression ──

  describe "encode_expression — subquery" do
    test "encodes scalar subquery" do
      expr =
        PlanEncoder.encode_expression(
          {:subquery, :scalar, {:plan_id, 42, {:sql, "SELECT 1", nil}}, []}
        )

      assert %Expression{expr_type: {:subquery_expression, sq}} = expr
      assert sq.plan_id == 42
      assert sq.subquery_type == :SUBQUERY_TYPE_SCALAR
    end

    test "encodes exists subquery" do
      expr =
        PlanEncoder.encode_expression(
          {:subquery, :exists, {:plan_id, 7, {:sql, "SELECT 1", nil}}, []}
        )

      assert %Expression{expr_type: {:subquery_expression, sq}} = expr
      assert sq.subquery_type == :SUBQUERY_TYPE_EXISTS
    end

    test "encodes in subquery with values" do
      expr =
        PlanEncoder.encode_expression(
          {:subquery, :in, {:plan_id, 10, {:sql, "SELECT 1", nil}},
           [in_values: [{:col, "x"}, {:col, "y"}]]}
        )

      assert %Expression{expr_type: {:subquery_expression, sq}} = expr
      assert sq.subquery_type == :SUBQUERY_TYPE_IN
      assert length(sq.in_subquery_values) == 2
    end

    test "encodes table_arg subquery" do
      expr =
        PlanEncoder.encode_expression(
          {:subquery, :table_arg, {:plan_id, 5, {:sql, "SELECT 1", nil}}, []}
        )

      assert %Expression{expr_type: {:subquery_expression, sq}} = expr
      assert sq.subquery_type == :SUBQUERY_TYPE_TABLE_ARG
    end
  end

  # ── Pivot aggregate ──

  describe "encode_relation — pivot aggregate" do
    test "encodes pivot aggregate" do
      plan_tuple =
        {:aggregate, {:sql, "SELECT * FROM t", nil}, :pivot, [{:col, "year"}],
         [{:fn, "sum", [{:col, "earnings"}], false}], {:col, "course"}, ["dotNET", "Java"]}

      {plan, _counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = plan
      assert agg.group_type == :GROUP_TYPE_PIVOT
      assert length(agg.grouping_expressions) == 1
      assert length(agg.aggregate_expressions) == 1
      assert %Spark.Connect.Aggregate.Pivot{} = agg.pivot
      assert {:unresolved_attribute, _} = agg.pivot.col.expr_type
      assert length(agg.pivot.values) == 2
      [v1, v2] = agg.pivot.values
      assert %Expression.Literal{literal_type: {:string, "dotNET"}} = v1
      assert %Expression.Literal{literal_type: {:string, "Java"}} = v2
    end

    test "encodes pivot aggregate with nil values" do
      plan_tuple =
        {:aggregate, {:sql, "SELECT * FROM t", nil}, :pivot, [{:col, "year"}],
         [{:fn, "sum", [{:col, "earnings"}], false}], {:col, "course"}, nil}

      {plan, _counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = plan
      assert agg.pivot.values == []
    end
  end

  # ── with_relations ──

  describe "encode_relation — with_relations" do
    test "encodes with_relations wrapper" do
      plan_tuple =
        {:with_relations, {:sql, "SELECT * FROM main", nil},
         [{:sql, "SELECT * FROM ref1", nil}, {:sql, "SELECT * FROM ref2", nil}]}

      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = wr.root
      assert length(wr.references) == 2
      assert counter == 4
    end
  end

  # ── encode_sql_argument for new types ──

  describe "encode_sql_argument handles new expression types" do
    test "handles window expression via SQL args" do
      plan = {:sql, "SELECT ?", [{:window, {:fn, "sum", [{:col, "x"}], false}, [], [], nil}]}
      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end

    test "handles lambda expression via SQL args" do
      plan =
        {:sql, "SELECT ?",
         [{:lambda, {:fn, "+", [{:lambda_var, "x"}, {:lit, 1}], false}, [{:lambda_var, "x"}]}]}

      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end

    test "handles lambda_var via SQL args" do
      plan = {:sql, "SELECT ?", [{:lambda_var, "x"}]}
      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end

    test "handles unresolved_extract_value via SQL args" do
      plan = {:sql, "SELECT ?", [{:unresolved_extract_value, {:col, "arr"}, {:lit, 0}}]}
      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end

    test "handles subquery via SQL args" do
      plan =
        {:sql, "SELECT ?", [{:subquery, :scalar, {:plan_id, 42, {:sql, "SELECT 1", nil}}, []}]}

      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end

    test "handles try_cast via SQL args" do
      plan = {:sql, "SELECT ?", [{:cast, {:col, "x"}, "int", :try}]}
      {_encoded, _counter} = PlanEncoder.encode(plan, 0)
    end
  end
end
