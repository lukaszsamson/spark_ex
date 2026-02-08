defmodule SparkEx.Connect.PlanEncoderM3Test do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Plan, Relation}

  describe "encode_relation/2 — aggregate" do
    test "encodes group-by aggregate" do
      plan_tuple =
        {:aggregate, {:sql, "SELECT * FROM t", nil}, :groupby, [{:col, "dept"}],
         [{:fn, "sum", [{:col, "salary"}], false}]}

      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = agg.input
      assert agg.group_type == :GROUP_TYPE_GROUPBY
      assert length(agg.grouping_expressions) == 1
      assert length(agg.aggregate_expressions) == 1

      [group_expr] = agg.grouping_expressions
      assert {:unresolved_attribute, %{unparsed_identifier: "dept"}} = group_expr.expr_type

      [agg_expr] = agg.aggregate_expressions
      assert {:unresolved_function, func} = agg_expr.expr_type
      assert func.function_name == "sum"
      assert counter == 2
    end

    test "encodes aggregate with multiple grouping and agg expressions" do
      plan_tuple =
        {:aggregate, {:sql, "SELECT * FROM t", nil}, :groupby, [{:col, "dept"}, {:col, "role"}],
         [{:fn, "count", [{:col, "id"}], false}, {:fn, "avg", [{:col, "salary"}], false}]}

      {plan, _counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = plan
      assert length(agg.grouping_expressions) == 2
      assert length(agg.aggregate_expressions) == 2
    end
  end

  describe "encode_relation/2 — join" do
    test "encodes inner join with condition" do
      condition = {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false}

      plan_tuple =
        {:join, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, condition, :inner,
         []}

      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:join, join}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = join.left
      assert %Relation{rel_type: {:sql, _}} = join.right
      assert join.join_type == :JOIN_TYPE_INNER
      assert join.using_columns == []
      assert {:unresolved_function, _} = join.join_condition.expr_type
      assert counter == 3
    end

    test "encodes left outer join" do
      condition = {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false}

      plan_tuple =
        {:join, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, condition, :left,
         []}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:join, join}}}} = plan
      assert join.join_type == :JOIN_TYPE_LEFT_OUTER
    end

    test "encodes join with using columns" do
      plan_tuple =
        {:join, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, nil, :inner,
         ["id", "name"]}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:join, join}}}} = plan
      assert join.join_condition == nil
      assert join.using_columns == ["id", "name"]
    end

    test "encodes cross join" do
      plan_tuple =
        {:join, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, nil, :cross, []}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:join, join}}}} = plan
      assert join.join_type == :JOIN_TYPE_CROSS
    end

    test "encodes all join types" do
      for {type, expected} <- [
            {:inner, :JOIN_TYPE_INNER},
            {:full, :JOIN_TYPE_FULL_OUTER},
            {:left, :JOIN_TYPE_LEFT_OUTER},
            {:right, :JOIN_TYPE_RIGHT_OUTER},
            {:left_anti, :JOIN_TYPE_LEFT_ANTI},
            {:left_semi, :JOIN_TYPE_LEFT_SEMI},
            {:cross, :JOIN_TYPE_CROSS}
          ] do
        plan_tuple =
          {:join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, nil, type, []}

        {plan, _} = PlanEncoder.encode(plan_tuple, 0)
        assert %Plan{op_type: {:root, %Relation{rel_type: {:join, join}}}} = plan
        assert join.join_type == expected
      end
    end

    test "raises for unsupported join type" do
      plan_tuple =
        {:join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, nil, :sideways, []}

      assert_raise ArgumentError, ~r/invalid join type/, fn ->
        PlanEncoder.encode(plan_tuple, 0)
      end
    end
  end

  describe "encode_relation/2 — deduplicate" do
    test "encodes deduplicate with all columns" do
      plan_tuple = {:deduplicate, {:sql, "SELECT * FROM t", nil}, [], true}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:deduplicate, dedup}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = dedup.input
      assert dedup.column_names == []
      assert dedup.all_columns_as_keys == true
      assert counter == 2
    end

    test "encodes deduplicate with specific columns" do
      plan_tuple = {:deduplicate, {:sql, "SELECT * FROM t", nil}, ["name", "age"], false}
      {plan, _} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:deduplicate, dedup}}}} = plan
      assert dedup.column_names == ["name", "age"]
      assert dedup.all_columns_as_keys == false
    end
  end

  describe "encode_relation/2 — set_operation" do
    test "encodes union all" do
      plan_tuple =
        {:set_operation, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, :union,
         true}

      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:set_op, set_op}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = set_op.left_input
      assert %Relation{rel_type: {:sql, _}} = set_op.right_input
      assert set_op.set_op_type == :SET_OP_TYPE_UNION
      assert set_op.is_all == true
      assert set_op.by_name == false
      assert set_op.allow_missing_columns == false
      assert counter == 3
    end

    test "encodes union distinct" do
      plan_tuple =
        {:set_operation, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, :union,
         false}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:set_op, set_op}}}} = plan
      assert set_op.set_op_type == :SET_OP_TYPE_UNION
      assert set_op.is_all == false
    end

    test "encodes intersect" do
      plan_tuple =
        {:set_operation, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil},
         :intersect, false}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:set_op, set_op}}}} = plan
      assert set_op.set_op_type == :SET_OP_TYPE_INTERSECT
    end

    test "encodes except" do
      plan_tuple =
        {:set_operation, {:sql, "SELECT * FROM a", nil}, {:sql, "SELECT * FROM b", nil}, :except,
         false}

      {plan, _} = PlanEncoder.encode(plan_tuple, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:set_op, set_op}}}} = plan
      assert set_op.set_op_type == :SET_OP_TYPE_EXCEPT
    end
  end

  describe "plan ID counter for M3 relations" do
    test "counter increments correctly for aggregate chain" do
      # aggregate -> read: 2 plan IDs
      plan =
        {:aggregate, {:read_data_source, "parquet", ["/p"], nil, %{}}, :groupby, [{:col, "x"}],
         [{:fn, "count", [{:col, "x"}], false}]}

      {_encoded, counter} = PlanEncoder.encode(plan, 0)
      assert counter == 2
    end

    test "counter increments correctly for join" do
      # join -> left_sql + right_sql: 3 plan IDs
      plan =
        {:join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, nil, :cross, []}

      {_encoded, counter} = PlanEncoder.encode(plan, 0)
      assert counter == 3
    end
  end
end
