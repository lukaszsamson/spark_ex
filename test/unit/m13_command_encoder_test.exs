defmodule SparkEx.M13.CommandEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.CommandEncoder

  # ── MergeIntoTableCommand ──

  describe "encode_command for merge_into_table" do
    test "encodes basic merge with update_all and insert_all" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "target_table", condition_expr,
         [{:update_star, nil, []}], [{:insert_star, nil, []}], [], false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      assert merge.target_table_name == "target_table"
      assert merge.source_table_plan != nil
      assert merge.merge_condition != nil
      assert merge.with_schema_evolution == false
      assert length(merge.match_actions) == 1
      assert length(merge.not_matched_actions) == 1
      assert merge.not_matched_by_source_actions == []
    end

    test "encodes merge with delete action" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [{:delete, nil, []}], [], [], false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      [action] = merge.match_actions
      assert {:merge_action, merge_action} = action.expr_type
      assert merge_action.action_type == :ACTION_TYPE_DELETE
      assert merge_action.condition == nil
      assert merge_action.assignments == []
    end

    test "encodes merge with update and assignments" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}

      assignments = [{{:col, "name"}, {:col, "s.name"}}, {{:col, "age"}, {:lit, 0}}]

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [{:update, nil, assignments}], [],
         [], false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      [action] = merge.match_actions
      assert {:merge_action, merge_action} = action.expr_type
      assert merge_action.action_type == :ACTION_TYPE_UPDATE
      assert length(merge_action.assignments) == 2

      [a1, a2] = merge_action.assignments
      assert a1.key != nil
      assert a1.value != nil
      assert a2.key != nil
      assert a2.value != nil
    end

    test "encodes merge with conditional action" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}
      action_cond = {:fn, ">", [{:col, "age"}, {:lit, 0}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [{:delete, action_cond, []}], [],
         [], false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      [action] = merge.match_actions
      assert {:merge_action, merge_action} = action.expr_type
      assert merge_action.condition != nil
    end

    test "encodes merge with schema evolution" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [], [], [], true}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      assert merge.with_schema_evolution == true
    end

    test "rewrites source-bound columns in merge condition" do
      source_plan = {:sql, "SELECT 1 AS id", nil}
      condition_expr = {:fn, "==", [{:col, "id", source_plan}, {:col, "id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [{:update_star, nil, []}], [], [],
         false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)
      assert {:merge_into_table_command, merge} = command.command_type

      assert {:unresolved_function, fn_expr} = merge.merge_condition.expr_type
      [left, _right] = fn_expr.arguments
      assert {:unresolved_attribute, left_attr} = left.expr_type
      assert is_integer(left_attr.plan_id)
    end

    test "encodes merge with not_matched_by_source actions" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "s.id"}, {:col, "t.id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [], [],
         [{:delete, nil, []}, {:update_star, nil, []}], false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:merge_into_table_command, merge} = command.command_type
      assert length(merge.not_matched_by_source_actions) == 2
    end

    test "encodes all action types" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "id"}, {:col, "id"}], false}

      for {action_type, expected_enum} <- [
            {:delete, :ACTION_TYPE_DELETE},
            {:insert, :ACTION_TYPE_INSERT},
            {:insert_star, :ACTION_TYPE_INSERT_STAR},
            {:update, :ACTION_TYPE_UPDATE},
            {:update_star, :ACTION_TYPE_UPDATE_STAR}
          ] do
        command_tuple =
          {:merge_into_table, source_plan, "t", condition_expr, [{action_type, nil, []}], [], [],
           false}

        {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)
        assert {:merge_into_table_command, merge} = command.command_type
        [action] = merge.match_actions
        assert {:merge_action, merge_action} = action.expr_type
        assert merge_action.action_type == expected_enum
      end
    end
  end

  # ── RegisterUDTF ──

  describe "encode_command for register_udtf" do
    test "encodes register_udtf command" do
      command_tuple =
        {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 0, "3.11", true}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_table_function, udtf} = command.command_type
      assert udtf.function_name == "my_udtf"
      assert udtf.deterministic == true
      assert {:python_udtf, python_udtf} = udtf.function
      assert python_udtf.command == <<1, 2, 3>>
      assert python_udtf.eval_type == 0
      assert python_udtf.python_ver == "3.11"
      assert python_udtf.return_type == nil
    end

    test "encodes register_udtf with return type" do
      return_type = %Spark.Connect.DataType{
        kind: {:struct, %Spark.Connect.DataType.Struct{}}
      }

      command_tuple =
        {:register_udtf, "my_udtf", <<1, 2, 3>>, return_type, 0, "3.11", true}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_table_function, udtf} = command.command_type
      assert {:python_udtf, python_udtf} = udtf.function
      assert python_udtf.return_type == return_type
    end

    test "encodes register_udtf as non-deterministic" do
      command_tuple =
        {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 0, "3.11", false}

      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_table_function, udtf} = command.command_type
      assert udtf.deterministic == false
    end
  end

  # ── Full encode/2 wrapping ──

  describe "encode/2" do
    test "wraps merge command in Plan" do
      source_plan = {:sql, "SELECT 1", nil}
      condition_expr = {:fn, "==", [{:col, "id"}, {:col, "id"}], false}

      command_tuple =
        {:merge_into_table, source_plan, "t", condition_expr, [{:update_star, nil, []}],
         [{:insert_star, nil, []}], [], false}

      {plan, _counter} = CommandEncoder.encode(command_tuple, 0)
      assert {:command, command} = plan.op_type
      assert {:merge_into_table_command, _} = command.command_type
    end

    test "wraps register_java_udf in Plan" do
      command_tuple = {:register_java_udf, "my_fn", "com.example.MyFn", nil, false}
      {plan, _counter} = CommandEncoder.encode(command_tuple, 0)
      assert {:command, command} = plan.op_type
      assert {:register_function, _} = command.command_type
    end

    test "wraps register_udtf in Plan" do
      command_tuple = {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 0, "3.11", true}
      {plan, _counter} = CommandEncoder.encode(command_tuple, 0)
      assert {:command, command} = plan.op_type
      assert {:register_table_function, _} = command.command_type
    end
  end
end
