defmodule SparkEx.Connect.PlanEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Expression, Plan, Relation, RelationCommon, SQL, Range}

  describe "encode/2 with SQL" do
    test "encodes simple SQL query" do
      {plan, counter} = PlanEncoder.encode({:sql, "SELECT 1", nil}, 0)

      assert %Plan{op_type: {:root, %Relation{} = rel}} = plan
      assert %RelationCommon{plan_id: 0} = rel.common
      assert {:sql, %SQL{query: "SELECT 1"}} = rel.rel_type
      assert counter == 1
    end

    test "encodes SQL with named arguments" do
      args = %{id: 42, name: "test"}
      {plan, counter} = PlanEncoder.encode({:sql, "SELECT :id, :name", args}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      assert %SQL{query: "SELECT :id, :name"} = sql
      assert map_size(sql.named_arguments) == 2

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:integer, 42}}}
             } = sql.named_arguments["id"]

      assert counter == 1
    end

    test "encodes SQL with positional arguments" do
      args = [1, "hello", true]
      {plan, counter} = PlanEncoder.encode({:sql, "SELECT ?, ?, ?", args}, 5)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      assert length(sql.pos_arguments) == 3
      assert counter == 6
    end

    test "encodes SQL arguments from Column expressions" do
      args = [%SparkEx.Column{expr: {:expr, "array(42)"}}]
      {plan, _counter} = PlanEncoder.encode({:sql, "SELECT element_at(?, 1)", args}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments
      assert {:expression_string, %{expression: "array(42)"}} = arg.expr_type
    end
  end

  describe "encode/2 with Range" do
    test "encodes range plan" do
      {plan, counter} = PlanEncoder.encode({:range, 0, 100, 1, nil}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:range, range}}}} = plan
      assert %Range{start: 0, end: 100, step: 1, num_partitions: nil} = range
      assert counter == 1
    end

    test "encodes range with partitions" do
      {plan, counter} = PlanEncoder.encode({:range, 10, 50, 2, 4}, 3)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:range, range}}}} = plan
      assert %Range{start: 10, end: 50, step: 2, num_partitions: 4} = range
      assert counter == 4
    end
  end

  describe "encode/2 with Limit" do
    test "encodes limit wrapping a SQL plan" do
      {plan, counter} = PlanEncoder.encode({:limit, {:sql, "SELECT 1", nil}, 10}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:limit, limit}}}} = plan
      assert %Spark.Connect.Limit{limit: 10, input: %Relation{rel_type: {:sql, _}}} = limit
      # limit gets id 0, child (sql) gets id 1
      assert counter == 2
    end
  end

  describe "encode_count/2" do
    test "wraps plan in aggregate count" do
      {plan, counter} = PlanEncoder.encode_count({:sql, "SELECT * FROM t", nil}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = plan
      assert %Spark.Connect.Aggregate{group_type: :GROUP_TYPE_GROUPBY} = agg
      assert %Relation{} = agg.input
      assert length(agg.aggregate_expressions) == 1
      assert counter == 2
    end
  end

  describe "literal encoding" do
    test "encodes various literal types" do
      # nil
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", [nil]}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:null, _}}}} =
               arg

      # boolean
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", [true]}, 0)
      %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:boolean, true}}}
             } = arg

      # float
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", [3.14]}, 0)
      %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:double, 3.14}}}
             } = arg

      # string
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", ["hi"]}, 0)
      %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:string, "hi"}}}
             } = arg

      # long (exceeds int32 range)
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", [3_000_000_000]}, 0)
      %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:long, 3_000_000_000}}}
             } = arg
    end
  end

  describe "plan ID counter" do
    test "increments monotonically across nested plans" do
      # limit -> sql: 2 plan IDs
      {_plan, counter} = PlanEncoder.encode({:limit, {:sql, "SELECT 1", nil}, 5}, 10)
      assert counter == 12

      # count wraps: sql + aggregate: 2 plan IDs
      {_plan, counter} = PlanEncoder.encode_count({:range, 0, 10, 1, nil}, 0)
      assert counter == 2
    end
  end

  describe "with_relations" do
    test "encodes root and reference relations" do
      plan =
        {:with_relations, {:sql, "SELECT 1", nil},
         [{:sql, "SELECT 2", nil}, {:range, 0, 3, 1, nil}]}

      {encoded, counter} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = encoded
      assert %Relation{rel_type: {:sql, _}} = wr.root
      assert length(wr.references) == 2
      assert [%Relation{}, %Relation{}] = wr.references
      assert counter == 4
    end
  end
end
