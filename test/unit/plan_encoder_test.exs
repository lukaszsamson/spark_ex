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

  describe "col_regex encoding" do
    test "encodes unresolved_regex expression" do
      {plan, _} =
        PlanEncoder.encode({:project, {:sql, "SELECT * FROM t", nil}, [{:col_regex, "^name"}]}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:project, project}}}} = plan
      [expr] = project.expressions
      assert {:unresolved_regex, %Expression.UnresolvedRegex{col_name: "^name"}} = expr.expr_type
    end

    test "encodes unresolved_regex expression with plan_id" do
      expr = PlanEncoder.encode_expression({:col_regex, "^name", 7})

      assert {:unresolved_regex, %Expression.UnresolvedRegex{col_name: "^name", plan_id: 7}} =
               expr.expr_type
    end
  end

  describe "metadata_column encoding" do
    test "encodes unresolved_attribute with metadata flag" do
      {plan, _} =
        PlanEncoder.encode(
          {:project, {:sql, "SELECT * FROM t", nil}, [{:metadata_col, "_meta"}]},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:project, project}}}} = plan
      [expr] = project.expressions

      assert {:unresolved_attribute,
              %Expression.UnresolvedAttribute{
                unparsed_identifier: "_meta",
                is_metadata_column: true
              }} =
               expr.expr_type
    end

    test "encodes unresolved_attribute with metadata flag and plan_id" do
      expr = PlanEncoder.encode_expression({:metadata_col, "_meta", 11})

      assert {:unresolved_attribute,
              %Expression.UnresolvedAttribute{
                unparsed_identifier: "_meta",
                plan_id: 11,
                is_metadata_column: true
              }} =
               expr.expr_type
    end
  end

  describe "column reference plan_id encoding" do
    test "encodes unresolved_attribute with plan_id" do
      expr = PlanEncoder.encode_expression({:col, "id", 9})

      assert {:unresolved_attribute,
              %Expression.UnresolvedAttribute{unparsed_identifier: "id", plan_id: 9}} =
               expr.expr_type
    end
  end

  describe "literal encoding" do
    test "encodes date/time literals" do
      date = ~D[2024-01-02]
      time = ~T[12:30:15.000123]
      naive = ~N[2024-01-02 12:30:15]

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:date, _}}}} =
               PlanEncoder.encode_expression({:lit, date})

      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:time, _}}}} =
               PlanEncoder.encode_expression({:lit, time})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:timestamp_ntz, _}}}
             } =
               PlanEncoder.encode_expression({:lit, naive})
    end

    test "encodes decimal/binary/complex literals" do
      assert %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:decimal, _}}}} =
               PlanEncoder.encode_expression({:lit, {:decimal, "12.34", 4, 2}})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:binary, <<1, 2>>}}}
             } =
               PlanEncoder.encode_expression({:lit, {:binary, <<1, 2>>}})

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:array,
                       %Expression.Literal.Array{
                         element_type: %Spark.Connect.DataType{kind: {:integer, _}},
                         elements: [_ | _]
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:array, [1, 2]}})

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:map,
                       %Expression.Literal.Map{
                         key_type: %Spark.Connect.DataType{kind: {:integer, _}},
                         value_type: %Spark.Connect.DataType{kind: {:integer, _}},
                         keys: [_ | _],
                         values: [_ | _]
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:map, %{1 => 2}}})

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:struct,
                       %Expression.Literal.Struct{
                         struct_type: %Spark.Connect.DataType{kind: {:struct, struct_type}},
                         elements: [_ | _]
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:struct, [1, "a"]}})

      assert length(struct_type.fields) == 2
      assert Enum.map(struct_type.fields, & &1.name) == ["col1", "col2"]
    end

    test "encodes empty complex literals with null type defaults" do
      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:array,
                       %Expression.Literal.Array{
                         element_type: %Spark.Connect.DataType{kind: {:null, _}},
                         elements: []
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:array, []}})

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:map,
                       %Expression.Literal.Map{
                         key_type: %Spark.Connect.DataType{kind: {:null, _}},
                         value_type: %Spark.Connect.DataType{kind: {:null, _}},
                         keys: [],
                         values: []
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:map, %{}}})

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:struct,
                       %Expression.Literal.Struct{
                         struct_type: %Spark.Connect.DataType{kind: {:struct, struct_type}},
                         elements: []
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, {:struct, []}})

      assert struct_type.fields == []
    end

    test "encodes interval literals" do
      assert %Expression{
               expr_type: {
                 :literal,
                 %Expression.Literal{
                   literal_type: {:calendar_interval, %Expression.Literal.CalendarInterval{}}
                 }
               }
             } =
               PlanEncoder.encode_expression({:lit, {:calendar_interval, 1, 2, 3}})

      assert %Expression{
               expr_type:
                 {:literal, %Expression.Literal{literal_type: {:year_month_interval, 12}}}
             } =
               PlanEncoder.encode_expression({:lit, {:year_month_interval, 12}})

      assert %Expression{
               expr_type:
                 {:literal, %Expression.Literal{literal_type: {:day_time_interval, 1_000}}}
             } =
                PlanEncoder.encode_expression({:lit, {:day_time_interval, 1_000}})
    end

    test "encodes explicit byte/short/float literals" do
      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:byte, 7}}}
             } = PlanEncoder.encode_expression({:lit, {:byte, 7}})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:short, 1024}}}
             } = PlanEncoder.encode_expression({:lit, {:short, 1024}})

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:float, 3.5}}}
             } = PlanEncoder.encode_expression({:lit, {:float, 3.5}})
    end
  end

  describe "subquery in encoding" do
    test "encodes in_subquery values" do
      expr =
        {:subquery, :in, {:plan_id, 7, {:sql, "SELECT * FROM t", nil}}, in_values: [{:col, "id"}]}

      encoded = PlanEncoder.encode_expression(expr)

      assert %Expression{expr_type: {:subquery_expression, subquery}} = encoded
      assert subquery.subquery_type == :SUBQUERY_TYPE_IN
      assert length(subquery.in_subquery_values) == 1
      assert subquery.plan_id == 7
    end
  end

  describe "call_function encoding" do
    test "encodes call_function expression" do
      expr = {:call_function, "my_fn", [{:lit, 1}, {:named_arg, "k", {:lit, 2}}]}
      encoded = PlanEncoder.encode_expression(expr)

      assert %Expression{expr_type: {:call_function, call}} = encoded
      assert %Spark.Connect.CallFunction{function_name: "my_fn"} = call
      assert length(call.arguments) == 2
    end
  end

  describe "named_argument encoding" do
    test "encodes named argument expression" do
      expr = {:named_arg, "key", {:lit, 10}}
      encoded = PlanEncoder.encode_expression(expr)

      assert %Expression{expr_type: {:named_argument_expression, named}} = encoded
      assert %Spark.Connect.NamedArgumentExpression{key: "key"} = named
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

  describe "encode/2 with ToSchema" do
    test "encodes ToSchema with DDL string" do
      {plan, _counter} =
        PlanEncoder.encode(
          {:with_relations, {:to_schema, {:sql, "SELECT 1", nil}, "id LONG"}, []},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, with_relations}}}} =
               plan

      assert %Spark.Connect.WithRelations{root: %Relation{rel_type: {:to_schema, to_schema}}} =
               with_relations

      assert %Spark.Connect.ToSchema{} = to_schema
      assert %Spark.Connect.DataType{kind: {:unparsed, _}} = to_schema.schema
    end

    test "encodes ToSchema with Struct type" do
      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :long, nullable: false, metadata: %{"comment" => "pk"})
        ])

      {plan, _counter} =
        PlanEncoder.encode(
          {:with_relations, {:to_schema, {:sql, "SELECT 1", nil}, schema}, []},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, with_relations}}}} =
               plan

      assert %Spark.Connect.WithRelations{root: %Relation{rel_type: {:to_schema, to_schema}}} =
               with_relations

      assert %Spark.Connect.ToSchema{} = to_schema
      assert %Spark.Connect.DataType{kind: {:struct, struct_schema}} = to_schema.schema
      assert [%Spark.Connect.DataType.StructField{} = field] = struct_schema.fields
      assert field.name == "id"
      assert field.nullable == false
      assert %Spark.Connect.DataType{kind: {:long, _}} = field.data_type
      assert Jason.decode!(field.metadata)["comment"] == "pk"
    end

    test "encodes ToSchema with DataType" do
      schema = %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}}

      {plan, _counter} =
        PlanEncoder.encode(
          {:with_relations, {:to_schema, {:sql, "SELECT 1", nil}, schema}, []},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, with_relations}}}} =
               plan

      assert %Spark.Connect.WithRelations{root: %Relation{rel_type: {:to_schema, to_schema}}} =
               with_relations

      assert %Spark.Connect.ToSchema{} = to_schema
      assert %Spark.Connect.DataType{kind: {:long, _}} = to_schema.schema
    end
  end

  describe "encode/2 with cached_remote_relation" do
    test "encodes cached remote relation" do
      {plan, _counter} =
        PlanEncoder.encode({:with_relations, {:cached_remote_relation, "rel-1"}, []}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, with_relations}}}} =
               plan

      assert %Spark.Connect.WithRelations{
               root: %Relation{rel_type: {:cached_remote_relation, cached}}
             } =
               with_relations

      assert %Spark.Connect.CachedRemoteRelation{relation_id: "rel-1"} = cached
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

  describe "literal encoding (sql arguments)" do
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

  describe "update_fields encoding" do
    test "encodes with_field expression" do
      expr = {:update_fields, {:col, "s"}, "name", {:lit, "bob"}}
      encoded = PlanEncoder.encode_expression(expr)

      assert %Expression{expr_type: {:update_fields, update}} = encoded
      assert update.field_name == "name"
      assert %Expression{expr_type: {:unresolved_attribute, _}} = update.struct_expression
      assert %Expression{expr_type: {:literal, _}} = update.value_expression
    end

    test "encodes drop_fields expression" do
      expr = {:update_fields, {:col, "s"}, "name", nil}
      encoded = PlanEncoder.encode_expression(expr)

      assert %Expression{expr_type: {:update_fields, update}} = encoded
      assert update.field_name == "name"
      assert update.value_expression == nil
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

  describe "plan-scoped expressions" do
    test "rewrites plan-scoped column references to plan_id and references" do
      source_plan = {:sql, "SELECT id FROM t", nil}
      plan = {:project, source_plan, [{:col, "id", source_plan}]}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = encoded
      assert length(wr.references) == 1
      assert %Relation{rel_type: {:project, project}} = wr.root
      [expr] = project.expressions

      assert {:unresolved_attribute, %Expression.UnresolvedAttribute{plan_id: plan_id}} =
               expr.expr_type

      assert is_integer(plan_id)
    end
  end

  describe "subquery with_relations" do
    test "adds referenced plans for subquery expressions" do
      subquery_plan = {:sql, "SELECT 1", nil}

      plan =
        {:project, {:sql, "SELECT * FROM t", nil},
         [{:subquery, :scalar, {:plan_id, 42, subquery_plan}, []}]}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = encoded
      assert length(wr.references) == 1
    end

    test "rewrites subquery table arg options" do
      table_arg =
        SparkEx.sql(self(), "SELECT * FROM t")
        |> SparkEx.DataFrame.as_table()
        |> SparkEx.TableArg.partition_by(["id"])
        |> SparkEx.TableArg.order_by(["id"])

      expr = SparkEx.TableArg.to_subquery_expr(table_arg)
      plan = {:project, {:sql, "SELECT * FROM t", nil}, [expr]}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = encoded
      assert length(wr.references) == 1
    end

    test "encode_expression rejects subquery plans without pre-wired plan_id" do
      assert_raise ArgumentError,
                   ~r/subquery expression requires an explicit plan_id reference/,
                   fn ->
                     PlanEncoder.encode_expression(
                       {:subquery, :scalar, {:sql, "SELECT 1", nil}, []}
                     )
                   end
    end

    test "rejects table_arg without options" do
      assert_raise ArgumentError, ~r/table_arg subquery requires/, fn ->
        PlanEncoder.encode_expression({:subquery, :table_arg, 1, []})
      end
    end

    test "rejects in subquery without values" do
      assert_raise ArgumentError, ~r/in subquery requires/, fn ->
        PlanEncoder.encode_expression({:subquery, :in, 1, []})
      end
    end
  end

  describe "collect_metrics encoding" do
    test "encodes collect_metrics plan" do
      plan = {:collect_metrics, {:sql, "SELECT * FROM t", nil}, "obs", [{:col, "x"}]}
      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:collect_metrics, cm}}}} = encoded
      assert %Spark.Connect.CollectMetrics{name: "obs"} = cm
      assert [%Expression{expr_type: {:unresolved_attribute, _}}] = cm.metrics
    end
  end

  describe "as_of_join encoding" do
    test "encodes as_of_join plan" do
      plan =
        {:as_of_join, {:sql, "SELECT * FROM t1", nil}, {:sql, "SELECT * FROM t2", nil},
         {:col, "t1"}, {:col, "t2"}, {:col, "id"}, [], "inner", {:lit, 5}, false, "forward"}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:as_of_join, as_of}}}} = encoded
      assert %Spark.Connect.AsOfJoin{join_type: "inner", allow_exact_matches: false} = as_of
    end
  end

  describe "lateral_join encoding" do
    test "encodes lateral_join plan" do
      plan =
        {:lateral_join, {:sql, "SELECT * FROM t1", nil}, {:sql, "SELECT * FROM t2", nil},
         {:col, "id"}, :left}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:lateral_join, lateral}}}} = encoded
      assert %Spark.Connect.LateralJoin{join_type: :JOIN_TYPE_LEFT_OUTER} = lateral
    end
  end

  describe "grouping_sets encoding" do
    test "encodes grouping sets aggregate" do
      plan =
        {:aggregate, {:sql, "SELECT * FROM t", nil}, :grouping_sets,
         [{:col, "id"}, {:col, "dept"}], [{:fn, "count", [{:lit, 1}], false}],
         [[{:col, "id"}], [{:col, "dept"}]]}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:aggregate, agg}}}} = encoded
      assert agg.group_type == :GROUP_TYPE_GROUPING_SETS
      assert length(agg.grouping_sets) == 2
    end
  end
end
