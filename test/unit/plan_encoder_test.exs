defmodule SparkEx.Connect.PlanEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.{DataFrame, Column}
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
      assert map_size(sql.args) == 2

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:integer, 42}}}
             } = sql.named_arguments["id"]

      assert counter == 1
    end

    test "encodes SQL with keyword list arguments as named arguments" do
      args = [id: 42, name: "test"]
      {plan, _counter} = PlanEncoder.encode({:sql, "SELECT :id, :name", args}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      assert map_size(sql.named_arguments) == 2
      assert map_size(sql.args) == 2
      assert sql.pos_arguments == []
    end

    test "encodes SQL with positional arguments" do
      args = [1, "hello", true]
      {plan, counter} = PlanEncoder.encode({:sql, "SELECT ?, ?, ?", args}, 5)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      assert length(sql.pos_arguments) == 3
      assert length(sql.pos_args) == 3
      assert counter == 6
    end

    test "encodes SQL arguments from Column expressions" do
      args = [%SparkEx.Column{expr: {:expr, "array(42)"}}]
      {plan, _counter} = PlanEncoder.encode({:sql, "SELECT element_at(?, 1)", args}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, sql}}}} = plan
      [arg] = sql.pos_arguments
      assert {:expression_string, %{expression: "array(42)"}} = arg.expr_type
      assert sql.pos_args == []
    end
  end

  describe "join encoding with bound columns" do
    test "preserves expression join condition rather than rewriting to USING" do
      left = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM emp", nil}}
      right = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM dept", nil}}

      join_condition = Column.eq(DataFrame.col(left, "dept_id"), DataFrame.col(right, "dept_id"))
      joined = DataFrame.join(left, right, join_condition, :inner)
      {plan, _counter} = PlanEncoder.encode(joined.plan, 0)
      assert %Relation{rel_type: {:join, join}} = root_relation(plan)
      assert join.using_columns == []
      assert %Expression{expr_type: {:unresolved_function, top_fn}} = join.join_condition
      assert top_fn.function_name == "=="
      assert is_integer(join.left.common.plan_id)
      assert is_integer(join.right.common.plan_id)
    end

    test "maps multi-predicate join conditions to only left/right plan ids" do
      left = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM emp", nil}}
      right = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM dept", nil}}

      join_condition =
        Column.and_(
          Column.eq(DataFrame.col(left, "dept_id"), DataFrame.col(right, "dept_id")),
          Column.eq(DataFrame.col(left, "org_id"), DataFrame.col(right, "org_id"))
        )

      joined = DataFrame.join(left, right, join_condition, :inner)
      {plan, _counter} = PlanEncoder.encode(joined.plan, 0)
      assert %Relation{rel_type: {:join, join}} = root_relation(plan)
      assert %Expression{expr_type: {:unresolved_function, top_fn}} = join.join_condition

      condition_plan_ids =
        Enum.flat_map(top_fn.arguments, fn
          %Expression{expr_type: {:unresolved_function, nested_fn}} ->
            Enum.flat_map(nested_fn.arguments, fn
              %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
              _ -> []
            end)

          _ ->
            []
        end)

      assert length(condition_plan_ids) == 4

      assert Enum.uniq(condition_plan_ids) |> Enum.sort() ==
               Enum.sort([join.left.common.plan_id, join.right.common.plan_id])
    end

    test "maps bound self-join filter and projection to concrete join sides" do
      base = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM emp", nil}}
      e1 = DataFrame.alias_(base, "e1")
      e2 = DataFrame.alias_(base, "e2")

      planned =
        DataFrame.join(e1, e2, ["dept_id"], :inner)
        |> DataFrame.filter(Column.lt(DataFrame.col(e1, "emp_id"), DataFrame.col(e2, "emp_id")))
        |> DataFrame.select([DataFrame.col(e1, "name"), DataFrame.col(e2, "name")])

      {plan, _counter} = PlanEncoder.encode(planned.plan, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      assert %Relation{rel_type: {:filter, filter}} = project.input
      assert %Relation{rel_type: {:join, _join}} = filter.input

      assert %Expression{expr_type: {:unresolved_function, filter_fn}} = filter.condition

      filter_plan_ids =
        Enum.flat_map(filter_fn.arguments, fn
          %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
          _ -> []
        end)

      project_plan_ids =
        Enum.flat_map(project.expressions, fn
          %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
          _ -> []
        end)

      assert Enum.sort(Enum.uniq(filter_plan_ids)) == Enum.sort(Enum.uniq(project_plan_ids))
      assert Enum.uniq(filter_plan_ids) |> length() == 2
      assert Enum.all?(filter_plan_ids, &is_integer/1)
    end

    test "maps triple self-join filter and projection to all join inputs" do
      base = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM nums", nil}}
      a = DataFrame.alias_(base, "a")
      b = DataFrame.alias_(base, "b")
      c = DataFrame.alias_(base, "c")

      planned =
        DataFrame.cross_join(a, b)
        |> DataFrame.cross_join(c)
        |> DataFrame.filter(
          Column.and_(
            Column.lt(DataFrame.col(a, "n"), DataFrame.col(b, "n")),
            Column.lt(DataFrame.col(b, "n"), DataFrame.col(c, "n"))
          )
        )
        |> DataFrame.select([DataFrame.col(a, "n"), DataFrame.col(b, "n"), DataFrame.col(c, "n")])

      {plan, _counter} = PlanEncoder.encode(planned.plan, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      assert %Relation{rel_type: {:filter, filter}} = project.input
      assert %Relation{rel_type: {:join, outer_join}} = filter.input
      assert %Relation{rel_type: {:join, inner_join}} = outer_join.left

      assert %Expression{expr_type: {:unresolved_function, top_fn}} = filter.condition

      filter_plan_ids =
        Enum.flat_map(top_fn.arguments, fn
          %Expression{expr_type: {:unresolved_function, nested_fn}} ->
            Enum.flat_map(nested_fn.arguments, fn
              %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
              _ -> []
            end)

          _ ->
            []
        end)

      project_plan_ids =
        Enum.flat_map(project.expressions, fn
          %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
          _ -> []
        end)

      _join_plan_ids =
        Enum.sort([
          inner_join.left.common.plan_id,
          inner_join.right.common.plan_id,
          outer_join.right.common.plan_id
        ])

      assert Enum.sort(Enum.uniq(filter_plan_ids)) == Enum.sort(Enum.uniq(project_plan_ids))
      assert Enum.uniq(filter_plan_ids) |> length() == 3
      assert Enum.all?(filter_plan_ids, &is_integer/1)
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

    test "maps bound expressions to child relation plan id" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1 AS id, 2 AS name_a", nil}}
      selected = DataFrame.select(df, [DataFrame.col(df, "id"), DataFrame.col_regex(df, "^name")])
      {plan, _counter} = PlanEncoder.encode(selected.plan, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)

      plan_ids =
        Enum.flat_map(project.expressions, fn
          %Expression{expr_type: {:unresolved_attribute, attr}} -> [attr.plan_id]
          %Expression{expr_type: {:unresolved_regex, regex}} -> [regex.plan_id]
          _ -> []
        end)

      assert Enum.uniq(plan_ids) == [project.input.common.plan_id]
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

    test "encodes time literal with sub-second precision without double-scaling" do
      # ~T[12:00:00.1] => 43_200 seconds, 100_000 microseconds, precision 1.
      # Nanos must be raw microseconds * 1000, not scaled to precision 6.
      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:time, %Expression.Literal.Time{nano: 43_200_100_000_000, precision: 1}}
                  }}
             } = PlanEncoder.encode_expression({:lit, ~T[12:00:00.1]})
    end

    test "encodes time literal with full microsecond precision" do
      # ~T[12:30:45.123456] => seconds 45_045, micros 123_456, precision 6.
      expected_nanos = 45_045 * 1_000_000_000 + 123_456 * 1000

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:time, %Expression.Literal.Time{nano: ^expected_nanos, precision: 6}}
                  }}
             } = PlanEncoder.encode_expression({:lit, ~T[12:30:45.123456]})
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

    test "encodes plain Elixir list/map literals" do
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
             } = PlanEncoder.encode_expression({:lit, [1, 2, 3]})

      assert %Expression{
               expr_type: {:unresolved_function, unresolved_fn}
             } = PlanEncoder.encode_expression({:lit, %{"k" => 1}})

      assert unresolved_fn.function_name == "map"
      assert length(unresolved_fn.arguments) == 2

      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:array,
                       %Expression.Literal.Array{
                         element_type: %Spark.Connect.DataType{kind: {:array, _}},
                         elements: [_ | _]
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, [[1, 2], [3, 4]]})
    end

    test "encodes map literals with array values via map function" do
      assert %Expression{expr_type: {:unresolved_function, unresolved_fn}} =
               PlanEncoder.encode_expression({:lit, %{"scores" => [1, 2], "tags" => ["a", "b"]}})

      assert unresolved_fn.function_name == "map"
      assert length(unresolved_fn.arguments) == 4
    end

    test "encodes empty plain list/map literals via functions" do
      assert %Expression{expr_type: {:unresolved_function, array_fn}} =
               PlanEncoder.encode_expression({:lit, []})

      assert array_fn.function_name == "array"
      assert array_fn.arguments == []

      assert %Expression{expr_type: {:unresolved_function, map_fn}} =
               PlanEncoder.encode_expression({:lit, %{}})

      assert map_fn.function_name == "map"
      assert map_fn.arguments == []
    end

    test "encodes mixed scalar/nested map values as string map literals" do
      assert %Expression{expr_type: {:unresolved_function, unresolved_fn}} =
               PlanEncoder.encode_expression(
                 {:lit, %{"name" => "Alice", "addr" => %{"city" => "NYC"}}}
               )

      values =
        unresolved_fn.arguments
        |> Enum.chunk_every(2)
        |> Enum.map(fn
          [
            _key_expr,
            %Expression{
              expr_type: {:literal, %Expression.Literal{literal_type: {:string, value}}}
            }
          ] ->
            value

          _ ->
            nil
        end)

      assert Enum.any?(values, &(&1 == "Alice"))
      assert Enum.any?(values, &(&1 == "{\"city\":\"NYC\"}"))
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

    test "encodes integers outside int64 range as decimal literals" do
      assert %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type:
                      {:decimal,
                       %Expression.Literal.Decimal{
                         value: "9999999999999999999"
                       }}
                  }}
             } = PlanEncoder.encode_expression({:lit, 9_999_999_999_999_999_999})
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

      assert %Relation{rel_type: {:to_schema, to_schema}} = root_relation(plan)

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

      assert %Relation{rel_type: {:to_schema, to_schema}} = root_relation(plan)

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

      assert %Relation{rel_type: {:to_schema, to_schema}} = root_relation(plan)

      assert %Spark.Connect.ToSchema{} = to_schema
      assert %Spark.Connect.DataType{kind: {:long, _}} = to_schema.schema
    end
  end

  describe "encode/2 with cached_remote_relation" do
    test "encodes cached remote relation" do
      {plan, _counter} =
        PlanEncoder.encode({:with_relations, {:cached_remote_relation, "rel-1"}, []}, 0)

      assert %Relation{rel_type: {:cached_remote_relation, cached}} = root_relation(plan)

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
      assert %Relation{rel_type: {:project, project}} = root_relation(encoded)
      [expr] = project.expressions

      assert {:unresolved_attribute, %Expression.UnresolvedAttribute{plan_id: plan_id}} =
               expr.expr_type

      assert is_integer(plan_id)
      assert plan_id == project.input.common.plan_id
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

    test "encodes correlated exists subquery without plan encode crash" do
      main = {:sql, "SELECT * FROM main_t", nil}

      exists_plan =
        {:filter, {:sql, "SELECT * FROM lookup_t", nil},
         {:fn, "==", [{:col, "ref_id"}, {:col, "id", main}], false}}

      plan = {:filter, main, {:subquery, :exists, exists_plan, []}}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = encoded
      assert wr.references != []
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

    test "DataFrame-bound as-of columns remap to left/right child plan_ids" do
      left_plan = {:sql, "SELECT * FROM t1", nil}
      right_plan = {:sql, "SELECT * FROM t2", nil}

      plan =
        {:as_of_join, left_plan, right_plan, {:col, "ts", left_plan}, {:col, "ts", right_plan},
         {:lit, nil}, [], "inner", {:lit, nil}, true, "backward"}

      {encoded, _} = PlanEncoder.encode(plan, 0)

      assert %Relation{rel_type: {:as_of_join, as_of}} = root_relation(encoded)

      left_id = as_of.left.common.plan_id
      right_id = as_of.right.common.plan_id

      assert %Expression{
               expr_type:
                 {:unresolved_attribute,
                  %Expression.UnresolvedAttribute{
                    unparsed_identifier: "ts",
                    plan_id: ^left_id
                  }}
             } = as_of.left_as_of

      assert %Expression{
               expr_type:
                 {:unresolved_attribute,
                  %Expression.UnresolvedAttribute{
                    unparsed_identifier: "ts",
                    plan_id: ^right_id
                  }}
             } = as_of.right_as_of
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

  describe "Stream A — do_remap_expr_plan_ids traversal of new expression forms" do
    # Each clause added in this stream must descend into nested col references so
    # they get their plan_id rewritten; otherwise bound cols inside window
    # functions / subquery in_values / call_function / named_arg pass through
    # with stale synthetic ids that Spark Connect can't resolve.

    test "remaps col plan_id inside window partition and order specs" do
      child = {:sql, "SELECT 1 AS id, 2 AS val", nil}

      window_expr = {
        :window,
        {:fn, "sum", [{:col, "val", 999}], false},
        [{:col, "id", 999}],
        [{:sort_order, {:col, "val", 999}, :asc, :nulls_first}],
        nil
      }

      {plan, _} = PlanEncoder.encode({:project, child, [window_expr]}, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      input_plan_id = project.input.common.plan_id

      [%Expression{expr_type: {:window, window}}] = project.expressions

      partition_ids =
        Enum.flat_map(window.partition_spec, fn e ->
          case e.expr_type do
            {:unresolved_attribute, a} -> [a.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(partition_ids) == [input_plan_id]
    end

    test "remaps col plan_id inside call_function args" do
      child = {:sql, "SELECT 1 AS id", nil}
      call_expr = {:call_function, "my_udf", [{:col, "id", 999}]}
      {plan, _} = PlanEncoder.encode({:project, child, [call_expr]}, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      input_plan_id = project.input.common.plan_id

      [%Expression{expr_type: {:call_function, cf}}] = project.expressions

      ids =
        Enum.flat_map(cf.arguments, fn e ->
          case e.expr_type do
            {:unresolved_attribute, a} -> [a.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(ids) == [input_plan_id]
    end

    test "remaps col plan_id inside named_arg value" do
      child = {:sql, "SELECT 1 AS id", nil}
      named_expr = {:named_arg, "key", {:col, "id", 999}}
      {plan, _} = PlanEncoder.encode({:project, child, [named_expr]}, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      input_plan_id = project.input.common.plan_id

      [%Expression{expr_type: {:named_argument_expression, na}}] = project.expressions

      assert %Expression{expr_type: {:unresolved_attribute, attr}} = na.value
      assert attr.plan_id == input_plan_id
    end

    test "remaps col plan_id inside subquery in_values and threads state" do
      child = {:sql, "SELECT 1 AS id", nil}
      sub_plan_id = 77
      sub_plan = {:plan_id, sub_plan_id, {:sql, "SELECT 42 AS x", nil}}

      in_expr =
        {:subquery, :in, sub_plan, in_values: [{:col, "id", 999}, {:col, "id", 999}]}

      {plan, _} = PlanEncoder.encode({:project, child, [in_expr]}, 0)
      assert %Relation{rel_type: {:project, project}} = root_relation(plan)
      input_plan_id = project.input.common.plan_id

      [%Expression{expr_type: {:subquery_expression, sq}}] = project.expressions

      ids =
        Enum.flat_map(sq.in_subquery_values, fn e ->
          case e.expr_type do
            {:unresolved_attribute, a} -> [a.plan_id]
            _ -> []
          end
        end)

      assert Enum.all?(ids, &(&1 == input_plan_id))
    end

    test "aggregate remaps grouping and agg expressions with shared state (multi-candidate child)" do
      # aggregate over a join → child has two candidate plan_ids
      left = {:sql, "SELECT 1 AS dept, 2 AS salary", nil}
      right = {:sql, "SELECT 3 AS dept, 4 AS bonus", nil}
      join = {:join, left, right, nil, :inner, ["dept"]}
      grouping = [{:col, "dept", 999}]
      agg = [{:fn, "sum", [{:col, "salary", 888}], false}]
      {plan, _} = PlanEncoder.encode({:aggregate, join, :groupby, grouping, agg}, 0)

      assert %Relation{rel_type: {:aggregate, agg_proto}} = root_relation(plan)

      # Both foreign ids (999, 888) must be resolved to integer plan_ids
      [g_expr] = agg_proto.grouping_expressions
      assert {:unresolved_attribute, g_attr} = g_expr.expr_type
      assert is_integer(g_attr.plan_id)

      [a_expr] = agg_proto.aggregate_expressions
      assert {:unresolved_function, af} = a_expr.expr_type
      [arg] = af.arguments
      assert {:unresolved_attribute, a_attr} = arg.expr_type
      assert is_integer(a_attr.plan_id)
    end

    test "join condition preserves left/right assignment when right side is referenced first" do
      left = {:sql, "SELECT 1 AS id", nil}
      right = {:sql, "SELECT 2 AS id", nil}

      condition = {:fn, "==", [{:col, "id", right}, {:col, "id", left}], false}
      {plan, _} = PlanEncoder.encode({:join, left, right, condition, :inner, []}, 0)

      assert %Relation{rel_type: {:join, join}} = root_relation(plan)
      left_plan_id = join.left.common.plan_id
      right_plan_id = join.right.common.plan_id

      assert %Expression{expr_type: {:unresolved_function, eq}} = join.join_condition
      [right_arg, left_arg] = eq.arguments
      assert {:unresolved_attribute, right_attr} = right_arg.expr_type
      assert {:unresolved_attribute, left_attr} = left_arg.expr_type
      assert right_attr.plan_id == right_plan_id
      assert left_attr.plan_id == left_plan_id
    end

    test "aggregate over join preserves side assignment when grouping references right first" do
      left = {:sql, "SELECT 1 AS dept, 2 AS salary", nil}
      right = {:sql, "SELECT 3 AS dept, 4 AS bonus", nil}
      join = {:join, left, right, nil, :inner, ["dept"]}
      grouping = [{:col, "dept", right}]
      agg = [{:fn, "sum", [{:col, "salary", left}], false}]

      {plan, _} = PlanEncoder.encode({:aggregate, join, :groupby, grouping, agg}, 0)

      assert %Relation{rel_type: {:aggregate, aggregate}} = root_relation(plan)
      assert %Relation{rel_type: {:join, join}} = aggregate.input
      left_plan_id = join.left.common.plan_id
      right_plan_id = join.right.common.plan_id

      [grouping_expr] = aggregate.grouping_expressions
      assert {:unresolved_attribute, grouping_attr} = grouping_expr.expr_type
      assert grouping_attr.plan_id == right_plan_id

      [agg_expr] = aggregate.aggregate_expressions
      assert {:unresolved_function, sum} = agg_expr.expr_type
      [sum_arg] = sum.arguments
      assert {:unresolved_attribute, sum_attr} = sum_arg.expr_type
      assert sum_attr.plan_id == left_plan_id
    end
  end

  describe "Stream A — relation expression plan_id remap to encoded child input" do
    # The encoded child relation gets its own plan_id; expressions captured against
    # a foreign plan_id (e.g. via attach_with_relations / synthetic ids) must be
    # rewritten to point at that encoded input. Mirrors the project/filter pattern.

    test "sort remaps sort_order children to encoded input plan_id" do
      child = {:sql, "SELECT 1 AS id", nil}
      sort_orders = [{:sort_order, {:col, "id", 999}, :asc, :nulls_first}]
      {plan, _} = PlanEncoder.encode({:sort, child, sort_orders}, 0)
      assert %Relation{rel_type: {:sort, sort}} = root_relation(plan)
      input_plan_id = sort.input.common.plan_id

      ids =
        Enum.flat_map(sort.order, fn so ->
          case so.child.expr_type do
            {:unresolved_attribute, attr} -> [attr.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(ids) == [input_plan_id]
    end

    test "with_columns remaps alias body to encoded input plan_id" do
      child = {:sql, "SELECT 1 AS id", nil}
      aliases = [{:alias, {:col, "id", 999}, "id2"}]
      {plan, _} = PlanEncoder.encode({:with_columns, child, aliases}, 0)
      assert %Relation{rel_type: {:with_columns, wc}} = root_relation(plan)
      input_plan_id = wc.input.common.plan_id

      ids =
        Enum.flat_map(wc.aliases, fn a ->
          case a.expr.expr_type do
            {:unresolved_attribute, attr} -> [attr.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(ids) == [input_plan_id]
    end

    test "aggregate remaps grouping and aggregate expressions" do
      child = {:sql, "SELECT * FROM emp", nil}
      grouping = [{:col, "dept", 999}]
      aggs = [{:fn, "sum", [{:col, "salary", 999}], false}]
      {plan, _} = PlanEncoder.encode({:aggregate, child, :groupby, grouping, aggs}, 0)
      assert %Relation{rel_type: {:aggregate, agg}} = root_relation(plan)
      input_plan_id = agg.input.common.plan_id

      grouping_ids =
        Enum.flat_map(agg.grouping_expressions, fn e ->
          case e.expr_type do
            {:unresolved_attribute, attr} -> [attr.plan_id]
            _ -> []
          end
        end)

      agg_ids =
        Enum.flat_map(agg.aggregate_expressions, fn e ->
          case e.expr_type do
            {:unresolved_function, f} ->
              Enum.flat_map(f.arguments, fn arg ->
                case arg.expr_type do
                  {:unresolved_attribute, attr} -> [attr.plan_id]
                  _ -> []
                end
              end)

            _ ->
              []
          end
        end)

      assert Enum.uniq(grouping_ids) == [input_plan_id]
      assert Enum.uniq(agg_ids) == [input_plan_id]
    end

    test "drop col_exprs are remapped to encoded input plan_id" do
      child = {:sql, "SELECT 1 AS id, 2 AS x", nil}
      {plan, _} = PlanEncoder.encode({:drop, child, [], [{:col, "x", 999}]}, 0)
      assert %Relation{rel_type: {:drop, drop}} = root_relation(plan)
      input_plan_id = drop.input.common.plan_id

      ids =
        Enum.flat_map(drop.columns, fn e ->
          case e.expr_type do
            {:unresolved_attribute, attr} -> [attr.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(ids) == [input_plan_id]
    end

    test "repartition_by_expression remaps partition exprs" do
      child = {:sql, "SELECT 1 AS id", nil}

      {plan, _} =
        PlanEncoder.encode({:repartition_by_expression, child, [{:col, "id", 999}], 4}, 0)

      assert %Relation{rel_type: {:repartition_by_expression, r}} = root_relation(plan)

      ids =
        Enum.flat_map(r.partition_exprs, fn e ->
          case e.expr_type do
            {:unresolved_attribute, attr} -> [attr.plan_id]
            _ -> []
          end
        end)

      assert Enum.uniq(ids) == [r.input.common.plan_id]
    end
  end

  describe "Stream A — TVF / sample_by / lateral_join expression rewrite" do
    # GPT-02 / GPT-03 / GPT-24: rewrite_plan must descend into TVF args and
    # stat_sample_by col_expr; lateral_join must remap right-side TVF args
    # against the encoded left plan id.

    test "GPT-03: stat_sample_by col_expr with embedded plan resolves to encoded input" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1 AS dept", nil}}
      bound_col = {:col, "dept", df.plan}

      {plan, _} =
        PlanEncoder.encode(
          {:stat_sample_by, df.plan, bound_col, [{"a", 0.1}], 42},
          0
        )

      assert %Relation{rel_type: {:sample_by, sample}} = root_relation(plan)
      input_plan_id = sample.input.common.plan_id

      assert %Expression{expr_type: {:unresolved_attribute, attr}} = sample.col
      assert attr.plan_id == input_plan_id
    end

    test "GPT-02: table_valued_function args with embedded plan get assigned plan_ids" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT array(1,2,3) AS arr", nil}}
      bound_arg = {:col, "arr", df.plan}
      tvf = {:table_valued_function, "explode", [bound_arg]}

      {plan, _} = PlanEncoder.encode(tvf, 0)

      # Top-level wraps in WithRelations because the embedded plan was hoisted
      # into refs. The TVF arg's plan_id must be an integer (assigned by rewrite).
      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}} = plan

      assert %Relation{rel_type: {:unresolved_table_valued_function, tvf_proto}} = wr.root
      [arg] = tvf_proto.arguments
      assert %Expression{expr_type: {:unresolved_attribute, attr}} = arg
      assert is_integer(attr.plan_id)
      # And the with_relations wrapper carries the referenced plan.
      assert length(wr.references) == 1
    end

    test "GPT-24: lateral_join with TVF right side remaps args to encoded left plan_id (leaf left)" do
      left_df = %DataFrame{session: self(), plan: {:sql, "SELECT array(1,2,3) AS arr", nil}}
      bound_arg = {:col, "arr", left_df.plan}
      tvf = {:table_valued_function, "explode", [bound_arg]}

      {plan, _} =
        PlanEncoder.encode({:lateral_join, left_df.plan, tvf, nil, :inner}, 0)

      assert %Relation{rel_type: {:lateral_join, lj}} = root_relation(plan)
      left_plan_id = lj.left.common.plan_id

      assert %Relation{rel_type: {:unresolved_table_valued_function, tvf_proto}} = lj.right
      [arg] = tvf_proto.arguments
      assert %Expression{expr_type: {:unresolved_attribute, attr}} = arg
      assert attr.plan_id == left_plan_id
    end

    test "GPT-24: lateral_join remaps TVF args to non-leaf left plan_id (not its inputs)" do
      # left is a project (non-leaf) — args must point at the project's plan_id,
      # not the underlying SQL child's plan_id.
      left_df = %DataFrame{session: self(), plan: {:sql, "SELECT array(1,2,3) AS arr", nil}}
      project_plan = {:project, left_df.plan, [{:col, "arr"}]}
      bound_arg = {:col, "arr", project_plan}
      tvf = {:table_valued_function, "explode", [bound_arg]}

      {plan, _} =
        PlanEncoder.encode({:lateral_join, project_plan, tvf, nil, :inner}, 0)

      assert %Relation{rel_type: {:lateral_join, lj}} = root_relation(plan)
      left_plan_id = lj.left.common.plan_id
      assert %Relation{rel_type: {:project, project}} = lj.left
      sql_plan_id = project.input.common.plan_id

      assert %Relation{rel_type: {:unresolved_table_valued_function, tvf_proto}} = lj.right
      [arg] = tvf_proto.arguments
      assert %Expression{expr_type: {:unresolved_attribute, attr}} = arg
      # Must match the project's plan_id, NOT the SQL child's plan_id.
      assert attr.plan_id == left_plan_id
      refute attr.plan_id == sql_plan_id
    end
  end

  defp root_relation(%Plan{op_type: {:root, %Relation{rel_type: {:with_relations, wr}}}}),
    do: wr.root

  defp root_relation(%Plan{op_type: {:root, %Relation{} = relation}}), do: relation
end
