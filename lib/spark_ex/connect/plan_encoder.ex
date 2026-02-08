defmodule SparkEx.Connect.PlanEncoder do
  @moduledoc """
  Encodes SparkEx internal plan representations into Spark Connect protobuf messages.

  All encode functions accept a `plan_id_counter` (integer) and return
  `{encoded, new_counter}` to avoid GenServer call deadlocks when invoked
  from within Session handle_call.
  """

  alias Spark.Connect.{
    Expression,
    Plan,
    Relation,
    RelationCommon,
    SQL,
    Range,
    WithRelations
  }

  alias SparkEx.Column

  @doc """
  Encodes an internal plan into a `Spark.Connect.Plan` proto.

  Returns `{plan, new_counter}`.
  """
  @spec encode(term(), non_neg_integer()) :: {Plan.t(), non_neg_integer()}
  def encode(plan, counter) do
    {relation, counter} = encode_relation(plan, counter)
    {%Plan{op_type: {:root, relation}}, counter}
  end

  @doc """
  Encodes an internal plan into a `Spark.Connect.Relation` proto.

  Returns `{relation, new_counter}`.
  """
  @spec encode_relation(term(), non_neg_integer()) :: {Relation.t(), non_neg_integer()}
  def encode_relation({:sql, query, args}, counter) do
    {plan_id, counter} = next_id(counter)

    sql =
      case args do
        args when is_map(args) and map_size(args) > 0 ->
          named =
            Map.new(args, fn {k, v} ->
              {to_string(k), encode_sql_argument(v)}
            end)

          %SQL{query: query, named_arguments: named}

        args when is_list(args) and length(args) > 0 ->
          pos = Enum.map(args, &encode_sql_argument/1)
          %SQL{query: query, pos_arguments: pos}

        _ ->
          %SQL{query: query}
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:sql, sql}
    }

    {relation, counter}
  end

  # Query shapes requiring relation references (used by subquery/reference forms).
  def encode_relation({:with_relations, root_plan, reference_plans}, counter) do
    {plan_id, counter} = next_id(counter)
    {root, counter} = encode_relation(root_plan, counter)

    {references, counter} =
      Enum.map_reduce(reference_plans, counter, fn ref_plan, acc ->
        encode_relation(ref_plan, acc)
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:with_relations, %WithRelations{root: root, references: references}}
    }

    {relation, counter}
  end

  def encode_relation({:range, start, end_, step, num_partitions}, counter) do
    {plan_id, counter} = next_id(counter)

    range = %Range{
      start: start,
      end: end_,
      step: step,
      num_partitions: num_partitions
    }

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:range, range}
    }

    {relation, counter}
  end

  def encode_relation({:limit, child_plan, n}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:limit, %Spark.Connect.Limit{input: child, limit: n}}
    }

    {relation, counter}
  end

  # --- Milestone 2: Read ---

  def encode_relation({:read_named_table, table_name, options}, counter) do
    {plan_id, counter} = next_id(counter)

    read = %Spark.Connect.Read{
      is_streaming: false,
      read_type:
        {:named_table,
         %Spark.Connect.Read.NamedTable{
           unparsed_identifier: table_name,
           options: options
         }}
    }

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:read, read}
    }

    {relation, counter}
  end

  def encode_relation({:read_data_source, format, paths, schema, options}, counter) do
    {plan_id, counter} = next_id(counter)

    read = %Spark.Connect.Read{
      is_streaming: false,
      read_type:
        {:data_source,
         %Spark.Connect.Read.DataSource{
           format: format,
           schema: schema,
           paths: paths,
           options: options
         }}
    }

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:read, read}
    }

    {relation, counter}
  end

  # --- Milestone 2: Project ---

  def encode_relation({:project, child_plan, expressions}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)
    exprs = Enum.map(expressions, &encode_expression/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:project, %Spark.Connect.Project{input: child, expressions: exprs}}
    }

    {relation, counter}
  end

  # --- Milestone 2: Filter ---

  def encode_relation({:filter, child_plan, condition}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)
    cond_expr = encode_expression(condition)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:filter, %Spark.Connect.Filter{input: child, condition: cond_expr}}
    }

    {relation, counter}
  end

  # --- Milestone 2: Sort ---

  def encode_relation({:sort, child_plan, sort_orders}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)
    orders = Enum.map(sort_orders, &encode_sort_order/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:sort, %Spark.Connect.Sort{input: child, order: orders, is_global: true}}
    }

    {relation, counter}
  end

  # --- Milestone 2: WithColumns ---

  def encode_relation({:with_columns, child_plan, aliases}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    alias_protos =
      Enum.map(aliases, fn {:alias, expr, name} ->
        %Expression.Alias{
          expr: encode_expression(expr),
          name: [name]
        }
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:with_columns, %Spark.Connect.WithColumns{input: child, aliases: alias_protos}}
    }

    {relation, counter}
  end

  # --- Milestone 2: Drop ---

  def encode_relation({:drop, child_plan, column_names}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:drop, %Spark.Connect.Drop{input: child, column_names: column_names}}
    }

    {relation, counter}
  end

  # --- Milestone 2: ShowString ---

  def encode_relation({:show_string, child_plan, num_rows, truncate, vertical}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:show_string,
         %Spark.Connect.ShowString{
           input: child,
           num_rows: num_rows,
           truncate: truncate,
           vertical: vertical
         }}
    }

    {relation, counter}
  end

  @doc """
  Wraps a plan in an aggregate count(*) relation.

  Returns `{plan, new_counter}`.
  """
  @spec encode_count(term(), non_neg_integer()) :: {Plan.t(), non_neg_integer()}
  def encode_count(plan, counter) do
    {child, counter} = encode_relation(plan, counter)
    {plan_id, counter} = next_id(counter)

    count_fn = %Expression{
      expr_type:
        {:unresolved_function,
         %Expression.UnresolvedFunction{
           function_name: "count",
           arguments: [
             %Expression{
               expr_type:
                 {:literal,
                  %Expression.Literal{
                    literal_type: {:integer, 1}
                  }}
             }
           ],
           is_distinct: false
         }}
    }

    aggregate = %Spark.Connect.Aggregate{
      input: child,
      group_type: :GROUP_TYPE_GROUPBY,
      grouping_expressions: [],
      aggregate_expressions: [count_fn]
    }

    plan = %Plan{
      op_type:
        {:root,
         %Relation{
           common: %RelationCommon{plan_id: plan_id},
           rel_type: {:aggregate, aggregate}
         }}
    }

    {plan, counter}
  end

  # --- Expression Encoding ---

  @doc """
  Encodes an internal expression representation into a `Spark.Connect.Expression` proto.
  """
  @spec encode_expression(term()) :: Expression.t()
  def encode_expression({:col, name}) do
    %Expression{
      expr_type:
        {:unresolved_attribute, %Expression.UnresolvedAttribute{unparsed_identifier: name}}
    }
  end

  def encode_expression({:lit, value}) do
    encode_literal_expression(value)
  end

  def encode_expression({:expr, string}) do
    %Expression{
      expr_type: {:expression_string, %Expression.ExpressionString{expression: string}}
    }
  end

  def encode_expression({:fn, name, args, is_distinct}) do
    %Expression{
      expr_type:
        {:unresolved_function,
         %Expression.UnresolvedFunction{
           function_name: name,
           arguments: Enum.map(args, &encode_expression/1),
           is_distinct: is_distinct
         }}
    }
  end

  def encode_expression({:alias, expr, name}) do
    %Expression{
      expr_type:
        {:alias,
         %Expression.Alias{
           expr: encode_expression(expr),
           name: [name]
         }}
    }
  end

  def encode_expression({:cast, expr, type_str}) do
    %Expression{
      expr_type:
        {:cast,
         %Expression.Cast{
           expr: encode_expression(expr),
           cast_to_type: {:type_str, type_str}
         }}
    }
  end

  def encode_expression({:sort_order, expr, direction, null_ordering}) do
    %Expression{
      expr_type:
        {:sort_order,
         %Expression.SortOrder{
           child: encode_expression(expr),
           direction: encode_sort_direction(direction),
           null_ordering: encode_null_ordering(null_ordering)
         }}
    }
  end

  def encode_expression({:star}) do
    %Expression{
      expr_type: {:unresolved_star, %Expression.UnresolvedStar{}}
    }
  end

  def encode_expression({:star, target}) do
    %Expression{
      expr_type: {:unresolved_star, %Expression.UnresolvedStar{unparsed_target: target}}
    }
  end

  # --- Private ---

  defp next_id(counter), do: {counter, counter + 1}

  defp encode_literal_expression(value) do
    %Expression{expr_type: {:literal, encode_literal(value)}}
  end

  defp encode_sql_argument(%Column{expr: expr}) do
    encode_expression(expr)
  end

  defp encode_sql_argument({:col, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:lit, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:expr, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:fn, _, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:alias, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:sort_order, _, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:cast, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:star} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:star, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument(value), do: encode_literal_expression(value)

  defp encode_literal(nil),
    do: %Expression.Literal{literal_type: {:null, %Spark.Connect.DataType.NULL{}}}

  defp encode_literal(true), do: %Expression.Literal{literal_type: {:boolean, true}}
  defp encode_literal(false), do: %Expression.Literal{literal_type: {:boolean, false}}

  defp encode_literal(v) when is_integer(v) and v >= -2_147_483_648 and v <= 2_147_483_647 do
    %Expression.Literal{literal_type: {:integer, v}}
  end

  defp encode_literal(v) when is_integer(v) do
    %Expression.Literal{literal_type: {:long, v}}
  end

  defp encode_literal(v) when is_float(v) do
    %Expression.Literal{literal_type: {:double, v}}
  end

  defp encode_literal(v) when is_binary(v) do
    %Expression.Literal{literal_type: {:string, v}}
  end

  defp encode_sort_order({:sort_order, expr, direction, null_ordering}) do
    %Expression.SortOrder{
      child: encode_expression(expr),
      direction: encode_sort_direction(direction),
      null_ordering: encode_null_ordering(null_ordering)
    }
  end

  defp encode_sort_order({:col, _name} = col_expr) do
    %Expression.SortOrder{
      child: encode_expression(col_expr),
      direction: :SORT_DIRECTION_ASCENDING,
      null_ordering: :SORT_NULLS_UNSPECIFIED
    }
  end

  defp encode_sort_order(other) do
    %Expression.SortOrder{
      child: encode_expression(other),
      direction: :SORT_DIRECTION_ASCENDING,
      null_ordering: :SORT_NULLS_UNSPECIFIED
    }
  end

  defp encode_sort_direction(:asc), do: :SORT_DIRECTION_ASCENDING
  defp encode_sort_direction(:desc), do: :SORT_DIRECTION_DESCENDING

  defp encode_null_ordering(nil), do: :SORT_NULLS_UNSPECIFIED
  defp encode_null_ordering(:nulls_first), do: :SORT_NULLS_FIRST
  defp encode_null_ordering(:nulls_last), do: :SORT_NULLS_LAST
end
