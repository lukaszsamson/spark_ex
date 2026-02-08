defmodule SparkEx.Connect.PlanEncoder do
  @moduledoc """
  Encodes SparkEx internal plan representations into Spark Connect protobuf messages.

  All encode functions accept a `plan_id_counter` (integer) and return
  `{encoded, new_counter}` to avoid GenServer call deadlocks when invoked
  from within Session handle_call.
  """

  alias Spark.Connect.{
    CachedLocalRelation,
    ChunkedCachedLocalRelation,
    Expression,
    LocalRelation,
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

  # --- Local relation types ---

  def encode_relation({:local_relation, data, schema}, counter) do
    {plan_id, counter} = next_id(counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:local_relation, %LocalRelation{data: data, schema: schema}}
    }

    {relation, counter}
  end

  def encode_relation({:cached_local_relation, hash}, counter) do
    {plan_id, counter} = next_id(counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:cached_local_relation, %CachedLocalRelation{hash: hash}}
    }

    {relation, counter}
  end

  def encode_relation({:chunked_cached_local_relation, data_hashes, schema_hash}, counter) do
    {plan_id, counter} = next_id(counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:chunked_cached_local_relation,
         %ChunkedCachedLocalRelation{
           dataHashes: data_hashes,
           schemaHash: schema_hash
         }}
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
    encode_relation({:sort, child_plan, sort_orders, true}, counter)
  end

  def encode_relation({:sort, child_plan, sort_orders, is_global}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)
    orders = Enum.map(sort_orders, &encode_sort_order/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:sort, %Spark.Connect.Sort{input: child, order: orders, is_global: is_global}}
    }

    {relation, counter}
  end

  # --- Milestone 2: WithColumns ---

  def encode_relation({:with_columns, child_plan, aliases}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    alias_protos =
      Enum.map(aliases, fn
        {:alias, expr, name} ->
          %Expression.Alias{
            expr: encode_expression(expr),
            name: [name]
          }

        {:alias, expr, name, metadata} ->
          %Expression.Alias{
            expr: encode_expression(expr),
            name: [name],
            metadata: metadata
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

  # --- Milestone 3: Aggregate ---

  def encode_relation({:aggregate, child_plan, group_type, grouping_exprs, agg_exprs}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:aggregate,
         %Spark.Connect.Aggregate{
           input: child,
           group_type: encode_group_type(group_type),
           grouping_expressions: Enum.map(grouping_exprs, &encode_expression/1),
           aggregate_expressions: Enum.map(agg_exprs, &encode_expression/1)
         }}
    }

    {relation, counter}
  end

  # --- Milestone 3: Join ---

  def encode_relation(
        {:join, left_plan, right_plan, join_condition, join_type, using_columns},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {left, counter} = encode_relation(left_plan, counter)
    {right, counter} = encode_relation(right_plan, counter)

    join_cond =
      case join_condition do
        nil -> nil
        expr -> encode_expression(expr)
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:join,
         %Spark.Connect.Join{
           left: left,
           right: right,
           join_condition: join_cond,
           join_type: encode_join_type(join_type),
           using_columns: using_columns || []
         }}
    }

    {relation, counter}
  end

  # --- Milestone 3: Deduplicate ---

  def encode_relation({:deduplicate, child_plan, column_names, all_columns}, counter) do
    encode_relation({:deduplicate, child_plan, column_names, all_columns, false}, counter)
  end

  def encode_relation(
        {:deduplicate, child_plan, column_names, all_columns, within_watermark},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:deduplicate,
         %Spark.Connect.Deduplicate{
           input: child,
           column_names: column_names || [],
           all_columns_as_keys: all_columns,
           within_watermark: within_watermark
         }}
    }

    {relation, counter}
  end

  # --- Milestone 3: SetOperation ---

  def encode_relation({:set_operation, left_plan, right_plan, op_type, is_all}, counter) do
    encode_relation({:set_operation, left_plan, right_plan, op_type, is_all, []}, counter)
  end

  def encode_relation({:set_operation, left_plan, right_plan, op_type, is_all, opts}, counter) do
    {plan_id, counter} = next_id(counter)
    {left, counter} = encode_relation(left_plan, counter)
    {right, counter} = encode_relation(right_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:set_op,
         %Spark.Connect.SetOperation{
           left_input: left,
           right_input: right,
           set_op_type: encode_set_op_type(op_type),
           is_all: is_all,
           by_name: Keyword.get(opts, :by_name, false),
           allow_missing_columns: Keyword.get(opts, :allow_missing_columns, false)
         }}
    }

    {relation, counter}
  end

  # --- Milestone 10: DataFrame Parity Pack A ---

  def encode_relation({:offset, child_plan, n}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:offset, %Spark.Connect.Offset{input: child, offset: n}}
    }

    {relation, counter}
  end

  def encode_relation({:tail, child_plan, n}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:tail, %Spark.Connect.Tail{input: child, limit: n}}
    }

    {relation, counter}
  end

  def encode_relation({:to_df, child_plan, column_names}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:to_df, %Spark.Connect.ToDF{input: child, column_names: column_names}}
    }

    {relation, counter}
  end

  def encode_relation({:with_columns_renamed, child_plan, renames}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    rename_protos =
      Enum.map(renames, fn {old_name, new_name} ->
        %Spark.Connect.WithColumnsRenamed.Rename{
          col_name: to_string(old_name),
          new_col_name: to_string(new_name)
        }
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:with_columns_renamed,
         %Spark.Connect.WithColumnsRenamed{input: child, renames: rename_protos}}
    }

    {relation, counter}
  end

  def encode_relation({:repartition, child_plan, num_partitions, shuffle}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:repartition,
         %Spark.Connect.Repartition{
           input: child,
           num_partitions: num_partitions,
           shuffle: shuffle
         }}
    }

    {relation, counter}
  end

  def encode_relation({:repartition_by_expression, child_plan, exprs, num_partitions}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:repartition_by_expression,
         %Spark.Connect.RepartitionByExpression{
           input: child,
           partition_exprs: Enum.map(exprs, &encode_expression/1),
           num_partitions: num_partitions
         }}
    }

    {relation, counter}
  end

  def encode_relation(
        {:sample, child_plan, lower_bound, upper_bound, with_replacement, seed,
         deterministic_order},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:sample,
         %Spark.Connect.Sample{
           input: child,
           lower_bound: lower_bound,
           upper_bound: upper_bound,
           with_replacement: with_replacement,
           seed: seed,
           deterministic_order: deterministic_order
         }}
    }

    {relation, counter}
  end

  def encode_relation({:hint, child_plan, name, parameters}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:hint,
         %Spark.Connect.Hint{
           input: child,
           name: name,
           parameters: Enum.map(parameters, &encode_expression/1)
         }}
    }

    {relation, counter}
  end

  def encode_relation(
        {:unpivot, child_plan, ids, values, variable_column_name, value_column_name},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    values_proto =
      case values do
        nil ->
          nil

        vals when is_list(vals) ->
          %Spark.Connect.Unpivot.Values{values: Enum.map(vals, &encode_expression/1)}
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:unpivot,
         %Spark.Connect.Unpivot{
           input: child,
           ids: Enum.map(ids, &encode_expression/1),
           values: values_proto,
           variable_column_name: variable_column_name,
           value_column_name: value_column_name
         }}
    }

    {relation, counter}
  end

  def encode_relation({:transpose, child_plan, index_columns}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:transpose,
         %Spark.Connect.Transpose{
           input: child,
           index_columns: Enum.map(index_columns, &encode_expression/1)
         }}
    }

    {relation, counter}
  end

  def encode_relation({:html_string, child_plan, num_rows, truncate}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:html_string,
         %Spark.Connect.HtmlString{input: child, num_rows: num_rows, truncate: truncate}}
    }

    {relation, counter}
  end

  def encode_relation({:with_watermark, child_plan, event_time, delay_threshold}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:with_watermark,
         %Spark.Connect.WithWatermark{
           input: child,
           event_time: event_time,
           delay_threshold: delay_threshold
         }}
    }

    {relation, counter}
  end

  def encode_relation({:subquery_alias, child_plan, alias_name}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:subquery_alias, %Spark.Connect.SubqueryAlias{input: child, alias: alias_name}}
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

  def encode_expression({:subquery, _plan, _kind}) do
    raise ArgumentError,
          "subquery expressions require relation-reference encoding which is not yet wired"
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
  defp encode_sql_argument({:subquery, _, _} = expr), do: encode_expression(expr)
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

  defp encode_join_type(:inner), do: :JOIN_TYPE_INNER
  defp encode_join_type(:full), do: :JOIN_TYPE_FULL_OUTER
  defp encode_join_type(:left), do: :JOIN_TYPE_LEFT_OUTER
  defp encode_join_type(:right), do: :JOIN_TYPE_RIGHT_OUTER
  defp encode_join_type(:left_anti), do: :JOIN_TYPE_LEFT_ANTI
  defp encode_join_type(:left_semi), do: :JOIN_TYPE_LEFT_SEMI
  defp encode_join_type(:cross), do: :JOIN_TYPE_CROSS
  defp encode_join_type(other), do: raise(ArgumentError, "invalid join type: #{inspect(other)}")

  defp encode_group_type(:groupby), do: :GROUP_TYPE_GROUPBY
  defp encode_group_type(:rollup), do: :GROUP_TYPE_ROLLUP
  defp encode_group_type(:cube), do: :GROUP_TYPE_CUBE
  defp encode_group_type(:pivot), do: :GROUP_TYPE_PIVOT

  defp encode_set_op_type(:union), do: :SET_OP_TYPE_UNION
  defp encode_set_op_type(:intersect), do: :SET_OP_TYPE_INTERSECT
  defp encode_set_op_type(:except), do: :SET_OP_TYPE_EXCEPT
end
