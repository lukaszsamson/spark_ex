defmodule SparkEx.Connect.PlanEncoder do
  @moduledoc """
  Encodes SparkEx internal plan representations into Spark Connect protobuf messages.

  All encode functions accept a `plan_id_counter` (integer) and return
  `{encoded, new_counter}` to avoid GenServer call deadlocks when invoked
  from within Session handle_call.
  """

  alias Spark.Connect.{
    CachedLocalRelation,
    CachedRemoteRelation,
    Catalog,
    ChunkedCachedLocalRelation,
    DataType,
    Expression,
    LocalRelation,
    Plan,
    Relation,
    RelationCommon,
    SQL,
    ToSchema,
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
    {plan, counter} = attach_with_relations(plan, counter)

    case plan do
      {:compressed_operation, data, op_type, codec} ->
        compressed = %Plan.CompressedOperation{
          data: data,
          op_type: op_type,
          compression_codec: codec
        }

        {%Plan{op_type: {:compressed_operation, compressed}}, counter}

      _ ->
        {relation, counter} = encode_relation(plan, counter)
        {%Plan{op_type: {:root, relation}}, counter}
    end
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

  def encode_relation({:plan_id, plan_id, plan}, counter) do
    {relation, counter} = encode_relation(plan, counter)
    relation = put_in(relation.common.plan_id, plan_id)
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

  def encode_relation({:cached_remote_relation, relation_id}, counter) do
    {plan_id, counter} = next_id(counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:cached_remote_relation, %CachedRemoteRelation{relation_id: relation_id}}
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
    encode_relation({:read_data_source, format, paths, schema, options, []}, counter)
  end

  def encode_relation({:read_data_source, format, paths, schema, options, predicates}, counter) do
    {plan_id, counter} = next_id(counter)

    read = %Spark.Connect.Read{
      is_streaming: false,
      read_type:
        {:data_source,
         %Spark.Connect.Read.DataSource{
           format: format,
           schema: schema,
           paths: paths,
           options: options,
           predicates: predicates
         }}
    }

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:read, read}
    }

    {relation, counter}
  end

  # --- Milestone 14: Streaming Reads ---

  def encode_relation({:read_named_table_streaming, table_name, options}, counter) do
    {plan_id, counter} = next_id(counter)

    read = %Spark.Connect.Read{
      is_streaming: true,
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

  def encode_relation({:read_data_source_streaming, format, paths, schema, options}, counter) do
    {plan_id, counter} = next_id(counter)

    read = %Spark.Connect.Read{
      is_streaming: true,
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
    encode_relation({:drop, child_plan, column_names, []}, counter)
  end

  def encode_relation({:drop, child_plan, column_names, col_exprs}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    columns = Enum.map(col_exprs, &encode_expression/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:drop, %Spark.Connect.Drop{input: child, column_names: column_names, columns: columns}}
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

  def encode_relation(
        {:aggregate, child_plan, :grouping_sets, grouping_exprs, agg_exprs, grouping_sets},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    grouping_sets_proto =
      Enum.map(grouping_sets, fn set ->
        %Spark.Connect.Aggregate.GroupingSets{
          grouping_set: Enum.map(set, &encode_expression/1)
        }
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:aggregate,
         %Spark.Connect.Aggregate{
           input: child,
           group_type: :GROUP_TYPE_GROUPING_SETS,
           grouping_expressions: Enum.map(grouping_exprs, &encode_expression/1),
           aggregate_expressions: Enum.map(agg_exprs, &encode_expression/1),
           grouping_sets: grouping_sets_proto
         }}
    }

    {relation, counter}
  end

  def encode_relation(
        {:aggregate, child_plan, :pivot, grouping_exprs, agg_exprs, pivot_col, pivot_values},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    pivot_values_protos =
      case pivot_values do
        nil -> []
        vals -> Enum.map(vals, &encode_literal/1)
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:aggregate,
         %Spark.Connect.Aggregate{
           input: child,
           group_type: :GROUP_TYPE_PIVOT,
           grouping_expressions: Enum.map(grouping_exprs, &encode_expression/1),
           aggregate_expressions: Enum.map(agg_exprs, &encode_expression/1),
           pivot: %Spark.Connect.Aggregate.Pivot{
             col: encode_expression(pivot_col),
             values: pivot_values_protos
           }
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

  # --- Parse (CSV/JSON parsing within DataFrames) ---

  def encode_relation({:parse, child_plan, format, schema, options}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    format_enum =
      case format do
        :csv -> :PARSE_FORMAT_CSV
        :json -> :PARSE_FORMAT_JSON
        _ -> :PARSE_FORMAT_UNSPECIFIED
      end

    schema_proto =
      case schema do
        nil -> nil
        s -> encode_schema(s)
      end

    options_map =
      case options do
        nil -> %{}
        opts when is_map(opts) -> Map.new(opts, fn {k, v} -> {to_string(k), to_string(v)} end)
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:parse,
         %Spark.Connect.Parse{
           input: child,
           format: format_enum,
           schema: schema_proto,
           options: options_map
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

  def encode_relation({:to_schema, child_plan, schema}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:to_schema, %ToSchema{input: child, schema: encode_schema(schema)}}
    }

    {relation, counter}
  end

  def encode_relation(
        {:as_of_join, left_plan, right_plan, left_as_of, right_as_of, join_expr, using_columns,
         join_type, tolerance, allow_exact_matches, direction},
        counter
      ) do
    {plan_id, counter} = next_id(counter)
    {left, counter} = encode_relation(left_plan, counter)
    {right, counter} = encode_relation(right_plan, counter)

    as_of_join = %Spark.Connect.AsOfJoin{
      left: left,
      right: right
    }

    as_of_join =
      if left_as_of == {:lit, nil},
        do: as_of_join,
        else: %{as_of_join | left_as_of: encode_expression(left_as_of)}

    as_of_join =
      if right_as_of == {:lit, nil},
        do: as_of_join,
        else: %{as_of_join | right_as_of: encode_expression(right_as_of)}

    as_of_join =
      if join_expr == {:lit, nil},
        do: as_of_join,
        else: %{as_of_join | join_expr: encode_expression(join_expr)}

    as_of_join =
      if using_columns == [],
        do: as_of_join,
        else: %{as_of_join | using_columns: using_columns}

    as_of_join =
      if join_type == nil,
        do: as_of_join,
        else: %{as_of_join | join_type: to_string(join_type)}

    as_of_join =
      if tolerance == {:lit, nil},
        do: as_of_join,
        else: %{as_of_join | tolerance: encode_expression(tolerance)}

    as_of_join =
      if allow_exact_matches == nil,
        do: as_of_join,
        else: %{as_of_join | allow_exact_matches: allow_exact_matches}

    as_of_join =
      if direction == nil,
        do: as_of_join,
        else: %{as_of_join | direction: direction}

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:as_of_join, as_of_join}
    }

    {relation, counter}
  end

  def encode_relation({:lateral_join, left_plan, right_plan, join_condition, join_type}, counter) do
    {plan_id, counter} = next_id(counter)
    {left, counter} = encode_relation(left_plan, counter)
    {right, counter} = encode_relation(right_plan, counter)

    lateral_join =
      case join_condition do
        nil ->
          %Spark.Connect.LateralJoin{
            left: left,
            right: right,
            join_type: encode_join_type(join_type)
          }

        _ ->
          %Spark.Connect.LateralJoin{
            left: left,
            right: right,
            join_condition: encode_expression(join_condition),
            join_type: encode_join_type(join_type)
          }
      end

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:lateral_join, lateral_join}
    }

    {relation, counter}
  end

  def encode_relation({:collect_metrics, child_plan, name, metrics}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:collect_metrics,
         %Spark.Connect.CollectMetrics{
           input: child,
           name: name,
           metrics: Enum.map(metrics, &encode_expression/1)
         }}
    }

    {relation, counter}
  end

  # --- NA Relations ---

  def encode_relation({:na_fill, child_plan, cols, values}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:fill_na,
         %Spark.Connect.NAFill{
           input: child,
           cols: cols,
           values: Enum.map(values, &encode_na_literal/1)
         }}
    }

    {relation, counter}
  end

  def encode_relation({:na_drop, child_plan, cols, min_non_nulls}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:drop_na,
         %Spark.Connect.NADrop{
           input: child,
           cols: cols,
           min_non_nulls: min_non_nulls
         }}
    }

    {relation, counter}
  end

  def encode_relation({:na_replace, child_plan, cols, replacements}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    encoded_replacements =
      Enum.map(replacements, fn {old_val, new_val} ->
        %Spark.Connect.NAReplace.Replacement{
          old_value: encode_na_replace_literal(old_val),
          new_value: encode_na_replace_literal(new_val)
        }
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:replace,
         %Spark.Connect.NAReplace{
           input: child,
           cols: cols,
           replacements: encoded_replacements
         }}
    }

    {relation, counter}
  end

  # --- Stat Relations ---

  def encode_relation({:stat_describe, child_plan, cols}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:describe, %Spark.Connect.StatDescribe{input: child, cols: cols}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_summary, child_plan, statistics}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:summary, %Spark.Connect.StatSummary{input: child, statistics: statistics}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_corr, child_plan, col1, col2, method}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:corr, %Spark.Connect.StatCorr{input: child, col1: col1, col2: col2, method: method}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_cov, child_plan, col1, col2}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:cov, %Spark.Connect.StatCov{input: child, col1: col1, col2: col2}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_crosstab, child_plan, col1, col2}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:crosstab, %Spark.Connect.StatCrosstab{input: child, col1: col1, col2: col2}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_freq_items, child_plan, cols, support}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:freq_items, %Spark.Connect.StatFreqItems{input: child, cols: cols, support: support}}
    }

    {relation, counter}
  end

  def encode_relation({:stat_approx_quantile, child_plan, cols, probs, rel_error}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:approx_quantile,
         %Spark.Connect.StatApproxQuantile{
           input: child,
           cols: cols,
           probabilities: probs,
           relative_error: rel_error
         }}
    }

    {relation, counter}
  end

  def encode_relation({:stat_sample_by, child_plan, col_expr, fractions, seed}, counter) do
    {plan_id, counter} = next_id(counter)
    {child, counter} = encode_relation(child_plan, counter)

    encoded_fractions =
      Enum.map(fractions, fn {stratum, fraction} ->
        %Spark.Connect.StatSampleBy.Fraction{
          stratum: encode_literal(stratum),
          fraction: fraction
        }
      end)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:sample_by,
         %Spark.Connect.StatSampleBy{
           input: child,
           col: encode_expression(col_expr),
           fractions: encoded_fractions,
           seed: seed
         }}
    }

    {relation, counter}
  end

  # --- Table-Valued Functions ---

  def encode_relation({:table_valued_function, function_name, arg_exprs}, counter) do
    {plan_id, counter} = next_id(counter)

    encoded_args = Enum.map(arg_exprs, &encode_expression/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:unresolved_table_valued_function,
         %Spark.Connect.UnresolvedTableValuedFunction{
           function_name: function_name,
           arguments: encoded_args
         }}
    }

    {relation, counter}
  end

  # --- Inline UDTF as Relation ---

  def encode_relation(
        {:inline_udtf, function_name, arg_exprs, python_command, return_type, eval_type,
         python_ver, deterministic},
        counter
      ) do
    {plan_id, counter} = next_id(counter)

    python_udtf = %Spark.Connect.PythonUDTF{
      return_type: return_type,
      eval_type: eval_type,
      command: python_command,
      python_ver: python_ver
    }

    encoded_args = Enum.map(arg_exprs, &encode_expression/1)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type:
        {:common_inline_user_defined_table_function,
         %Spark.Connect.CommonInlineUserDefinedTableFunction{
           function_name: function_name,
           deterministic: deterministic,
           arguments: encoded_args,
           function: {:python_udtf, python_udtf}
         }}
    }

    {relation, counter}
  end

  # --- Catalog operations ---

  def encode_relation({:catalog, cat_plan}, counter) do
    {plan_id, counter} = next_id(counter)

    relation = %Relation{
      common: %RelationCommon{plan_id: plan_id},
      rel_type: {:catalog, %Catalog{cat_type: encode_catalog_type(cat_plan)}}
    }

    {relation, counter}
  end

  @doc """
  Wraps a plan in an aggregate count(*) relation.

  Returns `{plan, new_counter}`.
  """
  @spec encode_count(term(), non_neg_integer()) :: {Plan.t(), non_neg_integer()}
  def encode_count(plan, counter) do
    {plan, counter} = attach_with_relations(plan, counter)
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

  def encode_expression({:metadata_col, name}) do
    %Expression{
      expr_type:
        {:unresolved_attribute,
         %Expression.UnresolvedAttribute{unparsed_identifier: name, is_metadata_column: true}}
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

  def encode_expression({:col_regex, name}) do
    %Expression{expr_type: {:unresolved_regex, %Expression.UnresolvedRegex{col_name: name}}}
  end

  # PySpark special-cases count(col("*")) to count(lit(1)) because
  # Spark Connect does not support UnresolvedStar inside count
  def encode_expression({:fn, "count", [{:star}], false}) do
    encode_expression({:fn, "count", [{:lit, 1}], false})
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

  def encode_expression({:call_function, name, args}) do
    %Expression{
      expr_type:
        {:call_function,
         %Spark.Connect.CallFunction{
           function_name: name,
           arguments: Enum.map(args, &encode_expression/1)
         }}
    }
  end

  def encode_expression({:named_arg, key, value}) do
    %Expression{
      expr_type:
        {:named_argument_expression,
         %Spark.Connect.NamedArgumentExpression{key: key, value: encode_expression(value)}}
    }
  end

  def encode_expression({:alias, expr, name}) when is_binary(name) do
    %Expression{
      expr_type:
        {:alias,
         %Expression.Alias{
           expr: encode_expression(expr),
           name: [name]
         }}
    }
  end

  def encode_expression({:alias, expr, names}) when is_list(names) do
    %Expression{
      expr_type:
        {:alias,
         %Expression.Alias{
           expr: encode_expression(expr),
           name: names
         }}
    }
  end

  def encode_expression({:alias, expr, name, metadata_json}) when is_binary(name) do
    %Expression{
      expr_type:
        {:alias,
         %Expression.Alias{
           expr: encode_expression(expr),
           name: [name],
           metadata: metadata_json
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

  def encode_expression({:cast, expr, type_str, mode})
      when mode in [:try, :legacy, :ansi] do
    eval_mode =
      case mode do
        :try -> :EVAL_MODE_TRY
        :legacy -> :EVAL_MODE_LEGACY
        :ansi -> :EVAL_MODE_ANSI
      end

    %Expression{
      expr_type:
        {:cast,
         %Expression.Cast{
           expr: encode_expression(expr),
           cast_to_type: {:type_str, type_str},
           eval_mode: eval_mode
         }}
    }
  end

  # --- Window expression ---

  def encode_expression({:window, fn_expr, partition_spec, order_spec, frame_spec}) do
    %Expression{
      expr_type:
        {:window,
         %Expression.Window{
           window_function: encode_expression(fn_expr),
           partition_spec: Enum.map(partition_spec, &encode_expression/1),
           order_spec: Enum.map(order_spec, &encode_sort_order/1),
           frame_spec: encode_window_frame(frame_spec)
         }}
    }
  end

  # --- Unresolved extract value (array[i], map[key], struct.field) ---

  def encode_expression({:unresolved_extract_value, child, extraction}) do
    %Expression{
      expr_type:
        {:unresolved_extract_value,
         %Expression.UnresolvedExtractValue{
           child: encode_expression(child),
           extraction: encode_expression(extraction)
         }}
    }
  end

  # --- Struct update fields (withField/dropFields) ---

  def encode_expression({:update_fields, struct_expr, field_name, value_expr})
      when is_binary(field_name) do
    %Expression{
      expr_type:
        {:update_fields,
         %Expression.UpdateFields{
           struct_expression: encode_expression(struct_expr),
           field_name: field_name,
           value_expression: maybe_encode_update_value(value_expr)
         }}
    }
  end

  # --- Lambda function ---

  def encode_expression({:lambda, body, variables}) do
    %Expression{
      expr_type:
        {:lambda_function,
         %Expression.LambdaFunction{
           function: encode_expression(body),
           arguments:
             Enum.map(variables, fn {:lambda_var, name} ->
               %Expression.UnresolvedNamedLambdaVariable{name_parts: [name]}
             end)
         }}
    }
  end

  def encode_expression({:lambda_var, name}) do
    %Expression{
      expr_type:
        {:unresolved_named_lambda_variable,
         %Expression.UnresolvedNamedLambdaVariable{name_parts: [name]}}
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

  def encode_expression({:direct_shuffle_partition_id, child_expr}) do
    %Expression{
      expr_type:
        {:direct_shuffle_partition_id,
         %Expression.DirectShufflePartitionID{
           child: encode_expression(child_expr)
         }}
    }
  end

  def encode_expression({:outer, child_expr}) do
    %Expression{
      expr_type:
        {:unresolved_function,
         %Expression.UnresolvedFunction{
           function_name: "outer",
           arguments: [encode_expression(child_expr)],
           is_distinct: false
         }}
    }
  end

  # --- Subquery expression ---
  # The subquery expression references a plan via plan_id. The caller is responsible
  # for encoding the referenced plan and wrapping the root in {:with_relations, ...}.

  def encode_expression({:subquery, subquery_type, plan_id, opts}) when is_integer(plan_id) do
    validate_subquery_opts!(subquery_type, opts)

    subquery_type_enum =
      case subquery_type do
        :scalar -> :SUBQUERY_TYPE_SCALAR
        :exists -> :SUBQUERY_TYPE_EXISTS
        :table_arg -> :SUBQUERY_TYPE_TABLE_ARG
        :in -> :SUBQUERY_TYPE_IN
      end

    in_values =
      case Keyword.get(opts, :in_values) do
        nil -> []
        vals -> Enum.map(vals, &encode_expression/1)
      end

    table_arg_options =
      case Keyword.get(opts, :table_arg_options) do
        nil ->
          nil

        tao ->
          %Spark.Connect.SubqueryExpression.TableArgOptions{
            partition_spec: Enum.map(Keyword.get(tao, :partition_spec, []), &encode_expression/1),
            order_spec: Enum.map(Keyword.get(tao, :order_spec, []), &encode_sort_order/1),
            with_single_partition: Keyword.get(tao, :with_single_partition)
          }
      end

    %Expression{
      expr_type:
        {:subquery_expression,
         %Spark.Connect.SubqueryExpression{
           plan_id: plan_id,
           subquery_type: subquery_type_enum,
           in_subquery_values: in_values,
           table_arg_options: table_arg_options
         }}
    }
  end

  def encode_expression({:subquery, subquery_type, referenced_plan, opts}) do
    {plan_id, _plan} = extract_referenced_plan_id(referenced_plan)
    encode_expression({:subquery, subquery_type, plan_id, opts})
  end

  # --- Private ---

  defp next_id(counter), do: {counter, counter + 1}

  defp attach_with_relations({:compressed_operation, _, _, _} = plan, counter) do
    {plan, counter}
  end

  defp attach_with_relations(plan, counter) do
    {updated_plan, refs, counter} = collect_subquery_references(plan, counter)

    case refs do
      [] ->
        {updated_plan, counter}

      refs ->
        wrapped = {:with_relations, updated_plan, Enum.map(refs, & &1.plan)}
        {wrapped, counter}
    end
  end

  defp collect_subquery_references(plan, counter) do
    {updated_plan, _plan_ids, refs, counter} = rewrite_plan(plan, %{}, [], counter)
    {updated_plan, Enum.reverse(refs), counter}
  end

  defp rewrite_plan({:with_relations, root_plan, reference_plans}, plan_ids, refs, counter) do
    {root_plan, plan_ids, refs, counter} = rewrite_plan(root_plan, plan_ids, refs, counter)

    {reference_plans, {plan_ids, refs, counter}} =
      Enum.map_reduce(reference_plans, {plan_ids, refs, counter}, fn ref_plan, {ids, acc, ctr} ->
        {updated_ref, ids, acc, ctr} = rewrite_plan(ref_plan, ids, acc, ctr)
        {updated_ref, {ids, acc, ctr}}
      end)

    {{:with_relations, root_plan, reference_plans}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:sql, query, args}, plan_ids, refs, counter) do
    {args, plan_ids, refs, counter} = rewrite_args(args, plan_ids, refs, counter)
    {{:sql, query, args}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:range, _start, _end, _step, _num_partitions} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:local_relation, _data, _schema} = plan, plan_ids, refs, counter),
    do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:cached_local_relation, _hash} = plan, plan_ids, refs, counter),
    do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:cached_remote_relation, relation_id} = plan, plan_ids, refs, counter)
       when is_binary(relation_id),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:chunked_cached_local_relation, _data_hashes, _schema_hash} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:to_schema, child_plan, schema}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:to_schema, child_plan, schema}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:limit, child_plan, n}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:limit, child_plan, n}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:read_named_table, _table_name, _options} = plan, plan_ids, refs, counter),
    do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:read_data_source, _format, _paths, _schema, _options} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:read_data_source, _format, _paths, _schema, _options, _predicates} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:read_named_table_streaming, _table_name, _options} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:read_data_source_streaming, _format, _paths, _schema, _options} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:project, child_plan, expressions}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {expressions, plan_ids, refs, counter} =
      rewrite_expr_list(expressions, plan_ids, refs, counter)

    {{:project, child_plan, expressions}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:filter, child_plan, condition}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {condition, plan_ids, refs, counter} = rewrite_expr(condition, plan_ids, refs, counter)
    {{:filter, child_plan, condition}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:sort, child_plan, sort_orders}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {sort_orders, plan_ids, refs, counter} =
      rewrite_sort_orders(sort_orders, plan_ids, refs, counter)

    {{:sort, child_plan, sort_orders}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:sort, child_plan, sort_orders, is_global}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {sort_orders, plan_ids, refs, counter} =
      rewrite_sort_orders(sort_orders, plan_ids, refs, counter)

    {{:sort, child_plan, sort_orders, is_global}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:with_columns, child_plan, aliases}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {aliases, plan_ids, refs, counter} = rewrite_aliases(aliases, plan_ids, refs, counter)
    {{:with_columns, child_plan, aliases}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:drop, child_plan, column_names}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:drop, child_plan, column_names}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:show_string, child_plan, num_rows, truncate, vertical},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:show_string, child_plan, num_rows, truncate, vertical}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:aggregate, child_plan, group_type, grouping_exprs, agg_exprs},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {grouping_exprs, plan_ids, refs, counter} =
      rewrite_expr_list(grouping_exprs, plan_ids, refs, counter)

    {agg_exprs, plan_ids, refs, counter} = rewrite_expr_list(agg_exprs, plan_ids, refs, counter)
    {{:aggregate, child_plan, group_type, grouping_exprs, agg_exprs}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:aggregate, child_plan, :pivot, grouping_exprs, agg_exprs, pivot_col, pivot_values},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {grouping_exprs, plan_ids, refs, counter} =
      rewrite_expr_list(grouping_exprs, plan_ids, refs, counter)

    {agg_exprs, plan_ids, refs, counter} = rewrite_expr_list(agg_exprs, plan_ids, refs, counter)
    {pivot_col, plan_ids, refs, counter} = rewrite_expr(pivot_col, plan_ids, refs, counter)

    {{:aggregate, child_plan, :pivot, grouping_exprs, agg_exprs, pivot_col, pivot_values},
     plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:aggregate, child_plan, :grouping_sets, grouping_exprs, agg_exprs, grouping_sets},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {grouping_exprs, plan_ids, refs, counter} =
      rewrite_expr_list(grouping_exprs, plan_ids, refs, counter)

    {agg_exprs, plan_ids, refs, counter} = rewrite_expr_list(agg_exprs, plan_ids, refs, counter)

    {grouping_sets, plan_ids, refs, counter} =
      Enum.reduce(grouping_sets, {[], plan_ids, refs, counter}, fn set,
                                                                   {sets_acc, plan_ids, refs,
                                                                    counter} ->
        {set, plan_ids, refs, counter} = rewrite_expr_list(set, plan_ids, refs, counter)
        {[set | sets_acc], plan_ids, refs, counter}
      end)

    grouping_sets = Enum.reverse(grouping_sets)

    {{:aggregate, child_plan, :grouping_sets, grouping_exprs, agg_exprs, grouping_sets}, plan_ids,
     refs, counter}
  end

  defp rewrite_plan(
         {:join, left_plan, right_plan, join_condition, join_type, using_columns},
         plan_ids,
         refs,
         counter
       ) do
    {left_plan, plan_ids, refs, counter} = rewrite_plan(left_plan, plan_ids, refs, counter)
    {right_plan, plan_ids, refs, counter} = rewrite_plan(right_plan, plan_ids, refs, counter)

    {join_condition, plan_ids, refs, counter} =
      case join_condition do
        nil -> {nil, plan_ids, refs, counter}
        expr -> rewrite_expr(expr, plan_ids, refs, counter)
      end

    {{:join, left_plan, right_plan, join_condition, join_type, using_columns}, plan_ids, refs,
     counter}
  end

  defp rewrite_plan(
         {:deduplicate, child_plan, column_names, all_columns} = _plan,
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:deduplicate, child_plan, column_names, all_columns}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:deduplicate, child_plan, column_names, all_columns, within_watermark},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {{:deduplicate, child_plan, column_names, all_columns, within_watermark}, plan_ids, refs,
     counter}
  end

  defp rewrite_plan(
         {:set_operation, left_plan, right_plan, op_type, is_all},
         plan_ids,
         refs,
         counter
       ) do
    {left_plan, plan_ids, refs, counter} = rewrite_plan(left_plan, plan_ids, refs, counter)
    {right_plan, plan_ids, refs, counter} = rewrite_plan(right_plan, plan_ids, refs, counter)
    {{:set_operation, left_plan, right_plan, op_type, is_all}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:set_operation, left_plan, right_plan, op_type, is_all, opts},
         plan_ids,
         refs,
         counter
       ) do
    {left_plan, plan_ids, refs, counter} = rewrite_plan(left_plan, plan_ids, refs, counter)
    {right_plan, plan_ids, refs, counter} = rewrite_plan(right_plan, plan_ids, refs, counter)
    {{:set_operation, left_plan, right_plan, op_type, is_all, opts}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:offset, child_plan, n}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:offset, child_plan, n}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:tail, child_plan, n}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:tail, child_plan, n}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:to_df, child_plan, column_names}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:to_df, child_plan, column_names}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:with_columns_renamed, child_plan, renames}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:with_columns_renamed, child_plan, renames}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:repartition, child_plan, num_partitions, shuffle}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:repartition, child_plan, num_partitions, shuffle}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:repartition_by_expression, child_plan, exprs, num_partitions},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {exprs, plan_ids, refs, counter} = rewrite_expr_list(exprs, plan_ids, refs, counter)
    {{:repartition_by_expression, child_plan, exprs, num_partitions}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:parse, child_plan, format, schema, options},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:parse, child_plan, format, schema, options}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:sample, child_plan, lower, upper, with_replacement, seed, deterministic},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {{:sample, child_plan, lower, upper, with_replacement, seed, deterministic}, plan_ids, refs,
     counter}
  end

  defp rewrite_plan({:hint, child_plan, name, parameters}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {parameters, plan_ids, refs, counter} = rewrite_expr_list(parameters, plan_ids, refs, counter)
    {{:hint, child_plan, name, parameters}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:unpivot, child_plan, ids, values, variable_column_name, value_column_name},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {ids, plan_ids, refs, counter} = rewrite_expr_list(ids, plan_ids, refs, counter)

    {values, plan_ids, refs, counter} =
      case values do
        nil -> {nil, plan_ids, refs, counter}
        vals -> rewrite_expr_list(vals, plan_ids, refs, counter)
      end

    {{:unpivot, child_plan, ids, values, variable_column_name, value_column_name}, plan_ids, refs,
     counter}
  end

  defp rewrite_plan({:transpose, child_plan, index_columns}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)

    {index_columns, plan_ids, refs, counter} =
      rewrite_expr_list(index_columns, plan_ids, refs, counter)

    {{:transpose, child_plan, index_columns}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:html_string, child_plan, num_rows, truncate}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:html_string, child_plan, num_rows, truncate}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:with_watermark, child_plan, event_time, delay_threshold},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:with_watermark, child_plan, event_time, delay_threshold}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:subquery_alias, child_plan, alias_name}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:subquery_alias, child_plan, alias_name}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:as_of_join, left_plan, right_plan, left_as_of, right_as_of, join_expr, using_columns,
          join_type, tolerance, allow_exact_matches, direction},
         plan_ids,
         refs,
         counter
       ) do
    {left_plan, plan_ids, refs, counter} = rewrite_plan(left_plan, plan_ids, refs, counter)
    {right_plan, plan_ids, refs, counter} = rewrite_plan(right_plan, plan_ids, refs, counter)
    {left_as_of, plan_ids, refs, counter} = rewrite_expr(left_as_of, plan_ids, refs, counter)
    {right_as_of, plan_ids, refs, counter} = rewrite_expr(right_as_of, plan_ids, refs, counter)
    {join_expr, plan_ids, refs, counter} = rewrite_expr(join_expr, plan_ids, refs, counter)
    {tolerance, plan_ids, refs, counter} = rewrite_expr(tolerance, plan_ids, refs, counter)

    {{:as_of_join, left_plan, right_plan, left_as_of, right_as_of, join_expr, using_columns,
      join_type, tolerance, allow_exact_matches, direction}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:lateral_join, left_plan, right_plan, join_condition, join_type},
         plan_ids,
         refs,
         counter
       ) do
    {left_plan, plan_ids, refs, counter} = rewrite_plan(left_plan, plan_ids, refs, counter)
    {right_plan, plan_ids, refs, counter} = rewrite_plan(right_plan, plan_ids, refs, counter)

    {join_condition, plan_ids, refs, counter} =
      rewrite_expr(join_condition, plan_ids, refs, counter)

    {{:lateral_join, left_plan, right_plan, join_condition, join_type}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:collect_metrics, child_plan, name, metrics}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {metrics, plan_ids, refs, counter} = rewrite_expr_list(metrics, plan_ids, refs, counter)
    {{:collect_metrics, child_plan, name, metrics}, plan_ids, refs, counter}
  end

  # ── NA operations ──

  defp rewrite_plan({:na_fill, child_plan, cols, values}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:na_fill, child_plan, cols, values}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:na_drop, child_plan, cols, min_non_nulls}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:na_drop, child_plan, cols, min_non_nulls}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:na_replace, child_plan, cols, replacements}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:na_replace, child_plan, cols, replacements}, plan_ids, refs, counter}
  end

  # ── Stat operations ──

  defp rewrite_plan({:stat_describe, child_plan, cols}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_describe, child_plan, cols}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:stat_summary, child_plan, statistics}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_summary, child_plan, statistics}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:stat_corr, child_plan, col1, col2, method}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_corr, child_plan, col1, col2, method}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:stat_cov, child_plan, col1, col2}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_cov, child_plan, col1, col2}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:stat_crosstab, child_plan, col1, col2}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_crosstab, child_plan, col1, col2}, plan_ids, refs, counter}
  end

  defp rewrite_plan({:stat_freq_items, child_plan, cols, support}, plan_ids, refs, counter) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_freq_items, child_plan, cols, support}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:stat_approx_quantile, child_plan, cols, probs, rel_error},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_approx_quantile, child_plan, cols, probs, rel_error}, plan_ids, refs, counter}
  end

  defp rewrite_plan(
         {:stat_sample_by, child_plan, col_expr, fractions, seed},
         plan_ids,
         refs,
         counter
       ) do
    {child_plan, plan_ids, refs, counter} = rewrite_plan(child_plan, plan_ids, refs, counter)
    {{:stat_sample_by, child_plan, col_expr, fractions, seed}, plan_ids, refs, counter}
  end

  # ── Catalog / TVF / UDTF (leaf nodes — no child plan) ──

  defp rewrite_plan({:catalog, _} = plan, plan_ids, refs, counter),
    do: {plan, plan_ids, refs, counter}

  defp rewrite_plan({:table_valued_function, _name, _args} = plan, plan_ids, refs, counter),
    do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(
         {:inline_udtf, _name, _args, _command, _return_type, _eval_type, _python_ver,
          _deterministic} = plan,
         plan_ids,
         refs,
         counter
       ),
       do: {plan, plan_ids, refs, counter}

  defp rewrite_plan(other, _plan_ids, _refs, _counter) do
    raise ArgumentError, "unsupported plan tuple: #{inspect(other)}"
  end

  defp rewrite_args(nil, plan_ids, refs, counter), do: {nil, plan_ids, refs, counter}

  defp rewrite_args(args, plan_ids, refs, counter) when is_list(args) do
    rewrite_expr_list(args, plan_ids, refs, counter)
  end

  defp rewrite_args(args, plan_ids, refs, counter) when is_map(args) do
    {new_args, {plan_ids, refs, counter}} =
      Enum.map_reduce(args, {plan_ids, refs, counter}, fn {key, value}, {ids, acc, ctr} ->
        {updated, ids, acc, ctr} = rewrite_expr(value, ids, acc, ctr)
        {{key, updated}, {ids, acc, ctr}}
      end)

    {Map.new(new_args), plan_ids, refs, counter}
  end

  defp rewrite_expr_list(exprs, plan_ids, refs, counter) do
    {updated, {plan_ids, refs, counter}} =
      Enum.map_reduce(exprs, {plan_ids, refs, counter}, fn expr, {ids, acc, ctr} ->
        {updated_expr, ids, acc, ctr} = rewrite_expr(expr, ids, acc, ctr)
        {updated_expr, {ids, acc, ctr}}
      end)

    {updated, plan_ids, refs, counter}
  end

  defp rewrite_aliases(aliases, plan_ids, refs, counter) do
    {updated, {plan_ids, refs, counter}} =
      Enum.map_reduce(aliases, {plan_ids, refs, counter}, fn alias_item, {ids, acc, ctr} ->
        {alias_item, ids, acc, ctr} = rewrite_alias(alias_item, ids, acc, ctr)
        {alias_item, {ids, acc, ctr}}
      end)

    {updated, plan_ids, refs, counter}
  end

  defp rewrite_alias({:alias, expr, name}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:alias, expr, name}, plan_ids, refs, counter}
  end

  defp rewrite_alias({:alias, expr, name, metadata}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:alias, expr, name, metadata}, plan_ids, refs, counter}
  end

  defp rewrite_sort_orders(sort_orders, plan_ids, refs, counter) do
    {updated, {plan_ids, refs, counter}} =
      Enum.map_reduce(sort_orders, {plan_ids, refs, counter}, fn sort_order, {ids, acc, ctr} ->
        {sort_order, ids, acc, ctr} = rewrite_sort_order(sort_order, ids, acc, ctr)
        {sort_order, {ids, acc, ctr}}
      end)

    {updated, plan_ids, refs, counter}
  end

  defp rewrite_sort_order({:sort_order, expr, direction, null_ordering}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:sort_order, expr, direction, null_ordering}, plan_ids, refs, counter}
  end

  defp rewrite_sort_order(other, plan_ids, refs, counter) do
    {other, plan_ids, refs, counter}
  end

  defp rewrite_expr(%Column{expr: expr}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {%Column{expr: expr}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:col, _} = expr, plan_ids, refs, counter),
    do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:lit, _} = expr, plan_ids, refs, counter),
    do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:expr, _} = expr, plan_ids, refs, counter),
    do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:star} = expr, plan_ids, refs, counter), do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:star, _} = expr, plan_ids, refs, counter),
    do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:fn, name, args, is_distinct}, plan_ids, refs, counter) do
    {args, plan_ids, refs, counter} = rewrite_expr_list(args, plan_ids, refs, counter)
    {{:fn, name, args, is_distinct}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:alias, expr, name}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:alias, expr, name}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:cast, expr, type_str}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:cast, expr, type_str}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:cast, expr, type_str, mode}, plan_ids, refs, counter) do
    {expr, plan_ids, refs, counter} = rewrite_expr(expr, plan_ids, refs, counter)
    {{:cast, expr, type_str, mode}, plan_ids, refs, counter}
  end

  defp rewrite_expr(
         {:window, fn_expr, partition_spec, order_spec, frame_spec},
         plan_ids,
         refs,
         counter
       ) do
    {fn_expr, plan_ids, refs, counter} = rewrite_expr(fn_expr, plan_ids, refs, counter)

    {partition_spec, plan_ids, refs, counter} =
      rewrite_expr_list(partition_spec, plan_ids, refs, counter)

    {order_spec, plan_ids, refs, counter} =
      rewrite_sort_orders(order_spec, plan_ids, refs, counter)

    {{:window, fn_expr, partition_spec, order_spec, frame_spec}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:unresolved_extract_value, child, extraction}, plan_ids, refs, counter) do
    {child, plan_ids, refs, counter} = rewrite_expr(child, plan_ids, refs, counter)
    {extraction, plan_ids, refs, counter} = rewrite_expr(extraction, plan_ids, refs, counter)
    {{:unresolved_extract_value, child, extraction}, plan_ids, refs, counter}
  end

  defp rewrite_expr(
         {:update_fields, struct_expr, field_name, value_expr},
         plan_ids,
         refs,
         counter
       )
       when is_binary(field_name) do
    {struct_expr, plan_ids, refs, counter} =
      rewrite_expr(struct_expr, plan_ids, refs, counter)

    {value_expr, plan_ids, refs, counter} =
      case value_expr do
        nil -> {nil, plan_ids, refs, counter}
        _ -> rewrite_expr(value_expr, plan_ids, refs, counter)
      end

    {{:update_fields, struct_expr, field_name, value_expr}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:lambda, body, variables}, plan_ids, refs, counter) do
    {body, plan_ids, refs, counter} = rewrite_expr(body, plan_ids, refs, counter)
    {{:lambda, body, variables}, plan_ids, refs, counter}
  end

  defp rewrite_expr({:lambda_var, _name} = expr, plan_ids, refs, counter),
    do: {expr, plan_ids, refs, counter}

  defp rewrite_expr({:subquery, subquery_type, referenced_plan, opts}, plan_ids, refs, counter)
       when is_list(opts) do
    {plan_ids, refs, plan_id, counter} = ensure_plan_id(referenced_plan, plan_ids, refs, counter)

    {opts, plan_ids, refs, counter} =
      case Keyword.get(opts, :table_arg_options) do
        nil ->
          {opts, plan_ids, refs, counter}

        table_opts ->
          {table_opts, plan_ids, refs, counter} =
            rewrite_table_arg_options(table_opts, plan_ids, refs, counter)

          {Keyword.put(opts, :table_arg_options, table_opts), plan_ids, refs, counter}
      end

    {opts, plan_ids, refs, counter} =
      case Keyword.get(opts, :in_values) do
        nil ->
          {opts, plan_ids, refs, counter}

        values ->
          {values, plan_ids, refs, counter} = rewrite_expr_list(values, plan_ids, refs, counter)
          {Keyword.put(opts, :in_values, values), plan_ids, refs, counter}
      end

    {{:subquery, subquery_type, plan_id, opts}, plan_ids, refs, counter}
  end

  defp rewrite_expr(value, plan_ids, refs, counter), do: {value, plan_ids, refs, counter}

  defp ensure_plan_id(referenced_plan, plan_ids, refs, counter) do
    {plan_id, referenced_plan, counter} = extract_referenced_plan_id(referenced_plan, counter)

    case Map.fetch(plan_ids, plan_id) do
      {:ok, existing} ->
        {plan_ids, refs, existing, counter}

      :error ->
        plan_ids = Map.put(plan_ids, plan_id, plan_id)
        refs = [%{id: plan_id, plan: referenced_plan} | refs]
        {plan_ids, refs, plan_id, counter}
    end
  end

  defp rewrite_table_arg_options(table_opts, plan_ids, refs, counter) do
    {partition_spec, plan_ids, refs, counter} =
      rewrite_expr_list(Keyword.get(table_opts, :partition_spec, []), plan_ids, refs, counter)

    {order_spec, plan_ids, refs, counter} =
      rewrite_sort_orders(Keyword.get(table_opts, :order_spec, []), plan_ids, refs, counter)

    table_opts =
      table_opts
      |> Keyword.put(:partition_spec, partition_spec)
      |> Keyword.put(:order_spec, order_spec)

    {table_opts, plan_ids, refs, counter}
  end

  defp extract_referenced_plan_id(%{plan_id: plan_id, plan: plan}, counter)
       when is_integer(plan_id) do
    {plan_id, plan, counter}
  end

  defp extract_referenced_plan_id({plan_id, plan}, counter) when is_integer(plan_id) do
    {plan_id, plan, counter}
  end

  defp extract_referenced_plan_id({:plan_id, plan_id, plan}, counter) when is_integer(plan_id) do
    {plan_id, plan, counter}
  end

  defp extract_referenced_plan_id(plan, counter) do
    {plan_id, counter} = next_id(counter)
    {plan_id, {:plan_id, plan_id, plan}, counter}
  end

  defp extract_referenced_plan_id(%{plan_id: plan_id, plan: plan}) when is_integer(plan_id) do
    {plan_id, plan}
  end

  defp extract_referenced_plan_id({plan_id, plan}) when is_integer(plan_id) do
    {plan_id, plan}
  end

  defp extract_referenced_plan_id({:plan_id, plan_id, plan}) when is_integer(plan_id) do
    {plan_id, plan}
  end

  defp extract_referenced_plan_id(plan) do
    raise ArgumentError,
          "subquery expression requires an explicit plan_id reference when encoded standalone. " <>
            "Wrap as {:plan_id, id, plan} (or %{plan_id: id, plan: plan}), " <>
            "or encode the containing plan via PlanEncoder.encode/2. " <>
            "Got: #{inspect(plan)}"
  end

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
  defp encode_sql_argument({:cast, _, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:star} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:star, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:window, _, _, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:unresolved_extract_value, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:lambda, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:lambda_var, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument({:subquery, _, _, _} = expr), do: encode_expression(expr)
  defp encode_sql_argument(value), do: encode_literal_expression(value)

  defp encode_schema(%DataType{} = schema), do: schema

  defp encode_schema(schema) when is_binary(schema) do
    %DataType{kind: {:unparsed, %DataType.Unparsed{data_type_string: schema}}}
  end

  defp encode_schema({:struct, _} = schema) do
    encode_schema(SparkEx.Types.schema_to_string(schema))
  end

  defp validate_subquery_opts!(:table_arg, opts) when is_list(opts) do
    case Keyword.get(opts, :table_arg_options) do
      nil ->
        raise ArgumentError, "table_arg subquery requires :table_arg_options"

      _ ->
        :ok
    end
  end

  defp validate_subquery_opts!(:in, opts) when is_list(opts) do
    case Keyword.get(opts, :in_values) do
      nil ->
        raise ArgumentError, "in subquery requires :in_values"

      [] ->
        raise ArgumentError, "in subquery requires non-empty :in_values"

      _ ->
        :ok
    end
  end

  defp validate_subquery_opts!(type, opts) when is_list(opts) do
    if Keyword.get(opts, :table_arg_options) do
      raise ArgumentError, "#{type} subquery does not accept :table_arg_options"
    end

    :ok
  end

  # ── Catalog type encoding ──

  defp encode_catalog_type({:current_database}),
    do: {:current_database, %Spark.Connect.CurrentDatabase{}}

  defp encode_catalog_type({:set_current_database, db_name}),
    do: {:set_current_database, %Spark.Connect.SetCurrentDatabase{db_name: db_name}}

  defp encode_catalog_type({:list_databases, pattern}),
    do: {:list_databases, %Spark.Connect.ListDatabases{pattern: pattern}}

  defp encode_catalog_type({:current_catalog}),
    do: {:current_catalog, %Spark.Connect.CurrentCatalog{}}

  defp encode_catalog_type({:set_current_catalog, catalog_name}),
    do: {:set_current_catalog, %Spark.Connect.SetCurrentCatalog{catalog_name: catalog_name}}

  defp encode_catalog_type({:list_catalogs, pattern}),
    do: {:list_catalogs, %Spark.Connect.ListCatalogs{pattern: pattern}}

  defp encode_catalog_type({:list_tables, db_name, pattern}),
    do: {:list_tables, %Spark.Connect.ListTables{db_name: db_name, pattern: pattern}}

  defp encode_catalog_type({:get_table, table_name, db_name}),
    do: {:get_table, %Spark.Connect.GetTable{table_name: table_name, db_name: db_name}}

  defp encode_catalog_type({:table_exists, table_name, db_name}),
    do: {:table_exists, %Spark.Connect.TableExists{table_name: table_name, db_name: db_name}}

  defp encode_catalog_type({:list_functions, db_name, pattern}),
    do: {:list_functions, %Spark.Connect.ListFunctions{db_name: db_name, pattern: pattern}}

  defp encode_catalog_type({:get_function, function_name, db_name}),
    do:
      {:get_function, %Spark.Connect.GetFunction{function_name: function_name, db_name: db_name}}

  defp encode_catalog_type({:function_exists, function_name, db_name}),
    do:
      {:function_exists,
       %Spark.Connect.FunctionExists{function_name: function_name, db_name: db_name}}

  defp encode_catalog_type({:list_columns, table_name, db_name}),
    do: {:list_columns, %Spark.Connect.ListColumns{table_name: table_name, db_name: db_name}}

  defp encode_catalog_type({:get_database, db_name}),
    do: {:get_database, %Spark.Connect.GetDatabase{db_name: db_name}}

  defp encode_catalog_type({:database_exists, db_name}),
    do: {:database_exists, %Spark.Connect.DatabaseExists{db_name: db_name}}

  defp encode_catalog_type(
         {:create_table, table_name, path, source, description, schema, options}
       ),
       do:
         {:create_table,
          %Spark.Connect.CreateTable{
            table_name: table_name,
            path: path,
            source: source,
            description: description,
            schema: schema,
            options: options || %{}
          }}

  defp encode_catalog_type({:create_external_table, table_name, path, source, schema, options}),
    do:
      {:create_external_table,
       %Spark.Connect.CreateExternalTable{
         table_name: table_name,
         path: path,
         source: source,
         schema: schema,
         options: options || %{}
       }}

  defp encode_catalog_type({:drop_temp_view, view_name}),
    do: {:drop_temp_view, %Spark.Connect.DropTempView{view_name: view_name}}

  defp encode_catalog_type({:drop_global_temp_view, view_name}),
    do: {:drop_global_temp_view, %Spark.Connect.DropGlobalTempView{view_name: view_name}}

  defp encode_catalog_type({:recover_partitions, table_name}),
    do: {:recover_partitions, %Spark.Connect.RecoverPartitions{table_name: table_name}}

  defp encode_catalog_type({:is_cached, table_name}),
    do: {:is_cached, %Spark.Connect.IsCached{table_name: table_name}}

  defp encode_catalog_type({:cache_table, table_name, storage_level}),
    do:
      {:cache_table,
       %Spark.Connect.CacheTable{table_name: table_name, storage_level: storage_level}}

  defp encode_catalog_type({:uncache_table, table_name}),
    do: {:uncache_table, %Spark.Connect.UncacheTable{table_name: table_name}}

  defp encode_catalog_type({:clear_cache}),
    do: {:clear_cache, %Spark.Connect.ClearCache{}}

  defp encode_catalog_type({:refresh_table, table_name}),
    do: {:refresh_table, %Spark.Connect.RefreshTable{table_name: table_name}}

  defp encode_catalog_type({:refresh_by_path, path}),
    do: {:refresh_by_path, %Spark.Connect.RefreshByPath{path: path}}

  # PySpark's NAFill._convert_value always uses long for ints (not integer)
  # because the Spark server only accepts Long/Double/String/Boolean in NAFill values.
  defp encode_na_literal(true), do: %Expression.Literal{literal_type: {:boolean, true}}
  defp encode_na_literal(false), do: %Expression.Literal{literal_type: {:boolean, false}}
  defp encode_na_literal(v) when is_integer(v), do: %Expression.Literal{literal_type: {:long, v}}
  defp encode_na_literal(v) when is_float(v), do: %Expression.Literal{literal_type: {:double, v}}
  defp encode_na_literal(v) when is_binary(v), do: %Expression.Literal{literal_type: {:string, v}}

  # PySpark's NAReplace._convert_int_to_float converts all integer keys/values to float
  defp encode_na_replace_literal(nil),
    do: %Expression.Literal{
      literal_type:
        {:null, %Spark.Connect.DataType{kind: {:null, %Spark.Connect.DataType.NULL{}}}}
    }

  defp encode_na_replace_literal(true), do: %Expression.Literal{literal_type: {:boolean, true}}
  defp encode_na_replace_literal(false), do: %Expression.Literal{literal_type: {:boolean, false}}

  defp encode_na_replace_literal(v) when is_integer(v),
    do: %Expression.Literal{literal_type: {:double, v / 1}}

  defp encode_na_replace_literal(v) when is_float(v),
    do: %Expression.Literal{literal_type: {:double, v}}

  defp encode_na_replace_literal(v) when is_binary(v),
    do: %Expression.Literal{literal_type: {:string, v}}

  defp encode_literal(nil),
    do: %Expression.Literal{
      literal_type:
        {:null, %Spark.Connect.DataType{kind: {:null, %Spark.Connect.DataType.NULL{}}}}
    }

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

  defp encode_literal(%Decimal{} = d) do
    encode_literal({:decimal, Decimal.to_string(d)})
  end

  defp encode_literal({:decimal, value}) when is_binary(value) do
    {precision, scale} = infer_decimal_precision_scale(value)

    %Expression.Literal{
      literal_type:
        {:decimal, %Expression.Literal.Decimal{value: value, precision: precision, scale: scale}}
    }
  end

  defp encode_literal({:decimal, value, precision, scale})
       when is_binary(value) and is_integer(precision) and is_integer(scale) do
    %Expression.Literal{
      literal_type:
        {:decimal, %Expression.Literal.Decimal{value: value, precision: precision, scale: scale}}
    }
  end

  defp encode_literal({:binary, value}) when is_binary(value) do
    %Expression.Literal{literal_type: {:binary, value}}
  end

  defp encode_literal({:calendar_interval, months, days, microseconds})
       when is_integer(months) and is_integer(days) and is_integer(microseconds) do
    %Expression.Literal{
      literal_type:
        {:calendar_interval,
         %Expression.Literal.CalendarInterval{
           months: months,
           days: days,
           microseconds: microseconds
         }}
    }
  end

  defp encode_literal({:year_month_interval, months}) when is_integer(months) do
    %Expression.Literal{literal_type: {:year_month_interval, months}}
  end

  defp encode_literal({:day_time_interval, microseconds}) when is_integer(microseconds) do
    %Expression.Literal{literal_type: {:day_time_interval, microseconds}}
  end

  defp encode_literal({:array, elements}) when is_list(elements) do
    %Expression.Literal{
      literal_type:
        {:array, %Expression.Literal.Array{elements: Enum.map(elements, &encode_literal/1)}}
    }
  end

  defp encode_literal({:map, map}) when is_map(map) do
    {keys, values} =
      map
      |> Enum.map(fn {k, v} -> {encode_literal(k), encode_literal(v)} end)
      |> Enum.unzip()

    %Expression.Literal{
      literal_type: {:map, %Expression.Literal.Map{keys: keys, values: values}}
    }
  end

  defp encode_literal({:struct, elements}) when is_list(elements) do
    %Expression.Literal{
      literal_type:
        {:struct, %Expression.Literal.Struct{elements: Enum.map(elements, &encode_literal/1)}}
    }
  end

  defp encode_literal(v) when is_binary(v) do
    %Expression.Literal{literal_type: {:string, v}}
  end

  defp encode_literal(%Date{} = date) do
    %Expression.Literal{literal_type: {:date, Date.diff(date, ~D[1970-01-01])}}
  end

  defp encode_literal(%DateTime{} = datetime) do
    %Expression.Literal{literal_type: {:timestamp, DateTime.to_unix(datetime, :microsecond)}}
  end

  defp encode_literal(%NaiveDateTime{} = datetime) do
    %Expression.Literal{
      literal_type:
        {:timestamp_ntz, NaiveDateTime.diff(datetime, ~N[1970-01-01 00:00:00], :microsecond)}
    }
  end

  defp encode_literal(%Time{} = time) do
    %Expression.Literal{
      literal_type: {:time, %Expression.Literal.Time{nano: time_to_nanos(time), precision: 6}}
    }
  end

  defp maybe_encode_update_value(nil), do: nil
  defp maybe_encode_update_value(expr), do: encode_expression(expr)

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
      null_ordering: :SORT_NULLS_FIRST
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

  defp encode_null_ordering(nil), do: :SORT_NULLS_FIRST
  defp encode_null_ordering(:nulls_first), do: :SORT_NULLS_FIRST
  defp encode_null_ordering(:nulls_last), do: :SORT_NULLS_LAST

  defp time_to_nanos(%Time{microsecond: {usec, precision}} = time) do
    {seconds, _usecs} = Time.to_seconds_after_midnight(time)
    normalized_usecs = normalize_microseconds(usec, precision)
    seconds * 1_000_000_000 + normalized_usecs * 1000
  end

  defp normalize_microseconds(usec, precision) when precision < 6 do
    usec * Integer.pow(10, 6 - precision)
  end

  defp normalize_microseconds(usec, _precision), do: usec

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

  # --- Window frame encoding ---

  defp encode_window_frame(nil), do: nil

  defp encode_window_frame({frame_type, lower, upper}) do
    %Expression.Window.WindowFrame{
      frame_type: encode_frame_type(frame_type),
      lower: encode_frame_boundary(lower, frame_type),
      upper: encode_frame_boundary(upper, frame_type)
    }
  end

  defp encode_frame_type(:rows), do: :FRAME_TYPE_ROW
  defp encode_frame_type(:range), do: :FRAME_TYPE_RANGE

  defp encode_frame_boundary(:current_row, _frame_type) do
    %Expression.Window.WindowFrame.FrameBoundary{boundary: {:current_row, true}}
  end

  defp encode_frame_boundary(:unbounded, _frame_type) do
    %Expression.Window.WindowFrame.FrameBoundary{boundary: {:unbounded, true}}
  end

  # ROW frames use 32-bit integer, RANGE frames use 64-bit long
  defp encode_frame_boundary(n, :rows) when is_integer(n) do
    %Expression.Window.WindowFrame.FrameBoundary{
      boundary: {:value, %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:integer, n}}}}}
    }
  end

  defp encode_frame_boundary(n, :range) when is_integer(n) do
    %Expression.Window.WindowFrame.FrameBoundary{
      boundary: {:value, %Expression{expr_type: {:literal, %Expression.Literal{literal_type: {:long, n}}}}}
    }
  end

  # Infers precision and scale from a decimal string value.
  # Matches PySpark's behavior of computing from the actual value.
  defp infer_decimal_precision_scale(value) when is_binary(value) do
    stripped = String.trim_leading(value, "-")

    case String.split(stripped, ".") do
      [int_part] ->
        precision = max(String.length(int_part), 1)
        {precision, 0}

      [int_part, frac_part] ->
        scale = String.length(frac_part)
        int_digits = String.length(String.trim_leading(int_part, "0"))
        precision = max(int_digits + scale, 1)
        {precision, scale}
    end
  end
end
