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
    Range
  }

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
              {to_string(k), encode_literal_expression(v)}
            end)

          %SQL{query: query, named_arguments: named}

        args when is_list(args) and length(args) > 0 ->
          pos = Enum.map(args, &encode_literal_expression/1)
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

  # --- Private ---

  defp next_id(counter), do: {counter, counter + 1}

  defp encode_literal_expression(value) do
    %Expression{expr_type: {:literal, encode_literal(value)}}
  end

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
end
