defmodule SparkEx.Wave1EncoderTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias Spark.Connect.Expression
  alias SparkEx.Column
  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.GroupedData

  defp literal(expr) do
    %Expression{expr_type: {:literal, lit}} = PlanEncoder.encode_expression(expr)
    lit
  end

  describe "T-04: non-finite float sentinels" do
    test "scalar sentinels encode as double literals, not strings" do
      assert %Expression.Literal{literal_type: {:double, :nan}} = literal({:lit, :nan})

      assert %Expression.Literal{literal_type: {:double, :infinity}} = literal({:lit, :infinity})

      assert %Expression.Literal{literal_type: {:double, :negative_infinity}} =
               literal({:lit, :neg_infinity})

      # Only Explorer's sentinel set is special-cased; other atoms stay strings.
      assert %Expression.Literal{literal_type: {:string, "negative_infinity"}} =
               literal({:lit, :negative_infinity})
    end

    test "sentinels survive protobuf encoding of the double field" do
      encoded = Protobuf.encode(PlanEncoder.encode_expression({:lit, :nan}))
      assert is_binary(encoded) and byte_size(encoded) > 0

      assert %Expression{
               expr_type: {:literal, %Expression.Literal{literal_type: {:double, :nan}}}
             } =
               Protobuf.decode(encoded, Expression)
    end

    test "array literal with sentinels infers double element type" do
      assert %Expression.Literal{
               literal_type:
                 {:array,
                  %Expression.Literal.Array{
                    element_type: %DataType{kind: {:double, _}},
                    elements: elements
                  }}
             } = literal({:lit, [:nan, 1.0, :infinity, :neg_infinity]})

      assert Enum.map(elements, & &1.literal_type) == [
               {:double, :nan},
               {:double, 1.0},
               {:double, :infinity},
               {:double, :negative_infinity}
             ]
    end

    test "map literal values with sentinels infer double value type" do
      assert %Expression.Literal{
               literal_type:
                 {:map, %Expression.Literal.Map{value_type: %DataType{kind: {:double, _}}}}
             } = literal({:lit, {:map, %{"a" => :nan}}})
    end

    test "ordinary atoms still encode as strings" do
      assert %Expression.Literal{literal_type: {:string, "hello"}} = literal({:lit, :hello})
    end
  end

  describe "T-05/T-40: star routing" do
    test "Functions.to_expr/1 routes star names to star expressions" do
      assert Functions.to_expr("*") == {:star}
      assert Functions.to_expr(".*") == {:star}
      assert Functions.to_expr("x.*") == {:star, "x.*"}
      assert Functions.to_expr("x") == {:col, "x"}
      assert Functions.to_expr(:x) == {:col, "x"}
    end

    test "count(\"*\") rewrites to count(1) on the wire" do
      assert %Column{expr: {:fn, "count", [{:star}], false}} = Functions.count("*")

      assert %Expression{
               expr_type:
                 {:unresolved_function,
                  %Expression.UnresolvedFunction{
                    function_name: "count",
                    arguments: [
                      %Expression{
                        expr_type: {:literal, %Expression.Literal{literal_type: {:integer, 1}}}
                      }
                    ]
                  }}
             } = PlanEncoder.encode_expression(Functions.count("*").expr)
    end

    test "encoder count-star rewrite covers the qualified and plan-bound star forms" do
      for star <- [{:star}, {:star, "x.*"}, {:star, nil, 7}, {:star, "x.*", 7}] do
        assert %Expression{
                 expr_type:
                   {:unresolved_function,
                    %Expression.UnresolvedFunction{
                      function_name: "count",
                      arguments: [
                        %Expression{
                          expr_type: {:literal, %Expression.Literal{literal_type: {:integer, 1}}}
                        }
                      ]
                    }}
               } = PlanEncoder.encode_expression({:fn, "count", [star], false})
      end
    end

    test "count with a real column is untouched" do
      assert %Expression{
               expr_type:
                 {:unresolved_function,
                  %Expression.UnresolvedFunction{
                    function_name: "count",
                    arguments: [%Expression{expr_type: {:unresolved_attribute, _}}]
                  }}
             } = PlanEncoder.encode_expression(Functions.count("x").expr)
    end

    test "count_distinct(\"*\") keeps the star (PySpark only special-cases count/1)" do
      assert %Column{expr: {:fn, "count", [{:star}], true}} = Functions.count_distinct("*")
    end

    test "DataFrame.col/2 routes stars to plan-bound star expressions" do
      df = %DataFrame{session: nil, plan: {:plan_id, 42, {:range, 0, 1, 1, nil}}}

      assert %Column{expr: {:star, nil, {:plan_id, 42, _}}} = DataFrame.col(df, "*")
      assert %Column{expr: {:star, "x.*", {:plan_id, 42, _}}} = DataFrame.col(df, "x.*")
      assert %Column{expr: {:col, "x", {:plan_id, 42, _}}} = DataFrame.col(df, "x")
    end

    test "GroupedData.count/2 routes star column names to star expressions" do
      gd = grouped()

      assert %DataFrame{plan: {:plan_id, _, {:aggregate, _, :groupby, _, aggs}}} =
               GroupedData.count(gd, "*")

      assert aggs == [{:fn, "count", [{:star}], false}]

      assert %DataFrame{plan: {:plan_id, _, {:aggregate, _, :groupby, _, aggs2}}} =
               GroupedData.count(gd, ["x.*"])

      assert aggs2 == [{:fn, "count", [{:star, "x.*"}], false}]
    end

    test "T-40: agg map form leaves the star key unaliased" do
      assert %DataFrame{plan: {:plan_id, _, {:aggregate, _, :groupby, _, aggs}}} =
               GroupedData.agg(grouped(), %{"*" => "count"})

      assert aggs == [{:fn, "count", [{:star}], false}]
    end

    test "agg map form still aliases ordinary keys" do
      assert %DataFrame{plan: {:plan_id, _, {:aggregate, _, :groupby, _, aggs}}} =
               GroupedData.agg(grouped(), %{"x" => "sum"})

      assert aggs == [{:alias, {:fn, "sum", [{:col, "x"}], false}, "sum(x)"}]
    end
  end

  describe "T-06: out-of-int64 integers in collections" do
    test "array literal with an out-of-int64 element uses a decimal element type" do
      assert %Expression.Literal{
               literal_type:
                 {:array,
                  %Expression.Literal.Array{
                    element_type: %DataType{kind: {:decimal, %DataType.Decimal{precision: 19}}},
                    elements: [first, second]
                  }}
             } = literal({:lit, [9_223_372_036_854_775_808, 1]})

      assert {:decimal, %Expression.Literal.Decimal{value: "9223372036854775808", precision: 19}} =
               first.literal_type

      assert {:decimal, %Expression.Literal.Decimal{value: "1", precision: 19}} =
               second.literal_type
    end

    test "map literal with an out-of-int64 value promotes siblings consistently" do
      assert %Expression.Literal{
               literal_type:
                 {:map,
                  %Expression.Literal.Map{
                    value_type: %DataType{kind: {:decimal, _}},
                    values: values
                  }}
             } =
               literal(
                 {:lit,
                  {:map, %{"a" => 170_141_183_460_469_231_731_687_303_715_884_105_728, "b" => 2}}}
               )

      assert Enum.all?(values, &match?({:decimal, _}, &1.literal_type))
    end

    test "in-range integers still infer long" do
      assert %Expression.Literal{
               literal_type:
                 {:array, %Expression.Literal.Array{element_type: %DataType{kind: {:long, _}}}}
             } = literal({:lit, [9_223_372_036_854_775_807, 1]})
    end
  end

  describe "T-07: instr takes the substring as a literal" do
    test "instr/2 encodes the second argument as a string literal" do
      assert %Column{expr: {:fn, "instr", [{:col, "t"}, {:lit, "x"}], false}} =
               Functions.instr("t", "x")
    end
  end

  describe "T-25: first_value/last_value" do
    test "emit their own function names" do
      assert %Column{expr: {:fn, "first_value", [{:col, "x"}], false}} =
               Functions.first_value("x")

      assert %Column{expr: {:fn, "last_value", [{:col, "x"}], false}} = Functions.last_value("x")
    end

    test "still support ignore_nulls" do
      assert %Column{expr: {:fn, "first_value", [{:col, "x"}, {:lit, true}], false}} =
               Functions.first_value("x", ignore_nulls: true)
    end

    test "first/last are unchanged" do
      assert %Column{expr: {:fn, "first", [{:col, "x"}], false}} = Functions.first("x")
      assert %Column{expr: {:fn, "last", [{:col, "x"}], false}} = Functions.last("x")
    end
  end

  describe "T-26: empty grouped aggregate sets" do
    test "agg/2 with an empty list still raises (PySpark asserts non-empty exprs)" do
      assert_raise ArgumentError, ~r/at least one aggregate/, fn ->
        GroupedData.agg(grouped(), [])
      end
    end

    test "agg/2 with an empty map is accepted" do
      assert %DataFrame{plan: {:plan_id, _, {:aggregate, _, :groupby, _, []}}} =
               GroupedData.agg(grouped(), %{})
    end
  end

  defp grouped do
    df = %DataFrame{session: nil, plan: {:range, 0, 1, 1, nil}}
    DataFrame.group_by(df, ["a"])
  end
end
