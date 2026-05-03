defmodule SparkEx.BugsPlan4.StreamATest do
  use ExUnit.Case, async: true

  alias SparkEx.{Column, Functions}
  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.Expression

  # ── A1: Fresh lambda variable names per build_lambda/2 ──

  describe "A1 — fresh lambda variable names" do
    test "fresh_lambda_name/1 returns base with positive integer suffix" do
      a = Functions.fresh_lambda_name("x")
      b = Functions.fresh_lambda_name("x")

      assert is_binary(a)
      assert is_binary(b)
      assert a != b
      assert String.starts_with?(a, "x_")
      assert String.starts_with?(b, "x_")
      assert Regex.match?(~r/^x_\d+$/, a)
    end

    test "build_lambda emits a fresh suffix on each call" do
      r1 = Functions.transform(Functions.col("arr"), fn x -> Column.plus(x, Functions.lit(1)) end)
      r2 = Functions.transform(Functions.col("arr"), fn x -> Column.plus(x, Functions.lit(1)) end)

      assert %Column{expr: {:fn, "transform", [_, {:lambda, _, [{:lambda_var, n1}]}], false}} = r1
      assert %Column{expr: {:fn, "transform", [_, {:lambda, _, [{:lambda_var, n2}]}], false}} = r2

      assert n1 != n2
      assert String.starts_with?(n1, "x_")
      assert String.starts_with?(n2, "x_")
    end

    test "nested transform inside aggregate does not alias outer lambda var" do
      result =
        Functions.aggregate(Functions.col("arr"), Functions.lit(0), fn acc, x ->
          inner =
            Functions.transform(Functions.col("inner"), fn x_inner ->
              Column.plus(x_inner, Functions.lit(1))
            end)

          Column.plus(acc, Column.plus(x, inner))
        end)

      # Each lambda declaration must use distinct variable names across
      # all nested lambdas in the tree, so the encoded plan cannot capture
      # the wrong scope.
      declared = collect_declared_lambda_vars(result.expr)
      assert MapSet.size(MapSet.new(declared)) == length(declared)
    end

    test "exists inside transform: each lambda gets its own variable" do
      result =
        Functions.transform(Functions.col("arr"), fn x ->
          Functions.exists(x, fn y -> Column.gt(y, Functions.lit(0)) end)
        end)

      declared = collect_declared_lambda_vars(result.expr)
      # Both `transform` and `exists` declare an `x` lambda var; with fresh
      # naming the two declarations must end up distinct.
      assert MapSet.size(MapSet.new(declared)) == length(declared)
      assert length(declared) == 2
      assert Enum.all?(declared, &String.starts_with?(&1, "x_"))
    end

    test "map_filter with overlapping key/value names against outer aggregate" do
      result =
        Functions.aggregate(Functions.col("xs"), Functions.lit(0), fn acc, _x ->
          inner =
            Functions.map_filter(Functions.col("m"), fn _k, v ->
              Column.gt(v, Functions.lit(0))
            end)

          Column.plus(acc, inner)
        end)

      declared = collect_declared_lambda_vars(result.expr)
      assert MapSet.size(MapSet.new(declared)) == length(declared)
    end

    test "encoded LambdaFunction and UnresolvedNamedLambdaVariable use the suffixed name" do
      r = Functions.transform(Functions.col("arr"), fn x -> Column.plus(x, Functions.lit(1)) end)

      assert %Column{expr: {:fn, "transform", [_, lambda_term], false}} = r
      assert {:lambda, _, [{:lambda_var, name}]} = lambda_term
      assert String.starts_with?(name, "x_")

      encoded = PlanEncoder.encode_expression(lambda_term)

      assert %Expression{
               expr_type:
                 {:lambda_function,
                  %Expression.LambdaFunction{
                    arguments: [
                      %Expression.UnresolvedNamedLambdaVariable{name_parts: [^name]}
                    ]
                  }}
             } = encoded
    end
  end

  # ── A2: to_expr/1 clause order for boolean / nil / atom ──

  describe "A2 — to_expr/1 boolean and nil handling" do
    test "true / false become literals, not column refs" do
      assert {:lit, true} = Functions.to_expr(true)
      assert {:lit, false} = Functions.to_expr(false)
    end

    test "nil becomes a nil literal" do
      assert {:lit, nil} = Functions.to_expr(nil)
    end

    test "atom column names still resolve as column refs" do
      assert {:col, "my_col"} = Functions.to_expr(:my_col)
    end

    test "binary column names still resolve as column refs" do
      assert {:col, "name"} = Functions.to_expr("name")
    end

    test "numbers continue to be literals" do
      assert {:lit, 42} = Functions.to_expr(42)
      assert {:lit, 3.14} = Functions.to_expr(3.14)
    end

    test "call_function round-trip: booleans go as literals, not as columns" do
      result =
        Functions.call_function("if", [Functions.col("x"), true, false])

      assert %Column{
               expr: {:call_function, "if", [{:col, "x"}, {:lit, true}, {:lit, false}]}
             } = result
    end
  end

  # ── A3: position/2 and position/3 ──

  describe "A3 — position/2 and position/3" do
    test "position/2 with column structs" do
      result = Functions.position(Functions.col("substr"), Functions.col("str"))

      assert %Column{
               expr: {:fn, "position", [{:col, "substr"}, {:col, "str"}], false}
             } = result
    end

    test "position/2 with bare-string column names on both sides" do
      result = Functions.position("substr", "str")

      assert %Column{
               expr: {:fn, "position", [{:col, "substr"}, {:col, "str"}], false}
             } = result
    end

    test "position/3 with integer start wraps it as a literal" do
      result = Functions.position("substr", "str", 5)

      assert %Column{
               expr: {:fn, "position", [{:col, "substr"}, {:col, "str"}, {:lit, 5}], false}
             } = result
    end

    test "position/3 with column start passes through as a column ref" do
      result = Functions.position("substr", "str", Functions.col("offset"))

      assert %Column{
               expr: {:fn, "position", [{:col, "substr"}, {:col, "str"}, {:col, "offset"}], false}
             } = result
    end

    test "position/3 with bare-string start resolves as a column ref" do
      result = Functions.position("substr", "str", "offset")

      assert %Column{
               expr: {:fn, "position", [{:col, "substr"}, {:col, "str"}, {:col, "offset"}], false}
             } = result
    end

    test "position/3 rejects non-integer / non-column start" do
      assert_raise ArgumentError, fn ->
        Functions.position("substr", "str", 3.14)
      end
    end
  end

  # Collect just the declared variable names from each lambda node in the tree.
  defp collect_declared_lambda_vars(expr), do: do_collect_declared(expr, []) |> Enum.reverse()

  defp do_collect_declared({:lambda, body, vars}, acc) do
    declared =
      Enum.map(vars, fn {:lambda_var, name} -> name end)

    acc = Enum.reduce(declared, acc, fn n, a -> [n | a] end)
    do_collect_declared(body, acc)
  end

  defp do_collect_declared({:fn, _name, args, _distinct}, acc) when is_list(args) do
    Enum.reduce(args, acc, &do_collect_declared/2)
  end

  defp do_collect_declared({:call_function, _name, args}, acc) when is_list(args) do
    Enum.reduce(args, acc, &do_collect_declared/2)
  end

  defp do_collect_declared(list, acc) when is_list(list) do
    Enum.reduce(list, acc, &do_collect_declared/2)
  end

  defp do_collect_declared(tuple, acc) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reduce(acc, &do_collect_declared/2)
  end

  defp do_collect_declared(_, acc), do: acc
end
