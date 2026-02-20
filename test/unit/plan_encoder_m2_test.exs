defmodule SparkEx.Connect.PlanEncoderM2Test do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Expression, Plan, Relation}

  describe "encode_relation/2 — read_named_table" do
    test "encodes named table read" do
      {plan, counter} = PlanEncoder.encode({:read_named_table, "my_db.my_table", %{}}, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:read, read}}}} = plan
      assert {:named_table, nt} = read.read_type
      assert nt.unparsed_identifier == "my_db.my_table"
      assert nt.options == %{}
      assert read.is_streaming == false
      assert counter == 1
    end

    test "encodes named table with options" do
      opts = %{"key" => "val"}
      {plan, _} = PlanEncoder.encode({:read_named_table, "t", opts}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:read, read}}}} = plan
      {:named_table, nt} = read.read_type
      assert nt.options == %{"key" => "val"}
    end
  end

  describe "encode_relation/2 — read_data_source" do
    test "encodes parquet data source" do
      {plan, counter} =
        PlanEncoder.encode(
          {:read_data_source, "parquet", ["/data/file.parquet"], nil, %{}},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:read, read}}}} = plan
      assert {:data_source, ds} = read.read_type
      assert ds.format == "parquet"
      assert ds.paths == ["/data/file.parquet"]
      assert ds.schema == nil
      assert ds.options == %{}
      assert counter == 1
    end

    test "encodes csv data source with schema and options" do
      {plan, _} =
        PlanEncoder.encode(
          {:read_data_source, "csv", ["/data/f.csv"], "id INT", %{"header" => "true"}},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:read, read}}}} = plan
      {:data_source, ds} = read.read_type
      assert ds.format == "csv"
      assert ds.schema == "id INT"
      assert ds.options == %{"header" => "true"}
    end

    test "encodes data source predicates" do
      {plan, _} =
        PlanEncoder.encode(
          {:read_data_source, "jdbc", [], nil,
           %{"url" => "jdbc:sqlite:/tmp/db", "dbtable" => "t"}, ["id = 1", "id = 3"]},
          0
        )

      assert %Plan{op_type: {:root, %Relation{rel_type: {:read, read}}}} = plan
      {:data_source, ds} = read.read_type
      assert ds.predicates == ["id = 1", "id = 3"]
    end
  end

  describe "encode_relation/2 — project" do
    test "encodes project with column references" do
      plan_tuple = {:project, {:sql, "SELECT * FROM t", nil}, [{:col, "a"}, {:col, "b"}]}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:project, proj}}}} = plan

      assert %Relation{rel_type: {:sql, %Spark.Connect.SQL{query: "SELECT * FROM t"}}} =
               proj.input

      assert length(proj.expressions) == 2

      [expr_a, expr_b] = proj.expressions
      assert {:unresolved_attribute, %{unparsed_identifier: "a"}} = expr_a.expr_type
      assert {:unresolved_attribute, %{unparsed_identifier: "b"}} = expr_b.expr_type
      assert counter == 2
    end
  end

  describe "encode_relation/2 — filter" do
    test "encodes filter with condition" do
      condition = {:fn, ">", [{:col, "age"}, {:lit, 18}], false}
      plan_tuple = {:filter, {:sql, "SELECT * FROM t", nil}, condition}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:filter, filt}}}} = plan

      assert %Relation{rel_type: {:sql, %Spark.Connect.SQL{query: "SELECT * FROM t"}}} =
               filt.input

      assert {:unresolved_function, func} = filt.condition.expr_type
      assert func.function_name == ">"
      assert length(func.arguments) == 2
      assert counter == 2
    end
  end

  describe "encode_relation/2 — sort" do
    test "encodes sort with sort orders" do
      sort_orders = [
        {:sort_order, {:col, "name"}, :asc, nil},
        {:sort_order, {:col, "age"}, :desc, :nulls_last}
      ]

      plan_tuple = {:sort, {:sql, "SELECT * FROM t", nil}, sort_orders}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:sort, sort}}}} = plan

      assert %Relation{rel_type: {:sql, %Spark.Connect.SQL{query: "SELECT * FROM t"}}} =
               sort.input

      assert sort.is_global == true
      assert length(sort.order) == 2

      [order1, order2] = sort.order
      assert %Expression.SortOrder{} = order1
      assert order1.direction == :SORT_DIRECTION_ASCENDING
      assert order1.null_ordering == :SORT_NULLS_FIRST

      assert %Expression.SortOrder{} = order2
      assert order2.direction == :SORT_DIRECTION_DESCENDING
      assert order2.null_ordering == :SORT_NULLS_LAST
      assert counter == 2
    end
  end

  describe "encode_relation/2 — with_columns" do
    test "encodes with_columns with aliases" do
      aliases = [{:alias, {:fn, "+", [{:col, "a"}, {:lit, 1}], false}, "a_plus_1"}]
      plan_tuple = {:with_columns, {:sql, "SELECT * FROM t", nil}, aliases}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:with_columns, wc}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = wc.input
      assert [%Expression.Alias{} = alias_proto] = wc.aliases
      assert alias_proto.name == ["a_plus_1"]
      assert {:unresolved_function, _} = alias_proto.expr.expr_type
      assert counter == 2
    end
  end

  describe "encode_relation/2 — drop" do
    test "encodes drop with column names" do
      plan_tuple = {:drop, {:sql, "SELECT * FROM t", nil}, ["temp", "debug"]}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:drop, drop_rel}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = drop_rel.input
      assert drop_rel.column_names == ["temp", "debug"]
      assert counter == 2
    end
  end

  describe "encode_relation/2 — show_string" do
    test "encodes show_string with parameters" do
      plan_tuple = {:show_string, {:sql, "SELECT 1", nil}, 20, 30, false}
      {plan, counter} = PlanEncoder.encode(plan_tuple, 0)

      assert %Plan{op_type: {:root, %Relation{rel_type: {:show_string, show}}}} = plan
      assert %Relation{rel_type: {:sql, _}} = show.input
      assert show.num_rows == 20
      assert show.truncate == 30
      assert show.vertical == false
      assert counter == 2
    end
  end

  describe "encode_expression/1" do
    test "encodes column reference" do
      expr = PlanEncoder.encode_expression({:col, "name"})
      assert {:unresolved_attribute, attr} = expr.expr_type
      assert attr.unparsed_identifier == "name"
    end

    test "encodes literal values" do
      for {value, expected_type} <- [
            {42, {:integer, 42}},
            {"hello", {:string, "hello"}},
            {true, {:boolean, true}},
            {3.14, {:double, 3.14}},
            {3_000_000_000, {:long, 3_000_000_000}}
          ] do
        expr = PlanEncoder.encode_expression({:lit, value})
        assert {:literal, lit} = expr.expr_type
        assert lit.literal_type == expected_type
      end
    end

    test "encodes nil literal" do
      expr = PlanEncoder.encode_expression({:lit, nil})
      assert {:literal, lit} = expr.expr_type
      assert {:null, _} = lit.literal_type
    end

    test "encodes expression string" do
      expr = PlanEncoder.encode_expression({:expr, "age + 1"})
      assert {:expression_string, es} = expr.expr_type
      assert es.expression == "age + 1"
    end

    test "encodes unresolved function" do
      expr = PlanEncoder.encode_expression({:fn, "count", [{:col, "x"}], false})
      assert {:unresolved_function, func} = expr.expr_type
      assert func.function_name == "count"
      assert length(func.arguments) == 1
      assert func.is_distinct == false
    end

    test "encodes distinct function" do
      expr = PlanEncoder.encode_expression({:fn, "count", [{:col, "x"}], true})
      assert {:unresolved_function, func} = expr.expr_type
      assert func.is_distinct == true
    end

    test "encodes alias expression" do
      expr = PlanEncoder.encode_expression({:alias, {:col, "x"}, "renamed"})
      assert {:alias, alias_expr} = expr.expr_type
      assert alias_expr.name == ["renamed"]
      assert {:unresolved_attribute, _} = alias_expr.expr.expr_type
    end

    test "encodes cast expression" do
      expr = PlanEncoder.encode_expression({:cast, {:col, "x"}, "int"})
      assert {:cast, cast_expr} = expr.expr_type
      assert {:unresolved_attribute, _} = cast_expr.expr.expr_type
      assert {:type_str, "int"} = cast_expr.cast_to_type
    end

    test "encodes sort order" do
      expr = PlanEncoder.encode_expression({:sort_order, {:col, "x"}, :desc, :nulls_first})
      assert {:sort_order, so} = expr.expr_type
      assert {:unresolved_attribute, _} = so.child.expr_type
      assert so.direction == :SORT_DIRECTION_DESCENDING
      assert so.null_ordering == :SORT_NULLS_FIRST
    end

    test "encodes unresolved star" do
      expr = PlanEncoder.encode_expression({:star})
      assert {:unresolved_star, _} = expr.expr_type
    end

    test "encodes unresolved star with target" do
      expr = PlanEncoder.encode_expression({:star, "t"})
      assert {:unresolved_star, star} = expr.expr_type
      assert star.unparsed_target == "t"
    end
  end

  describe "nested expression encoding" do
    test "encodes deeply nested expressions" do
      # (a > 10) AND (b < 20)
      nested =
        {:fn, "and",
         [
           {:fn, ">", [{:col, "a"}, {:lit, 10}], false},
           {:fn, "<", [{:col, "b"}, {:lit, 20}], false}
         ], false}

      expr = PlanEncoder.encode_expression(nested)
      assert {:unresolved_function, and_func} = expr.expr_type
      assert and_func.function_name == "and"
      assert length(and_func.arguments) == 2

      [left, right] = and_func.arguments
      assert {:unresolved_function, gt_func} = left.expr_type
      assert gt_func.function_name == ">"
      assert {:unresolved_function, lt_func} = right.expr_type
      assert lt_func.function_name == "<"
    end
  end

  describe "plan ID counter management" do
    test "counter increments for each relation node in a chain" do
      # project -> filter -> read: 3 plan IDs
      plan =
        {:project,
         {:filter, {:read_data_source, "parquet", ["/p"], nil, %{}},
          {:fn, ">", [{:col, "x"}, {:lit, 0}], false}}, [{:col, "x"}]}

      {_encoded, counter} = PlanEncoder.encode(plan, 0)
      assert counter == 3
    end
  end
end
