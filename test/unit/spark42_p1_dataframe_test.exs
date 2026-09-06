defmodule SparkEx.Unit.Spark42P1DataFrameTest do
  use ExUnit.Case, async: true

  alias SparkEx.{DataFrame, Functions, GroupedData}
  alias Spark.Connect.DataType

  defmodule SchemaSession do
    use GenServer

    def start_link(state), do: GenServer.start_link(__MODULE__, state)
    @impl true
    def init(state), do: {:ok, state}
    @impl true
    def handle_call({:analyze_schema, _}, _, {owner, schema} = state) do
      send(owner, :schema_rpc)
      {:reply, {:ok, schema}, state}
    end

    def handle_call({:analyze_ddl_parse, ddl}, _, {owner, schema} = state) do
      send(owner, {:ddl_rpc, ddl})
      {:reply, {:ok, schema}, state}
    end
  end

  defp struct_type(fields) do
    %DataType{
      kind:
        {:struct,
         %DataType.Struct{
           fields:
             Enum.map(fields, fn {name, type} ->
               %DataType.StructField{name: name, data_type: type}
             end)
         }}
    }
  end

  defp long, do: %DataType{kind: {:long, %DataType.Long{}}}

  test "grouping ordinals bind resolved top-level fields and retain names and explicit origins" do
    df = %{
      DataFrame.new(self(), {:sql, "unused", nil})
      | _schema: struct_type([{"a.b", long()}, {"other", long()}])
    }

    bound = DataFrame.col(df, "other")

    for group <- [&DataFrame.group_by/2, &DataFrame.rollup/2, &DataFrame.cube/2] do
      assert group.(df, [1, "other", bound]).grouping_exprs == [
               DataFrame.col(df, "`a.b`").expr,
               {:col, "other"},
               bound.expr
             ]

      for ordinal <- [0, -1, 3] do
        assert_raise ArgumentError, fn -> group.(df, [ordinal]) end
      end
    end

    grouped = DataFrame.grouping_sets(df, [[1], [], [2]], [1, 2])
    assert grouped.grouping_sets == [[DataFrame.col(df, "`a.b`").expr], [], [bound.expr]]
    assert grouped.grouping_exprs == [DataFrame.col(df, "`a.b`").expr, bound.expr]
  end

  test "nested grouping sets fetch schema once and invalid ordinals fail before RPC" do
    {:ok, session} = SchemaSession.start_link({self(), struct_type([{"id", long()}])})
    df = DataFrame.new(session, {:sql, "unused", nil})
    DataFrame.grouping_sets(df, [[1], [], [1]], [1])
    assert_received :schema_rpc
    refute_received :schema_rpc
    assert_raise ArgumentError, fn -> DataFrame.grouping_sets(df, [[1], [0]]) end
    refute_received :schema_rpc
  end

  test "ordinary and unknown column references stay lazy with their intended origins" do
    df = DataFrame.new(self(), {:sql, "unused", nil})
    other = DataFrame.new(self(), {:sql, "unrelated", nil})
    bound = DataFrame.col(other, "missing.nested")
    result = DataFrame.select(df, ["missing", "*", bound])
    assert {:plan_id, _, {:project, _, [{:col, "missing"}, {:star}, expr]}} = result.plan
    assert expr == bound.expr
    {encoded, _} = SparkEx.Connect.PlanEncoder.encode(result.plan, 0)
    {:root, %{rel_type: {:with_relations, relations}}} = encoded.op_type
    {:project, project} = relations.root.rel_type
    [reference] = relations.references
    assert {:sql, %{query: "unrelated"}} = reference.rel_type
    [name, star, origin] = project.expressions
    assert {:unresolved_attribute, %{plan_id: nil}} = name.expr_type
    assert {:unresolved_star, %{plan_id: nil}} = star.expr_type
    origin_id = reference.common.plan_id
    assert {:unresolved_attribute, %{plan_id: ^origin_id}} = origin.expr_type
    assert DataFrame.group_by(df, ["missing"]).grouping_exprs == [{:col, "missing"}]
    assert DataFrame.grouping_sets(df, [[]]).grouping_sets == [[]]
  end

  test "numeric shortcuts resolve nested and quoted names while implicit expansion stays top-level" do
    schema =
      struct_type([
        {"b", struct_type([{"c", long()}, {"a`b", long()}])},
        {"b.c", long()},
        {"text", %DataType{kind: {:string, %DataType.String{}}}}
      ])

    gd = %{
      (DataFrame.new(self(), {:sql, "unused", nil})
       |> DataFrame.group_by([]))
      | cached_schema: schema
    }

    for name <- ["b.c", "`b.c`", "`b`.`c`", "b.`a``b`"] do
      result = GroupedData.sum(gd, name)

      assert {:plan_id, _, {:aggregate, _, :groupby, [], [{:fn, "sum", [{:col, ^name}], false}]}} =
               result.plan
    end

    for name <- ["text", "b", "b.missing", "b..c", "`b.c", "b.`c`x"] do
      assert_raise ArgumentError, ~r/expected numeric columns/, fn ->
        GroupedData.sum(gd, name)
      end
    end

    assert {:plan_id, _, {:aggregate, _, :groupby, [], [_]}} = GroupedData.sum(gd).plan
  end

  test "Parse analyzes only DDL strings and stores the concrete schema" do
    schema = struct_type([{"nested", struct_type([{"leaf", long()}])}])
    {:ok, session} = SchemaSession.start_link({self(), schema})
    df = DataFrame.new(session, {:sql, "unused", nil})
    result = DataFrame.parse(df, :xml, "nested STRUCT<leaf: BIGINT>")
    assert_received {:ddl_rpc, "nested STRUCT<leaf: BIGINT>"}
    refute_received :schema_rpc
    assert {:plan_id, _, {:parse, _, :xml, ^schema, nil}} = result.plan
    DataFrame.parse(df, :json, schema)
    refute_received {:ddl_rpc, _}
  end

  test "zip index is lazy distributed sequence projection and accepts duplicate names" do
    df = DataFrame.new(self(), {:sql, "unused", nil})

    for name <- ["index", "id", "a.b"] do
      assert {:plan_id, _,
              {:project, plan,
               [{:star, nil, plan}, {:alias, {:fn, "distributed_sequence_id", [], false}, ^name}]}} =
               DataFrame.zip_with_index(df, name).plan
    end

    assert %DataFrame{} = DataFrame.zip_with_index(df)
    assert %DataFrame{} = DataFrame.parse(df, :xml)
    assert %DataFrame{} = DataFrame.select(df, [Functions.col("id")])
  end
end
