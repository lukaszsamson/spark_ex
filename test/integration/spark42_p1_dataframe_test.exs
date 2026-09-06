defmodule SparkEx.Integration.Spark42P1DataFrameTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Functions, GroupedData, Session}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "zip indices are consecutive longs across partitions, including empty and duplicate names",
       %{session: session} do
    df =
      SparkEx.range(session, 37)
      |> DataFrame.repartition(4)
      |> DataFrame.zip_with_index("position")

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.sort(Enum.map(rows, & &1["position"])) == Enum.to_list(0..36)
    assert {:ok, [{"id", "bigint"}, {"position", "bigint"}]} = DataFrame.dtypes(df)

    assert {:ok, []} =
             SparkEx.range(session, 0) |> DataFrame.zip_with_index() |> DataFrame.collect()

    assert {:ok, ["id", "id"]} =
             SparkEx.range(session, 1) |> DataFrame.zip_with_index("id") |> DataFrame.columns()
  end

  test "grouping ordinals select literal dotted fields and nested numeric names resolve", %{
    session: session
  } do
    df =
      SparkEx.sql(session, "SELECT 1 AS `a.b`, named_struct('c', 2, 'a`b', 3, 'text', 'x') AS b")

    for grouping <- [&DataFrame.group_by/2, &DataFrame.rollup/2, &DataFrame.cube/2] do
      result =
        grouping.(df, [1]) |> GroupedData.agg([Column.alias_(Functions.sum("b.c"), "total")])

      assert {:ok, rows} = DataFrame.collect(result)
      assert Enum.any?(rows, &(&1["a.b"] == 1 and &1["total"] == 2))
    end

    for name <- ["b.c", "b.`a``b`", "`a.b`"] do
      assert {:ok, [_]} =
               df |> DataFrame.group_by([]) |> GroupedData.sum(name) |> DataFrame.collect()
    end

    assert_raise ArgumentError, fn ->
      df |> DataFrame.group_by([]) |> GroupedData.sum("b.text")
    end

    assert {:ok, rows} =
             df
             |> DataFrame.grouping_sets([[1], []], [1])
             |> GroupedData.count()
             |> DataFrame.collect()

    assert length(rows) == 2
  end

  test "name resolution after replacement, nested projections, aliases and using joins", %{
    session: session
  } do
    original = SparkEx.sql(session, "SELECT 1 AS id, named_struct('leaf', 3) AS nested")
    replaced = DataFrame.with_column(original, "id", Functions.lit(7))
    assert {:ok, [%{"id" => 7}]} = replaced |> DataFrame.select(["id"]) |> DataFrame.collect()

    assert {:ok, [%{"leaf" => 3}]} =
             original |> DataFrame.select(["nested.leaf"]) |> DataFrame.collect()

    assert {:ok, [%{"id" => 1}]} =
             original
             |> DataFrame.alias_("left")
             |> DataFrame.select(["left.id"])
             |> DataFrame.collect()

    left = DataFrame.alias_(original, "l")
    right = DataFrame.alias_(original, "r")
    joined = DataFrame.join(left, right, ["id"])
    assert {:ok, [%{"id" => 1}]} = joined |> DataFrame.select(["id"]) |> DataFrame.collect()
    assert {:ok, [_]} = original |> DataFrame.select(["*"]) |> DataFrame.collect()

    assert {:ok, [_]} =
             DataFrame.join(
               left,
               right,
               Column.eq(DataFrame.col(left, "id"), DataFrame.col(right, "id"))
             )
             |> DataFrame.select([DataFrame.col(left, "id")])
             |> DataFrame.collect()

    assert {:ok, [%{"id" => 1}]} =
             SparkEx.sql(
               session,
               "SELECT id FROM (SELECT 1 id, 2 a) NATURAL JOIN (SELECT 1 id, 3 b)"
             )
             |> DataFrame.collect()
  end

  @tag min_spark: "4.2"
  test "strict origin resolution rejects unrelated references and allows opt-out for replaced columns",
       %{session: session} do
    key = "spark.sql.analyzer.strictDataFrameColumnResolution"
    first = SparkEx.sql(session, "SELECT 1 AS id")
    unrelated = SparkEx.sql(session, "SELECT 2 AS id")
    invalid = DataFrame.select(first, [DataFrame.col(unrelated, "id")])

    try do
      assert :ok = Session.config_set(session, [{key, "true"}])
      assert {:error, _} = DataFrame.collect(invalid)

      replaced =
        first
        |> DataFrame.with_column("id", Column.cast(Functions.col("id"), "string"))
        |> DataFrame.select([DataFrame.col(first, "id")])

      assert {:error, %{error_class: "CANNOT_RESOLVE_DATAFRAME_COLUMN"}} =
               DataFrame.collect(replaced)

      assert :ok = Session.config_set(session, [{key, "false"}])
      assert {:ok, [%{"id" => "1"}]} = DataFrame.collect(replaced)

      assert {:error, %{error_class: "CANNOT_RESOLVE_DATAFRAME_COLUMN"}} =
               DataFrame.collect(invalid)
    after
      Session.config_unset(session, [key])
    end
  end

  @tag min_spark: "4.2"
  test "empty grouping sets produce grand totals with configurable legacy behavior", %{
    session: session
  } do
    key = "spark.sql.analyzer.lowerEmptyGroupingSetToGlobalAggregate.enabled"

    try do
      for enabled <- ["true", "false"], count <- [0, 2] do
        assert :ok = Session.config_set(session, [{key, enabled}])
        df = SparkEx.range(session, count)

        for group <- [
              DataFrame.cube(df, []),
              DataFrame.rollup(df, []),
              DataFrame.grouping_sets(df, [[]])
            ] do
          aggregate =
            GroupedData.agg(group, [
              Functions.expr("count(*) AS n"),
              Functions.expr("sum(id) AS total"),
              Functions.expr("grouping_id() AS gid")
            ])

          assert {:ok, rows} = DataFrame.collect(aggregate)

          if count == 0 and enabled == "false" do
            assert rows == []
          else
            assert rows == [
                     %{"n" => count, "total" => if(count == 0, do: nil, else: 1), "gid" => 0}
                   ]
          end
        end
      end
    after
      Session.config_unset(session, [key])
    end
  end

  @tag min_spark: "4.2"
  test "aggregate NULL treatment is SQL syntax with unordered null-retaining semantics", %{
    session: session
  } do
    for function <- ["array_agg", "collect_list", "collect_set"],
        treatment <- ["IGNORE", "RESPECT"] do
      query =
        "SELECT #{function}(v) #{treatment} NULLS AS values FROM VALUES (1), (NULL), (1), (2) AS t(v)"

      assert {:ok, [%{"values" => values}]} = SparkEx.sql(session, query) |> DataFrame.collect()
      expected = if function == "collect_set", do: [1, 2], else: [1, 1, 2]
      expected = if treatment == "RESPECT", do: [nil | expected], else: expected
      assert Enum.sort(values) == Enum.sort(expected)

      assert {:ok, [%{"values" => []}]} =
               SparkEx.sql(
                 session,
                 "SELECT #{function}(v) #{treatment} NULLS AS values FROM (SELECT CAST(NULL AS INT) v WHERE false)"
               )
               |> DataFrame.collect()
    end
  end

  @tag min_spark: "4.2"
  test "release geospatial default and explicit TIME feature guards", %{session: session} do
    geo = "spark.sql.geospatial.enabled"
    time = "spark.sql.timeType.enabled"

    try do
      assert {:ok, [_]} =
               SparkEx.sql(
                 session,
                 "SELECT ST_AsBinary(ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040'))) AS p"
               )
               |> DataFrame.collect()

      assert :ok = Session.config_set(session, [{geo, "false"}, {time, "false"}])

      assert {:error, _} =
               SparkEx.sql(
                 session,
                 "SELECT ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040'))"
               )
               |> DataFrame.collect()

      assert {:error, _} = SparkEx.sql(session, "SELECT TIME '12:34:56'") |> DataFrame.collect()
      assert :ok = Session.config_set(session, [{time, "true"}])
      assert {:ok, [_]} = SparkEx.sql(session, "SELECT TIME '12:34:56'") |> DataFrame.collect()
    after
      Session.config_unset(session, [geo, time])
    end
  end

  @tag min_spark: "4.2"
  test "Spark JSON Arrow metadata survives collect, Explorer and raw Arrow complex fields", %{
    session: session
  } do
    nested = Column.alias_(Functions.lit(1), "leaf", metadata: %{"nested" => "present"})

    df =
      SparkEx.range(session, 1)
      |> DataFrame.select([
        Column.alias_(Functions.struct([nested]), "s", metadata: %{"owner" => "spark-ex"}),
        Column.alias_(Functions.expr("array(named_struct('x', 2))"), "a",
          metadata: %{"custom" => [1, 2]}
        ),
        Column.alias_(Functions.expr("map('key', named_struct('x', 3))"), "m",
          metadata: %{"unknown::key" => true}
        )
      ])

    assert {:ok, schema} = DataFrame.schema(df)
    assert {:struct, struct} = schema.kind
    assert Jason.decode!(hd(struct.fields).metadata) == %{"owner" => "spark-ex"}
    assert {:struct, nested_struct} = hd(struct.fields).data_type.kind
    assert Jason.decode!(hd(nested_struct.fields).metadata) == %{"nested" => "present"}
    assert {:ok, [_]} = DataFrame.collect(df)
    assert {:ok, explorer} = DataFrame.to_explorer(df)
    assert Explorer.DataFrame.n_rows(explorer) == 1
    assert {:ok, arrow} = DataFrame.to_arrow(df)
    bytes = if is_binary(arrow), do: arrow, else: IO.iodata_to_binary(arrow)
    assert bytes =~ "SPARK::metadata::json"
    assert bytes =~ "unknown::key"
    assert {:ok, ^schema} = DataFrame.schema(df)
  end

  @tag min_spark: "4.2"
  test "Parse normalizes complex DDL through Spark without executing input rows", %{
    session: session
  } do
    json =
      SparkEx.sql(session, ~s(SELECT '{"a.b":{"items":[1,2],"labels":{"key":"value"}}}' AS value))

    parsed_json =
      DataFrame.parse(json, :json, "`a.b` STRUCT<items: ARRAY<INT>, labels: MAP<STRING, STRING>>")

    assert {:ok, [%{"items" => [1, 2], "label" => "value"}]} =
             parsed_json
             |> DataFrame.select([
               Functions.expr("`a.b`.items AS items"),
               Functions.expr("`a.b`.labels['key'] AS label")
             ])
             |> DataFrame.collect()

    xml = SparkEx.sql(session, "SELECT '<ROW><parent><child>7</child></parent></ROW>' AS value")

    assert {:ok, [%{"child" => 7}]} =
             xml
             |> DataFrame.parse(:xml, "parent STRUCT<child: INT>", %{"rowTag" => "ROW"})
             |> DataFrame.select(["parent.child"])
             |> DataFrame.collect()

    csv = SparkEx.sql(session, "SELECT '1.25,text' AS value")
    parsed_csv = DataFrame.parse(csv, :csv, "`a.b` DECIMAL(10,2), text STRING COMMENT 'kept'")
    assert {:ok, schema} = DataFrame.schema(parsed_csv)
    {:struct, struct} = schema.kind
    assert Jason.decode!(Enum.at(struct.fields, 1).metadata)["comment"] == "kept"

    assert {:ok, [%{"text" => "text"}]} =
             parsed_csv |> DataFrame.select(["text"]) |> DataFrame.collect()

    unevaluated = SparkEx.sql(session, "SELECT raise_error('input must remain lazy') AS value")
    assert %DataFrame{} = DataFrame.parse(unevaluated, :json, "nested STRUCT<value: INT>")
  end

  test "JSON rows preserve nested values, omitted nulls and quoted column names", %{
    session: session
  } do
    df =
      SparkEx.sql(
        session,
        "SELECT named_struct('x', 1) AS `a.b`, CAST(NULL AS STRING) AS missing"
      )

    assert {:ok, [%{"value" => value}]} = df |> DataFrame.to_json_rows() |> DataFrame.collect()
    assert Jason.decode!(value) == %{"a.b" => %{"x" => 1}}
  end
end
