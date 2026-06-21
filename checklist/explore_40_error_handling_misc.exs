# Round 16b: Error handling, type mismatch edge cases, DataFrame.transform,
# lateral_join attempts, advanced SQL patterns, config edge cases
alias SparkEx.{DataFrame, Column, Catalog, Writer}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 80, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# Error handling edge cases — do we get good errors?
# =============================================

run.("1. filter with wrong column type (string > int)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}
  ], schema: "name STRING")

  df |> DataFrame.filter(Column.gt(col("name"), lit(42))) |> DataFrame.collect()
end)

run.("2. agg without group_by", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.agg([
    sum(col("val")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.collect()
end)

run.("3. select nonexistent column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}
  ], schema: "id INT")

  df |> DataFrame.select([col("nonexistent")]) |> DataFrame.collect()
end)

run.("4. join on nonexistent column", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  DataFrame.join(df1, df2, ["nonexistent"]) |> DataFrame.collect()
end)

run.("5. cast invalid (string to date)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "not-a-date"}
  ], schema: "val STRING")

  df
  |> DataFrame.select([
    Column.cast(col("val"), "DATE") |> Column.alias_("dt"),
    Column.try_cast(col("val"), "DATE") |> Column.alias_("dt_try")
  ])
  |> DataFrame.collect()
end)

run.("6. division by zero in aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 0}
  ], schema: "val INT")

  df
  |> DataFrame.select([
    Column.divide(sum(col("val")), count(lit(1))) |> Column.alias_("avg_manual"),
    Column.divide(lit(100), sum(col("val"))) |> Column.alias_("div_by_zero")
  ])
  |> DataFrame.collect()
end)

run.("7. invalid SQL", fn ->
  SparkEx.sql(s, "SELEKT * FORM table") |> DataFrame.collect()
end)

run.("8. SQL with syntax error in nested query", fn ->
  SparkEx.sql(s, """
    SELECT * FROM (
      SELECT id, FROM VALUES (1) AS t(id)
    )
  """) |> DataFrame.collect()
end)

# =============================================
# DataFrame.transform (Elixir function application)
# =============================================

run.("9. DataFrame.transform with function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  add_doubled = fn df ->
    DataFrame.with_column(df, "doubled", Column.multiply(col("val"), lit(2)))
  end

  add_label = fn df ->
    DataFrame.with_column(df, "label",
      concat_ws("", [lit("item_"), Column.cast(col("id"), "STRING")]))
  end

  df
  |> DataFrame.transform(add_doubled)
  |> DataFrame.transform(add_label)
  |> DataFrame.collect()
end)

# =============================================
# Advanced aggregation patterns
# =============================================

run.("10. GroupedData.count (no args)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 1},
    %{"grp" => "a", "val" => 2},
    %{"grp" => "b", "val" => 3}
  ], schema: "grp STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.count()
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

run.("11. GroupedData.min/max/sum/avg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 10},
    %{"grp" => "a", "val" => 20},
    %{"grp" => "b", "val" => 30}
  ], schema: "grp STRING, val INT")

  grouped = DataFrame.group_by(df, [col("grp")])
  {:ok, mins} = SparkEx.GroupedData.min(grouped) |> DataFrame.order_by([asc(col("grp"))]) |> DataFrame.collect()
  {:ok, maxs} = SparkEx.GroupedData.max(grouped) |> DataFrame.order_by([asc(col("grp"))]) |> DataFrame.collect()
  {:ok, sums} = SparkEx.GroupedData.sum(grouped) |> DataFrame.order_by([asc(col("grp"))]) |> DataFrame.collect()
  {:ok, avgs} = SparkEx.GroupedData.avg(grouped) |> DataFrame.order_by([asc(col("grp"))]) |> DataFrame.collect()
  {mins, maxs, sums, avgs}
end)

run.("12. rollup aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "region" => "east", "val" => 100},
    %{"dept" => "eng", "region" => "west", "val" => 200},
    %{"dept" => "sales", "region" => "east", "val" => 150},
    %{"dept" => "sales", "region" => "west", "val" => 250}
  ], schema: "dept STRING, region STRING, val INT")

  DataFrame.rollup(df, [col("dept"), col("region")])
  |> SparkEx.GroupedData.agg([
    sum(col("val")) |> Column.alias_("total")
  ])
  |> DataFrame.order_by([asc(col("dept")), asc(col("region"))])
  |> DataFrame.collect()
end)

run.("13. cube aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "region" => "east", "val" => 100},
    %{"dept" => "eng", "region" => "west", "val" => 200},
    %{"dept" => "sales", "region" => "east", "val" => 150},
    %{"dept" => "sales", "region" => "west", "val" => 250}
  ], schema: "dept STRING, region STRING, val INT")

  DataFrame.cube(df, [col("dept"), col("region")])
  |> SparkEx.GroupedData.agg([
    sum(col("val")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("dept")), asc(col("region"))])
  |> DataFrame.collect()
end)

# =============================================
# Advanced column expression patterns
# =============================================

run.("14. when/otherwise multi-chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i * 10} end),
    schema: "val INT")

  df
  |> DataFrame.select([
    col("val"),
    when_(Column.lt(col("val"), lit(30)), lit("low"))
    |> Column.when_(Column.lt(col("val"), lit(70)), lit("mid"))
    |> Column.otherwise(lit("high"))
    |> Column.alias_("tier")
  ])
  |> DataFrame.collect()
end)

run.("15. nested when/otherwise", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"age" => 10, "score" => 90},
    %{"age" => 25, "score" => 60},
    %{"age" => 40, "score" => 80},
    %{"age" => 60, "score" => 50}
  ], schema: "age INT, score INT")

  df
  |> DataFrame.select([
    col("age"), col("score"),
    when_(
      Column.and_(Column.lt(col("age"), lit(18)), Column.gte(col("score"), lit(80))),
      lit("young prodigy")
    )
    |> Column.when_(Column.gte(col("age"), lit(50)), lit("senior"))
    |> Column.when_(Column.gte(col("score"), lit(70)), lit("skilled"))
    |> Column.otherwise(lit("average"))
    |> Column.alias_("classification")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Writer edge cases: insert_into
# =============================================

run.("16. insert_into existing table", fn ->
  tbl = "sfx.sfx_dev_warehouse.spark_ex_ins_#{run_id}"

  {:ok, initial} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")
  initial |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, extra} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob"},
    %{"id" => 3, "name" => "Carol"}
  ], schema: "id INT, name STRING")
  extra |> DataFrame.write() |> Writer.insert_into(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# Catalog edge cases
# =============================================

run.("17. Catalog.list_functions", fn ->
  {:ok, fns} = Catalog.list_functions(s)
  IO.puts("  Total functions: #{length(fns)}")
  {:ok, fns |> Enum.take(5) |> Enum.map(& &1.name)}
end)

run.("18. Catalog.get_function", fn ->
  Catalog.get_function(s, "concat")
end)

# =============================================
# DataFrame operations on single-row results
# =============================================

run.("19. Single row: count, first, head, is_empty?", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 42, "name" => "solo"}
  ], schema: "id INT, name STRING")

  {:ok, cnt} = DataFrame.count(df)
  {:ok, first} = DataFrame.first(df)
  {:ok, head} = DataFrame.head(df, 1)
  {:ok, empty} = DataFrame.is_empty?(df)
  {cnt, first, head, empty}
end)

run.("20. head(0) on non-empty DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")
  DataFrame.head(df, 0)
end)

# =============================================
# DataFrame reuse patterns
# =============================================

run.("21. Same DataFrame used in multiple independent ops", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  {:ok, filtered} = df |> DataFrame.filter(Column.gt(col("val"), lit(50))) |> DataFrame.collect()
  {:ok, agged} = df |> DataFrame.select([sum(col("val")) |> Column.alias_("total")]) |> DataFrame.collect()
  {:ok, counted} = DataFrame.count(df)
  {filtered, agged, counted}
end)

# =============================================
# SparkEx.range edge cases
# =============================================

run.("22. range with step 0 (error expected)", fn ->
  SparkEx.range(s, 0, 10, 0) |> DataFrame.collect()
end)

run.("23. range with negative step", fn ->
  SparkEx.range(s, 10, 0, -1) |> DataFrame.collect()
end)

run.("24. range with start > end (positive step)", fn ->
  SparkEx.range(s, 10, 0, 1) |> DataFrame.collect()
end)

# =============================================
# Complex pipeline edge cases
# =============================================

run.("25. 10-column withColumn chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"base" => 1}
  ], schema: "base INT")

  result = Enum.reduce(1..10, df, fn i, acc ->
    DataFrame.with_column(acc, "col_#{i}",
      Column.multiply(col("base"), lit(i)))
  end)

  result |> DataFrame.columns()
end)

run.("26. Multiple filters chained (AND composition)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"id" => i, "even" => rem(i, 2) == 0, "gt50" => i > 50}
    end),
    schema: "id INT, even BOOLEAN, gt50 BOOLEAN")

  df
  |> DataFrame.filter(Column.eq(col("even"), lit(true)))
  |> DataFrame.filter(Column.eq(col("gt50"), lit(true)))
  |> DataFrame.filter(Column.lt(col("id"), lit(80)))
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("27. select → with_column → drop → rename chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5}
  ], schema: "a INT, b INT, c INT, d INT, e INT")

  df
  |> DataFrame.select([col("a"), col("b"), col("c"), col("d")])
  |> DataFrame.with_column("sum_ab", Column.plus(col("a"), col("b")))
  |> DataFrame.drop(["c"])
  |> DataFrame.with_column_renamed("d", "renamed_d")
  |> DataFrame.collect()
end)

# =============================================
# Exec info and plan inspection
# =============================================

run.("28. execution_info after collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  df |> DataFrame.collect()
  DataFrame.execution_info(df)
end)

run.("29. explain extended", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}
  ], schema: "id INT, val INT")

  {:ok, plan} = df
    |> DataFrame.filter(Column.gt(col("val"), lit(5)))
    |> DataFrame.explain("extended")

  IO.puts("  Extended plan length: #{String.length(plan)} chars")
  {:ok, :plan_retrieved}
end)

run.("30. explain formatted", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.filter(Column.gt(col("val"), lit(5)))
  |> DataFrame.explain("formatted")
end)

IO.puts("=== Error handling and misc edge case tests complete ===")
