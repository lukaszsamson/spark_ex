# Round 29b: Wild combinations - remaining tests after session crash on lit(overflow)
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1]

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
# 8g. lit with empty string (from before crash)
# =============================================

run.("8g. lit with empty string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit("") |> Column.alias_("empty")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Nested operations on computed columns
# =============================================

run.("9a. withColumn referencing previous withColumn", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")
  df
  |> DataFrame.with_column("y", Column.multiply(col("x"), lit(2)))
  |> DataFrame.with_column("z", Column.plus(col("x"), col("y")))
  |> DataFrame.with_column("w", Column.multiply(col("z"), col("y")))
  |> DataFrame.collect()
end)

run.("9b. filter on computed column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")
  df
  |> DataFrame.with_column("squared", Column.multiply(col("val"), col("val")))
  |> DataFrame.filter(Column.gt(col("squared"), lit(25)))
  |> DataFrame.select([col("val"), col("squared")])
  |> DataFrame.collect()
end)

# =============================================
# 10. Multiple aggregations chained
# =============================================

run.("10. agg result → filter → transform", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "A", "val" => 10}, %{"dept" => "A", "val" => 20},
    %{"dept" => "B", "val" => 30}, %{"dept" => "B", "val" => 40}
  ], schema: "dept STRING, val INT")

  DataFrame.group_by(df, [col("dept")])
  |> GroupedData.agg([sum(col("val")) |> Column.alias_("total"), avg(col("val")) |> Column.alias_("avg_val")])
  |> DataFrame.filter(Column.gt(col("total"), lit(25)))
  |> DataFrame.with_column("normalized", Column.divide(col("total"), col("avg_val")))
  |> DataFrame.collect()
end)

# =============================================
# 11. Complex SQL mixed with DataFrame API
# =============================================

run.("11a. SQL subquery → DataFrame transform", fn ->
  sql_df = SparkEx.sql(s, "SELECT id, id * 10 AS val FROM (SELECT explode(sequence(1, 10)) AS id)")
  sql_df
  |> DataFrame.filter(Column.gt(col("val"), lit(30)))
  |> DataFrame.with_column("category",
    when_(Column.gt(col("val"), lit(70)), lit("high"))
    |> Column.otherwise(lit("medium")))
  |> DataFrame.collect()
end)

run.("11b. SQL UDF + DataFrame pipeline", fn ->
  source = SparkEx.sql(s, "SELECT posexplode(array('alpha', 'beta', 'gamma', 'delta')) AS (idx, word)")
  DataFrame.create_or_replace_temp_view(source, "words_#{run_id}")

  result = SparkEx.Reader.table(s, "words_#{run_id}")
  |> DataFrame.with_column("upper_word", upper(col("word")))
  |> DataFrame.with_column("word_len", SparkEx.Functions.length(col("word")))
  |> DataFrame.sort([desc(col("word_len"))])
  |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "words_#{run_id}")
  result
end)

# =============================================
# 12. Column expression nesting depth
# =============================================

run.("12a. deeply nested arithmetic (10 levels)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 2}], schema: "x INT")
  expr = Enum.reduce(1..10, col("x"), fn _, acc -> Column.plus(acc, lit(1)) end)
  df |> DataFrame.select([expr |> Column.alias_("result")]) |> DataFrame.collect()
end)

run.("12b. deeply nested when_/otherwise (10 levels)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(0..10, fn i -> %{"val" => i} end), schema: "val INT")

  expr = Enum.reduce(0..9, nil, fn i, acc ->
    cond_expr = Column.eq(col("val"), lit(i))
    label = lit("label_#{i}")
    case acc do
      nil -> when_(cond_expr, label)
      chain -> Column.when_(chain, cond_expr, label)
    end
  end) |> Column.otherwise(lit("other"))

  df |> DataFrame.select([col("val"), expr |> Column.alias_("label")])
  |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

run.("12c. 20 chained filters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  Enum.reduce(1..20, df, fn i, acc ->
    DataFrame.filter(acc, Column.gt(col("id"), lit(i)))
  end) |> DataFrame.count()
end)

# =============================================
# 13. Mixed types in operations
# =============================================

run.("13a. concat mixing types with cast", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30, "score" => 95.5}
  ], schema: "name STRING, age INT, score DOUBLE")
  df |> DataFrame.select([
    concat([col("name"), lit(": age="), Column.cast(col("age"), "STRING"),
            lit(", score="), Column.cast(col("score"), "STRING")])
    |> Column.alias_("summary")
  ]) |> DataFrame.collect()
end)

run.("13b. array of computed expressions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")
  df |> DataFrame.select([
    array([col("x"), Column.plus(col("x"), lit(1)), Column.multiply(col("x"), lit(2)), lit(100)])
    |> Column.alias_("computed_array")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Pivot with complex aggregation
# =============================================

run.("14. pivot with multi-group", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"year" => 2024, "quarter" => "Q1", "region" => "US", "revenue" => 100},
    %{"year" => 2024, "quarter" => "Q2", "region" => "US", "revenue" => 150},
    %{"year" => 2024, "quarter" => "Q1", "region" => "EU", "revenue" => 80},
    %{"year" => 2024, "quarter" => "Q2", "region" => "EU", "revenue" => 120},
    %{"year" => 2025, "quarter" => "Q1", "region" => "US", "revenue" => 200}
  ], schema: "year INT, quarter STRING, region STRING, revenue INT")

  DataFrame.group_by(df, [col("region"), col("year")])
  |> GroupedData.pivot("quarter", ["Q1", "Q2"])
  |> GroupedData.agg([sum(col("revenue"))])
  |> DataFrame.sort([asc(col("region")), asc(col("year"))])
  |> DataFrame.collect()
end)

# =============================================
# 15. DataFrame.observe (metrics)
# =============================================

run.("15. DataFrame.observe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.observe("my_metrics", [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Special characters in column names
# =============================================

run.("16a. column names with spaces", fn ->
  SparkEx.sql(s, "SELECT 1 AS `my column`, 2 AS `another col`") |> DataFrame.collect()
end)

run.("16b. column names with dots/dashes", fn ->
  SparkEx.sql(s, "SELECT 1 AS `col.with.dots`, 2 AS `col-with-dashes`") |> DataFrame.collect()
end)

run.("16c. backtick column reference after SQL", fn ->
  SparkEx.sql(s, "SELECT 1 AS `my column`")
  |> DataFrame.select([col("`my column`") |> Column.alias_("renamed")])
  |> DataFrame.collect()
end)

# =============================================
# 17. Lateral column alias
# =============================================

run.("17. lateral column alias via SQL", fn ->
  SparkEx.sql(s, "SELECT id, id * 10 AS val, val + 1 AS val_plus FROM (SELECT 5 AS id)")
  |> DataFrame.collect()
end)

# =============================================
# 18. Long strings
# =============================================

run.("18a. 10K character string", fn ->
  long_str = String.duplicate("x", 10_000)
  {:ok, df} = SparkEx.create_dataframe(s, [%{"text" => long_str}], schema: "text STRING")
  {:ok, result} = df |> DataFrame.select([SparkEx.Functions.length(col("text")) |> Column.alias_("len")]) |> DataFrame.collect()
  result
end)

# =============================================
# 19. Multiple temp views cross-referenced
# =============================================

run.("19. temp views joined via SQL", fn ->
  {:ok, users} = SparkEx.create_dataframe(s, [
    %{"uid" => 1, "name" => "Alice"}, %{"uid" => 2, "name" => "Bob"}
  ], schema: "uid INT, name STRING")
  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"oid" => 100, "uid" => 1, "amount" => 50},
    %{"oid" => 101, "uid" => 1, "amount" => 30},
    %{"oid" => 102, "uid" => 2, "amount" => 70}
  ], schema: "oid INT, uid INT, amount INT")

  DataFrame.create_or_replace_temp_view(users, "users_#{run_id}")
  DataFrame.create_or_replace_temp_view(orders, "orders_#{run_id}")

  result = SparkEx.sql(s, "SELECT u.name, SUM(o.amount) AS total FROM users_#{run_id} u JOIN orders_#{run_id} o ON u.uid = o.uid GROUP BY u.name ORDER BY total DESC")
  |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "users_#{run_id}")
  DataFrame.drop_temp_view(s, "orders_#{run_id}")
  result
end)

# =============================================
# 20. Chained renames + transforms
# =============================================

run.("20. with_columns_renamed + transforms", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"firstName" => "Alice", "lastName" => "Smith", "yearOfBirth" => 1990}
  ], schema: "firstName STRING, lastName STRING, yearOfBirth INT")

  df
  |> DataFrame.with_columns_renamed(fn name -> Macro.underscore(name) end)
  |> DataFrame.with_column("full_name", concat([col("first_name"), lit(" "), col("last_name")]))
  |> DataFrame.with_column("age", Column.minus(lit(2026), col("year_of_birth")))
  |> DataFrame.select([col("full_name"), col("age")])
  |> DataFrame.collect()
end)

# =============================================
# 21. 1000-row create_dataframe
# =============================================

run.("21. 1000-row create_dataframe + agg", fn ->
  data = Enum.map(1..1000, fn i -> %{"id" => i, "val" => rem(i * 7, 100)} end)
  {:ok, df} = SparkEx.create_dataframe(s, data, schema: "id INT, val INT")
  df |> DataFrame.group_by([col("val")])
  |> GroupedData.agg([count(col("id")) |> Column.alias_("cnt")])
  |> DataFrame.sort([desc(col("cnt"))]) |> DataFrame.limit(5) |> DataFrame.collect()
end)

# =============================================
# 22. Nested SQL + DataFrame ops
# =============================================

run.("22. array of structs → explode → aggregate", fn ->
  df = SparkEx.sql(s, """
    SELECT array(
      named_struct('product', 'Widget', 'qty', 5, 'price', 10.0),
      named_struct('product', 'Gadget', 'qty', 3, 'price', 25.0),
      named_struct('product', 'Widget', 'qty', 2, 'price', 10.0)
    ) AS items
  """)
  df
  |> DataFrame.select([explode(col("items")) |> Column.alias_("item")])
  |> DataFrame.select([
    col("item.product") |> Column.alias_("product"),
    Column.multiply(col("item.qty"), col("item.price")) |> Column.alias_("line_total")
  ])
  |> DataFrame.group_by([col("product")])
  |> GroupedData.agg([sum(col("line_total")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# =============================================
# 23. Complex null logic
# =============================================

run.("23. non-null count per row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    col("a"), col("b"), col("c"),
    Column.plus(
      Column.plus(
        when_(Column.is_not_null(col("a")), lit(1)) |> Column.otherwise(lit(0)),
        when_(Column.is_not_null(col("b")), lit(1)) |> Column.otherwise(lit(0))
      ),
      when_(Column.is_not_null(col("c")), lit(1)) |> Column.otherwise(lit(0))
    ) |> Column.alias_("non_null_count")
  ]) |> DataFrame.collect()
end)

# =============================================
# 24. Multiple window specs on same query
# =============================================

run.("24. multiple window specs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "grp" => rem(i, 3), "val" => i * 10} end),
    schema: "id INT, grp INT, val INT")

  w_global = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("id"))])
  w_group = SparkEx.Window.partition_by([col("grp")]) |> WindowSpec.order_by([asc(col("id"))])
  w_sliding = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("id"))]) |> WindowSpec.rows_between(-2, 0)

  df |> DataFrame.select([
    col("id"), col("grp"), col("val"),
    row_number() |> Column.over(w_global) |> Column.alias_("global_rn"),
    row_number() |> Column.over(w_group) |> Column.alias_("group_rn"),
    sum(col("val")) |> Column.over(w_global) |> Column.alias_("running_total"),
    avg(col("val")) |> Column.over(w_sliding) |> Column.alias_("moving_avg_3")
  ]) |> DataFrame.collect()
end)

# =============================================
# 25. Sample edge cases
# =============================================

run.("25a. sample fraction 0.0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, result} = DataFrame.sample(df, 0.0) |> DataFrame.collect()
  length(result)
end)

run.("25b. sample fraction 1.0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, result} = DataFrame.sample(df, 1.0) |> DataFrame.collect()
  length(result)
end)

# =============================================
# 26. More creative combos
# =============================================

run.("26a. explode + collect_list roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b", "c"]},
    %{"id" => 2, "tags" => ["d", "e"]}
  ], schema: "id INT, tags ARRAY<STRING>")

  # explode → group_by → collect_list should roundtrip
  df
  |> DataFrame.select([col("id"), explode(col("tags")) |> Column.alias_("tag")])
  |> DataFrame.group_by([col("id")])
  |> GroupedData.agg([collect_list(col("tag")) |> Column.alias_("tags")])
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("26b. collect_set (deduplicated)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 1, "val" => "b"},
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "x"}
  ], schema: "id INT, val STRING")

  DataFrame.group_by(df, [col("id")])
  |> GroupedData.agg([collect_set(col("val")) |> Column.alias_("unique_vals")])
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("26c. explode_outer (preserves nulls)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [10, 20]},
    %{"id" => 2, "arr" => nil},
    %{"id" => 3, "arr" => []}
  ], schema: "id INT, arr ARRAY<INT>")

  df |> DataFrame.select([col("id"), explode_outer(col("arr")) |> Column.alias_("val")])
  |> DataFrame.collect()
end)

run.("26d. posexplode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => ["alpha", "beta", "gamma"]}
  ], schema: "arr ARRAY<STRING>")

  df |> DataFrame.select([posexplode(col("arr")) |> Column.alias_("pos_val")])
  |> DataFrame.collect()
end)

run.("26e. stack (unpivot via SQL function)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "q1" => 100, "q2" => 200, "q3" => 300}
  ], schema: "id INT, q1 INT, q2 INT, q3 INT")

  # Use stack via SQL on temp view
  DataFrame.create_or_replace_temp_view(df, "stack_test_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT id, stack(3, 'Q1', q1, 'Q2', q2, 'Q3', q3) AS (quarter, revenue)
    FROM stack_test_#{run_id}
  """) |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "stack_test_#{run_id}")
  result
end)

run.("26f. inline (explode array of structs)", fn ->
  df = SparkEx.sql(s, """
    SELECT inline(array(struct(1, 'a'), struct(2, 'b'), struct(3, 'c')))
  """)
  df |> DataFrame.collect()
end)

run.("26g. grouping_id with cube", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "x", "b" => "p", "val" => 10},
    %{"a" => "x", "b" => "q", "val" => 20},
    %{"a" => "y", "b" => "p", "val" => 30}
  ], schema: "a STRING, b STRING, val INT")

  DataFrame.cube(df, [col("a"), col("b")])
  |> GroupedData.agg([
    sum(col("val")) |> Column.alias_("total"),
    grouping_id([col("a"), col("b")]) |> Column.alias_("gid")
  ])
  |> DataFrame.sort([asc(col("gid")), asc(col("a")), asc(col("b"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 29b wild combinations complete ===")
