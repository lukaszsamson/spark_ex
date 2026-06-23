# Round 31: Deep edge corners — TVFs, lateral joins, observe listeners, Avro/Protobuf/XML serialization,
# complex window frames, UDFs, table valued functions, approx_count_distinct with rsd, sample_by edge cases
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

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
# 1. TableValuedFunction: posexplode via TVF API
# =============================================

run.("1a. TVF posexplode", fn ->
  tvf = SparkEx.tvf(s)
  posed = SparkEx.TableValuedFunction.call(tvf, "posexplode", [
    array([lit(10), lit(20), lit(30)])
  ])
  posed |> DataFrame.collect()
end)

run.("1b. TVF posexplode_outer", fn ->
  tvf = SparkEx.tvf(s)
  posed = SparkEx.TableValuedFunction.call(tvf, "posexplode_outer", [
    array([lit(10), lit(20)])
  ])
  posed |> DataFrame.collect()
end)

run.("1c. TVF range", fn ->
  tvf = SparkEx.tvf(s)
  r = SparkEx.TableValuedFunction.call(tvf, "range", [lit(0), lit(10), lit(2)])
  r |> DataFrame.collect()
end)

# =============================================
# 2. Lateral join (DataFrame API)
# =============================================

run.("2a. lateral_join with explode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [10, 20, 30]},
    %{"id" => 2, "items" => [40, 50]}
  ], schema: "id INT, items ARRAY<INT>")

  # lateral_join: join with a generator function
  DataFrame.lateral_join(df, explode(Column.outer(col("items"))) |> Column.alias_("item"))
  |> DataFrame.collect()
end)

run.("2b. lateral_join with posexplode (SQL approach)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b"]},
    %{"id" => 2, "tags" => ["c"]}
  ], schema: "id INT, tags ARRAY<STRING>")
  DataFrame.create_or_replace_temp_view(df, "lateral_test_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT id, pos, tag
    FROM lateral_test_#{run_id}
    LATERAL VIEW posexplode(tags) AS pos, tag
  """) |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "lateral_test_#{run_id}")
  result
end)

# =============================================
# 3. Complex window frames: range_between
# =============================================

run.("3a. range_between window", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"ts" => i * 100, "val" => i} end),
    schema: "ts INT, val INT")
  w = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("ts"))])
    |> WindowSpec.range_between(-200, 0)
  df |> DataFrame.select([
    col("ts"), col("val"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("range_sum")
  ]) |> DataFrame.collect()
end)

run.("3b. rows_between with unbounded preceding/following", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"val" => i * 10} end),
    schema: "val INT")
  w_all = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("val"))])
    |> WindowSpec.rows_between(-999999999, 999999999)
  w_preceding = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("val"))])
    |> WindowSpec.rows_between(-999999999, 0)
  df |> DataFrame.select([
    col("val"),
    sum(col("val")) |> Column.over(w_all) |> Column.alias_("total"),
    sum(col("val")) |> Column.over(w_preceding) |> Column.alias_("running_sum")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. approx_count_distinct with rsd parameter
# =============================================

run.("4. approx_count_distinct with rsd", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10000, fn i -> %{"val" => rem(i, 500)} end),
    schema: "val INT")
  DataFrame.select(df, [
    approx_count_distinct(col("val")) |> Column.alias_("default_rsd"),
    approx_count_distinct(col("val"), 0.01) |> Column.alias_("precise")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Serialization: to_json/from_json with options
# =============================================

run.("5a. to_json with options", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('name', 'Alice', 'score', 95.5, 'active', true) AS data
  """)
  df |> DataFrame.select([
    to_json(col("data")) |> Column.alias_("json_default"),
    to_json(col("data"), %{"pretty" => "true"}) |> Column.alias_("json_pretty")
  ]) |> DataFrame.collect()
end)

run.("5b. to_csv / from_csv roundtrip", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 'hello', 'c', 3.14) AS data")
  csv_df = df |> DataFrame.select([to_csv(col("data")) |> Column.alias_("csv_str")])
  csv_df |> DataFrame.select([
    col("csv_str"),
    from_csv(col("csv_str"), "a INT, b STRING, c DOUBLE") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Complex expression: CASE WHEN with aggregates in subquery
# =============================================

run.("6. complex CASE WHEN with percentiles", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")
  DataFrame.create_or_replace_temp_view(df, "perc_test_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT val,
      CASE
        WHEN val <= (SELECT percentile_approx(val, 0.25) FROM perc_test_#{run_id}) THEN 'Q1'
        WHEN val <= (SELECT percentile_approx(val, 0.5) FROM perc_test_#{run_id}) THEN 'Q2'
        WHEN val <= (SELECT percentile_approx(val, 0.75) FROM perc_test_#{run_id}) THEN 'Q3'
        ELSE 'Q4'
      END AS quartile
    FROM perc_test_#{run_id}
    ORDER BY val
  """)
  # Just get counts per quartile
  result
  |> DataFrame.group_by([col("quartile")])
  |> GroupedData.agg([count(col("val")) |> Column.alias_("cnt")])
  |> DataFrame.sort([asc(col("quartile"))])
  |> DataFrame.collect()
  |> (fn {:ok, rows} ->
    DataFrame.drop_temp_view(s, "perc_test_#{run_id}")
    rows
  end).()
end)

# =============================================
# 7. DataFrame.observe with listener
# =============================================

run.("7. observe + get_observation_listeners", fn ->
  listeners = {:not_supported, "get_observation_listeners removed from public API"}
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i} end), schema: "val INT")
  # observe attaches metrics collection, result should still be the data
  {:ok, result} = df |> DataFrame.observe("test_obs_#{run_id}", [
    count(col("val")) |> Column.alias_("cnt"),
    avg(col("val")) |> Column.alias_("avg_val")
  ]) |> DataFrame.collect()
  {length(result), listeners}
end)

# =============================================
# 8. DataFrame.hint (various hints)
# =============================================

run.("8a. broadcast hint", fn ->
  {:ok, big} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  {:ok, small} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "label" => "one"}, %{"id" => 50, "label" => "fifty"}
  ], schema: "id INT, label STRING")

  DataFrame.join(big, DataFrame.hint(small, "broadcast"), col("id"), "inner")
  |> DataFrame.collect()
end)

run.("8b. coalesce hint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.hint("coalesce", [lit(1)]) |> DataFrame.count()
end)

# =============================================
# 9. Complex GroupedData: grouping sets via SQL
# =============================================

run.("9. grouping sets via SQL", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"region" => "US", "product" => "A", "sales" => 100},
    %{"region" => "US", "product" => "B", "sales" => 200},
    %{"region" => "EU", "product" => "A", "sales" => 150},
    %{"region" => "EU", "product" => "B", "sales" => 250}
  ], schema: "region STRING, product STRING, sales INT")
  DataFrame.create_or_replace_temp_view(df, "gstest_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT region, product, SUM(sales) AS total
    FROM gstest_#{run_id}
    GROUP BY GROUPING SETS ((region, product), (region), (product), ())
    ORDER BY region NULLS LAST, product NULLS LAST
  """) |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "gstest_#{run_id}")
  result
end)

# =============================================
# 10. Column.substr (3-arg)
# =============================================

run.("10. Column.substr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "Hello, World!"}], schema: "txt STRING")
  df |> DataFrame.select([
    Column.substr(col("txt"), lit(1), lit(5)) |> Column.alias_("first5"),
    Column.substr(col("txt"), lit(8), lit(5)) |> Column.alias_("world")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Complex data: nested struct + array + map combined
# =============================================

run.("11. deeply nested complex type construction via SQL", fn ->
  SparkEx.sql(s, """
    SELECT
      named_struct(
        'user', named_struct('name', 'Alice', 'age', 30),
        'orders', array(
          named_struct('id', 1, 'items', map('widget', 5, 'gadget', 2)),
          named_struct('id', 2, 'items', map('doohickey', 1))
        ),
        'metadata', map('region', 'US', 'tier', 'gold')
      ) AS record
  """)
  |> DataFrame.select([
    col("record.user.name") |> Column.alias_("name"),
    col("record.user.age") |> Column.alias_("age"),
    size(col("record.orders")) |> Column.alias_("order_count"),
    map_keys(col("record.metadata")) |> Column.alias_("meta_keys")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 12. Catalog: table_exists?, database_exists?, is_cached?
# =============================================

run.("12a. table_exists? / database_exists?", fn ->
  te = Catalog.table_exists?(s, "sfx.sfx_dev_warehouse.nonexistent_table_xyz")
  de = Catalog.database_exists?(s, "sfx_dev_warehouse")
  {te, de}
end)

run.("12b. cache + is_cached? + uncache", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.create_or_replace_temp_view(df, "cache_test_#{run_id}")

  before = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.cache_table(s, "cache_test_#{run_id}")
  during = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.uncache_table(s, "cache_test_#{run_id}")
  after_ = Catalog.is_cached?(s, "cache_test_#{run_id}")
  DataFrame.drop_temp_view(s, "cache_test_#{run_id}")
  %{before: before, during: during, after: after_}
end)

# =============================================
# 13. select_expr (SQL expressions in select)
# =============================================

run.("13. select_expr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "score" => 85},
    %{"name" => "Bob", "score" => 92}
  ], schema: "name STRING, score INT")
  df |> DataFrame.select_expr(["name", "score * 1.1 AS adjusted_score", "UPPER(name) AS upper_name"])
  |> DataFrame.collect()
end)

# =============================================
# 14. Multiple DataFrame operations chained in complex pipeline
# =============================================

run.("14. complex ETL pipeline", fn ->
  # Generate source data
  {:ok, transactions} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{
        "customer_id" => rem(i, 20) + 1,
        "amount" => rem(i * 7, 100) + 1,
        "category" => Enum.at(["food", "tech", "clothing", "other"], rem(i, 4))
      }
    end), schema: "customer_id INT, amount INT, category STRING")

  # Step 1: categorize amounts
  enriched = transactions
  |> DataFrame.with_column("size",
    when_(Column.gt(col("amount"), lit(70)), lit("large"))
    |> Column.otherwise(
      when_(Column.gt(col("amount"), lit(30)), lit("medium"))
      |> Column.otherwise(lit("small"))))

  # Step 2: aggregate by customer and category
  agg = enriched
  |> DataFrame.group_by([col("customer_id"), col("category")])
  |> GroupedData.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(col("amount")) |> Column.alias_("txn_count"),
    avg(col("amount")) |> Column.alias_("avg_amount")
  ])

  # Step 3: window: rank customers by total within each category
  w = SparkEx.Window.partition_by([col("category")]) |> WindowSpec.order_by([desc(col("total"))])

  result = agg
  |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("rank"), lit(3)))
  |> DataFrame.sort([asc(col("category")), asc(col("rank"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  IO.puts("  Top 3 customers per category: #{length(rows)} rows")
  rows
end)

# =============================================
# 15. Empty operations
# =============================================

run.("15a. group_by on empty DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, val INT")
  DataFrame.group_by(df, [col("id")])
  |> GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

run.("15b. union of empty with non-empty", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT")
  {:ok, full} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  DataFrame.union(empty, full) |> DataFrame.collect()
end)

run.("15c. join with empty DataFrame", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "a"}], schema: "id INT, val STRING")
  {:ok, right} = SparkEx.create_dataframe(s, [], schema: "id INT, label STRING")
  DataFrame.join(left, right, col("id"), "left") |> DataFrame.collect()
end)

# =============================================
# 16. coalesce across many nullable columns
# =============================================

run.("16. coalesce across 5 nullable columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil, "c" => nil, "d" => nil, "e" => 99},
    %{"a" => nil, "b" => 42, "c" => nil, "d" => nil, "e" => nil},
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5}
  ], schema: "a INT, b INT, c INT, d INT, e INT")
  df |> DataFrame.select([
    coalesce([col("a"), col("b"), col("c"), col("d"), col("e")]) |> Column.alias_("first_nonnull")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. DataFrame.to (alias for Writer)
# =============================================

run.("17. DataFrame.to (type coercion)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.to("val LONG") |> DataFrame.schema()
end)

# =============================================
# 18. Complex expression: map_from_arrays from computed columns
# =============================================

run.("18. map_from_arrays from computed values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")
  df |> DataFrame.select([
    map_from_arrays(
      array([lit("square"), lit("cube"), lit("double")]),
      array([Column.multiply(col("x"), col("x")),
             Column.multiply(Column.multiply(col("x"), col("x")), col("x")),
             Column.multiply(col("x"), lit(2))])
    ) |> Column.alias_("computations")
  ]) |> DataFrame.collect()
end)

# =============================================
# 19. DataFrame.describe / summary
# =============================================

run.("19a. describe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0, "cat" => Enum.at(["A", "B", "C"], rem(i, 3))} end),
    schema: "val DOUBLE, cat STRING")
  DataFrame.describe(df) |> DataFrame.collect()
end)

run.("19b. summary with custom stats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.summary(df, ["count", "min", "25%", "50%", "75%", "max"]) |> DataFrame.collect()
end)

IO.puts("=== Round 31 complete ===")
