# Round 54: Final creative hunt — Writer options, streaming edge cases,
# concurrent operations, rare Column methods, DataFrame.to_df,
# session configuration, SparkEx.range
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
# 1. SparkEx.range
# =============================================

run.("1a. SparkEx.range basic", fn ->
  SparkEx.range(s, 10) |> DataFrame.collect()
end)

run.("1b. SparkEx.range with start/end/step", fn ->
  SparkEx.range(s, 0, 100, 10) |> DataFrame.collect()
end)

# =============================================
# 2. DataFrame.to_df (rename columns)
# =============================================

run.("2. DataFrame.to_df rename", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2}], schema: "a INT, b INT")
  DataFrame.to_df(df, ["x", "y"]) |> DataFrame.collect()
end)

# =============================================
# 3. DataFrame.distinct / drop_duplicates
# =============================================

run.("3a. distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"}, %{"id" => 1, "val" => "b"}
  ], schema: "id INT, val STRING")
  DataFrame.distinct(df) |> DataFrame.sort([asc(col("id")), asc(col("val"))]) |> DataFrame.collect()
end)

run.("3b. drop_duplicates with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 1, "val" => "b"},
    %{"id" => 2, "val" => "a"}, %{"id" => 2, "val" => "c"}
  ], schema: "id INT, val STRING")
  DataFrame.drop_duplicates(df, ["id"]) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
end)

# =============================================
# 4. DataFrame.repartition / coalesce
# =============================================

run.("4a. repartition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  before = DataFrame.rdd_num_partitions_approx(df)
  repartitioned = DataFrame.repartition(df, 5)
  after_ = DataFrame.rdd_num_partitions_approx(repartitioned)
  %{before: before, after: after_}
end)

run.("4b. coalesce", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  repartitioned = DataFrame.repartition(df, 10)
  before = DataFrame.rdd_num_partitions_approx(repartitioned)
  coalesced = DataFrame.coalesce(repartitioned, 2)
  after_ = DataFrame.rdd_num_partitions_approx(coalesced)
  %{before: before, after: after_}
end)

# =============================================
# 5. SparkEx configuration
# =============================================

run.("5a. SparkEx.conf_get", fn ->
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
end)

run.("5b. SparkEx.conf_set + conf_get", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "50"}])
  result = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  # Reset
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "1000"}])
  result
end)

run.("5c. SparkEx.conf_get_all", fn ->
  result = SparkEx.config_get_all(s)
  # Just show count and sample
  case result do
    {:ok, configs} -> %{count: length(configs), sample: Enum.take(configs, 5)}
    other -> other
  end
end)

# =============================================
# 6. Column.drop_fields on struct
# =============================================

run.("6a. Column.drop_fields", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS s")
  df |> DataFrame.select([
    col("s"),
    Column.drop_fields(col("s"), ["b"]) |> Column.alias_("without_b")
  ]) |> DataFrame.collect()
end)

run.("6b. Column.with_field", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  df |> DataFrame.select([
    col("person"),
    Column.with_field(col("person"), "email", lit("alice@test.com"))
    |> Column.alias_("with_email")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Column.try_cast (safe casting)
# =============================================

run.("7. try_cast various types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "42"}, %{"val" => "hello"}, %{"val" => "3.14"}, %{"val" => nil}
  ], schema: "val STRING")

  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "INT") |> Column.alias_("as_int"),
    Column.try_cast(col("val"), "DOUBLE") |> Column.alias_("as_double")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Multiple writers from same DataFrame
# =============================================

run.("8. Write same DataFrame to two tables", fn ->
  t1 = "sfx.sfx_dev_warehouse.spark_ex_r54_a_#{run_id}"
  t2 = "sfx.sfx_dev_warehouse.spark_ex_r54_b_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 100} end),
    schema: "id INT, val INT")

  # Write to both
  DataFrame.write_v2(df, t1) |> SparkEx.WriterV2.create()
  DataFrame.write_v2(df, t2)
    |> SparkEx.WriterV2.partitioned_by([col("id")])
    |> SparkEx.WriterV2.create()

  {:ok, rows1} = SparkEx.Reader.table(s, t1) |> DataFrame.collect()
  {:ok, rows2} = SparkEx.Reader.table(s, t2) |> DataFrame.collect()

  Catalog.drop_table(s, t1, if_exists: true, purge: true)
  Catalog.drop_table(s, t2, if_exists: true, purge: true)
  %{table1_count: length(rows1), table2_count: length(rows2)}
end)

# =============================================
# 9. Two concurrent streaming queries
# =============================================

run.("9. Two concurrent streaming queries", fn ->
  alias SparkEx.{StreamReader, StreamWriter, StreamingQuery}

  stream1 = StreamReader.rate(s, rows_per_second: 5)
  {:ok, q1} = stream1
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("concurrent_a_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  stream2 = StreamReader.rate(s, rows_per_second: 10)
  {:ok, q2} = stream2
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("concurrent_b_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)

  {:ok, r1} = SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM concurrent_a_#{run_id}")
    |> DataFrame.collect()
  {:ok, r2} = SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM concurrent_b_#{run_id}")
    |> DataFrame.collect()

  # List active
  active = SparkEx.StreamingQueryManager.list_active(s)

  StreamingQuery.stop(q1)
  StreamingQuery.stop(q2)

  %{query1_rows: hd(r1)["cnt"], query2_rows: hd(r2)["cnt"], active_queries: active}
end)

# =============================================
# 10. Column.over with different window specs
# =============================================

run.("10. Same function, different windows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"dept" => Enum.at(["A", "B"], rem(i, 2)),
        "val" => rem(i * 1337, 100)}
    end), schema: "dept STRING, val INT")

  w_global = SparkEx.Window.order_by([asc(col("val"))])
  w_dept = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([asc(col("val"))])

  df
  |> DataFrame.with_column("global_rank", row_number() |> Column.over(w_global))
  |> DataFrame.with_column("dept_rank", row_number() |> Column.over(w_dept))
  |> DataFrame.with_column("global_pct", percent_rank() |> Column.over(w_global))
  |> DataFrame.with_column("dept_pct", percent_rank() |> Column.over(w_dept))
  |> DataFrame.sort([asc(col("dept")), asc(col("val"))])
  |> DataFrame.limit(10)
  |> DataFrame.collect()
end)

# =============================================
# 11. Complex: multi-step ETL with all features combined
# =============================================

run.("11. Full ETL: range → transform → window → filter → pivot → write → read → agg", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r54_etl_#{run_id}"

  # Generate data
  {:ok, raw} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"customer_id" => rem(i, 20) + 1,
        "product" => Enum.at(["Widget", "Gadget", "Tool"], rem(i, 3)),
        "amount" => (rem(i * 997, 200) + 10) * 1.0,
        "month" => rem(i, 12) + 1}
    end), schema: "customer_id INT, product STRING, amount DOUBLE, month INT")

  # Transform
  w = SparkEx.Window.partition_by([col("customer_id")]) |> WindowSpec.order_by([asc(col("month"))])

  enriched = raw
    |> DataFrame.fillna(%{"amount" => 0.0})
    |> DataFrame.with_column("running_total", sum(col("amount")) |> Column.over(w))
    |> DataFrame.with_column("order_rank", row_number() |> Column.over(w))
    |> DataFrame.filter(Column.lte(col("order_rank"), lit(6)))

  # Write
  DataFrame.write_v2(enriched, table)
    |> SparkEx.WriterV2.partitioned_by([col("product")])
    |> SparkEx.WriterV2.create()

  # Read back and aggregate
  {:ok, agg} = SparkEx.Reader.table(s, table)
    |> DataFrame.group_by([col("product")])
    |> GroupedData.agg([
      count(col("customer_id")) |> Column.alias_("orders"),
      sum(col("amount")) |> Column.alias_("total_revenue"),
      avg(col("amount")) |> Column.alias_("avg_order"),
      count_distinct(col("customer_id")) |> Column.alias_("unique_customers")
    ])
    |> DataFrame.sort([desc(col("total_revenue"))])
    |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  agg
end)

# =============================================
# 12. Edge: DataFrame.cache / unpersist
# =============================================

run.("12. cache → count → unpersist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end), schema: "id INT")

  cached = DataFrame.cache(df)
  {:ok, count1} = DataFrame.count(cached)
  {:ok, level1} = DataFrame.storage_level(cached)

  DataFrame.unpersist(cached)
  {:ok, level2} = DataFrame.storage_level(cached)

  %{count: count1, cached_level: level1, unpersisted_level: level2}
end)

# =============================================
# 13. Edge: DataFrame.count vs show
# =============================================

run.("13a. DataFrame.count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..42, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.count(df)
end)

run.("13b. DataFrame.show (string repr)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

# =============================================
# 14. Edge: empty DataFrame operations
# =============================================

run.("14. Operations on empty DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, val STRING")
  {:ok, count} = DataFrame.count(df)
  {:ok, rows} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.collect()
  {:ok, cols} = DataFrame.columns(df)
  {:ok, dtypes} = DataFrame.dtypes(df)
  %{count: count, rows: rows, columns: cols, dtypes: dtypes}
end)

# =============================================
# 15. Complex: DataFrame.transform pipeline with error handling
# =============================================

run.("15. Transform pipeline with coalesce fallback", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "primary" => "A", "secondary" => nil, "tertiary" => "C"},
    %{"id" => 2, "primary" => nil, "secondary" => "B", "tertiary" => nil},
    %{"id" => 3, "primary" => nil, "secondary" => nil, "tertiary" => nil},
    %{"id" => 4, "primary" => "D", "secondary" => "E", "tertiary" => "F"}
  ], schema: "id INT, primary STRING, secondary STRING, tertiary STRING")

  df
  |> DataFrame.transform(fn d ->
    DataFrame.with_column(d, "best_value",
      coalesce([col("primary"), col("secondary"), col("tertiary"), lit("UNKNOWN")]))
  end)
  |> DataFrame.transform(fn d ->
    DataFrame.with_column(d, "null_count",
      Column.plus(
        Column.plus(
          Column.cast(Column.is_null(col("primary")), "INT"),
          Column.cast(Column.is_null(col("secondary")), "INT")),
        Column.cast(Column.is_null(col("tertiary")), "INT")))
  end)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 54 complete ===")
