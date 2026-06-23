# Round 42b (safe): Re-run catalog cache tests with correct API, test remaining areas
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, StreamReader, StreamWriter, StreamingQuery, Catalog}
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

# NOTE: cache_table with storage_level: "MEMORY_ONLY" CRASHES SESSION (EX-111)
# The storage_level expects a StorageLevel struct, not a string.

# =============================================
# 1. Catalog cache lifecycle (without storage_level string)
# =============================================

run.("1. Catalog cache lifecycle", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "cache_test_#{run_id}")

  r1 = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.cache_table(s, "cache_test_#{run_id}")
  r2 = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.uncache_table(s, "cache_test_#{run_id}")
  r3 = Catalog.is_cached?(s, "cache_test_#{run_id}")

  DataFrame.drop_temp_view(df, "cache_test_#{run_id}")
  %{before_cache: r1, after_cache: r2, after_uncache: r3}
end)

# =============================================
# 2. Catalog.refresh_table on temp view
# =============================================

run.("2. Catalog.refresh_table", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "refresh_test_#{run_id}")
  result = Catalog.refresh_table(s, "refresh_test_#{run_id}")
  DataFrame.drop_temp_view(df, "refresh_test_#{run_id}")
  result
end)

# =============================================
# 3. Catalog.recover_partitions (expect error for non-partitioned)
# =============================================

run.("3. Catalog.recover_partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "recover_test_#{run_id}")
  result = Catalog.recover_partitions(s, "recover_test_#{run_id}")
  DataFrame.drop_temp_view(df, "recover_test_#{run_id}")
  result
end)

# =============================================
# 4. WriterV2 builder patterns (without executing writes)
# =============================================

run.("4a. WriterV2 table_property + cluster_by (struct build)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "a"}], schema: "id INT, val STRING")
  writer = SparkEx.DataFrame.write_v2(df, "test_table_#{run_id}")
    |> SparkEx.WriterV2.table_property("write.format.default", "parquet")
    |> SparkEx.WriterV2.cluster_by(["id"])
  %{type: writer.__struct__, table: writer.table_name}
end)

run.("4b. WriterV2 table_properties (map)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  writer = SparkEx.DataFrame.write_v2(df, "test_table2_#{run_id}")
    |> SparkEx.WriterV2.table_properties(%{"key1" => "val1", "key2" => "val2"})
  %{type: writer.__struct__, props: writer.table_properties}
end)

# =============================================
# 5. DataFrame.checkpoint / local_checkpoint (may require SparkContext)
# =============================================

run.("5a. DataFrame.checkpoint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.checkpoint(df)
end)

run.("5b. DataFrame.local_checkpoint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.local_checkpoint(df)
end)

# =============================================
# 6. Complex: streaming with foreach_batch (if available)
# =============================================

run.("6. StreamWriter.foreach_batch struct inspection", fn ->
  # Just check if the API accepts the foreach_batch function struct
  stream = StreamReader.rate(s, rows_per_second: 5)
  writer = stream |> DataFrame.write_stream()
  %{type: writer.__struct__, fields: Map.keys(writer)}
end)

# =============================================
# 7. Column.isin with list
# =============================================

run.("7. Column.isin with list of values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.isin(col("val"), [2, 4, 6, 8]))
    |> DataFrame.sort([asc(col("val"))])
    |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.describe / summary
# =============================================

run.("8a. describe with specific stats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.5, "grp" => rem(i, 3)} end),
    schema: "val DOUBLE, grp INT")
  DataFrame.describe(df, ["count", "mean", "stddev", "min", "max"]) |> DataFrame.collect()
end)

run.("8b. summary with specific percentiles", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end), schema: "val INT")
  DataFrame.summary(df, ["count", "25%", "50%", "75%"]) |> DataFrame.collect()
end)

# =============================================
# 9. Complex: multi-window with nth_value + lag + lead
# =============================================

run.("9. multi-window pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"dept" => Enum.at(["A", "B"], rem(i, 2)), "salary" => 30000 + i * 1000}
    end), schema: "dept STRING, salary INT")

  w = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([asc(col("salary"))])

  df |> DataFrame.select([
    col("dept"), col("salary"),
    lag(col("salary"), offset: 1) |> Column.over(w) |> Column.alias_("prev_salary"),
    lead(col("salary"), offset: 1) |> Column.over(w) |> Column.alias_("next_salary"),
    nth_value(col("salary"), 3, false) |> Column.over(w) |> Column.alias_("third_salary"),
    row_number() |> Column.over(w) |> Column.alias_("rank")
  ]) |> DataFrame.sort([asc(col("dept")), asc(col("salary"))]) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: try_* with date arithmetic
# =============================================

run.("10. try_add with date/interval", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dt" => "2024-02-28"}], schema: "dt STRING")
  # Use SQL for interval since make_interval not supported on Spark 3.5
  SparkEx.sql(s, """
    SELECT
      DATE '2024-02-28' AS dt,
      try_add(DATE '2024-02-28', INTERVAL 1 DAY) AS next_day,
      try_add(DATE '2024-02-28', INTERVAL 1 MONTH) AS next_month
  """) |> DataFrame.collect()
end)

# =============================================
# 11. Complex: cross_join + window ranking
# =============================================

run.("11. cross_join product with ranking", fn ->
  {:ok, products} = SparkEx.create_dataframe(s, [
    %{"prod" => "A", "price" => 10.0},
    %{"prod" => "B", "price" => 20.0},
    %{"prod" => "C", "price" => 15.0}
  ], schema: "prod STRING, price DOUBLE")

  {:ok, qtys} = SparkEx.create_dataframe(s, [
    %{"qty" => 1}, %{"qty" => 5}, %{"qty" => 10}
  ], schema: "qty INT")

  DataFrame.cross_join(products, qtys)
  |> DataFrame.with_column("total", Column.multiply(col("price"), Column.cast(col("qty"), "DOUBLE")))
  |> DataFrame.transform(fn d ->
    w = SparkEx.Window.order_by([desc(col("total"))])
    d |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  end)
  |> DataFrame.filter(Column.lte(col("rank"), lit(5)))
  |> DataFrame.sort([asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 12. Catalog.clear_cache + cache lifecycle verification
# =============================================

run.("12. clear_cache clears all cached tables", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")
  DataFrame.create_temp_view(df1, "cc1_#{run_id}")
  DataFrame.create_temp_view(df2, "cc2_#{run_id}")

  Catalog.cache_table(s, "cc1_#{run_id}")
  Catalog.cache_table(s, "cc2_#{run_id}")
  c1_before = Catalog.is_cached?(s, "cc1_#{run_id}")
  c2_before = Catalog.is_cached?(s, "cc2_#{run_id}")

  Catalog.clear_cache(s)
  c1_after = Catalog.is_cached?(s, "cc1_#{run_id}")
  c2_after = Catalog.is_cached?(s, "cc2_#{run_id}")

  DataFrame.drop_temp_view(df1, "cc1_#{run_id}")
  DataFrame.drop_temp_view(df2, "cc2_#{run_id}")

  %{cc1_before: c1_before, cc2_before: c2_before, cc1_after: c1_after, cc2_after: c2_after}
end)

# =============================================
# 13. Catalog.create_table (managed table in default catalog)
# =============================================

run.("13. Catalog.create_table", fn ->
  Catalog.create_table(s, "test_ct_#{run_id}", schema: "id INT, name STRING")
end)

# =============================================
# 14. Streaming: await_termination with timeout
# =============================================

run.("14. StreamingQuery.await_termination with timeout", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("await_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  result = StreamingQuery.await_termination(query, 2000)
  StreamingQuery.stop(query)
  result
end)

# =============================================
# 15. Streaming: recent_progress / last_progress
# =============================================

run.("15. StreamingQuery progress inspection", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("prog_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)
  recent = StreamingQuery.recent_progress(query)
  last = StreamingQuery.last_progress(query)
  StreamingQuery.stop(query)
  %{recent_count: (case recent do {:ok, list} -> length(list); other -> other end),
    last: last}
end)

IO.puts("=== Round 42b complete ===")
