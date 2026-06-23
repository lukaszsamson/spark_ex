# Round 42: Untested APIs — Catalog cache, StreamReader file sources, try_* arithmetic,
# make_interval, nth_value, DataFrame.offset/cross_join/with_watermark, WriterV2 cluster_by
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

# =============================================
# 1. try_add / try_subtract / try_multiply / try_divide
# =============================================

run.("1a. try_add (normal)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 10, "b" => 20}], schema: "a INT, b INT")
  df |> DataFrame.select([
    try_add(col("a"), col("b")) |> Column.alias_("sum")
  ]) |> DataFrame.collect()
end)

run.("1b. try_add with overflow (should return null, not crash)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2_147_483_647, "b" => 1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    try_add(col("a"), col("b")) |> Column.alias_("overflow_sum")
  ]) |> DataFrame.collect()
end)

run.("1c. try_subtract", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 10, "b" => 20}], schema: "a INT, b INT")
  df |> DataFrame.select([
    try_subtract(col("a"), col("b")) |> Column.alias_("diff")
  ]) |> DataFrame.collect()
end)

run.("1d. try_multiply with overflow", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2_147_483_647, "b" => 2}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    try_multiply(col("a"), col("b")) |> Column.alias_("overflow_prod")
  ]) |> DataFrame.collect()
end)

run.("1e. try_divide (normal and by zero)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 3}, %{"a" => 10, "b" => 0}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    try_divide(col("a"), col("b")) |> Column.alias_("div_result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. make_interval / try_make_interval
# =============================================

run.("2a. make_interval (days + hours)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    make_interval(days: lit(5), hours: lit(3)) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

run.("2b. try_make_interval (should not crash on overflow)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    try_make_interval(years: lit(999999999)) |> Column.alias_("big_interval")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. nth_value
# =============================================

run.("3. nth_value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"grp" => rem(i, 2), "val" => i * 10} end),
    schema: "grp INT, val INT")
  w = SparkEx.Window.partition_by([col("grp")]) |> WindowSpec.order_by([asc(col("val"))])
  df |> DataFrame.select([
    col("grp"), col("val"),
    nth_value(col("val"), 2, false) |> Column.over(w) |> Column.alias_("second_val")
  ]) |> DataFrame.sort([asc(col("grp")), asc(col("val"))]) |> DataFrame.collect()
end)

# =============================================
# 4. lag / lead with defaults
# =============================================

run.("4a. lag with default", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 100} end),
    schema: "id INT, val INT")
  w = SparkEx.Window.order_by([asc(col("id"))])
  df |> DataFrame.select([
    col("id"), col("val"),
    lag(col("val"), offset: 1, default: -1) |> Column.over(w) |> Column.alias_("prev_val")
  ]) |> DataFrame.collect()
end)

run.("4b. lead with default and offset 2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 100} end),
    schema: "id INT, val INT")
  w = SparkEx.Window.order_by([asc(col("id"))])
  df |> DataFrame.select([
    col("id"), col("val"),
    lead(col("val"), offset: 2, default: 0) |> Column.over(w) |> Column.alias_("next2_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. raise_error
# =============================================

run.("5. raise_error (should produce SPARK error)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    raise_error(lit("intentional test error")) |> Column.alias_("err")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. assert_true with error class (3-arg form)
# =============================================

run.("6a. assert_true passing", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 5}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    assert_true(Column.gt(col("val"), lit(0)), "val must be positive") |> Column.alias_("check")
  ]) |> DataFrame.collect()
end)

run.("6b. assert_true failing", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -1}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    assert_true(Column.gt(col("val"), lit(0)), "val must be positive") |> Column.alias_("check")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.offset
# =============================================

run.("7. DataFrame.offset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.offset(5) |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.cross_join
# =============================================

run.("8. cross_join", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"a" => 1}, %{"a" => 2}], schema: "a INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"b" => "x"}, %{"b" => "y"}], schema: "b STRING")
  DataFrame.cross_join(df1, df2) |> DataFrame.collect()
end)

# =============================================
# 9. Catalog cache operations
# =============================================

run.("9a. Catalog.clear_cache", fn ->
  Catalog.clear_cache(s)
end)

run.("9b. Catalog.is_cached? on temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "cache_test_#{run_id}")
  r1 = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.cache_table(s, "cache_test_#{run_id}")
  r2 = Catalog.is_cached?(s, "cache_test_#{run_id}")
  Catalog.uncache_table(s, "cache_test_#{run_id}")
  r3 = Catalog.is_cached?(s, "cache_test_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "cache_test_#{run_id}")
  %{before_cache: r1, after_cache: r2, after_uncache: r3}
end)

# =============================================
# 10. Catalog.refresh_table / recover_partitions
# =============================================

run.("10a. Catalog.refresh_table on temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "refresh_test_#{run_id}")
  result = Catalog.refresh_table(s, "refresh_test_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "refresh_test_#{run_id}")
  result
end)

run.("10b. Catalog.recover_partitions (expect error for non-partitioned)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "recover_test_#{run_id}")
  result = Catalog.recover_partitions(s, "recover_test_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "recover_test_#{run_id}")
  result
end)

# =============================================
# 11. DataFrame.with_watermark (streaming context)
# =============================================

run.("11. with_watermark on streaming DataFrame", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.with_watermark("timestamp", "10 seconds")
    |> DataFrame.select([col("timestamp"), col("value")])

  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("wm_test_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(query)
  StreamingQuery.stop(query)
  %{active: active}
end)

# =============================================
# 12. StreamWriter.partition_by
# =============================================

run.("12. StreamWriter.partition_by", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.with_column("part", Column.mod(col("value"), lit(3)))

  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("part_test_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.partition_by(["part"])
    |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(query)
  StreamingQuery.stop(query)
  %{active: active}
end)

# =============================================
# 13. StreamWriter.trigger variations
# =============================================

run.("13a. trigger processing_time", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("trig_pt_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.trigger(processing_time: "5 seconds")
    |> StreamWriter.start()

  Process.sleep(6000)
  {:ok, result} = SparkEx.Reader.table(s, "trig_pt_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)
  %{count: length(result)}
end)

run.("13b. trigger once", fn ->
  stream = StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("trig_once_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.trigger(once: true)
    |> StreamWriter.start()

  Process.sleep(5000)
  active = StreamingQuery.is_active?(query)
  if active == {:ok, true}, do: StreamingQuery.stop(query)
  %{active_after_wait: active}
end)

# =============================================
# 14. DataFrame.same_semantics / semantic_hash
# =============================================

run.("14a. same_semantics (identical)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"a" => 1}], schema: "a INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"a" => 1}], schema: "a INT")
  # Two separately created DataFrames — may or may not be "same semantics"
  DataFrame.same_semantics(df1, df2)
end)

run.("14b. same_semantics (different transforms)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2}], schema: "a INT, b INT")
  df1 = df |> DataFrame.select([col("a")])
  df2 = df |> DataFrame.select([col("b")])
  DataFrame.same_semantics(df1, df2)
end)

run.("14c. semantic_hash", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1}], schema: "a INT")
  h1 = DataFrame.semantic_hash(df)
  h2 = DataFrame.semantic_hash(df |> DataFrame.filter(Column.gt(col("a"), lit(0))))
  %{hash1: h1, hash2: h2}
end)

# =============================================
# 15. Column.try_cast
# =============================================

run.("15a. try_cast valid", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "123"}], schema: "val STRING")
  df |> DataFrame.select([
    Column.try_cast(col("val"), "INT") |> Column.alias_("as_int")
  ]) |> DataFrame.collect()
end)

run.("15b. try_cast invalid (should return null, not error)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "abc"}, %{"val" => "123"}, %{"val" => "45.6"}
  ], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "INT") |> Column.alias_("as_int")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Column.with_field / drop_fields on structs
# =============================================

run.("16a. with_field (add field to struct)", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  df |> DataFrame.select([
    Column.with_field(col("person"), "title", lit("Dr.")) |> Column.alias_("enriched")
  ]) |> DataFrame.collect()
end)

run.("16b. drop_fields (remove field from struct)", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30, 'city', 'NYC') AS person")
  df |> DataFrame.select([
    Column.drop_fields(col("person"), ["city"]) |> Column.alias_("trimmed")
  ]) |> DataFrame.collect()
end)

run.("16c. with_field then drop_fields chain", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS data")
  df |> DataFrame.select([
    col("data")
    |> Column.with_field("d", lit(4))
    |> Column.drop_fields(["b", "c"])
    |> Column.alias_("modified")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. StreamReader with file sources
# =============================================

run.("17a. StreamReader.json (S3 path — just test builder, don't start)", fn ->
  reader = StreamReader.json(s, "s3://nonexistent-bucket/path/", schema: "id INT, val STRING")
  # Just verify the struct is built correctly
  %{type: reader.__struct__, is_df: is_struct(reader, SparkEx.DataFrame)}
end)

run.("17b. StreamReader.csv builder", fn ->
  reader = StreamReader.csv(s, "s3://nonexistent-bucket/csv/",
    schema: "name STRING, age INT",
    option: [{"header", "true"}, {"delimiter", ","}])
  %{type: reader.__struct__, is_df: is_struct(reader, SparkEx.DataFrame)}
end)

# =============================================
# 18. Complex: streaming with watermark + windowed aggregation
# =============================================

run.("18. streaming with watermark + window aggregation", fn ->
  stream = StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.with_watermark("timestamp", "5 seconds")
    |> DataFrame.group_by([window(col("timestamp"), "5 seconds")])
    |> GroupedData.agg([
      count(col("value")) |> Column.alias_("cnt"),
      sum(col("value")) |> Column.alias_("total")
    ])

  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("wm_agg_#{run_id}")
    |> StreamWriter.output_mode("update")
    |> StreamWriter.start()

  Process.sleep(6000)
  {:ok, result} = SparkEx.Reader.table(s, "wm_agg_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)
  %{count: length(result), sample: Enum.take(result, 2)}
end)

# =============================================
# 19. Complex: try_* arithmetic pipeline
# =============================================

run.("19. try_* arithmetic pipeline with overflow detection", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2_147_483_647, "b" => 1},
    %{"a" => 100, "b" => 200},
    %{"a" => -2_147_483_648, "b" => -1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    try_add(col("a"), col("b")) |> Column.alias_("try_sum"),
    try_subtract(col("a"), col("b")) |> Column.alias_("try_diff"),
    try_multiply(col("a"), col("b")) |> Column.alias_("try_prod"),
    try_divide(col("a"), col("b")) |> Column.alias_("try_div"),
    # Regular ops for comparison
    Column.plus(col("a"), col("b")) |> Column.alias_("regular_sum")
  ]) |> DataFrame.collect()
end)

# =============================================
# 20. Catalog.refresh_by_path
# =============================================

run.("20. Catalog.refresh_by_path (expect error for non-file table)", fn ->
  Catalog.refresh_by_path(s, "s3://nonexistent-bucket/fake-path/")
end)

# =============================================
# 21. Complex: cross_join with filter (Cartesian product)
# =============================================

run.("21. cross_join cartesian product with post-filter", fn ->
  {:ok, left} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  {:ok, right} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"y" => i} end), schema: "y INT")
  DataFrame.cross_join(left, right)
  |> DataFrame.filter(Column.gt(col("x"), col("y")))
  |> DataFrame.sort([asc(col("x")), asc(col("y"))])
  |> DataFrame.collect()
end)

# =============================================
# 22. Catalog.cache_table with storage_level
# =============================================

run.("22. cache_table with storage_level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_temp_view(df, "cache_sl_#{run_id}")
  result = Catalog.cache_table(s, "cache_sl_#{run_id}", storage_level: "MEMORY_ONLY")
  Catalog.uncache_table(s, "cache_sl_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "cache_sl_#{run_id}")
  result
end)

IO.puts("=== Round 42 complete ===")
