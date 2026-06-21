# Round 26: Streaming queries, GroupedData edge cases, Column ops, session utilities
alias SparkEx.{DataFrame, Column, StreamWriter}
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
# 1. Streaming: create rate source, write to memory, query
# =============================================

run.("1a. StreamReader.rate + write to memory sink", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)

  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_test_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(2000)
  query
end)

run.("1b. StreamingQuery.id", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_id_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(1000)
  id = SparkEx.StreamingQuery.id(query)
  run_id_val = SparkEx.StreamingQuery.run_id(query)
  name = SparkEx.StreamingQuery.name(query)
  SparkEx.StreamingQuery.stop(query)
  {id, run_id_val, name}
end)

run.("1c. StreamingQuery.is_active? and status", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_status_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(1000)
  active = SparkEx.StreamingQuery.is_active?(query)
  status = SparkEx.StreamingQuery.status(query)
  SparkEx.StreamingQuery.stop(query)
  active_after = SparkEx.StreamingQuery.is_active?(query)
  {active, status, active_after}
end)

run.("1d. StreamingQuery.recent_progress and last_progress", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_progress_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(3000)
  recent = SparkEx.StreamingQuery.recent_progress(query)
  last = SparkEx.StreamingQuery.last_progress(query)
  SparkEx.StreamingQuery.stop(query)
  {recent, last}
end)

run.("1e. StreamingQuery.explain", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_explain_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(1000)
  result = SparkEx.StreamingQuery.explain(query)
  SparkEx.StreamingQuery.stop(query)
  result
end)

run.("1f. StreamingQuery.exception (no error — should be nil)", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_exception_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(1000)
  result = SparkEx.StreamingQuery.exception(query)
  SparkEx.StreamingQuery.stop(query)
  result
end)

run.("1g. StreamingQuery.process_all_available", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_paa_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(1000)
  result = SparkEx.StreamingQuery.process_all_available(query)
  SparkEx.StreamingQuery.stop(query)
  result
end)

# =============================================
# 2. StreamWriter with trigger options
# =============================================

run.("2a. StreamWriter with trigger processing_time", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_trigger_pt_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.trigger(processing_time: "5 seconds")
  |> StreamWriter.start()

  Process.sleep(2000)
  SparkEx.StreamingQuery.stop(query)
  :ok
end)

run.("2b. StreamWriter with trigger once", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_trigger_once_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.trigger(once: true)
  |> StreamWriter.start()

  Process.sleep(3000)
  active = SparkEx.StreamingQuery.is_active?(query)
  SparkEx.StreamingQuery.stop(query)
  {:trigger_once_active_after_3s, active}
end)

run.("2c. StreamWriter with trigger available_now", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("rate_trigger_avail_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.trigger(available_now: true)
  |> StreamWriter.start()

  Process.sleep(3000)
  active = SparkEx.StreamingQuery.is_active?(query)
  SparkEx.StreamingQuery.stop(query)
  {:trigger_available_now_active_after_3s, active}
end)

# =============================================
# 3. GroupedData edge cases
# =============================================

run.("3a. GroupedData.agg with multiple aggregations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "salary" => 100, "bonus" => 10},
    %{"dept" => "Eng", "salary" => 120, "bonus" => 15},
    %{"dept" => "HR", "salary" => 90, "bonus" => 5}
  ], schema: "dept STRING, salary INT, bonus INT")

  DataFrame.group_by(df, [col("dept")])
  |> SparkEx.GroupedData.agg([
    sum(col("salary")) |> Column.alias_("total_salary"),
    avg(col("salary")) |> Column.alias_("avg_salary"),
    max(col("bonus")) |> Column.alias_("max_bonus"),
    count(col("dept")) |> Column.alias_("count")
  ])
  |> DataFrame.collect()
end)

run.("3b. GroupedData.min with column names (shortcut)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "salary" => 100},
    %{"dept" => "Eng", "salary" => 120},
    %{"dept" => "HR", "salary" => 90}
  ], schema: "dept STRING, salary INT")

  DataFrame.group_by(df, [col("dept")])
  |> SparkEx.GroupedData.min(["salary"])
  |> DataFrame.collect()
end)

run.("3c. GroupedData.max with column names", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "salary" => 100},
    %{"dept" => "Eng", "salary" => 120},
    %{"dept" => "HR", "salary" => 90}
  ], schema: "dept STRING, salary INT")

  DataFrame.group_by(df, [col("dept")])
  |> SparkEx.GroupedData.max(["salary"])
  |> DataFrame.collect()
end)

run.("3d. rollup aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "salary" => 100},
    %{"dept" => "Eng", "team" => "B", "salary" => 120},
    %{"dept" => "HR", "team" => "A", "salary" => 90}
  ], schema: "dept STRING, team STRING, salary INT")

  DataFrame.rollup(df, [col("dept"), col("team")])
  |> SparkEx.GroupedData.agg([sum(col("salary")) |> Column.alias_("total")])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

run.("3e. cube aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "salary" => 100},
    %{"dept" => "Eng", "team" => "B", "salary" => 120},
    %{"dept" => "HR", "team" => "A", "salary" => 90}
  ], schema: "dept STRING, team STRING, salary INT")

  DataFrame.cube(df, [col("dept"), col("team")])
  |> SparkEx.GroupedData.agg([sum(col("salary")) |> Column.alias_("total")])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

# =============================================
# 4. Column operations not yet tested
# =============================================

run.("4a. Column.drop_fields on struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30, "city" => "NY"}}
  ], schema: "info STRUCT<name: STRING, age: INT, city: STRING>")

  df |> DataFrame.select([
    Column.drop_fields(col("info"), ["city"]) |> Column.alias_("trimmed")
  ]) |> DataFrame.collect()
end)

run.("4b. Column.with_field on struct (add/replace)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30}}
  ], schema: "info STRUCT<name: STRING, age: INT>")

  df |> DataFrame.select([
    Column.with_field(col("info"), "verified", lit(true)) |> Column.alias_("extended")
  ]) |> DataFrame.collect()
end)

run.("4c. Column.get_field on struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30}}
  ], schema: "info STRUCT<name: STRING, age: INT>")

  df |> DataFrame.select([
    Column.get_field(col("info"), "name") |> Column.alias_("name")
  ]) |> DataFrame.collect()
end)

run.("4d. Column.get_item on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    Column.get_item(col("arr"), 0) |> Column.alias_("first"),
    Column.get_item(col("arr"), 2) |> Column.alias_("third")
  ]) |> DataFrame.collect()
end)

run.("4e. Column.get_item on map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"key1" => "val1", "key2" => "val2"}}
  ], schema: "m MAP<STRING, STRING>")

  df |> DataFrame.select([
    Column.get_item(col("m"), "key1") |> Column.alias_("v1"),
    Column.get_item(col("m"), "nonexistent") |> Column.alias_("missing")
  ]) |> DataFrame.collect()
end)

run.("4f. Column.bitwiseOR, bitwiseAND, bitwiseXOR", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 0b1010, "b" => 0b1100}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    Column.bitwise_or(col("a"), col("b")) |> Column.alias_("or_"),
    Column.bitwise_and(col("a"), col("b")) |> Column.alias_("and_"),
    Column.bitwise_xor(col("a"), col("b")) |> Column.alias_("xor_")
  ]) |> DataFrame.collect()
end)

run.("4g. Column.contains, startswith, endswith (string patterns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice Smith"},
    %{"name" => "Bob Jones"},
    %{"name" => "Alice Jones"}
  ], schema: "name STRING")

  df |> DataFrame.select([
    col("name"),
    Column.contains(col("name"), lit("Alice")) |> Column.alias_("has_alice"),
    Column.startswith(col("name"), lit("Alice")) |> Column.alias_("starts_alice"),
    Column.endswith(col("name"), lit("Jones")) |> Column.alias_("ends_jones")
  ]) |> DataFrame.collect()
end)

run.("4h. Column.substr with position and length", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    Column.substr(col("text"), 1, 5) |> Column.alias_("sub1"),
    Column.substr(col("text"), 7, 2_147_483_647) |> Column.alias_("sub2")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Session-level utilities
# =============================================

run.("5a. SparkEx.format_sql", fn ->
  SparkEx.format_sql("select a, b from t where x > 1 and y < 2 group by a order by b")
end)

run.("5b. SparkEx.config_get_all", fn ->
  SparkEx.config_get_all(s)
end)

# =============================================
# 6. DataFrame.cache and uncache lifecycle
# =============================================

run.("6. cache + uncache lifecycle", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  cached = DataFrame.cache(df)
  sl_before = DataFrame.storage_level(cached)
  _ = DataFrame.collect(cached)
  sl_after = DataFrame.storage_level(cached)
  DataFrame.unpersist(cached)
  sl_unpersisted = DataFrame.storage_level(cached)
  {sl_before, sl_after, sl_unpersisted}
end)

# =============================================
# 7. DataFrame.repartition and coalesce
# =============================================

run.("7a. repartition(4) and check partition count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  repartitioned = DataFrame.repartition(df, 4)
  # Check partition id range
  repartitioned
  |> DataFrame.select([spark_partition_id() |> Column.alias_("pid")])
  |> DataFrame.distinct()
  |> DataFrame.sort([asc(col("pid"))])
  |> DataFrame.collect()
end)

run.("7b. coalesce(1) single partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  coalesced = DataFrame.coalesce(df, 1)
  coalesced
  |> DataFrame.select([spark_partition_id() |> Column.alias_("pid")])
  |> DataFrame.distinct()
  |> DataFrame.collect()
end)

run.("7c. repartition by column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "id" => 1},
    %{"dept" => "Eng", "id" => 2},
    %{"dept" => "HR", "id" => 3},
    %{"dept" => "HR", "id" => 4}
  ], schema: "dept STRING, id INT")

  repartitioned = DataFrame.repartition(df, [col("dept")])
  repartitioned
  |> DataFrame.select([col("dept"), spark_partition_id() |> Column.alias_("pid")])
  |> DataFrame.distinct()
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. Complex window functions
# =============================================

run.("8a. ntile window function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..12, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])

  df |> DataFrame.select([
    col("id"), col("val"),
    ntile(4) |> Column.over(w) |> Column.alias_("quartile")
  ]) |> DataFrame.collect()
end)

run.("8b. rank and dense_rank", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "salary" => 100},
    %{"dept" => "Eng", "salary" => 120},
    %{"dept" => "Eng", "salary" => 120},
    %{"dept" => "HR", "salary" => 90}
  ], schema: "dept STRING, salary INT")

  w = SparkEx.Window.partition_by([col("dept")])
  |> SparkEx.WindowSpec.order_by([desc(col("salary"))])

  df |> DataFrame.select([
    col("dept"), col("salary"),
    rank() |> Column.over(w) |> Column.alias_("rank"),
    dense_rank() |> Column.over(w) |> Column.alias_("dense_rank"),
    row_number() |> Column.over(w) |> Column.alias_("row_num")
  ]) |> DataFrame.collect()
end)

run.("8c. percent_rank and cume_dist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])

  df |> DataFrame.select([
    col("id"),
    percent_rank() |> Column.over(w) |> Column.alias_("pct_rank"),
    cume_dist() |> Column.over(w) |> Column.alias_("cume_dist")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. DataFrame.join edge cases
# =============================================

run.("9a. left_semi join", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"},
    %{"id" => 3, "val" => "c"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1},
    %{"id" => 3}
  ], schema: "id INT")

  DataFrame.join(df1, df2, ["id"], "left_semi") |> DataFrame.collect()
end)

run.("9b. left_anti join", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"},
    %{"id" => 3, "val" => "c"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1},
    %{"id" => 3}
  ], schema: "id INT")

  DataFrame.join(df1, df2, ["id"], "left_anti") |> DataFrame.collect()
end)

run.("9c. join on expression (not column name)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id2" => 1, "label" => "one"},
    %{"id2" => 2, "label" => "two"}
  ], schema: "id2 INT, label STRING")

  DataFrame.join(df1, df2, Column.eq(col("id"), col("id2")))
  |> DataFrame.select([col("id"), col("val"), col("label")])
  |> DataFrame.collect()
end)

# =============================================
# 10. SparkEx.StreamingQueryManager
# =============================================

run.("10a. list_active", fn ->
  SparkEx.StreamingQueryManager.list_active(s)
end)

run.("10b. get_query by non-existent id", fn ->
  SparkEx.StreamingQueryManager.get(s, "00000000-0000-0000-0000-000000000000")
end)

IO.puts("=== Round 26 tests complete ===")
