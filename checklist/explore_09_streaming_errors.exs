# Exploratory: streaming edge cases, error handling, concurrent ops
alias SparkEx.{DataFrame, Column, StreamReader, StreamWriter, StreamingQuery, StreamingQueryManager}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === Streaming: multiple queries at once ===
run.("1. Multiple concurrent streaming queries", fn ->
  {:ok, q1} = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("multi_q1_#{run_id}")
    |> StreamWriter.start()

  {:ok, q2} = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("multi_q2_#{run_id}")
    |> StreamWriter.start()

  Process.sleep(2000)

  active = StreamingQueryManager.list_active(s)
  IO.inspect(active, label: "  active queries")

  StreamingQuery.stop(q1)
  StreamingQuery.stop(q2)
  active
end)

# === Streaming: query with watermark ===
run.("2. Streaming with watermark (rate source)", fn ->
  {:ok, q} = StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.with_watermark("timestamp", "10 seconds")
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("watermark_#{run_id}")
    |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(q)
  StreamingQuery.stop(q)
  active
end)

# === Streaming: explain ===
run.("3. Streaming explain", fn ->
  rate_df = StreamReader.rate(s, rows_per_second: 10)
  DataFrame.explain(rate_df)
end)

# === Error handling edge cases ===
run.("4. Access column that doesn't exist", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([col("nonexistent")])
  |> DataFrame.collect()
end)

run.("5. Division by zero", fn ->
  SparkEx.sql(s, "SELECT 1 AS a, 0 AS b")
  |> DataFrame.select([Column.divide(col("a"), col("b")) |> Column.alias_("result")])
  |> DataFrame.collect()
end)

run.("6. Cast impossible value", fn ->
  SparkEx.sql(s, "SELECT 'not_a_number' AS val")
  |> DataFrame.select([Column.cast(col("val"), "INT") |> Column.alias_("casted")])
  |> DataFrame.collect()
end)

run.("7. Invalid SQL", fn ->
  SparkEx.sql(s, "SELEC INVALID") |> DataFrame.collect()
end)

run.("8. Ambiguous column reference", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id, 'a' AS name")
  df2 = SparkEx.sql(s, "SELECT 1 AS id, 'b' AS tag")
  DataFrame.join(df1, df2, ["id"], :inner)
  |> DataFrame.select([col("id"), col("name"), col("tag")])
  |> DataFrame.collect()
end)

run.("9. Join on non-existent column", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id")
  df2 = SparkEx.sql(s, "SELECT 1 AS key")
  DataFrame.join(df1, df2, ["id"], :inner) |> DataFrame.collect()
end)

run.("10. Aggregate without group_by columns in select", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'a') AS t(id, grp)")
  |> DataFrame.group_by([col("grp")])
  |> DataFrame.agg([sum(col("id")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# === Concurrent read operations ===
run.("11. Concurrent collects via tasks", fn ->
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3) AS t(id)")
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (4),(5),(6) AS t(id)")

  t1 = Task.async(fn -> DataFrame.collect(df1) end)
  t2 = Task.async(fn -> DataFrame.collect(df2) end)

  r1 = Task.await(t1, 10_000)
  r2 = Task.await(t2, 10_000)
  %{r1: r1, r2: r2}
end)

# === Schema inspection after transforms ===
run.("12. Schema after complex transform", fn ->
  SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name, 100.5 AS salary")
  |> DataFrame.with_column("bonus", Column.multiply(col("salary"), lit(0.1)))
  |> DataFrame.with_column("total", Column.plus(col("salary"), col("bonus")))
  |> DataFrame.dtypes()
end)

# === Very wide schema (20+ columns) ===
run.("13. Wide schema (20 columns)", fn ->
  cols = Enum.map(1..20, fn i -> "#{i} AS col_#{i}" end) |> Enum.join(", ")
  SparkEx.sql(s, "SELECT #{cols}")
  |> DataFrame.columns()
end)

# === Reuse DataFrame in multiple operations ===
run.("14. Reuse same DataFrame in multiple ops", fn ->
  base = SparkEx.sql(s, "SELECT * FROM VALUES (1,'a',10), (2,'b',20), (3,'c',30) AS t(id, name, val)")
  r1 = DataFrame.filter(base, Column.gt(col("val"), lit(15))) |> DataFrame.count()
  r2 = DataFrame.filter(base, Column.lt(col("val"), lit(25))) |> DataFrame.count()
  r3 = DataFrame.agg(base, [sum(col("val")) |> Column.alias_("total")]) |> DataFrame.collect()
  %{gt_15: r1, lt_25: r2, total: r3}
end)

# === Caching + unpersist ===
run.("15. Persist + query + unpersist", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3),(4),(5) AS t(id)")
  %DataFrame{} = DataFrame.persist(df)
  r1 = DataFrame.count(df)
  sl = DataFrame.storage_level(df)
  %DataFrame{} = DataFrame.unpersist(df)
  %{count: r1, storage_level: sl}
end)

# === Temp view lifecycle ===
run.("16. Temp view: create, query, replace, query, drop", fn ->
  df1 = SparkEx.sql(s, "SELECT 'v1' AS version, 100 AS val")
  :ok = DataFrame.create_or_replace_temp_view(df1, "lifecycle_view_#{run_id}")
  r1 = SparkEx.sql(s, "SELECT * FROM lifecycle_view_#{run_id}") |> DataFrame.collect()

  df2 = SparkEx.sql(s, "SELECT 'v2' AS version, 200 AS val")
  :ok = DataFrame.create_or_replace_temp_view(df2, "lifecycle_view_#{run_id}")
  r2 = SparkEx.sql(s, "SELECT * FROM lifecycle_view_#{run_id}") |> DataFrame.collect()

  SparkEx.Catalog.drop_temp_view(s, "lifecycle_view_#{run_id}")
  %{v1: r1, v2: r2}
end)

# === Execution info ===
run.("17. execution_info after collect", fn ->
  df = SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM VALUES (1),(2),(3),(4),(5) AS t(id)")
  {:ok, _} = DataFrame.collect(df)
  DataFrame.execution_info(df)
end)

# === same_semantics ===
run.("18. same_semantics on equivalent plans", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id") |> DataFrame.filter(Column.eq(col("id"), lit(1)))
  df2 = SparkEx.sql(s, "SELECT 1 AS id WHERE 1 = 1")
  DataFrame.same_semantics(df1, df2)
end)

IO.puts("=== Streaming/Errors exploration complete ===")
