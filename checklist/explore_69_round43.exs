# Round 43: Hunt for more ProtobufEncodeError crashes + creative edge cases
# Focus: invalid types passed to protobuf-expecting fields, WriterV2, complex streaming
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
# 1. WriterV2 builder pattern (correct API)
# =============================================

run.("1a. WriterV2 builder via write_v2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "a"}], schema: "id INT, val STRING")
  writer = DataFrame.write_v2(df, "test_wv2_#{run_id}")
  %{type: writer.__struct__, table: writer.table_name, fields: Map.keys(writer)}
end)

run.("1b. WriterV2 with using + options + cluster_by", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "a"}], schema: "id INT, val STRING")
  writer = DataFrame.write_v2(df, "test_wv2b_#{run_id}")
    |> SparkEx.WriterV2.using("parquet")
    |> SparkEx.WriterV2.option("compression", "snappy")
    |> SparkEx.WriterV2.table_property("description", "test table")
    |> SparkEx.WriterV2.cluster_by(["id"])
  %{provider: writer.provider, options: writer.options,
    props: writer.table_properties, cluster: writer.cluster_by}
end)

# =============================================
# 2. Hunt: persist with invalid storage_level (similar to EX-111)
# =============================================

run.("2. DataFrame.persist with invalid storage_level string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.persist(df, storage_level: "INVALID_LEVEL")
end)

# =============================================
# 3. Hunt: StreamWriter with invalid trigger values
# =============================================

run.("3a. StreamWriter.trigger with invalid processing_time type", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  writer = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("bad_trig_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.trigger(processing_time: 5000)  # integer instead of string
  %{trigger: writer.trigger}
end)

run.("3b. StreamWriter.trigger then start with integer processing_time", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("bad_trig2_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.trigger(processing_time: 5000)  # integer instead of string
    |> StreamWriter.start()

  Process.sleep(2000)
  StreamingQuery.stop(query)
  :ok
end)

# =============================================
# 4. Hunt: Column.cast with invalid type string
# =============================================

run.("4a. Column.cast with nonsense type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 1}], schema: "val INT")
  df |> DataFrame.select([
    Column.cast(col("val"), "NONSENSE_TYPE") |> Column.alias_("casted")
  ]) |> DataFrame.collect()
end)

run.("4b. Column.cast with empty string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 1}], schema: "val INT")
  df |> DataFrame.select([
    Column.cast(col("val"), "") |> Column.alias_("casted")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Hunt: DataFrame.to with invalid schema string
# =============================================

run.("5. DataFrame.to with malformed schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.to("not a valid schema!!!") |> DataFrame.collect()
end)

# =============================================
# 6. Hunt: create_dataframe with mismatched schema
# =============================================

run.("6a. create_dataframe with fewer schema cols than data", fn ->
  SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "a", "extra" => true}], schema: "id INT")
end)

run.("6b. create_dataframe with more schema cols than data", fn ->
  SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT, val STRING, extra BOOLEAN")
end)

# =============================================
# 7. Hunt: SQL injection via column names
# =============================================

run.("7a. column name with SQL injection attempt", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.with_column("'; DROP TABLE test; --", lit(42)) |> DataFrame.collect()
end)

run.("7b. table alias with special characters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.alias_(df, "my`table") |> DataFrame.collect()
end)

# =============================================
# 8. Hunt: empty DataFrame edge cases
# =============================================

run.("8a. empty DataFrame operations chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, val STRING")
  df
  |> DataFrame.filter(Column.gt(col("id"), lit(0)))
  |> DataFrame.with_column("new_col", lit("test"))
  |> DataFrame.group_by([col("val")])
  |> GroupedData.agg([count(col("id")) |> Column.alias_("cnt")])
  |> DataFrame.collect()
end)

run.("8b. union of empty and non-empty DataFrames", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT")
  {:ok, full} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  DataFrame.union(empty, full) |> DataFrame.collect()
end)

run.("8c. join with empty DataFrame", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT, val STRING")
  {:ok, full} = SparkEx.create_dataframe(s, [%{"id" => 1, "name" => "a"}], schema: "id INT, name STRING")
  DataFrame.join(empty, full, col("id"), "inner") |> DataFrame.collect()
end)

# =============================================
# 9. Complex: recursive-like CTE via SQL
# =============================================

run.("9. fibonacci via SQL", fn ->
  SparkEx.sql(s, """
    WITH RECURSIVE fib(n, a, b) AS (
      SELECT 1, 0, 1
      UNION ALL
      SELECT n + 1, b, a + b FROM fib WHERE n < 20
    )
    SELECT n, a AS fib_value FROM fib ORDER BY n
  """) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: self-referential window
# =============================================

run.("10. window with expression-based partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i -> %{"val" => i, "grp" => rem(i, 3)} end),
    schema: "val INT, grp INT")

  # Partition by computed expression
  w = SparkEx.Window.partition_by([
    when_(Column.eq(col("grp"), lit(0)), lit("zero"))
    |> Column.otherwise(lit("nonzero"))
  ]) |> WindowSpec.order_by([asc(col("val"))])

  df |> DataFrame.select([
    col("val"), col("grp"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("running_sum"),
    row_number() |> Column.over(w) |> Column.alias_("rank")
  ]) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.limit(10) |> DataFrame.collect()
end)

# =============================================
# 11. Complex: nested DataFrame.transform chains
# =============================================

run.("11. triple transform chain with cross-references", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")

  result = df
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("y", Column.multiply(col("x"), lit(2)))
  end)
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("z", Column.plus(col("x"), col("y")))
  end)
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("w",
      when_(Column.gt(col("z"), lit(15)), lit("big"))
      |> Column.otherwise(lit("small")))
  end)
  |> DataFrame.collect()

  {:ok, rows} = result
  Enum.take(rows, 5)
end)

# =============================================
# 12. Hunt: DataFrame.select with no columns
# =============================================

run.("12a. select with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([]) |> DataFrame.collect()
end)

run.("12b. select with duplicate columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([col("id"), col("id")]) |> DataFrame.collect()
end)

# =============================================
# 13. Hunt: GroupedData edge cases
# =============================================

run.("13a. group_by with no aggregation columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"val" => rem(i, 2)} end), schema: "val INT")
  df |> DataFrame.group_by([col("val")]) |> GroupedData.agg([]) |> DataFrame.collect()
end)

run.("13b. group_by on all columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => 1, "b" => 2}, %{"a" => 1, "b" => 3}
  ], schema: "a INT, b INT")
  df |> DataFrame.group_by([col("a"), col("b")])
    |> GroupedData.agg([count(lit(1)) |> Column.alias_("cnt")])
    |> DataFrame.sort([asc(col("a")), asc(col("b"))])
    |> DataFrame.collect()
end)

# =============================================
# 14. Hunt: very long column alias
# =============================================

run.("14. very long column alias (1000 chars)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  long_name = String.duplicate("x", 1000)
  df |> DataFrame.select([col("id") |> Column.alias_(long_name)]) |> DataFrame.collect()
end)

# =============================================
# 15. Hunt: null literal in various positions
# =============================================

run.("15a. lit(nil) in select", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    col("id"),
    lit(nil) |> Column.alias_("null_col")
  ]) |> DataFrame.collect()
end)

run.("15b. filter with lit(nil)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => nil}], schema: "id INT")
  df |> DataFrame.filter(Column.eq(col("id"), lit(nil))) |> DataFrame.collect()
end)

# =============================================
# 16. Complex: 10-table union chain
# =============================================

run.("16. 10-table union chain", fn ->
  dfs = Enum.map(0..9, fn i ->
    {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => i, "batch" => i}], schema: "id INT, batch INT")
    df
  end)

  result = Enum.reduce(tl(dfs), hd(dfs), fn df, acc ->
    DataFrame.union(acc, df)
  end)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  rows
end)

# =============================================
# 17. Complex: nested aggregation (agg of agg)
# =============================================

run.("17. nested aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)),
        "team" => "T#{rem(i, 7)}",
        "score" => rem(i * 17, 100)}
    end), schema: "dept STRING, team STRING, score INT")

  # First aggregate by dept+team, then aggregate by dept
  df
  |> DataFrame.group_by([col("dept"), col("team")])
  |> GroupedData.agg([avg(col("score")) |> Column.alias_("team_avg")])
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([
    count(col("team")) |> Column.alias_("num_teams"),
    avg(col("team_avg")) |> Column.alias_("dept_avg"),
    max(col("team_avg")) |> Column.alias_("best_team_avg"),
    min(col("team_avg")) |> Column.alias_("worst_team_avg")
  ])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

# =============================================
# 18. Streaming: StreamingQueryManager operations
# =============================================

run.("18a. list_active streaming queries", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("list_q_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(1000)
  active = SparkEx.StreamingQueryManager.list_active(s)
  StreamingQuery.stop(query)
  active
end)

run.("18b. get streaming query by id", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("get_q_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(1000)
  # Try to get the query by its ID
  id = query.id
  found = SparkEx.StreamingQueryManager.get(s, id)
  StreamingQuery.stop(query)
  %{id: id, found: found}
end)

# =============================================
# 19. Hunt: DataFrame.to with nil/empty schema
# =============================================

run.("19a. DataFrame.to with nil", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.to(nil) |> DataFrame.collect()
end)

run.("19b. DataFrame.to with empty string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.to("") |> DataFrame.collect()
end)

IO.puts("=== Round 43 complete ===")
