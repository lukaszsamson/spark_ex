# Round 47: Hunt remaining protobuf validation gaps + untested API corners
# Focus: functions that accept keyword opts and pass them to protobuf,
# DataFrame.sample correct API, Catalog.create_external_table, edge cases
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
# 1. DataFrame.sample correct API investigation
# =============================================

run.("1a. sample positional args (false, 0.1)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, rows} = df |> DataFrame.sample(false, 0.1) |> DataFrame.collect()
  length(rows)
end)

run.("1b. sample positional args with seed (false, 0.1, 42)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, rows} = df |> DataFrame.sample(false, 0.1, 42) |> DataFrame.collect()
  length(rows)
end)

run.("1c. sample with keyword (with_replacement: false, fraction: 0.1, seed: 42)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.sample(with_replacement: false, fraction: 0.1, seed: 42) |> DataFrame.collect()
end)

# =============================================
# 2. Hunt: Catalog.create_table with opts
# =============================================

run.("2a. Catalog.create_table with schema string", fn ->
  Catalog.create_table(s, "sfx.sfx_dev_warehouse.test_ct_#{run_id}",
    schema: "id INT, name STRING")
end)

# =============================================
# 3. Hunt: StreamingQuery.await_termination correct keyword API
# =============================================

run.("3. StreamingQuery.await_termination with timeout keyword", fn ->
  alias SparkEx.{StreamReader, StreamWriter, StreamingQuery}
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("await_kw_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  result = StreamingQuery.await_termination(query, timeout: 2)
  StreamingQuery.stop(query)
  result
end)

# =============================================
# 4. Hunt: DataFrame.describe correct API
# =============================================

run.("4a. describe with column names", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.5, "grp" => rem(i, 3)} end),
    schema: "val DOUBLE, grp INT")
  DataFrame.describe(df, ["val"]) |> DataFrame.collect()
end)

run.("4b. describe with no args", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.5, "grp" => rem(i, 3)} end),
    schema: "val DOUBLE, grp INT")
  DataFrame.describe(df) |> DataFrame.collect()
end)

# =============================================
# 5. Hunt: DataFrame.to_local_iterator
# =============================================

run.("5. to_local_iterator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  iter = DataFrame.to_local_iterator(df)
  case iter do
    {:ok, rows} -> %{type: :ok, count: length(rows), sample: Enum.take(rows, 3)}
    other -> other
  end
end)

# =============================================
# 6. Hunt: DataFrame.foreach
# =============================================

run.("6. foreach (should just iterate without returning data)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.foreach_local(df, fn _row -> :ok end)
end)

# =============================================
# 7. Hunt: DataFrame.rdd_num_partitions
# =============================================

run.("7. rdd_num_partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.rdd_num_partitions_approx(df)
end)

# =============================================
# 8. Complex: map_filter higher-order function
# =============================================

run.("8. map_filter HOF", fn ->
  df = SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3, 'd', 4) AS m")
  df |> DataFrame.select([
    col("m"),
    map_filter(col("m"), fn _k, v -> Column.gt(v, lit(2)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Complex: transform_keys / transform_values HOF
# =============================================

run.("9a. transform_keys", fn ->
  df = SparkEx.sql(s, "SELECT map('hello', 1, 'world', 2) AS m")
  df |> DataFrame.select([
    transform_keys(col("m"), fn k, _v -> upper(k) end) |> Column.alias_("upper_keys")
  ]) |> DataFrame.collect()
end)

run.("9b. transform_values", fn ->
  df = SparkEx.sql(s, "SELECT map('a', 10, 'b', 20) AS m")
  df |> DataFrame.select([
    transform_values(col("m"), fn _k, v -> Column.multiply(v, lit(2)) end)
    |> Column.alias_("doubled_vals")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: zip_with HOF
# =============================================

run.("10. zip_with", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [10, 20, 30]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    zip_with(col("a"), col("b"), fn x, y -> Column.plus(x, y) end)
    |> Column.alias_("summed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Complex: exists / forall HOF on arrays
# =============================================

run.("11a. exists (any element > 5)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [1, 2, 6]},
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    exists(col("arr"), fn x -> Column.gt(x, lit(5)) end) |> Column.alias_("has_gt5")
  ]) |> DataFrame.collect()
end)

run.("11b. forall (all elements > 0)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [0, 1, 2]},
    %{"arr" => [-1, 5, 10]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    forall(col("arr"), fn x -> Column.gt(x, lit(0)) end) |> Column.alias_("all_positive")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Complex: DataFrame.select_expr
# =============================================

run.("12. select_expr with SQL expressions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 20}
  ], schema: "a INT, b INT")
  df |> DataFrame.select_expr(["a", "b", "a + b AS sum", "a * b AS product"])
    |> DataFrame.collect()
end)

# =============================================
# 13. Complex: from_json with options
# =============================================

run.("13. from_json with options map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => "{\"name\": \"Alice\", \"date\": \"2024-01-15\"}"}
  ], schema: "json STRING")
  df |> DataFrame.select([
    from_json(col("json"), "name STRING, date DATE",
      %{"dateFormat" => "yyyy-MM-dd"}) |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Complex: mixed HOF pipeline
# =============================================

run.("14. mixed HOF pipeline: transform → filter → aggregate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"scores" => [85, 92, 78, 95, 60, 88]},
    %{"scores" => [70, 55, 90, 82, 75, 68]}
  ], schema: "scores ARRAY<INT>")

  df |> DataFrame.select([
    col("scores"),
    # Step 1: filter to passing scores (>= 70)
    filter(col("scores"), fn x -> Column.gte(x, lit(70)) end) |> Column.alias_("passing"),
    # Step 2: transform passing scores to letter grades
    SparkEx.Functions.transform(
      filter(col("scores"), fn x -> Column.gte(x, lit(70)) end),
      fn x ->
        when_(Column.gte(x, lit(90)), lit("A"))
        |> Column.otherwise(
          when_(Column.gte(x, lit(80)), lit("B"))
          |> Column.otherwise(lit("C")))
      end) |> Column.alias_("grades"),
    # Step 3: count passing
    size(filter(col("scores"), fn x -> Column.gte(x, lit(70)) end))
    |> Column.alias_("passing_count"),
    # Step 4: aggregate to sum of passing
    aggregate(
      filter(col("scores"), fn x -> Column.gte(x, lit(70)) end),
      lit(0), fn acc, x -> Column.plus(acc, x) end)
    |> Column.alias_("passing_total")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Complex: Catalog.get_database / get_table
# =============================================

run.("15a. get_database", fn ->
  Catalog.get_database(s, "sfx_dev_warehouse")
end)

run.("15b. Catalog.list_tables in known db", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse")
end)

# =============================================
# 16. Hunt: Column.isin with empty list
# =============================================

run.("16. Column.isin with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.filter(Column.isin(col("id"), [])) |> DataFrame.collect()
end)

# =============================================
# 17. Hunt: DataFrame.with_column overwriting existing column
# =============================================

run.("17. with_column overwriting existing column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => 10}], schema: "id INT, val INT")
  df |> DataFrame.with_column("val", Column.multiply(col("val"), lit(2)))
    |> DataFrame.collect()
end)

# =============================================
# 18. Hunt: DataFrame.drop non-existent column
# =============================================

run.("18. drop non-existent column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.drop(["nonexistent"]) |> DataFrame.collect()
end)

# =============================================
# 19. Complex: multiple aggregation functions on same column
# =============================================

run.("19. 10 aggregations on same column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  df |> DataFrame.select([
    count(col("val")) |> Column.alias_("count"),
    sum(col("val")) |> Column.alias_("sum"),
    avg(col("val")) |> Column.alias_("avg"),
    min(col("val")) |> Column.alias_("min"),
    max(col("val")) |> Column.alias_("max"),
    stddev(col("val")) |> Column.alias_("stddev"),
    variance(col("val")) |> Column.alias_("variance"),
    kurtosis(col("val")) |> Column.alias_("kurtosis"),
    skewness(col("val")) |> Column.alias_("skewness"),
    percentile_approx(col("val"), 0.5, 100) |> Column.alias_("median")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 47 complete ===")
