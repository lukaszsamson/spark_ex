# Round 38: UDF registration, streaming with correct pattern, MergeIntoWriter,
# creative edge cases, more function coverage
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, StreamReader, StreamWriter, StreamingQuery, WriterV2}
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
# 1. UDF registration
# =============================================

run.("1a. register_java with existing Java function", fn ->
  # Try registering a known Java UDF class
  SparkEx.UDFRegistration.register_java(s, "my_abs_#{run_id}", "java.lang.Math", "INT")
end)

run.("1b. register_java with non-existent class", fn ->
  SparkEx.UDFRegistration.register_java(s, "bad_udf_#{run_id}", "com.nonexistent.FakeClass", "INT")
end)

# =============================================
# 2. Streaming with correct pattern (write_stream)
# =============================================

run.("2a. streaming with write_stream builder", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.with_column("doubled", Column.multiply(col("value"), lit(2)))

  query = stream
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("ws38_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.trigger(processing_time: "1 seconds")
  |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(query)
  name = StreamingQuery.name(query)
  exc = StreamingQuery.exception(query)
  {:ok, result} = SparkEx.Reader.table(s, "ws38_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)

  %{active: active, name: name, exception: exc, count: length(result), sample: Enum.take(result, 2)}
end)

# =============================================
# 3. Streaming: status and recent_progress
# =============================================

run.("3. StreamingQuery status and recent_progress", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  query = stream
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("ws38_status_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(3000)
  status = StreamingQuery.status(query)
  progress = StreamingQuery.recent_progress(query)
  StreamingQuery.stop(query)
  %{status: status, progress_count: length(progress || [])}
end)

# =============================================
# 4. DataFrame.with_column chain → collect → re-collect
# =============================================

run.("4. double collect (same DataFrame collected twice)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")
  enriched = df |> DataFrame.with_column("doubled", Column.multiply(col("id"), lit(2)))

  {:ok, first} = DataFrame.collect(enriched)
  {:ok, second} = DataFrame.collect(enriched)
  {first == second, length(first), length(second)}
end)

# =============================================
# 5. Complex: Multi-level aggregation pipeline
# =============================================

run.("5. multi-level aggregation (agg → agg → window)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.flat_map(1..4, fn dept ->
      Enum.map(1..10, fn emp ->
        %{"dept_id" => dept, "emp_id" => dept * 100 + emp,
          "salary" => 50_000 + rem(:rand.uniform(50_000), 50_000)}
      end)
    end), schema: "dept_id INT, emp_id INT, salary INT")

  # Level 1: dept-level stats
  dept_stats = df
    |> DataFrame.group_by([col("dept_id")])
    |> GroupedData.agg([
      count(col("emp_id")) |> Column.alias_("emp_count"),
      avg(col("salary")) |> Column.alias_("avg_salary"),
      max(col("salary")) |> Column.alias_("max_salary")
    ])

  # Level 2: rank departments
  w = SparkEx.Window.partition_by([lit(true)])
    |> WindowSpec.order_by([desc(col("avg_salary"))])

  dept_stats
    |> DataFrame.with_column("dept_rank", row_number() |> Column.over(w))
    |> DataFrame.sort([asc(col("dept_rank"))])
    |> DataFrame.collect()
end)

# =============================================
# 6. DataFrame.coalesce
# =============================================

run.("6. coalesce (reduce partitions)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  before = DataFrame.rdd_num_partitions_approx(df |> DataFrame.repartition(8))
  after_ = DataFrame.rdd_num_partitions_approx(df |> DataFrame.repartition(8) |> DataFrame.coalesce(2))
  {before, after_}
end)

# =============================================
# 7. DataFrame.drop multiple columns
# =============================================

run.("7a. drop single column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  df |> DataFrame.drop(["b"]) |> DataFrame.columns()
end)

run.("7b. drop multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4}
  ], schema: "a INT, b INT, c INT, d INT")
  df |> DataFrame.drop(["b", "d"]) |> DataFrame.columns()
end)

run.("7c. drop non-existent column (should be no-op?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}
  ], schema: "a INT, b INT")
  df |> DataFrame.drop(["nonexistent"]) |> DataFrame.columns()
end)

run.("7d. drop ALL columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}
  ], schema: "a INT, b INT")
  df |> DataFrame.drop(["a", "b"]) |> DataFrame.collect()
end)

# =============================================
# 8. Creative: union of differently-ordered columns
# =============================================

run.("8. union_by_name with different column order", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"c" => 30, "a" => 10, "b" => 20}
  ], schema: "c INT, a INT, b INT")
  DataFrame.union_by_name(df1, df2) |> DataFrame.collect()
end)

# =============================================
# 9. Creative: self-join with aliases
# =============================================

run.("9. self-join using aliases", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "parent_id" => nil, "name" => "CEO"},
    %{"id" => 2, "parent_id" => 1, "name" => "VP"},
    %{"id" => 3, "parent_id" => 2, "name" => "Director"},
    %{"id" => 4, "parent_id" => 2, "name" => "Manager"}
  ], schema: "id INT, parent_id INT, name STRING")

  emp = DataFrame.alias_(df, "emp")
  mgr = DataFrame.alias_(df, "mgr")

  DataFrame.join(emp, mgr,
    Column.eq(col("emp.parent_id"), col("mgr.id")), :left)
  |> DataFrame.select([
    col("emp.name") |> Column.alias_("employee"),
    col("mgr.name") |> Column.alias_("manager")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 10. Creative: dynamic SQL generation
# =============================================

run.("10. SQL with CASE WHEN and aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"score" => rem(i * 7, 100), "subject" => Enum.at(["Math", "Science", "English"], rem(i, 3))}
    end), schema: "score INT, subject STRING")

  DataFrame.create_or_replace_temp_view(df, "scores_#{run_id}")

  SparkEx.sql(s, """
    SELECT subject,
      COUNT(*) AS total,
      SUM(CASE WHEN score >= 70 THEN 1 ELSE 0 END) AS passing,
      ROUND(AVG(score), 1) AS avg_score,
      PERCENTILE(score, 0.5) AS median_score
    FROM scores_#{run_id}
    GROUP BY subject
    ORDER BY subject
  """) |> DataFrame.collect()
end)

# =============================================
# 11. Creative: nested DataFrame operations
# =============================================

run.("11. create DataFrame from collected results", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  {:ok, rows} = DataFrame.collect(df1)
  # Double all values and create new DataFrame
  doubled_rows = Enum.map(rows, fn row ->
    %{"id" => row["id"], "val" => row["val"] * 2}
  end)

  {:ok, df2} = SparkEx.create_dataframe(s, doubled_rows, schema: "id INT, val INT")
  DataFrame.collect(df2)
end)

# =============================================
# 12. Edge: Unicode in column names and values
# =============================================

run.("12a. unicode in values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello 世界 🌍"}
  ], schema: "text STRING")
  DataFrame.collect(df)
end)

run.("12b. unicode in column names", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"名前" => "太郎", "年齢" => 25}
  ], schema: "`名前` STRING, `年齢` INT")
  DataFrame.collect(df)
end)

# =============================================
# 13. Edge: very large number of columns
# =============================================

run.("13. DataFrame with 100 columns", fn ->
  row = Enum.reduce(1..100, %{}, fn i, acc ->
    Map.put(acc, "col_#{String.pad_leading(to_string(i), 3, "0")}", i)
  end)
  schema = Enum.map_join(1..100, ", ", fn i ->
    "col_#{String.pad_leading(to_string(i), 3, "0")} INT"
  end)
  {:ok, df} = SparkEx.create_dataframe(s, [row], schema: schema)
  columns = DataFrame.columns(df)
  count = DataFrame.count(df)
  {columns |> elem(1) |> length(), count}
end)

# =============================================
# 14. Edge: empty string vs null
# =============================================

run.("14. empty string vs null distinction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => ""}, %{"val" => nil}, %{"val" => "hello"}
  ], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    isnull(col("val")) |> Column.alias_("is_null"),
    Column.eq(col("val"), lit("")) |> Column.alias_("is_empty"),
    length(col("val")) |> Column.alias_("len")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Functions: coalesce (expression, not DataFrame)
# =============================================

run.("15. coalesce expression (first non-null)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => 3},
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  df |> DataFrame.select([
    coalesce([col("a"), col("b"), col("c")]) |> Column.alias_("first_non_null")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Functions: array_contains / array_position / element_at
# =============================================

run.("16. array functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    array_contains(col("arr"), lit(30)) |> Column.alias_("has_30"),
    array_position(col("arr"), lit(30)) |> Column.alias_("pos_30"),
    element_at(col("arr"), lit(3)) |> Column.alias_("at_3"),
    size(col("arr")) |> Column.alias_("sz"),
    array_min(col("arr")) |> Column.alias_("min"),
    array_max(col("arr")) |> Column.alias_("max")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. Functions: map_keys / map_values / map_entries
# =============================================

run.("17. map functions", fn ->
  df = SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  df |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values"),
    element_at(col("m"), lit("b")) |> Column.alias_("b_val"),
    size(col("m")) |> Column.alias_("sz")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. Functions: explode / posexplode
# =============================================

run.("18. explode and collect_list roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b", "c"]},
    %{"id" => 2, "tags" => ["d", "e"]}
  ], schema: "id INT, tags ARRAY<STRING>")

  exploded = df |> DataFrame.select([col("id"), explode(col("tags")) |> Column.alias_("tag")])
  {:ok, exploded_rows} = DataFrame.collect(exploded)

  # Re-aggregate
  reagg = exploded
    |> DataFrame.group_by([col("id")])
    |> GroupedData.agg([collect_list(col("tag")) |> Column.alias_("tags_reagg")])
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  {exploded_rows, reagg}
end)

# =============================================
# 19. Functions: date arithmetic edge cases
# =============================================

run.("19. date arithmetic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2024-02-29"}
  ], schema: "dt STRING")
  df |> DataFrame.select([
    Column.cast(col("dt"), "DATE") |> Column.alias_("date"),
    date_add(Column.cast(col("dt"), "DATE"), 1) |> Column.alias_("next_day"),
    date_sub(Column.cast(col("dt"), "DATE"), 1) |> Column.alias_("prev_day"),
    add_months(col("dt"), 1) |> Column.alias_("plus_month"),
    add_months(col("dt"), 12) |> Column.alias_("plus_year"),
    datediff(Column.cast(lit("2024-12-31"), "DATE"), Column.cast(col("dt"), "DATE"))
      |> Column.alias_("days_to_eoy"),
    year(Column.cast(col("dt"), "DATE")) |> Column.alias_("yr"),
    month(Column.cast(col("dt"), "DATE")) |> Column.alias_("mo"),
    dayofmonth(Column.cast(col("dt"), "DATE")) |> Column.alias_("day"),
    dayofweek(Column.cast(col("dt"), "DATE")) |> Column.alias_("dow"),
    dayofyear(Column.cast(col("dt"), "DATE")) |> Column.alias_("doy")
  ]) |> DataFrame.collect()
end)

# =============================================
# 20. Functions: format_number / format_string
# =============================================

run.("20. format_number and format_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1234567.891}
  ], schema: "val DOUBLE")
  df |> DataFrame.select([
    format_number(col("val"), 2) |> Column.alias_("formatted"),
    format_string("Value is: %.2f", [col("val")]) |> Column.alias_("msg")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 38 complete ===")
