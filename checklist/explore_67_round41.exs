# Round 41: UDF registration correct API, MergeIntoWriter, streaming correct pattern,
# more creative edge cases
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, StreamReader, StreamWriter, StreamingQuery, MergeIntoWriter}
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
# 1. UDF registration correct API
# =============================================

run.("1a. register_java (correct: name, class_name, opts)", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_abs_#{run_id}", "java.lang.Math",
    return_type: "DOUBLE")
end)

run.("1b. register_java aggregate", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_agg_#{run_id}", "org.apache.spark.sql.MyUDAF",
    return_type: "DOUBLE", aggregate: true)
end)

run.("1c. register_java no opts", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_no_opts_#{run_id}", "java.lang.Math")
end)

# =============================================
# 2. Streaming with correct destructuring pattern
# =============================================

run.("2. streaming full lifecycle", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.with_column("doubled", Column.multiply(col("value"), lit(2)))

  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("ws41_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)

  active = StreamingQuery.is_active?(query)
  name = StreamingQuery.name(query)
  exc = StreamingQuery.exception(query)
  status = StreamingQuery.status(query)

  {:ok, result} = SparkEx.Reader.table(s, "ws41_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)

  %{active: active, name: name, exception: exc, status: status,
    count: length(result), sample: Enum.take(result, 2)}
end)

# =============================================
# 3. Streaming with trigger options
# =============================================

run.("3a. streaming with available_now trigger", fn ->
  stream = StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("ws41_avail_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.trigger(available_now: true)
    |> StreamWriter.start()

  # available_now should complete quickly
  Process.sleep(5000)
  active = StreamingQuery.is_active?(query)

  if active == {:ok, true} do
    StreamingQuery.stop(query)
  end

  {:ok, result} = SparkEx.Reader.table(s, "ws41_avail_#{run_id}") |> DataFrame.collect()
  %{active_after_wait: active, count: length(result)}
end)

# =============================================
# 4. MergeIntoWriter basic structure
# =============================================

run.("4a. MergeIntoWriter struct creation", fn ->
  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "new1"},
    %{"id" => 2, "val" => "new2"}
  ], schema: "id INT, val STRING")

  # Build a merge writer using correct API
  writer = %MergeIntoWriter{
    source_df: source,
    target_table: "test_table",
    condition: Column.eq(col("source.id"), col("target.id"))
  }
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()

  %{
    target_table: writer.target_table,
    match_actions: length(writer.match_actions),
    not_matched_actions: length(writer.not_matched_actions)
  }
end)

# =============================================
# 5. DataFrame.freqItems (direct)
# =============================================

run.("5. freqItems direct call", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => rem(i, 5), "grp" => rem(i, 3)} end),
    schema: "val INT, grp INT")
  DataFrame.Stat.freq_items(df, ["val", "grp"]) |> DataFrame.collect()
end)

# =============================================
# 6. Complex: chained DataFrame transforms pipeline
# =============================================

run.("6. complex pipeline: create → enrich → filter → group → window → sort", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{
        "emp_id" => i,
        "dept" => Enum.at(["Engineering", "Sales", "Marketing", "Support"], rem(i, 4)),
        "salary" => 40000 + rem(i * 997, 60000),
        "years" => rem(i, 15) + 1
      }
    end), schema: "emp_id INT, dept STRING, salary INT, years INT")

  # Pipeline
  result = df
  |> DataFrame.transform(fn d ->
    # Add salary band
    d |> DataFrame.with_column("band",
      when_(Column.gte(col("salary"), lit(80000)), lit("Senior"))
      |> Column.otherwise(
        when_(Column.gte(col("salary"), lit(60000)), lit("Mid"))
        |> Column.otherwise(lit("Junior"))))
  end)
  |> DataFrame.filter(Column.gt(col("years"), lit(3)))
  |> DataFrame.group_by([col("dept"), col("band")])
  |> GroupedData.agg([
    count(col("emp_id")) |> Column.alias_("headcount"),
    avg(col("salary")) |> Column.alias_("avg_salary"),
    avg(col("years")) |> Column.alias_("avg_tenure")
  ])
  |> DataFrame.transform(fn d ->
    w = SparkEx.Window.partition_by([col("dept")])
      |> WindowSpec.order_by([desc(col("headcount"))])
    d |> DataFrame.with_column("rank_in_dept", row_number() |> Column.over(w))
  end)
  |> DataFrame.sort([asc(col("dept")), asc(col("rank_in_dept"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  IO.puts("  #{length(rows)} rows")
  Enum.take(rows, 4)
end)

# =============================================
# 7. Edge: create_dataframe with boolean and mixed types
# =============================================

run.("7a. boolean column operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => true, "b" => false},
    %{"a" => false, "b" => true},
    %{"a" => true, "b" => true}
  ], schema: "a BOOLEAN, b BOOLEAN")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.and_(col("a"), col("b")) |> Column.alias_("and_"),
    Column.or_(col("a"), col("b")) |> Column.alias_("or_"),
    Column.not_(col("a")) |> Column.alias_("not_a")
  ]) |> DataFrame.collect()
end)

run.("7b. boolean aggregates", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"flag" => true}, %{"flag" => true}, %{"flag" => false}, %{"flag" => true}
  ], schema: "flag BOOLEAN")
  df |> DataFrame.select([
    bool_and(col("flag")) |> Column.alias_("all_true"),
    bool_or(col("flag")) |> Column.alias_("any_true"),
    count_if(col("flag")) |> Column.alias_("true_count")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Edge: Column.astype (alias for cast)
# =============================================

run.("8. Column.astype", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    Column.astype(col("val"), "DOUBLE") |> Column.alias_("as_double"),
    Column.astype(col("val"), "STRING") |> Column.alias_("as_string")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Edge: DataFrame.alias_ for subquery-like usage
# =============================================

run.("9. DataFrame.alias_ with select", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  aliased = DataFrame.alias_(df, "people")
  aliased |> DataFrame.select([
    col("people.id") |> Column.alias_("person_id"),
    col("people.name") |> Column.alias_("person_name")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Edge: very large SQL query
# =============================================

run.("10. complex SQL with 10 CTEs", fn ->
  # Build a chain of CTEs
  SparkEx.sql(s, """
    WITH
      base AS (SELECT id FROM range(1, 101)),
      evens AS (SELECT id FROM base WHERE id % 2 = 0),
      odds AS (SELECT id FROM base WHERE id % 2 = 1),
      by3 AS (SELECT id FROM base WHERE id % 3 = 0),
      by5 AS (SELECT id FROM base WHERE id % 5 = 0),
      by6 AS (SELECT id FROM evens INTERSECT SELECT id FROM by3),
      by15 AS (SELECT id FROM by3 INTERSECT SELECT id FROM by5),
      by30 AS (SELECT id FROM by6 INTERSECT SELECT id FROM by5),
      stats AS (
        SELECT
          (SELECT COUNT(*) FROM evens) AS even_count,
          (SELECT COUNT(*) FROM odds) AS odd_count,
          (SELECT COUNT(*) FROM by3) AS by3_count,
          (SELECT COUNT(*) FROM by5) AS by5_count,
          (SELECT COUNT(*) FROM by6) AS by6_count,
          (SELECT COUNT(*) FROM by15) AS by15_count,
          (SELECT COUNT(*) FROM by30) AS by30_count
      )
    SELECT * FROM stats
  """) |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.to (write to table format)
# =============================================

run.("11. DataFrame.to (cast to schema)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42.5}
  ], schema: "id INT, val DOUBLE")
  # to() should cast to target schema
  df |> DataFrame.to("id LONG, val FLOAT") |> DataFrame.dtypes()
end)

# =============================================
# 12. Functions: nanvl
# =============================================

run.("12. nanvl", fn ->
  df = SparkEx.sql(s, """
    SELECT CAST('NaN' AS DOUBLE) AS a, 42.0 AS b
    UNION ALL
    SELECT 1.0 AS a, 2.0 AS b
  """)
  df |> DataFrame.select([
    col("a"), col("b"),
    nanvl(col("a"), col("b")) |> Column.alias_("nanvl_result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Functions: typeof
# =============================================

run.("13. typeof", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"i" => 1, "d" => 1.5, "s" => "hello", "b" => true}
  ], schema: "i INT, d DOUBLE, s STRING, b BOOLEAN")
  df |> DataFrame.select([
    typeof(col("i")) |> Column.alias_("type_i"),
    typeof(col("d")) |> Column.alias_("type_d"),
    typeof(col("s")) |> Column.alias_("type_s"),
    typeof(col("b")) |> Column.alias_("type_b")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Functions: current_date / current_timestamp
# =============================================

run.("14. current_date / current_timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    current_date() |> Column.alias_("today"),
    current_timestamp() |> Column.alias_("now")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Functions: arrays_zip
# =============================================

run.("15. arrays_zip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"names" => ["Alice", "Bob", "Charlie"], "ages" => [30, 25, 35]}
  ], schema: "names ARRAY<STRING>, ages ARRAY<INT>")
  df |> DataFrame.select([
    arrays_zip([col("names"), col("ages")]) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Functions: map_from_arrays / map_from_entries
# =============================================

run.("16a. map_from_arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"keys" => ["a", "b", "c"], "vals" => [1, 2, 3]}
  ], schema: "keys ARRAY<STRING>, vals ARRAY<INT>")
  df |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("mp")
  ]) |> DataFrame.collect()
end)

run.("16b. map_from_entries", fn ->
  df = SparkEx.sql(s, """
    SELECT array(struct('a', 1), struct('b', 2)) AS entries
  """)
  df |> DataFrame.select([
    map_from_entries(col("entries")) |> Column.alias_("mp")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. Edge: negative array indexing
# =============================================

run.("17. element_at with negative index", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    element_at(col("arr"), lit(1)) |> Column.alias_("first"),
    element_at(col("arr"), lit(-1)) |> Column.alias_("last"),
    element_at(col("arr"), lit(-2)) |> Column.alias_("second_last")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. Functions: array_distinct / array_union / array_intersect / array_except
# =============================================

run.("18. array set operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 2, 3, 3], "b" => [2, 3, 4, 5]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    array_distinct(col("a")) |> Column.alias_("distinct_a"),
    array_union(col("a"), col("b")) |> Column.alias_("union_"),
    array_intersect(col("a"), col("b")) |> Column.alias_("intersect_"),
    array_except(col("a"), col("b")) |> Column.alias_("except_")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 41 complete ===")
