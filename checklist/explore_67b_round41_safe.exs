# Round 41 (safe): Skip session-crashing UDF test, test everything else
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

# NOTE: register_java with return_type: "DOUBLE" CRASHES SESSION (EX-110)
# The return_type expects a DataType struct, not a string.

# Test without return_type
run.("1. register_java without return_type (should be safe)", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_no_rt_#{run_id}", "java.lang.Math")
end)

# =============================================
# 2. Streaming
# =============================================

run.("2. streaming full lifecycle", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
    |> DataFrame.with_column("doubled", Column.multiply(col("value"), lit(2)))

  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("ws41b_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)

  active = StreamingQuery.is_active?(query)
  name = StreamingQuery.name(query)
  exc = StreamingQuery.exception(query)
  status = StreamingQuery.status(query)

  {:ok, result} = SparkEx.Reader.table(s, "ws41b_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)

  %{active: active, name: name, exception: exc, status: status,
    count: length(result), sample: Enum.take(result, 2)}
end)

# =============================================
# 3. MergeIntoWriter
# =============================================

run.("3. MergeIntoWriter struct creation", fn ->
  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "new1"}
  ], schema: "id INT, val STRING")

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
# 4. Complex pipeline
# =============================================

run.("4. complex pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{
        "emp_id" => i,
        "dept" => Enum.at(["Eng", "Sales", "Mktg", "Supp"], rem(i, 4)),
        "salary" => 40000 + rem(i * 997, 60000),
        "years" => rem(i, 15) + 1
      }
    end), schema: "emp_id INT, dept STRING, salary INT, years INT")

  result = df
  |> DataFrame.transform(fn d ->
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
    avg(col("salary")) |> Column.alias_("avg_salary")
  ])
  |> DataFrame.sort([asc(col("dept")), desc(col("headcount"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  Enum.take(rows, 5)
end)

# =============================================
# 5. Boolean operations
# =============================================

run.("5a. boolean logic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => true, "b" => false}, %{"a" => false, "b" => true}, %{"a" => true, "b" => true}
  ], schema: "a BOOLEAN, b BOOLEAN")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.and_(col("a"), col("b")) |> Column.alias_("and_"),
    Column.or_(col("a"), col("b")) |> Column.alias_("or_"),
    Column.not_(col("a")) |> Column.alias_("not_a")
  ]) |> DataFrame.collect()
end)

run.("5b. boolean aggregates", fn ->
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
# 6. Column.astype
# =============================================

run.("6. Column.astype", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    Column.astype(col("val"), "DOUBLE") |> Column.alias_("as_double"),
    Column.astype(col("val"), "STRING") |> Column.alias_("as_string")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Complex SQL
# =============================================

run.("7. complex SQL with 10 CTEs", fn ->
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
# 8. DataFrame.to
# =============================================

run.("8. DataFrame.to (cast to schema)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => 42.5}], schema: "id INT, val DOUBLE")
  df |> DataFrame.to("id LONG, val FLOAT") |> DataFrame.dtypes()
end)

# =============================================
# 9. nanvl / typeof / current_date / current_timestamp
# =============================================

run.("9a. nanvl", fn ->
  df = SparkEx.sql(s, "SELECT CAST('NaN' AS DOUBLE) AS a, 42.0 AS b UNION ALL SELECT 1.0, 2.0")
  df |> DataFrame.select([
    col("a"), col("b"),
    nanvl(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("9b. typeof", fn ->
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

run.("9c. current_date/timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    current_date() |> Column.alias_("today"),
    current_timestamp() |> Column.alias_("now")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Array operations
# =============================================

run.("10a. arrays_zip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"names" => ["Alice", "Bob"], "ages" => [30, 25]}
  ], schema: "names ARRAY<STRING>, ages ARRAY<INT>")
  df |> DataFrame.select([
    arrays_zip([col("names"), col("ages")]) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

run.("10b. map_from_arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"keys" => ["a", "b"], "vals" => [1, 2]}
  ], schema: "keys ARRAY<STRING>, vals ARRAY<INT>")
  df |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("mp")
  ]) |> DataFrame.collect()
end)

run.("10c. map_from_entries", fn ->
  df = SparkEx.sql(s, "SELECT array(struct('a', 1), struct('b', 2)) AS entries")
  df |> DataFrame.select([
    map_from_entries(col("entries")) |> Column.alias_("mp")
  ]) |> DataFrame.collect()
end)

run.("10d. element_at with negative index", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    element_at(col("arr"), lit(1)) |> Column.alias_("first"),
    element_at(col("arr"), lit(-1)) |> Column.alias_("last")
  ]) |> DataFrame.collect()
end)

run.("10e. array set operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 2, 3], "b" => [2, 3, 4]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    array_distinct(col("a")) |> Column.alias_("distinct_a"),
    array_union(col("a"), col("b")) |> Column.alias_("union_"),
    array_intersect(col("a"), col("b")) |> Column.alias_("intersect_"),
    array_except(col("a"), col("b")) |> Column.alias_("except_")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. DataFrame.alias_ for qualified access
# =============================================

run.("11. DataFrame.alias_ with qualified column access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  aliased = DataFrame.alias_(df, "people")
  aliased |> DataFrame.select([
    col("people.id") |> Column.alias_("pid"),
    col("people.name") |> Column.alias_("pname")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 41 (safe) complete ===")
