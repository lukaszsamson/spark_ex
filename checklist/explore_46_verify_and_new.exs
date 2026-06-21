# Round 25: Verify from_json/merge bugs + new untested APIs
alias SparkEx.{DataFrame, Column, MergeIntoWriter}
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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r25_#{run_id}"

# =============================================
# 1. Verify from_json via SQL (bypass create_dataframe encoding)
# =============================================

run.("1a. from_json via SQL literal (verify schema format)", fn ->
  SparkEx.sql(s, """
    SELECT from_json('{"name":"Alice","age":30}', 'name STRING, age INT') AS parsed
  """) |> DataFrame.collect()
end)

run.("1b. from_json via SQL with STRUCT schema", fn ->
  SparkEx.sql(s, """
    SELECT from_json('{"name":"Alice","age":30}', 'STRUCT<name: STRING, age: INT>') AS parsed
  """) |> DataFrame.collect()
end)

run.("1c. from_json via SparkEx with STRUCT schema format", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => ~s({"name":"Alice","age":30})}
  ], schema: "json_str STRING")

  df |> DataFrame.select([
    from_json(col("json_str"), "STRUCT<name: STRING, age: INT>") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("1d. from_json options with STRUCT schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => ~s({"name":"Alice","age":30})}
  ], schema: "json_str STRING")

  df |> DataFrame.select([
    from_json(col("json_str"), "STRUCT<name: STRING, age: INT>", %{"mode" => "PERMISSIVE"})
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("1e. verify json_str value via SQL after create_dataframe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => ~s({"name":"Alice","age":30})}
  ], schema: "json_str STRING")

  # Just collect the raw string to see if it's corrupted
  df |> DataFrame.collect()
end)

# =============================================
# 2. MergeIntoWriter with qualified column names
# =============================================

run.("2a. MergeIntoWriter with source alias + qualified cols", fn ->
  tbl = "#{tbl_base}_merge_q1"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice', 90), (2, 'Bob', 80)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "score" => 95},
    %{"id" => 3, "name" => "Carol", "score" => 85}
  ], schema: "id INT, name STRING, score INT")

  # Alias the source and use qualified names
  source_aliased = DataFrame.alias_(source, "src")
  source_aliased
  |> DataFrame.merge_into(tbl, Column.eq(col("src.id"), col("#{tbl}.id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2b. MergeIntoWriter with short table name in condition", fn ->
  tbl = "#{tbl_base}_merge_q2"
  short_tbl = "spark_ex_r25_#{run_id}_merge_q2"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, val INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 10), (2, 20)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => 99},
    %{"id" => 3, "val" => 30}
  ], schema: "id INT, val INT")

  source_aliased = DataFrame.alias_(source, "s")
  source_aliased
  |> DataFrame.merge_into(tbl, Column.eq(col("s.id"), col("#{short_tbl}.id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2c. MergeIntoWriter using SQL-based source (not create_dataframe)", fn ->
  tbl = "#{tbl_base}_merge_q3"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice'), (2, 'Bob')") |> DataFrame.collect()

  # Use SQL-based source that has known plan structure
  source = SparkEx.sql(s, "SELECT 2 AS id, 'Bobby' AS name UNION ALL SELECT 3, 'Carol'")
  source_aliased = DataFrame.alias_(source, "src")

  source_aliased
  |> DataFrame.merge_into(tbl, Column.eq(col("src.id"), col("id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 3. DataFrame.with_columns_renamed
# =============================================

run.("3a. with_columns_renamed (map variant)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"old_name" => 1, "another_col" => 2}
  ], schema: "old_name INT, another_col INT")

  df |> DataFrame.with_columns_renamed(%{"old_name" => "new_name"}) |> DataFrame.collect()
end)

run.("3b. with_columns_renamed (function variant)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"camelCase" => 1, "AnotherCase" => 2}
  ], schema: "camelCase INT, AnotherCase INT")

  df |> DataFrame.with_columns_renamed(fn name ->
    name |> Macro.underscore()
  end) |> DataFrame.collect()
end)

# =============================================
# 4. DataFrame.transform
# =============================================

run.("4. DataFrame.transform (pipeline helper)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.transform(fn d ->
    DataFrame.filter(d, Column.gt(col("val"), lit(5)))
  end)
  |> DataFrame.transform(fn d ->
    DataFrame.select(d, [col("id")])
  end)
  |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.input_files
# =============================================

run.("5a. input_files on created dataframe (empty)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.input_files(df)
end)

run.("5b. input_files on table-backed dataframe", fn ->
  tbl = "#{tbl_base}_input_files"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  table_df = SparkEx.Reader.table(s, tbl)
  result = DataFrame.input_files(table_df)
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 6. Tags (add_tag, remove_tag, get_tags, clear_tags)
# =============================================

run.("6a. add_tag + get_tags", fn ->
  SparkEx.add_tag(s, "test_tag_#{run_id}")
  tags = SparkEx.get_tags(s)
  SparkEx.remove_tag(s, "test_tag_#{run_id}")
  tags
end)

run.("6b. add multiple tags + clear_tags", fn ->
  SparkEx.add_tag(s, "tag_a_#{run_id}")
  SparkEx.add_tag(s, "tag_b_#{run_id}")
  before = SparkEx.get_tags(s)
  SparkEx.clear_tags(s)
  after_ = SparkEx.get_tags(s)
  {before, after_}
end)

# =============================================
# 7. Interrupt operations
# =============================================

run.("7. interrupt_all (no running ops)", fn ->
  SparkEx.interrupt_all(s)
end)

# =============================================
# 8. DataFrame.na() sub-operations
# =============================================

run.("8a. na().drop (default — any)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"},
    %{"a" => nil, "b" => "y"},
    %{"a" => 3, "b" => nil}
  ], schema: "a INT, b STRING")

  DataFrame.na(df) |> DataFrame.dropna() |> DataFrame.collect()
end)

run.("8b. na().fill (default value)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil}
  ], schema: "a INT, b STRING")

  DataFrame.na(df) |> DataFrame.fillna(%{"a" => 0, "b" => "unknown"}) |> DataFrame.collect()
end)

run.("8c. na().replace", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  DataFrame.na(df) |> DataFrame.replace(%{1 => 100, 2 => 200}) |> DataFrame.collect()
end)

# =============================================
# 9. DataFrame.stat() sub-operations
# =============================================

run.("9a. stat().corr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"x" => i, "y" => i * 2 + :rand.uniform(5)} end),
    schema: "x INT, y INT")

  DataFrame.stat(df) |> DataFrame.corr("x", "y")
end)

run.("9b. stat().cov", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"x" => i, "y" => i * 2} end),
    schema: "x INT, y INT")

  DataFrame.stat(df) |> DataFrame.cov("x", "y")
end)

run.("9c. stat().approx_quantile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  DataFrame.stat(df) |> DataFrame.approx_quantile("val", [0.25, 0.5, 0.75], 0.01)
end)

run.("9d. stat().freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1}, %{"a" => 1}, %{"a" => 1}, %{"a" => 2}, %{"a" => 3}
  ], schema: "a INT")

  DataFrame.stat(df) |> DataFrame.freq_items(["a"], 0.4) |> DataFrame.collect()
end)

run.("9e. stat().crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"gender" => "M", "dept" => "Eng"},
    %{"gender" => "F", "dept" => "Eng"},
    %{"gender" => "M", "dept" => "HR"},
    %{"gender" => "F", "dept" => "HR"},
    %{"gender" => "F", "dept" => "Eng"}
  ], schema: "gender STRING, dept STRING")

  DataFrame.stat(df) |> DataFrame.crosstab("gender", "dept") |> DataFrame.collect()
end)

run.("9f. stat().sample_by", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"label" => rem(i, 3)} end),
    schema: "label INT")

  DataFrame.stat(df) |> DataFrame.sample_by(col("label"), %{0 => 0.5, 1 => 0.5, 2 => 0.5}) |> DataFrame.collect()
end)

# =============================================
# 10. DataFrame.with_watermark (for streaming)
# =============================================

run.("10. with_watermark on batch DF (should work or error gracefully)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"event_time" => "2025-01-01 10:00:00", "value" => 1}
  ], schema: "event_time STRING, value INT")

  df
  |> DataFrame.select([Column.cast(col("event_time"), "TIMESTAMP") |> Column.alias_("event_time"), col("value")])
  |> DataFrame.with_watermark("event_time", "10 minutes")
  |> DataFrame.collect()
end)

# =============================================
# 11. StreamReader.rate (memory-safe stream source)
# =============================================

run.("11. StreamReader.rate basic", fn ->
  rate_df = SparkEx.StreamReader.rate(s, rows_per_second: 10)
  DataFrame.is_streaming?(rate_df)
end)

# =============================================
# 12. DataFrame.drop_duplicates_within_watermark
# =============================================

run.("12. drop_duplicates_within_watermark on batch (should error gracefully)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}
  ], schema: "id INT, val INT")

  df |> DataFrame.drop_duplicates_within_watermark([col("id")]) |> DataFrame.collect()
end)

# =============================================
# 13. Progress handler registration
# =============================================

run.("13. register + remove progress handler", fn ->
  handler = fn progress -> IO.inspect(progress, label: "  progress") end
  SparkEx.register_progress_handler(s, handler)
  # Do a simple query to trigger progress
  {:ok, _} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  SparkEx.remove_progress_handler(s, handler)
  :ok
end)

# =============================================
# 14. SparkEx.is_stopped
# =============================================

run.("14. is_stopped on active session", fn ->
  SparkEx.is_stopped(s)
end)

# =============================================
# 15. DataFrame.columns, dtypes, schema, print_schema
# =============================================

run.("15a. DataFrame.columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => "x", "c" => 3.14}],
    schema: "a INT, b STRING, c DOUBLE")
  DataFrame.columns(df)
end)

run.("15b. DataFrame.dtypes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => "x"}], schema: "a INT, b STRING")
  DataFrame.dtypes(df)
end)

run.("15c. DataFrame.schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1}], schema: "a INT")
  DataFrame.schema(df)
end)

run.("15d. DataFrame.print_schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice", "scores" => [90, 85]}}
  ], schema: "id INT, info STRUCT<name: STRING, scores: ARRAY<INT>>")
  DataFrame.print_schema(df)
end)

# =============================================
# 16. DataFrame.explain variants
# =============================================

run.("16a. explain :simple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, mode: :simple)
end)

run.("16b. explain :extended", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, mode: :extended)
end)

run.("16c. explain :cost", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, mode: :cost)
end)

run.("16d. explain :codegen", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, mode: :codegen)
end)

run.("16e. explain :formatted", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, mode: :formatted)
end)

# =============================================
# 17. DataFrame.to (cast types)
# =============================================

run.("17a. DataFrame.to (INT -> BIGINT)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.to("id BIGINT") |> DataFrame.dtypes()
end)

run.("17b. DataFrame.to (INT -> STRING)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.to("id STRING") |> DataFrame.collect()
end)

# =============================================
# 18. DataFrame.offset (skip N rows — Spark 3.4+)
# =============================================

run.("18. DataFrame.offset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.offset(5) |> DataFrame.collect()
end)

# =============================================
# 19. Column.isin with literals
# =============================================

run.("19a. isin with list of literals", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 4}, %{"val" => 5}
  ], schema: "val INT")

  df |> DataFrame.filter(Column.isin(col("val"), [1, 3, 5])) |> DataFrame.collect()
end)

run.("19b. isin with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}
  ], schema: "val INT")

  df |> DataFrame.filter(Column.isin(col("val"), [])) |> DataFrame.collect()
end)

# =============================================
# 20. Column.between edge cases
# =============================================

run.("20a. between with same lower and upper", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 5}, %{"val" => 10}
  ], schema: "val INT")

  df |> DataFrame.filter(Column.between(col("val"), 5, 5)) |> DataFrame.collect()
end)

# =============================================
# 21. GroupedData.pivot with values
# =============================================

run.("21. GroupedData.pivot with explicit values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "quarter" => "Q1", "revenue" => 100},
    %{"dept" => "Eng", "quarter" => "Q2", "revenue" => 150},
    %{"dept" => "HR", "quarter" => "Q1", "revenue" => 50},
    %{"dept" => "HR", "quarter" => "Q2", "revenue" => 60}
  ], schema: "dept STRING, quarter STRING, revenue INT")

  DataFrame.group_by(df, [col("dept")])
  |> SparkEx.GroupedData.pivot("quarter", ["Q1", "Q2"])
  |> SparkEx.GroupedData.agg([sum(col("revenue")) |> Column.alias_("rev")])
  |> DataFrame.collect()
end)

# =============================================
# 22. DataFrame.exceptAll / intersectAll
# =============================================

run.("22a. exceptAll (preserves duplicates)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 3}
  ], schema: "val INT")

  DataFrame.except_all(df1, df2) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

run.("22b. intersectAll (preserves duplicates)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}
  ], schema: "val INT")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 1}
  ], schema: "val INT")

  DataFrame.intersect_all(df1, df2) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

# =============================================
# 23. Column.cast with complex types
# =============================================

run.("23a. cast INT to DECIMAL(10,2)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    Column.cast(col("val"), "DECIMAL(10,2)") |> Column.alias_("dec_val")
  ]) |> DataFrame.collect()
end)

run.("23b. cast STRING to DATE", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dt" => "2025-06-15"}], schema: "dt STRING")
  df |> DataFrame.select([
    Column.cast(col("dt"), "DATE") |> Column.alias_("date_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 24. DataFrame.hint with multiple args
# =============================================

run.("24a. hint broadcast", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1, "v" => "x"}], schema: "id INT, v STRING")

  DataFrame.join(DataFrame.hint(df2, "broadcast"), df1, ["id"]) |> DataFrame.collect()
end)

run.("24b. hint repartition with num partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  df |> DataFrame.hint("repartition", [4]) |> DataFrame.collect()
end)

# =============================================
# 25. DataFrame.unpivot (melt)
# =============================================

run.("25. unpivot multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "q1" => 100, "q2" => 200, "q3" => 300}
  ], schema: "id INT, q1 INT, q2 INT, q3 INT")

  DataFrame.unpivot(df, [col("id")], [col("q1"), col("q2"), col("q3")], "quarter", "revenue")
  |> DataFrame.collect()
end)

IO.puts("=== Round 25 tests complete ===")
