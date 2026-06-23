# Round 25b: Verify from_json and merge bugs with correct approaches
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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r25b_#{run_id}"

# =============================================
# 1. from_json with SQL-created source (bypasses create_dataframe)
# =============================================

run.("1a. from_json DDL schema (SQL source)", fn ->
  df = SparkEx.sql(s, "SELECT '{\"name\":\"Alice\",\"age\":30}' AS json_str")
  df |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("1b. from_json STRUCT schema (SQL source)", fn ->
  df = SparkEx.sql(s, "SELECT '{\"name\":\"Alice\",\"age\":30}' AS json_str")
  df |> DataFrame.select([
    from_json(col("json_str"), "STRUCT<name: STRING, age: INT>") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("1c. from_json with PERMISSIVE option (SQL source)", fn ->
  df = SparkEx.sql(s, "SELECT '{\"name\":\"Alice\",\"age\":30}' AS json_str")
  df |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT", %{"mode" => "PERMISSIVE"})
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("1d. from_json with malformed JSON + PERMISSIVE (SQL source)", fn ->
  df = SparkEx.sql(s, "SELECT 'not json' AS json_str")
  df |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT", %{"mode" => "PERMISSIVE"})
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Verify explain correct syntax
# =============================================

run.("2a. explain :simple (correct syntax)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, :simple)
end)

run.("2b. explain :extended", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, :extended)
end)

run.("2c. explain :formatted", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df, :formatted)
end)

# =============================================
# 3. MergeIntoWriter - try all approaches
# =============================================

run.("3a. MergeIntoWriter: merge with SQL MERGE (verify Spark MERGE works)", fn ->
  tbl = "#{tbl_base}_sql_merge"
  src_tbl = "#{tbl_base}_sql_src"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice'), (2, 'Bob')") |> DataFrame.collect()
  SparkEx.sql(s, "CREATE TABLE #{src_tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{src_tbl} VALUES (2, 'Bobby'), (3, 'Carol')") |> DataFrame.collect()

  SparkEx.sql(s, """
    MERGE INTO #{tbl} t
    USING #{src_tbl} s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """) |> DataFrame.collect()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{src_tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3b. MergeIntoWriter: using SQL-based temp view as source", fn ->
  tbl = "#{tbl_base}_merge_view"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, val INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 10), (2, 20)") |> DataFrame.collect()

  # Create temp view from SQL
  source = SparkEx.sql(s, "SELECT 2 AS id, 99 AS val UNION ALL SELECT 3, 30")
  DataFrame.create_or_replace_temp_view(source, "merge_src_#{run_id}")

  # Use the view via Reader.table
  src_df = SparkEx.Reader.table(s, "merge_src_#{run_id}")
  src_df
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "merge_src_#{run_id}")
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3c. MergeIntoWriter: pass condition via .on() separately", fn ->
  tbl = "#{tbl_base}_merge_on"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, val INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 10), (2, 20)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => 99},
    %{"id" => 3, "val" => 30}
  ], schema: "id INT, val INT")

  source
  |> DataFrame.merge_into(tbl)
  |> MergeIntoWriter.on(Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 4. try_make_interval - verify the error
# =============================================

run.("4a. try_make_interval with bare integers", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dummy" => 1}], schema: "dummy INT")
  df |> DataFrame.select([
    try_make_interval(days: 5, hours: 3) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

run.("4b. try_make_interval with lit() wrapped values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dummy" => 1}], schema: "dummy INT")
  df |> DataFrame.select([
    try_make_interval(days: lit(5), hours: lit(3)) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

run.("4c. try_make_interval with column values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"d" => 5, "h" => 3}
  ], schema: "d INT, h INT")

  df |> DataFrame.select([
    try_make_interval(days: col("d"), hours: col("h")) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. MergeIntoWriter.when_not_matched_by_source_update_all
# =============================================

run.("5. when_not_matched_by_source_update_all", fn ->
  tbl = "#{tbl_base}_merge_nmbsua"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, active BOOLEAN) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, true), (2, true), (3, true)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "active" => true}
  ], schema: "id INT, active BOOLEAN")

  # Set unmatched target rows to inactive
  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_not_matched_by_source_update(%{"active" => lit(false)})
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 6. Additional DataFrame operations
# =============================================

run.("6a. DataFrame.take (alias for head with count)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")
  DataFrame.take(df, 3)
end)

run.("6b. DataFrame.tail", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")
  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.tail(3)
end)

run.("6c. DataFrame.first (single row)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 42, "name" => "first_row"}
  ], schema: "id INT, name STRING")
  DataFrame.first(df)
end)

run.("6d. DataFrame.distinct + count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 3}
  ], schema: "val INT")

  {df |> DataFrame.distinct() |> DataFrame.count(), df |> DataFrame.count()}
end)

run.("6e. DataFrame.describe (multiple columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 10.5},
    %{"a" => 2, "b" => 20.5},
    %{"a" => 3, "b" => 30.5}
  ], schema: "a INT, b DOUBLE")
  DataFrame.describe(df) |> DataFrame.collect()
end)

run.("6f. DataFrame.summary", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 4}, %{"val" => 5}
  ], schema: "val INT")
  DataFrame.summary(df) |> DataFrame.collect()
end)

run.("6g. DataFrame.show (string output)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

IO.puts("=== Round 25b tests complete ===")
