# Round 15b: Writer edge cases, catalog operations, checkpoint, config, views
# (MergeIntoWriter test moved to separate script due to session crash risk)
alias SparkEx.{DataFrame, Column, Catalog, Writer, Reader}
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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_wr_#{run_id}"

# === 5. DataFrame.to schema cast ===
run.("5. DataFrame.to: cast entire schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "amount" => 42}
  ], schema: "id INT, amount INT")

  df
  |> DataFrame.to("id BIGINT, amount DOUBLE")
  |> DataFrame.dtypes()
end)

# === 6. SparkEx.spark_version ===
run.("6. spark_version", fn ->
  SparkEx.spark_version(s)
end)

# === 7. DataFrame.schema ===
run.("7. DataFrame.schema on complex types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b"], "meta" => %{"k" => "v"}}
  ], schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>")
  DataFrame.schema(df)
end)

# === 8. DataFrame.print_schema ===
run.("8. DataFrame.print_schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "test", "score" => 42}}
  ], schema: "id INT, info STRUCT<name: STRING, score: INT>")
  DataFrame.print_schema(df)
end)

# === 9. DataFrame.show ===
run.("9. DataFrame.show", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

# === 10. Reader.table ===
run.("10. Reader.table", fn ->
  tbl = "#{tbl_base}_reader"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "test"}
  ], schema: "id INT, val STRING")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s) |> Reader.table(tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# === 11. DataFrame.input_files on table ===
run.("11. input_files", fn ->
  tbl = "#{tbl_base}_ifiles"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}
  ], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.input_files()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# === 12. Catalog.list_columns on temp view ===
run.("12. Catalog.list_columns on temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "test", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")

  DataFrame.create_or_replace_temp_view(df, "cat_test_view_#{run_id}")
  result = Catalog.list_columns(s, "cat_test_view_#{run_id}")
  DataFrame.drop_temp_view(s, "cat_test_view_#{run_id}")
  result
end)

# === 13. Catalog.is_cached? and cache_table/uncache_table ===
run.("13. Catalog.is_cached? and cache_table/uncache_table", fn ->
  tbl = "#{tbl_base}_cache"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}
  ], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  before = Catalog.is_cached?(s, tbl)
  Catalog.cache_table(s, tbl)
  during = Catalog.is_cached?(s, tbl)
  Catalog.uncache_table(s, tbl)
  after_ = Catalog.is_cached?(s, tbl)

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  {before, during, after_}
end)

# === 14. DataFrame.local_checkpoint ===
run.("14. DataFrame.local_checkpoint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end),
    schema: "id INT")

  cp = DataFrame.local_checkpoint(df)
  DataFrame.collect(cp)
end)

# === 15. SparkEx.interrupt_all ===
run.("15. interrupt_all (no running ops)", fn ->
  SparkEx.interrupt_all(s)
end)

# === 16. Tags lifecycle ===
run.("16. Tags: add, get, remove, clear", fn ->
  SparkEx.add_tag(s, "test_tag_1")
  SparkEx.add_tag(s, "test_tag_2")
  tags_before = SparkEx.get_tags(s)
  SparkEx.remove_tag(s, "test_tag_1")
  tags_after = SparkEx.get_tags(s)
  SparkEx.clear_tags(s)
  tags_cleared = SparkEx.get_tags(s)
  {tags_before, tags_after, tags_cleared}
end)

# === 17. Temp view lifecycle ===
run.("17. create_temp_view → create_or_replace → drop", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")

  view_name = "lifecycle_view_#{run_id}"
  DataFrame.create_temp_view(df1, view_name)
  {:ok, r1} = SparkEx.sql(s, "SELECT * FROM #{view_name}") |> DataFrame.collect()

  DataFrame.create_or_replace_temp_view(df2, view_name)
  {:ok, r2} = SparkEx.sql(s, "SELECT * FROM #{view_name}") |> DataFrame.collect()

  DataFrame.drop_temp_view(s, view_name)
  {r1, r2}
end)

# === 18. create_temp_view duplicate error ===
run.("18. create_temp_view duplicate name error", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")

  view_name = "dup_view_#{run_id}"
  DataFrame.create_temp_view(df1, view_name)
  result = DataFrame.create_temp_view(df2, view_name)
  DataFrame.drop_temp_view(s, view_name)
  result
end)

# === 19. Global temp view ===
run.("19. Global temp view lifecycle", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 42}], schema: "id INT")
  view_name = "global_view_#{run_id}"

  DataFrame.create_or_replace_global_temp_view(df, view_name)
  result = SparkEx.sql(s, "SELECT * FROM global_temp.#{view_name}") |> DataFrame.collect()
  DataFrame.drop_global_temp_view(s, view_name)
  result
end)

# === 20. DataFrame.tree_string ===
run.("20. tree_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "test"}}
  ], schema: "id INT, info STRUCT<name: STRING>")
  DataFrame.tree_string(df)
end)

# === 21. Catalog.get_database ===
run.("21. Catalog.get_database", fn ->
  Catalog.get_database(s, "sfx_dev_warehouse")
end)

# === 22. Catalog.function_exists? ===
run.("22. Catalog.function_exists?", fn ->
  {
    Catalog.function_exists?(s, "concat"),
    Catalog.function_exists?(s, "nonexistent_function_#{run_id}")
  }
end)

# === 23. Catalog.clear_cache ===
run.("23. Catalog.clear_cache", fn ->
  Catalog.clear_cache(s)
end)

# === 24. Catalog.refresh_table ===
run.("24. Catalog.refresh_table (on temp view)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  view_name = "refresh_test_#{run_id}"
  DataFrame.create_or_replace_temp_view(df, view_name)
  result = Catalog.refresh_table(s, view_name)
  DataFrame.drop_temp_view(s, view_name)
  result
end)

# === 25. MergeIntoWriter via SQL (workaround) ===
run.("25. MERGE INTO via SQL (safe approach)", fn ->
  tbl = "#{tbl_base}_merge_sql"

  SparkEx.sql(s, """
    CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg
  """) |> DataFrame.collect()

  SparkEx.sql(s, """
    INSERT INTO #{tbl} VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Carol', 70)
  """) |> DataFrame.collect()

  SparkEx.sql(s, """
    MERGE INTO #{tbl} AS t
    USING (SELECT * FROM VALUES (2, 'Bob', 95), (4, 'Dave', 85) AS s(id, name, score)) AS s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """) |> DataFrame.collect()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

IO.puts("=== Writer, catalog edge case tests complete ===")
