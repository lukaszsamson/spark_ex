# Round 15b: Writer edge cases, catalog operations, MergeIntoWriter,
# checkpoint, schema manipulation, config operations
alias SparkEx.{DataFrame, Column, Catalog, Writer, Reader, MergeIntoWriter}
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

# === 1. Catalog operations ===
run.("1a. Catalog.current_catalog", fn ->
  Catalog.current_catalog(s)
end)

run.("1b. Catalog.list_catalogs", fn ->
  Catalog.list_catalogs(s)
end)

run.("1c. Catalog.list_databases", fn ->
  Catalog.list_databases(s)
end)

run.("1d. Catalog.database_exists?", fn ->
  {
    Catalog.database_exists?(s, "sfx_dev_warehouse"),
    Catalog.database_exists?(s, "nonexistent_db_#{run_id}")
  }
end)

run.("1e. Catalog.table_exists? with FQN", fn ->
  {
    Catalog.table_exists?(s, "sfx.sfx_dev_warehouse.trades"),
    Catalog.table_exists?(s, "sfx.sfx_dev_warehouse.nonexistent_table_#{run_id}")
  }
end)

run.("1f. Catalog.list_tables with pattern", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse")
end)

# === 2. Config operations ===
run.("2a. config_get spark.sql.shuffle.partitions", fn ->
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
end)

run.("2b. config_set and config_get roundtrip", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "10"}])
  result = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  # Reset
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "200"}])
  result
end)

run.("2c. config_is_modifiable", fn ->
  SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions", "spark.master"])
end)

run.("2d. config_get_all", fn ->
  {:ok, all} = SparkEx.config_get_all(s)
  IO.puts("  Total config keys: #{length(all)}")
  {:ok, Enum.take(all, 3)}
end)

# === 3. Writer edge cases ===
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_wr_#{run_id}"

run.("3a. Writer with options: write Parquet with compression", fn ->
  tbl = "#{tbl_base}_parquet"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"},
    %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  df |> DataFrame.write()
  |> Writer.option("compression", "snappy")
  |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3b. Writer save_as_table with partition_by", fn ->
  tbl = "#{tbl_base}_part"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i ->
      %{"id" => i, "region" => "r#{rem(i, 3)}", "val" => i * 10}
    end),
    schema: "id INT, region STRING, val INT")

  df |> DataFrame.write()
  |> Writer.partition_by(["region"])
  |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3c. Writer mode overwrite", fn ->
  tbl = "#{tbl_base}_overwrite"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "first"}
  ], schema: "id INT, val STRING")

  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 100, "val" => "overwritten"}
  ], schema: "id INT, val STRING")

  df2 |> DataFrame.write() |> Writer.mode("overwrite") |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3d. Writer mode append", fn ->
  tbl = "#{tbl_base}_append"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "first"}
  ], schema: "id INT, val STRING")

  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "second"}
  ], schema: "id INT, val STRING")

  df2 |> DataFrame.write() |> Writer.mode("append") |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# === 4. MergeIntoWriter ===
run.("4. MergeIntoWriter: upsert pattern", fn ->
  tbl = "#{tbl_base}_merge"

  # Create target table
  {:ok, target} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 90},
    %{"id" => 2, "name" => "Bob", "score" => 80},
    %{"id" => 3, "name" => "Carol", "score" => 70}
  ], schema: "id INT, name STRING, score INT")

  target |> DataFrame.write() |> Writer.save_as_table(tbl)

  # Create source (updates + inserts)
  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "score" => 95},     # update
    %{"id" => 4, "name" => "Dave", "score" => 85}      # insert
  ], schema: "id INT, name STRING, score INT")

  # Merge
  source
  |> DataFrame.merge_into(tbl)
  |> MergeIntoWriter.on(Column.eq(
    DataFrame.col(source, "id"),
    col("id")  # target column
  ))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

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

# === 12. create_dataframe → temp view → Catalog.list_columns ===
run.("12. Catalog.list_columns on temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "test", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")

  DataFrame.create_or_replace_temp_view(df, "cat_test_view_#{run_id}")
  result = Catalog.list_columns(s, "cat_test_view_#{run_id}")
  DataFrame.drop_temp_view(s, "cat_test_view_#{run_id}")
  result
end)

# === 13. Catalog.is_cached? ===
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

# === 14. DataFrame.checkpoint ===
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

# === 16. SparkEx.tags ===
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

# === 18. create_temp_view duplicate name error ===
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

IO.puts("=== Writer, catalog, and merge edge case tests complete ===")
