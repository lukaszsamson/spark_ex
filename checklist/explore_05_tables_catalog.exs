# Exploratory: table creation, catalog, DDL, Writer patterns
alias SparkEx.{DataFrame, Catalog, Writer}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()
io_root = "/tmp/spark_ex_test_#{run_id}"

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

fqn = "sfx.sfx_dev_warehouse.spark_ex_explore_#{run_id}"

# 1. Create table via save_as_table with complex types
run.("1. save_as_table with complex types", fn ->
  complex_df = SparkEx.sql(s, """
    SELECT 1 AS id,
           'Alice' AS name,
           array(1, 2, 3) AS scores,
           map('color', 'red', 'size', 'large') AS attrs,
           named_struct('street', '123 Main St', 'city', 'NYC') AS address
  """)
  DataFrame.write(complex_df) |> Writer.save_as_table(fqn)
end)

# 2. Read it back
run.("2. Read back complex table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.collect()
end)

# 3. insert_into with matching schema
run.("3. insert_into complex types", fn ->
  row2 = SparkEx.sql(s, """
    SELECT 2 AS id,
           'Bob' AS name,
           array(4, 5, 6) AS scores,
           map('color', 'blue', 'size', 'small') AS attrs,
           named_struct('street', '456 Oak Ave', 'city', 'LA') AS address
  """)
  DataFrame.write(row2) |> Writer.insert_into(fqn)
end)

# 4. Verify 2 rows
run.("4. Verify insert (should be 2 rows)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn} ORDER BY id") |> DataFrame.collect()
end)

# 5. save_as_table with mode :append
run.("5. save_as_table append mode", fn ->
  row3 = SparkEx.sql(s, """
    SELECT 3 AS id,
           'Carol' AS name,
           array(7, 8, 9) AS scores,
           map('color', 'green') AS attrs,
           named_struct('street', '789 Pine Rd', 'city', 'CHI') AS address
  """)
  DataFrame.write(row3) |> Writer.mode(:append) |> Writer.save_as_table(fqn)
end)

# 6. Verify 3 rows
run.("6. Verify append (should be 3 rows)", fn ->
  {:ok, cnt} = SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.count()
  cnt
end)

# 7. table_exists? and get_table
run.("7. table_exists?", fn ->
  Catalog.table_exists?(s, fqn)
end)

# 8. list_tables with pattern
run.("8. list_tables with pattern", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse", "spark_ex_explore*")
end)

# 9. Write to parquet with options
run.("9. Write parquet with compression", fn ->
  path = "#{io_root}/explore_compressed"
  df = SparkEx.sql(s, "SELECT * FROM #{fqn}")
  Writer.parquet(df, path, mode: :overwrite, options: [compression: "snappy"])
end)

# 10. Read back with schema
run.("10. Read back parquet", fn ->
  path = "#{io_root}/explore_compressed"
  SparkEx.Reader.parquet(s, path) |> DataFrame.collect()
end)

# 11. CTAS (Create Table As Select) via SQL
ctas_fqn = "sfx.sfx_dev_warehouse.spark_ex_ctas_#{run_id}"
run.("11. CTAS via SQL", fn ->
  SparkEx.sql(s, """
    CREATE TABLE #{ctas_fqn} AS
    SELECT id, name, scores FROM #{fqn} WHERE id <= 2
  """) |> DataFrame.collect()
end)

run.("11b. Verify CTAS", fn ->
  SparkEx.sql(s, "SELECT * FROM #{ctas_fqn}") |> DataFrame.collect()
end)

# 12. ALTER TABLE - add column
run.("12. ALTER TABLE ADD COLUMN", fn ->
  SparkEx.sql(s, "ALTER TABLE #{ctas_fqn} ADD COLUMNS (extra STRING)") |> DataFrame.collect()
end)

run.("12b. Verify altered schema", fn ->
  SparkEx.sql(s, "SELECT * FROM #{ctas_fqn}") |> DataFrame.dtypes()
end)

# 13. Create temp view from table, then query
run.("13. Temp view from table query", fn ->
  df_tbl = SparkEx.sql(s, "SELECT * FROM #{fqn}")
  :ok = DataFrame.create_temp_view(df_tbl, "explore_view")
  SparkEx.sql(s, "SELECT name, size(scores) AS num_scores FROM explore_view") |> DataFrame.collect()
end)

# 14. Write CSV with options
run.("14. Write CSV with options", fn ->
  path = "#{io_root}/explore_csv"
  simple_df = SparkEx.sql(s, "SELECT id, name FROM #{fqn}")
  Writer.csv(simple_df, path, mode: :overwrite, options: [header: "true", sep: "|"])
end)

run.("14b. Read back CSV with options", fn ->
  path = "#{io_root}/explore_csv"
  SparkEx.Reader.csv(s, path, header: true, sep: "|") |> DataFrame.collect()
end)

# 15. Write JSON
run.("15. Write JSON + read back", fn ->
  path = "#{io_root}/explore_json"
  simple_df = SparkEx.sql(s, "SELECT id, name FROM #{fqn}")
  Writer.json(simple_df, path, mode: :overwrite)
  SparkEx.Reader.json(s, path) |> DataFrame.collect()
end)

# Cleanup
IO.puts("--- Cleanup ---")
run.("Drop explore table", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{fqn} PURGE") |> DataFrame.collect()
end)

run.("Drop CTAS table", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{ctas_fqn} PURGE") |> DataFrame.collect()
end)

run.("Drop temp view", fn ->
  DataFrame.drop_temp_view(s, "explore_view")
end)

IO.puts("=== Tables/Catalog exploration complete ===")
