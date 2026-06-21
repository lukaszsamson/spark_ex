# Retest blocked items: save_as_table, insert_into, DDL, checkpoint
# Usage: mix run checklist/retest_blocked.exs

alias SparkEx.{DataFrame, Column, Catalog, Writer}
import SparkEx.Functions

IO.puts("=== Retest Blocked Items ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run_id = :erlang.system_time(:millisecond) |> to_string()

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 500)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# First, find a namespace we can use in sfx catalog
run.("List sfx databases", fn ->
  Catalog.list_databases(s)
end)

# === 19.8 save_as_table in sfx catalog ===
IO.puts("--- 19.8 save_as_table (sfx catalog) ---")

table_fqn = "sfx.sfx_dev_warehouse.spark_ex_test_#{run_id}"

run.("19.8 save_as_table (#{table_fqn})", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5) AS t(id, name, salary)")
  DataFrame.write(df) |> Writer.save_as_table(table_fqn)
end)

run.("verify save_as_table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{table_fqn} ORDER BY id") |> DataFrame.collect()
end)

# === 19.9 insert_into ===
run.("19.9 insert_into", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (3, 'Carol', 300.0) AS t(id, name, salary)")
  DataFrame.write(df) |> Writer.insert_into(table_fqn)
end)

run.("verify insert_into (should be 3 rows)", fn ->
  {:ok, count} = SparkEx.sql(s, "SELECT * FROM #{table_fqn}") |> DataFrame.count()
  {:ok, count}
end)

# === 37. DDL with fully qualified names ===
IO.puts("--- 37. DDL (sfx catalog) ---")

ddl_table = "sfx.sfx_dev_warehouse.spark_ex_ddl_#{run_id}"

run.("37.1 CREATE TABLE", fn ->
  SparkEx.sql(s, "CREATE TABLE #{ddl_table} (id INT, name STRING, val DOUBLE)") |> DataFrame.collect()
end)

run.("37.2 INSERT INTO", fn ->
  SparkEx.sql(s, "INSERT INTO #{ddl_table} VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5)") |> DataFrame.collect()
end)

run.("37.3 SELECT", fn ->
  SparkEx.sql(s, "SELECT * FROM #{ddl_table} ORDER BY id") |> DataFrame.collect()
end)

run.("37.4 ALTER TABLE ADD COLUMNS", fn ->
  SparkEx.sql(s, "ALTER TABLE #{ddl_table} ADD COLUMNS (extra STRING)") |> DataFrame.collect()
end)

run.("37.5 table_exists?", fn ->
  Catalog.table_exists?(s, "spark_ex_ddl_#{run_id}", "sfx_dev_warehouse")
end)

run.("37.6 get_table", fn ->
  Catalog.get_table(s, "spark_ex_ddl_#{run_id}", "sfx_dev_warehouse")
end)

# Cleanup - drop test tables
IO.puts("--- Cleanup ---")

run.("Drop save_as_table test", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_fqn}") |> DataFrame.collect()
end)

run.("Drop DDL test", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{ddl_table}") |> DataFrame.collect()
end)

run.("37.8 table_exists? after drop", fn ->
  Catalog.table_exists?(s, "spark_ex_ddl_#{run_id}", "sfx_dev_warehouse")
end)

# === 16.4-16.5 checkpoint (try with spark_catalog) ===
IO.puts("--- 16.4-16.5 checkpoint ---")

run.("16.4 checkpoint", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
  DataFrame.checkpoint(df)
end)

IO.puts("=== Retest Blocked Complete ===")
