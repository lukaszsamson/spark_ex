# Untested items from sections 31-35, 36, 37-39
# Usage: mix run checklist/untested_31_35_36_37.exs

alias SparkEx.{DataFrame, Column, Catalog}
import SparkEx.Functions

IO.puts("=== Untested: Sections 31-35, 36, 37-39 ===\n")

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

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 300) AS t(id, name, salary)")

# === 31. Hints ===
IO.puts("--- 31. Hints ---")

run.("31.1 broadcast hint", fn ->
  DataFrame.hint(df, "broadcast") |> DataFrame.collect()
end)

run.("31.2 coalesce hint", fn ->
  DataFrame.hint(df, "coalesce", [2]) |> DataFrame.collect()
end)

run.("31.3 repartition hint", fn ->
  DataFrame.hint(df, "repartition", [4]) |> DataFrame.collect()
end)

# === 32. Observations ===
IO.puts("--- 32. Observations ---")

run.("32.1 observe", fn ->
  DataFrame.observe(df, "test_obs", [
    count(lit(1)) |> Column.alias_("cnt"),
    sum(col("salary")) |> Column.alias_("total_sal")
  ]) |> DataFrame.collect()
end)

# === 34. Progress handlers ===
IO.puts("--- 34. Progress handlers ---")

run.("34.1 execution_info (last metrics)", fn ->
  # Execute something first to populate metrics
  exec_df = SparkEx.sql(s, "SELECT COUNT(*) FROM VALUES (1), (2), (3) AS t(x)")
  {:ok, _} = DataFrame.collect(exec_df)
  DataFrame.execution_info(exec_df)
end)

# === 35. Interruption ===
IO.puts("--- 35. Interruption ---")

run.("35.1 interrupt_all", fn ->
  SparkEx.interrupt_all(s)
end)

run.("35.2 interrupt_tag", fn ->
  SparkEx.interrupt_tag(s, "nonexistent_tag")
end)

# === 36.3 Cast failure ===
IO.puts("--- 36.3 Cast failure ---")

run.("36.3 try_cast failure", fn ->
  SparkEx.sql(s, "SELECT 'not_a_number' AS val")
  |> DataFrame.select([col("val") |> Column.try_cast("INT") |> Column.alias_("casted")])
  |> DataFrame.collect()
end)

run.("36.3b cast failure (strict)", fn ->
  SparkEx.sql(s, "SELECT 'not_a_number' AS val")
  |> DataFrame.select([col("val") |> Column.cast("INT") |> Column.alias_("casted")])
  |> DataFrame.collect()
end)

# === 37. DDL ===
IO.puts("--- 37. DDL ---")

table_name = "spark_ex_ddl_test_#{run_id}"

run.("37.1 CREATE TABLE via SQL", fn ->
  SparkEx.sql(s, "CREATE TABLE #{table_name} (id INT, name STRING, val DOUBLE) USING parquet") |> DataFrame.collect()
end)

run.("37.2 INSERT INTO via SQL", fn ->
  SparkEx.sql(s, "INSERT INTO #{table_name} VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5)") |> DataFrame.collect()
end)

run.("37.3 SELECT from created table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{table_name}") |> DataFrame.collect()
end)

run.("37.4 ALTER TABLE via SQL", fn ->
  SparkEx.sql(s, "ALTER TABLE #{table_name} ADD COLUMNS (extra STRING)") |> DataFrame.collect()
end)

run.("37.5 table_exists", fn ->
  Catalog.table_exists?(s, table_name)
end)

run.("37.6 get_table", fn ->
  Catalog.get_table(s, table_name)
end)

run.("37.7 DROP TABLE via SQL", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
end)

run.("37.8 table_exists after drop", fn ->
  Catalog.table_exists?(s, table_name)
end)

# === 38. Table-Valued Functions ===
IO.puts("--- 38. TVF ---")

run.("38.1 range TVF via SQL", fn ->
  SparkEx.sql(s, "SELECT * FROM range(10)") |> DataFrame.collect()
end)

run.("38.2 explode TVF via SQL", fn ->
  SparkEx.sql(s, "SELECT col FROM (SELECT array(1,2,3) AS arr) LATERAL VIEW explode(arr) AS col") |> DataFrame.collect()
end)

# === 39. UDF ===
IO.puts("--- 39. UDF ---")

run.("39.1 UDF via SQL (register class)", fn ->
  SparkEx.sql(s, "CREATE OR REPLACE TEMPORARY FUNCTION double_it AS 'org.apache.spark.sql.catalyst.expressions.Multiply'") |> DataFrame.collect()
end)

# Python UDFs are not available in Spark Connect without Python
run.("39.2 SQL-based UDF", fn ->
  SparkEx.sql(s, "CREATE OR REPLACE TEMPORARY FUNCTION add_ten(x INT) RETURNS INT RETURN x + 10") |> DataFrame.collect()
  SparkEx.sql(s, "SELECT add_ten(5) AS result") |> DataFrame.collect()
end)

# === 33. Artifacts (run last - can crash due to BUG-4 GRPC issue) ===
IO.puts("--- 33. Artifacts (isolated) ---")

task = Task.async(fn ->
  {:ok, s2} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
  content = "hello from spark_ex"
  result = SparkEx.add_artifacts(s2, [{"test_artifact_#{run_id}.txt", content}])
  result
end)

case Task.yield(task, 15_000) || Task.shutdown(task) do
  {:ok, result} -> IO.inspect(result, label: "  33.1 add_artifacts result")
  {:exit, reason} -> IO.puts("  33.1 add_artifacts CRASHED: #{inspect(reason)}")
  nil -> IO.puts("  33.1 add_artifacts timed out")
end
IO.puts("")

IO.puts("=== Sections 31-35, 36, 37-39 Complete ===")
