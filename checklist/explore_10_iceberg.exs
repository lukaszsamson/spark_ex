# Exploratory: Iceberg table operations, catalog deep dive, schema evolution
alias SparkEx.{DataFrame, Column, Catalog, Writer}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()

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

fqn = "sfx.sfx_dev_warehouse.spark_ex_ice_#{run_id}"

# === Create an Iceberg table with various types ===
run.("1. Create Iceberg table via SQL", fn ->
  SparkEx.sql(s, """
    CREATE TABLE #{fqn} (
      id INT,
      name STRING,
      salary DECIMAL(10,2),
      hire_date DATE,
      active BOOLEAN,
      tags ARRAY<STRING>,
      metadata MAP<STRING, STRING>
    )
  """) |> DataFrame.collect()
end)

# === Insert data ===
run.("2. Insert rows via SQL", fn ->
  SparkEx.sql(s, """
    INSERT INTO #{fqn} VALUES
      (1, 'Alice', 50000.00, DATE '2020-01-15', true, array('eng', 'lead'), map('level', 'senior')),
      (2, 'Bob', 60000.50, DATE '2019-06-01', true, array('sales'), map('level', 'mid')),
      (3, 'Carol', 55000.75, DATE '2021-03-20', false, array('eng'), map('level', 'junior'))
  """) |> DataFrame.collect()
end)

# === Read back and verify all types decode correctly ===
run.("3. Read back all types", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn} ORDER BY id") |> DataFrame.collect()
end)

# === Schema inspection ===
run.("4. dtypes of Iceberg table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.dtypes()
end)

run.("5. schema of Iceberg table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.schema()
end)

# === insert_into via Writer API ===
run.("6. insert_into via Writer", fn ->
  row = SparkEx.sql(s, """
    SELECT 4 AS id, 'Dave' AS name, CAST(70000.00 AS DECIMAL(10,2)) AS salary,
           DATE '2018-11-10' AS hire_date, true AS active,
           array('hr', 'manager') AS tags, map('level', 'senior') AS metadata
  """)
  DataFrame.write(row) |> Writer.insert_into(fqn)
end)

run.("7. Verify 4 rows", fn ->
  {:ok, cnt} = SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.count()
  cnt
end)

# === ALTER TABLE: add column ===
run.("8. ALTER TABLE ADD COLUMN", fn ->
  SparkEx.sql(s, "ALTER TABLE #{fqn} ADD COLUMNS (department STRING)") |> DataFrame.collect()
end)

run.("9. Verify new column (should be null for existing rows)", fn ->
  SparkEx.sql(s, "SELECT id, name, department FROM #{fqn} ORDER BY id") |> DataFrame.collect()
end)

# === UPDATE via SQL ===
run.("10. UPDATE via SQL", fn ->
  SparkEx.sql(s, "UPDATE #{fqn} SET department = 'Engineering' WHERE id IN (1, 3)") |> DataFrame.collect()
end)

run.("11. Verify update", fn ->
  SparkEx.sql(s, "SELECT id, name, department FROM #{fqn} ORDER BY id") |> DataFrame.collect()
end)

# === DELETE via SQL ===
run.("12. DELETE via SQL", fn ->
  SparkEx.sql(s, "DELETE FROM #{fqn} WHERE active = false") |> DataFrame.collect()
end)

run.("13. Verify delete (should be 3 rows)", fn ->
  {:ok, cnt} = SparkEx.sql(s, "SELECT * FROM #{fqn}") |> DataFrame.count()
  cnt
end)

# === Time travel (Iceberg snapshots) ===
run.("14. Show table history", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn}.history") |> DataFrame.collect()
end)

run.("15. Show table snapshots", fn ->
  SparkEx.sql(s, "SELECT * FROM #{fqn}.snapshots") |> DataFrame.collect()
end)

# === Catalog inspection ===
run.("16. table_exists?", fn ->
  Catalog.table_exists?(s, fqn)
end)

run.("17. get_table", fn ->
  Catalog.get_table(s, "spark_ex_ice_#{run_id}", "sfx_dev_warehouse")
end)

run.("18. list_columns", fn ->
  Catalog.list_columns(s, "spark_ex_ice_#{run_id}", "sfx_dev_warehouse")
end)

# === read table via Reader API ===
run.("19. Read via SparkEx.read |> table", fn ->
  SparkEx.read(s) |> SparkEx.Reader.table(fqn) |> DataFrame.collect()
end)

# === Partitioned Iceberg table ===
part_fqn = "sfx.sfx_dev_warehouse.spark_ex_part_#{run_id}"

run.("20. Create partitioned Iceberg table", fn ->
  SparkEx.sql(s, """
    CREATE TABLE #{part_fqn} (
      id INT, region STRING, amount DOUBLE
    ) PARTITIONED BY (region)
  """) |> DataFrame.collect()
end)

run.("21. Insert into partitioned table", fn ->
  SparkEx.sql(s, """
    INSERT INTO #{part_fqn} VALUES
      (1, 'East', 100.0), (2, 'West', 200.0), (3, 'East', 150.0), (4, 'West', 250.0)
  """) |> DataFrame.collect()
end)

run.("22. Query partitioned table with filter", fn ->
  SparkEx.sql(s, "SELECT * FROM #{part_fqn} WHERE region = 'East' ORDER BY id") |> DataFrame.collect()
end)

run.("23. Show partitions", fn ->
  SparkEx.sql(s, "SELECT * FROM #{part_fqn}.partitions") |> DataFrame.collect()
end)

# === MERGE INTO ===
run.("24. MERGE INTO", fn ->
  SparkEx.sql(s, """
    MERGE INTO #{fqn} AS target
    USING (SELECT 1 AS id, 'Alice Updated' AS name, CAST(55000.00 AS DECIMAL(10,2)) AS salary,
                  DATE '2020-01-15' AS hire_date, true AS active,
                  array('eng', 'lead', 'architect') AS tags,
                  map('level', 'principal') AS metadata,
                  'Engineering' AS department) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """) |> DataFrame.collect()
end)

run.("25. Verify merge", fn ->
  SparkEx.sql(s, "SELECT id, name, department FROM #{fqn} WHERE id = 1") |> DataFrame.collect()
end)

# === Cleanup ===
IO.puts("--- Cleanup ---")
run.("Drop ice table", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{fqn} PURGE") |> DataFrame.collect()
end)

run.("Drop partitioned table", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{part_fqn} PURGE") |> DataFrame.collect()
end)

run.("Verify dropped", fn ->
  Catalog.table_exists?(s, fqn)
end)

IO.puts("=== Iceberg exploration complete ===")
