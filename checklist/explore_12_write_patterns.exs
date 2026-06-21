# Exploratory: Write API patterns, partitionBy, modes, format options
alias SparkEx.{DataFrame, Column, Writer, Catalog}
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

base_fqn = "sfx.sfx_dev_warehouse.spark_ex_wr"

# === save_as_table with overwrite mode ===
tbl1 = "#{base_fqn}_overwrite_#{run_id}"

run.("1. save_as_table (create initial)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")
  |> DataFrame.write()
  |> Writer.save_as_table(tbl1)
end)

run.("2. Verify initial data", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl1} ORDER BY id") |> DataFrame.collect()
end)

run.("3. save_as_table with mode(:overwrite)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (10, 'Xavier'), (20, 'Yuki') AS t(id, name)")
  |> DataFrame.write()
  |> Writer.mode(:overwrite)
  |> Writer.save_as_table(tbl1)
end)

run.("4. Verify overwrite (should be Xavier/Yuki only)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl1} ORDER BY id") |> DataFrame.collect()
end)

# === save_as_table with append mode ===
run.("5. save_as_table with mode(:append)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (30, 'Zara') AS t(id, name)")
  |> DataFrame.write()
  |> Writer.mode(:append)
  |> Writer.save_as_table(tbl1)
end)

run.("6. Verify append (should be 3 rows)", fn ->
  {:ok, cnt} = SparkEx.sql(s, "SELECT * FROM #{tbl1}") |> DataFrame.count()
  cnt
end)

# === save_as_table with partition_by ===
tbl2 = "#{base_fqn}_partby_#{run_id}"

run.("7. save_as_table with partition_by", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'East', 100), (2, 'West', 200), (3, 'East', 150), (4, 'West', 250)
    AS t(id, region, amount)
  """)
  |> DataFrame.write()
  |> Writer.partition_by(["region"])
  |> Writer.save_as_table(tbl2)
end)

run.("8. Verify partitioned table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl2} ORDER BY id") |> DataFrame.collect()
end)

run.("9. Show partitions of partitioned table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl2}.partitions") |> DataFrame.collect()
end)

# === insert_into with different source schemas ===
run.("10. insert_into existing table", fn ->
  SparkEx.sql(s, "SELECT 5 AS id, 'East' AS region, 500 AS amount")
  |> DataFrame.write()
  |> Writer.insert_into(tbl2)
end)

run.("11. Verify insert (should be 5 rows)", fn ->
  {:ok, cnt} = SparkEx.sql(s, "SELECT * FROM #{tbl2}") |> DataFrame.count()
  cnt
end)

# === Writer.option ===
tbl3 = "#{base_fqn}_opts_#{run_id}"

run.("12. save_as_table with Writer.option", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
  |> DataFrame.write()
  |> Writer.option("mergeSchema", "true")
  |> Writer.save_as_table(tbl3)
end)

run.("13. Verify option table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl3}") |> DataFrame.collect()
end)

# === Writer.sort_by ===
run.("14. Writer.sort_by", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (3,'c'), (1,'a'), (2,'b') AS t(id, val)
  """)
  |> DataFrame.write()
  |> Writer.sort_by(["id"])
  |> Writer.mode(:overwrite)
  |> Writer.save_as_table(tbl3)
end)

run.("15. Verify sort_by", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl3}") |> DataFrame.collect()
end)

# === Writer.bucket_by ===
tbl4 = "#{base_fqn}_bucket_#{run_id}"

run.("16. Writer.bucket_by", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'a',10), (2,'b',20), (3,'c',30), (4,'a',40) AS t(id, grp, val)
  """)
  |> DataFrame.write()
  |> Writer.bucket_by(2, ["grp"])
  |> Writer.save_as_table(tbl4)
end)

run.("17. Verify bucket_by table", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl4}") |> DataFrame.collect()
end)

# === CTAS with complex types via Writer ===
tbl5 = "#{base_fqn}_complex_#{run_id}"

run.("18. save_as_table with arrays and structs", fn ->
  SparkEx.sql(s, """
    SELECT
      1 AS id,
      array(1, 2, 3) AS nums,
      named_struct('name', 'Alice', 'age', 30) AS info,
      map('k1', 'v1', 'k2', 'v2') AS meta
  """)
  |> DataFrame.write()
  |> Writer.save_as_table(tbl5)
end)

run.("19. Read back complex types", fn ->
  SparkEx.sql(s, "SELECT * FROM #{tbl5}") |> DataFrame.collect()
end)

# === Write empty DataFrame ===
tbl6 = "#{base_fqn}_empty_#{run_id}"

run.("20. save_as_table with empty DataFrame", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, name) WHERE false")
  |> DataFrame.write()
  |> Writer.save_as_table(tbl6)
end)

run.("21. Verify empty table exists and has schema", fn ->
  exists = Catalog.table_exists?(s, tbl6)
  schema = SparkEx.sql(s, "SELECT * FROM #{tbl6}") |> DataFrame.dtypes()
  cnt = SparkEx.sql(s, "SELECT * FROM #{tbl6}") |> DataFrame.count()
  %{exists: exists, schema: schema, count: cnt}
end)

# === Cleanup ===
IO.puts("--- Cleanup ---")
tables = [tbl1, tbl2, tbl3, tbl4, tbl5, tbl6]
for t <- tables do
  run.("Drop #{t}", fn ->
    SparkEx.sql(s, "DROP TABLE IF EXISTS #{t} PURGE") |> DataFrame.collect()
  end)
end

IO.puts("=== Write patterns exploration complete ===")
