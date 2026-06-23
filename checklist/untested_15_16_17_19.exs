# Untested items from sections 15, 16, 17, 19
# Usage: mix run checklist/untested_15_16_17_19.exs

alias SparkEx.{DataFrame, Reader, Writer}

IO.puts("=== Untested: Sections 15, 16, 17, 19 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

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

run_id = :erlang.system_time(:millisecond) |> to_string()

# === 15.5 Create or replace global temp view ===
IO.puts("--- 15.5 create_or_replace_global_temp_view ---")
run.("15.5 create_or_replace_global_temp_view", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS id, 'test' AS val")
  :ok = DataFrame.create_or_replace_global_temp_view(df, "test_global_#{run_id}")
  SparkEx.sql(s, "SELECT * FROM global_temp.test_global_#{run_id}") |> DataFrame.collect()
end)

# === 16.4-16.5 checkpoint ===
IO.puts("--- 16.4-16.5 checkpoint ---")

run.("16.4 checkpoint", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
  case DataFrame.checkpoint(df) do
    {:ok, cdf} -> DataFrame.collect(cdf)
    other -> other
  end
end)

run.("16.5 local_checkpoint", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
  case DataFrame.local_checkpoint(df, []) do
    {:ok, cdf} -> DataFrame.collect(cdf)
    other -> other
  end
end)

# === 17.4-17.5 Read ORC, text ===
IO.puts("--- 17.4-17.5 Read ORC/text ---")

# Write ORC first, then read it
run.("17.4 Read ORC (write then read)", fn ->
  orc_path = "hdfs:///tmp/spark_ex_test_orc_#{run_id}"
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")
  :ok = Writer.orc(df, orc_path, mode: :overwrite)
  Reader.orc(s, orc_path) |> DataFrame.collect()
end)

run.("17.5 Read text", fn ->
  text_path = "hdfs:///tmp/spark_ex_test_text_#{run_id}"
  df = SparkEx.sql(s, "SELECT 'hello world' AS value UNION ALL SELECT 'foo bar'")
  :ok = Writer.text(df, text_path, mode: :overwrite)
  Reader.text(s, text_path) |> DataFrame.collect()
end)

# === 17.8 Read with options ===
run.("17.8 Read CSV with options", fn ->
  csv_path = "hdfs:///tmp/spark_ex_test_csv_opts_#{run_id}"
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")
  :ok = Writer.csv(df, csv_path, mode: :overwrite, header: true)
  Reader.csv(s, csv_path, header: true, infer_schema: true) |> DataFrame.collect()
end)

# === 19.4-19.5 Write ORC/text ===
IO.puts("--- 19.4-19.5 Write ORC/text ---")

run.("19.4 Write ORC", fn ->
  orc_path2 = "hdfs:///tmp/spark_ex_test_orc2_#{run_id}"
  df = SparkEx.sql(s, "SELECT * FROM VALUES (10, 'x'), (20, 'y') AS t(id, val)")
  Writer.orc(df, orc_path2, mode: :overwrite)
end)

run.("19.5 Write text", fn ->
  text_path2 = "hdfs:///tmp/spark_ex_test_text2_#{run_id}"
  df = SparkEx.sql(s, "SELECT 'line one' AS value UNION ALL SELECT 'line two'")
  Writer.text(df, text_path2, mode: :overwrite)
end)

# === 19.7 Write modes ===
IO.puts("--- 19.7 Write modes ---")
base_path = "hdfs:///tmp/spark_ex_test_modes_#{run_id}"

run.("19.7a Write mode :overwrite", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, val)")
  :ok = Writer.parquet(df, base_path, mode: :overwrite)
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (2, 'b') AS t(id, val)")
  :ok = Writer.parquet(df2, base_path, mode: :overwrite)
  Reader.parquet(s, base_path) |> DataFrame.collect()
end)

run.("19.7b Write mode :append", fn ->
  append_path = "hdfs:///tmp/spark_ex_test_append_#{run_id}"
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, val)")
  :ok = Writer.parquet(df1, append_path, mode: :overwrite)
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (2, 'b') AS t(id, val)")
  :ok = Writer.parquet(df2, append_path, mode: :append)
  {:ok, count} = Reader.parquet(s, append_path) |> DataFrame.count()
  {:ok, count}
end)

run.("19.7c Write mode :error_if_exists", fn ->
  err_path = "hdfs:///tmp/spark_ex_test_err_#{run_id}"
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, val)")
  :ok = Writer.parquet(df, err_path, mode: :overwrite)
  # Should fail because path already exists
  Writer.parquet(df, err_path, mode: :error_if_exists)
end)

run.("19.7d Write mode :ignore", fn ->
  ign_path = "hdfs:///tmp/spark_ex_test_ign_#{run_id}"
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'original') AS t(id, val)")
  :ok = Writer.parquet(df1, ign_path, mode: :overwrite)
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (2, 'ignored') AS t(id, val)")
  :ok = Writer.parquet(df2, ign_path, mode: :ignore)
  Reader.parquet(s, ign_path) |> DataFrame.collect()
end)

# === 19.8 save_as_table ===
IO.puts("--- 19.8-19.9 save_as_table / insert_into ---")

run.("19.8 save_as_table", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(id, val)")
  DataFrame.write(df) |> Writer.save_as_table("spark_ex_test_table_#{run_id}")
end)

run.("19.9 insert_into", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (3, 'z') AS t(id, val)")
  DataFrame.write(df) |> Writer.insert_into("spark_ex_test_table_#{run_id}")
end)

# Verify
run.("verify save_as_table + insert_into", fn ->
  SparkEx.sql(s, "SELECT * FROM spark_ex_test_table_#{run_id} ORDER BY id") |> DataFrame.collect()
end)

# Cleanup
run.("cleanup table", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS spark_ex_test_table_#{run_id}") |> DataFrame.collect()
end)

IO.puts("=== Sections 15, 16, 17, 19 Complete ===")
