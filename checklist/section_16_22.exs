# Sections 16-22: Caching, Reading, Writing, Partitioning, Reshape
# Usage: mix run checklist/section_16_22.exs

alias SparkEx.{DataFrame, Column, Reader, Writer, Catalog}
import SparkEx.Functions

IO.puts("=== Sections 16-22 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run_id = :erlang.system_time(:millisecond) |> to_string()
tmp_base = "/tmp/spark_ex_test_#{run_id}"

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 300)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# === Section 16: DataFrame-Level Caching ===
IO.puts("--- Section 16: DataFrame-Level Caching ---\n")

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")

run.("16.1 persist", fn ->
  cached = DataFrame.persist(df)
  DataFrame.collect(cached)
end)

run.("16.2 storage_level", fn ->
  cached = DataFrame.persist(df)
  DataFrame.storage_level(cached)
end)

run.("16.3 unpersist", fn ->
  cached = DataFrame.persist(df)
  unpersisted = DataFrame.unpersist(cached)
  DataFrame.collect(unpersisted)
end)

# === Section 17: Reading from Different Formats ===
IO.puts("--- Section 17: Reading ---\n")

# First write test data
src = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, val)")

run.("17.0 Write test data (parquet)", fn ->
  src |> DataFrame.write() |> Writer.format("parquet") |> Writer.mode(:overwrite) |> Writer.save("#{tmp_base}/read_test.parquet")
end)

run.("17.0 Write test data (csv)", fn ->
  src |> DataFrame.write() |> Writer.format("csv") |> Writer.mode(:overwrite) |> Writer.option("header", "true") |> Writer.save("#{tmp_base}/read_test.csv")
end)

run.("17.0 Write test data (json)", fn ->
  src |> DataFrame.write() |> Writer.format("json") |> Writer.mode(:overwrite) |> Writer.save("#{tmp_base}/read_test.json")
end)

run.("17.1 Read Parquet", fn ->
  Reader.parquet(s, "#{tmp_base}/read_test.parquet") |> DataFrame.collect()
end)

run.("17.2 Read CSV", fn ->
  Reader.csv(s, "#{tmp_base}/read_test.csv", header: true, infer_schema: true) |> DataFrame.collect()
end)

run.("17.3 Read JSON", fn ->
  Reader.json(s, "#{tmp_base}/read_test.json") |> DataFrame.collect()
end)

run.("17.7 Read with explicit schema", fn ->
  SparkEx.read(s) |> Reader.format("csv") |> Reader.schema("id INT, val STRING") |> Reader.option("header", "true") |> Reader.load("#{tmp_base}/read_test.csv") |> DataFrame.collect()
end)

run.("17.9 input_files", fn ->
  Reader.parquet(s, "#{tmp_base}/read_test.parquet") |> DataFrame.input_files()
end)

# === Section 19: Writing to Different Formats ===
IO.puts("--- Section 19: Writing ---\n")

run.("19.1 Write Parquet", fn ->
  src |> DataFrame.write() |> Writer.format("parquet") |> Writer.mode(:overwrite) |> Writer.save("#{tmp_base}/parquet_out")
  # Read back and verify
  Reader.parquet(s, "#{tmp_base}/parquet_out") |> DataFrame.count()
end)

run.("19.2 Write CSV", fn ->
  src |> DataFrame.write() |> Writer.format("csv") |> Writer.mode(:overwrite) |> Writer.option("header", "true") |> Writer.save("#{tmp_base}/csv_out")
  Reader.csv(s, "#{tmp_base}/csv_out", header: true) |> DataFrame.count()
end)

run.("19.3 Write JSON", fn ->
  src |> DataFrame.write() |> Writer.format("json") |> Writer.mode(:overwrite) |> Writer.save("#{tmp_base}/json_out")
  Reader.json(s, "#{tmp_base}/json_out") |> DataFrame.count()
end)

run.("19.6 Write with partition_by", fn ->
  src |> DataFrame.write() |> Writer.format("parquet") |> Writer.mode(:overwrite) |> Writer.partition_by(["val"]) |> Writer.save("#{tmp_base}/partitioned_out")
  Reader.parquet(s, "#{tmp_base}/partitioned_out") |> DataFrame.count()
end)

run.("19.10 Round-trip", fn ->
  src |> DataFrame.write() |> Writer.format("parquet") |> Writer.mode(:overwrite) |> Writer.save("#{tmp_base}/roundtrip")
  read_back = Reader.parquet(s, "#{tmp_base}/roundtrip")
  {:ok, count} = DataFrame.count(read_back)
  {:ok, schema} = DataFrame.dtypes(read_back)
  %{count: count, schema: schema}
end)

# === Section 22: Partitioning & Repartitioning ===
IO.puts("--- Section 22: Partitioning ---\n")

range_df = SparkEx.range(s, 0, 100, 1)

run.("22.1 repartition by number", fn ->
  DataFrame.repartition(range_df, 4) |> DataFrame.count()
end)

run.("22.2 repartition by column", fn ->
  DataFrame.repartition(range_df, [col("id")]) |> DataFrame.count()
end)

run.("22.3 coalesce", fn ->
  DataFrame.coalesce(range_df, 2) |> DataFrame.count()
end)

run.("22.4 repartition_by_range", fn ->
  DataFrame.repartition_by_range(range_df, 4, [col("id")]) |> DataFrame.count()
end)

run.("22.5 sort_within_partitions", fn ->
  DataFrame.sort_within_partitions(range_df, [col("id") |> Column.asc()]) |> DataFrame.count()
end)

# === Section 23: Unpivot, Reshape & Parse ===
IO.puts("--- Section 23: Unpivot, Reshape ---\n")

reshape_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    ('Alice', 90, 85, 95),
    ('Bob', 80, 75, 88)
  AS t(name, math, science, english)
""")

run.("23.1 unpivot", fn ->
  DataFrame.unpivot(reshape_df, ["name"], ["math", "science", "english"], "subject", "score") |> DataFrame.collect()
end)

run.("23.2 transpose", fn ->
  DataFrame.transpose(reshape_df) |> DataFrame.collect()
end)

IO.puts("=== Sections 16-22 Complete ===")
