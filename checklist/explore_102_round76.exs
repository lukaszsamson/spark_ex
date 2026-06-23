# Round 76: Less-explored function paths and exotic patterns
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1]

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

# =============================================
# 1. DataFrame.foreach/foreach_partition with non-function
# =============================================

run.("1a. foreach with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.foreach_local(df, 42)
end)

run.("1b. foreach_partition with string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.foreach_partition_local(df, "not_a_func")
end)

# =============================================
# 2. SparkEx.range edge cases
# =============================================

run.("2a. range with float step", fn ->
  SparkEx.range(s, 0, 10, 1.5) |> DataFrame.collect()
end)

run.("2b. range with zero step", fn ->
  SparkEx.range(s, 0, 10, 0) |> DataFrame.collect()
end)

run.("2c. range with negative step", fn ->
  SparkEx.range(s, 10, 0, -1) |> DataFrame.collect()
end)

run.("2d. range with string start", fn ->
  SparkEx.range(s, "0", 10) |> DataFrame.collect()
end)

# =============================================
# 3. DataFrame.sample edge cases
# =============================================

run.("3a. sample with fraction > 1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, 2.0) |> DataFrame.collect()
end)

run.("3b. sample with negative fraction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, -0.5) |> DataFrame.collect()
end)

run.("3c. sample with with_replacement", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, 0.5, with_replacement: true) |> DataFrame.collect()
end)

run.("3d. sample with non-boolean with_replacement", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, 0.5, with_replacement: "yes") |> DataFrame.collect()
end)

run.("3e. sample with non-integer seed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, 0.5, seed: "abc") |> DataFrame.collect()
end)

# =============================================
# 4. DataFrame.limit/offset/tail edge cases
# =============================================

run.("4a. limit with negative", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.limit(df, -1) |> DataFrame.collect()
end)

run.("4b. limit with float", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.limit(df, 1.5) |> DataFrame.collect()
end)

run.("4c. offset with negative", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.offset(df, -1) |> DataFrame.collect()
end)

run.("4d. tail with float", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.tail(df, 1.5)
end)

run.("4e. limit with string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.limit(df, "10") |> DataFrame.collect()
end)

# =============================================
# 5. SparkEx.create_dataframe edge cases
# =============================================

run.("5a. create_dataframe with non-list data", fn ->
  SparkEx.create_dataframe(s, "not_a_list", schema: "x INT")
end)

run.("5b. create_dataframe with non-map elements", fn ->
  SparkEx.create_dataframe(s, [1, 2, 3], schema: "x INT")
end)

run.("5c. create_dataframe with non-string schema", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}], schema: 42)
end)

run.("5d. create_dataframe with nil schema", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}], schema: nil)
end)

run.("5e. create_dataframe with empty map", fn ->
  SparkEx.create_dataframe(s, [%{}], schema: "x INT")
end)

# =============================================
# 6. DataFrame.persist/unpersist/cache edge cases
# =============================================

run.("6a. persist with non-struct storage_level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.persist(df, "MEMORY_ONLY")
end)

run.("6b. unpersist with non-boolean blocking", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.unpersist(df, "yes")
end)

# =============================================
# 7. Catalog.cache_table/uncache_table/refresh_table edge cases
# =============================================

run.("7a. cache_table with non-string table name", fn ->
  Catalog.cache_table(s, 42)
end)

run.("7b. uncache_table with non-string table name", fn ->
  Catalog.uncache_table(s, 42)
end)

run.("7c. refresh_table with non-string table name", fn ->
  Catalog.refresh_table(s, 42)
end)

run.("7d. is_cached? with non-string table name", fn ->
  Catalog.is_cached?(s, 42)
end)

# =============================================
# 8. DataFrame.to with non-struct schema
# =============================================

run.("8a. to with string schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to(df, "x LONG") |> DataFrame.collect()
end)

run.("8b. to with integer schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to(df, 42) |> DataFrame.collect()
end)

IO.puts("=== Round 76 complete ===")
