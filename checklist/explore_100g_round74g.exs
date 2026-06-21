# Round 74g: More list-of-strings protobuf paths
# Pattern: functions that accept lists where elements should be strings but aren't validated
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

# 1. dropna subset with non-string list elements
run.("1. dropna subset with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => nil, "y" => 1}], schema: "x INT, y INT")
  DataFrame.dropna(df, subset: [42]) |> DataFrame.collect()
end)

# 2. fillna with map non-string keys
run.("2. fillna with integer key in map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => nil}], schema: "x INT")
  DataFrame.fillna(df, %{42 => 0}) |> DataFrame.collect()
end)

# 3. replace with non-string column list
run.("3. replace with integer in column list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.replace(df, [42], %{1 => 10}) |> DataFrame.collect()
end)

# 4. Writer.bucket_by with non-string column names
run_id = :erlang.system_time(:millisecond) |> to_string()

run.("4. Writer.bucket_by with integer col names", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.write(df)
  |> SparkEx.Writer.bucket_by(2, [42])
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r74g_bucket_#{run_id}")
end)

# 5. Catalog.list_columns with non-string table name
run.("5. Catalog.list_columns with integer table", fn ->
  Catalog.list_columns(s, 42)
end)

# 6. Catalog.table_exists with non-string
run.("6. Catalog.table_exists with integer table name", fn ->
  Catalog.table_exists?(s, 42)
end)

# 7. Catalog.get_table with non-string
run.("7. Catalog.get_table with integer table name", fn ->
  Catalog.get_table(s, 42)
end)

# 8. DataFrame.explain with non-string mode
run.("8. explain with integer mode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.explain(df, 42)
end)

# 9. DataFrame.schema_string on valid df (baseline)
run.("9. schema_string (baseline)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.schema(df)
end)

# 10. Catalog.set_current_database with non-string
run.("10. set_current_database with integer", fn ->
  Catalog.set_current_database(s, 42)
end)

# 11. Catalog.set_current_catalog with non-string
run.("11. set_current_catalog with integer", fn ->
  Catalog.set_current_catalog(s, 42)
end)

# 12. Catalog.list_tables with non-string db_name
run.("12. list_tables with integer db_name", fn ->
  Catalog.list_tables(s, 42)
end)

# 13. Catalog.list_databases with non-string catalog
run.("13. list_databases with integer catalog", fn ->
  Catalog.list_databases(s, 42)
end)

# 14. Catalog.database_exists with non-string
run.("14. database_exists with integer", fn ->
  Catalog.database_exists?(s, 42)
end)

# 15. Catalog.get_database with non-string
run.("15. get_database with integer", fn ->
  Catalog.get_database(s, 42)
end)

IO.puts("=== Round 74g complete ===")
