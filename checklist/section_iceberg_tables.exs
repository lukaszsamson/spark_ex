# Iceberg table read-only testing (2.12-2.20 using real tables)
# Usage: mix run checklist/section_iceberg_tables.exs

alias SparkEx.{DataFrame, Column, Catalog, Reader}
import SparkEx.Functions

IO.puts("=== Iceberg Table Testing ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: :infinity, printable_limit: 2000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# Use a dev warehouse table (safer to query)
real_table = "sfx.sfx_dev_warehouse.instruments"

# 2.13 Get table metadata
run.("2.13 Get table metadata (via SQL DESCRIBE)", fn ->
  SparkEx.sql(s, "DESCRIBE TABLE EXTENDED #{real_table}") |> DataFrame.collect()
end)

# 2.14 Table exists
run.("2.14 Table exists", fn ->
  Catalog.table_exists?(s, real_table)
end)

# 2.16 Schema via DataFrame
run.("2.16 Schema via DataFrame", fn ->
  Reader.table(s, real_table) |> DataFrame.dtypes()
end)

# 2.17 Row count
run.("2.17 Row count", fn ->
  Reader.table(s, real_table) |> DataFrame.count()
end)

# 2.18 Sample rows
run.("2.18 Sample rows", fn ->
  Reader.table(s, real_table) |> DataFrame.take(5)
end)

# Also try the dev report trades table
IO.puts("--- sfx.sfx_dev_report.trades ---\n")

trades_table = "sfx.sfx_dev_report.trades"

run.("Trades schema", fn ->
  Reader.table(s, trades_table) |> DataFrame.dtypes()
end)

run.("Trades count", fn ->
  Reader.table(s, trades_table) |> DataFrame.count()
end)

run.("Trades sample", fn ->
  Reader.table(s, trades_table) |> DataFrame.take(3)
end)

# Test reading from dev warehouse users
IO.puts("--- sfx.sfx_dev_warehouse.users ---\n")

users_table = "sfx.sfx_dev_warehouse.users"

run.("Users schema", fn ->
  Reader.table(s, users_table) |> DataFrame.dtypes()
end)

run.("Users count", fn ->
  Reader.table(s, users_table) |> DataFrame.count()
end)

run.("Users sample", fn ->
  Reader.table(s, users_table) |> DataFrame.take(3)
end)

IO.puts("=== Iceberg Table Testing Complete ===")
