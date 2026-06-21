# Section 2: Catalog-First Warehouse Familiarization
# Usage: mix run checklist/section_02_catalog.exs

alias SparkEx.{DataFrame, Column, Catalog, Reader}
import SparkEx.Functions

IO.puts("=== Section 2: Catalog-First Warehouse Familiarization ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Connected!\n")

# 2.1 Current catalog
IO.puts("2.1 Current catalog")
result = Catalog.current_catalog(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 2.2 List catalogs
IO.puts("2.2 List catalogs")
result = Catalog.list_catalogs(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 2.3 List catalogs with pattern
IO.puts("2.3 List catalogs with pattern")
result = Catalog.list_catalogs(s, "spark*")
IO.inspect(result, label: "  result")
IO.puts("")

# 2.4 Set current catalog
IO.puts("2.4 Set current catalog")
case Catalog.current_catalog(s) do
  {:ok, cat} ->
    result = Catalog.set_current_catalog(s, cat)
    IO.inspect(result, label: "  set back to #{cat}")
  err -> IO.inspect(err, label: "  skip (no current catalog)")
end
IO.puts("")

# 2.5 Current database
IO.puts("2.5 Current database")
result = Catalog.current_database(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 2.6 List databases
IO.puts("2.6 List databases")
result = Catalog.list_databases(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 2.7 Set current database
IO.puts("2.7 Set current database")
result = Catalog.set_current_database(s, "default")
IO.inspect(result, label: "  result")
IO.puts("")

# 2.8 Get database
IO.puts("2.8 Get database")
result = Catalog.get_database(s, "default")
IO.inspect(result, label: "  result")
IO.puts("")

# 2.9 Database exists
IO.puts("2.9 Database exists")
result = Catalog.database_exists?(s, "default")
IO.inspect(result, label: "  result")
IO.puts("")

# 2.10 List tables
IO.puts("2.10 List tables")
result = Catalog.list_tables(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 2.11 List tables with pattern
IO.puts("2.11 List tables with pattern")
result = Catalog.list_tables(s, nil, "*")
IO.inspect(result, label: "  result")
IO.puts("")

# Try listing tables from the sfx catalog
IO.puts("2.10b List tables from sfx catalog")
result = Catalog.set_current_catalog(s, "sfx")
IO.inspect(result, label: "  set catalog sfx")
result = Catalog.list_databases(s)
IO.inspect(result, label: "  databases in sfx")
result = Catalog.list_tables(s)
IO.inspect(result, label: "  tables in sfx")
IO.puts("")

# Try the fino catalog too
IO.puts("2.10c List tables from fino catalog")
result = Catalog.set_current_catalog(s, "fino")
IO.inspect(result, label: "  set catalog fino")
result = Catalog.list_databases(s)
IO.inspect(result, label: "  databases in fino")
result = Catalog.list_tables(s)
IO.inspect(result, label: "  tables in fino")
IO.puts("")

# Switch back to sfx as it's the default
Catalog.set_current_catalog(s, "sfx")

# 2.12-2.18: Pick a real table and inspect
IO.puts("2.12-2.18 Table inspection")
# Try to find a table — list tables in sfx namespaces
result = Catalog.list_tables(s)
case result do
  {:ok, tables} when tables != [] ->
    real_table = hd(tables)
    IO.inspect(real_table, label: "  picked table")

    # 2.13 Get table metadata
    IO.puts("\n2.13 Get table metadata")
    result = Catalog.get_table(s, real_table.name)
    IO.inspect(result, label: "  result")

    # 2.14 Table exists
    IO.puts("\n2.14 Table exists")
    result = Catalog.table_exists?(s, real_table.name)
    IO.inspect(result, label: "  result")

    # 2.15 List columns
    IO.puts("\n2.15 List columns")
    result = Catalog.list_columns(s, real_table.name)
    IO.inspect(result, label: "  result")

    # 2.16 Schema via DataFrame
    IO.puts("\n2.16 Schema via DataFrame")
    result = Reader.table(s, real_table.name) |> DataFrame.schema()
    IO.inspect(result, label: "  result")

    # 2.17 Row count
    IO.puts("\n2.17 Row count")
    result = Reader.table(s, real_table.name) |> DataFrame.count()
    IO.inspect(result, label: "  result")

    # 2.18 Sample rows
    IO.puts("\n2.18 Sample rows")
    result = Reader.table(s, real_table.name) |> DataFrame.take(5)
    IO.inspect(result, label: "  result", limit: :infinity, printable_limit: 500)

  {:ok, []} ->
    IO.puts("  No tables found in current catalog/database, trying SQL...")
    # Try direct SQL approach
    result = SparkEx.sql(s, "SHOW TABLES IN sfx") |> DataFrame.collect()
    IO.inspect(result, label: "  SQL SHOW TABLES sfx")
    result = SparkEx.sql(s, "SHOW NAMESPACES IN sfx") |> DataFrame.collect()
    IO.inspect(result, label: "  SQL SHOW NAMESPACES sfx")

  err ->
    IO.inspect(err, label: "  error listing tables")
end
IO.puts("")

# 2.21 List functions
IO.puts("2.21 List functions")
result = Catalog.list_functions(s)
case result do
  {:ok, fns} when is_list(fns) -> IO.puts("  OK — #{Kernel.length(fns)} functions listed")
  other -> IO.inspect(other, label: "  result")
end
IO.puts("")

# 2.22 Function exists
IO.puts("2.22 Function exists")
result = Catalog.function_exists?(s, "count")
IO.inspect(result, label: "  result")
IO.puts("")

IO.puts("=== Section 2 Complete ===")
