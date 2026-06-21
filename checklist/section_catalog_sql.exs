# Catalog exploration via SQL (workaround for list_tables API bug on Spark 3.5)
# Usage: mix run checklist/section_catalog_sql.exs

alias SparkEx.{DataFrame, Column, Reader}
import SparkEx.Functions

IO.puts("=== Catalog Exploration via SQL ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: :infinity, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# Explore namespaces
run.("SHOW NAMESPACES IN sfx", fn ->
  SparkEx.sql(s, "SHOW NAMESPACES IN sfx") |> DataFrame.collect()
end)

# List tables in a database
run.("SHOW TABLES IN sfx.sfx_dev_warehouse", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_warehouse") |> DataFrame.collect()
end)

run.("SHOW TABLES IN sfx.sfx_prd_warehouse", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_prd_warehouse") |> DataFrame.collect()
end)

run.("SHOW TABLES IN sfx.sfx_dev_report", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_report") |> DataFrame.collect()
end)

run.("SHOW TABLES IN sfx.sfx_prd_report", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_prd_report") |> DataFrame.collect()
end)

# Try to peek at a table
run.("SHOW TABLES IN sfx.sfx_prd_db_com_simplefx_landing", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_prd_db_com_simplefx_landing") |> DataFrame.collect()
end)

run.("SHOW TABLES IN sfx.sfx_dev_db_simplefx_release_landing", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_db_simplefx_release_landing") |> DataFrame.collect()
end)

IO.puts("=== Done ===")
