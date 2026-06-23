# Exploratory: Discover available tables and schemas for trade analysis
alias SparkEx.{DataFrame, Column, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 100, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === Discover catalogs and namespaces ===
run.("1. Current catalog", fn ->
  Catalog.current_catalog(s)
end)

run.("2. Current database", fn ->
  Catalog.current_database(s)
end)

run.("3. List catalogs", fn ->
  Catalog.list_catalogs(s)
end)

run.("4. List databases in sfx catalog", fn ->
  Catalog.list_databases(s)
end)

run.("5. List databases in fino catalog", fn ->
  Catalog.set_current_catalog(s, "fino")
  result = Catalog.list_databases(s)
  Catalog.set_current_catalog(s, "sfx")
  result
end)

# === List tables in various namespaces ===
run.("6. List tables in sfx.sfx_dev_warehouse", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse")
end)

run.("7. List tables in sfx default namespace", fn ->
  Catalog.list_tables(s)
end)

# Try common namespace patterns
for ns <- ["trades", "reports", "finance", "broker", "data", "warehouse", "analytics", "prod", "production"] do
  run.("8. Try namespace: sfx.#{ns}", fn ->
    Catalog.list_tables(s, ns)
  end)
end

# === Search for trade-related tables via SQL ===
run.("9. SHOW NAMESPACES in sfx", fn ->
  SparkEx.sql(s, "SHOW NAMESPACES IN sfx") |> DataFrame.collect()
end)

run.("10. SHOW NAMESPACES in fino", fn ->
  SparkEx.sql(s, "SHOW NAMESPACES IN fino") |> DataFrame.collect()
end)

# === List all tables in sfx_dev_warehouse ===
run.("11. SHOW TABLES in sfx.sfx_dev_warehouse", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_warehouse") |> DataFrame.collect()
end)

# === Try fino namespaces ===
run.("12. SHOW NAMESPACES IN fino", fn ->
  SparkEx.sql(s, "SHOW NAMESPACES IN fino") |> DataFrame.collect()
end)

IO.puts("=== Discovery complete ===")
