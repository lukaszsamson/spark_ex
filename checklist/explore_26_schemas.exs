# Exploratory: Discover schemas and sample data from trade-related tables
alias SparkEx.{DataFrame, Column, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 100, printable_limit: 8000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

landing = "sfx.sfx_core_dev_db_simplefx_core_dev_live_landing"

# === Report + warehouse tables ===
run.("1. SHOW TABLES in sfx.sfx_dev_report", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_report") |> DataFrame.collect()
end)

run.("2. SHOW TABLES in sfx.sfx_prd_report", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_prd_report") |> DataFrame.collect()
end)

run.("3. SHOW TABLES in sfx.sfx_prd_warehouse", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_prd_warehouse") |> DataFrame.collect()
end)

# === Schema of key tables ===
run.("4. Schema: all_market_trades", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_all_market_trades LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("5. Schema: daily_statements", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_daily_statements LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("6. Schema: daily_open_trades", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_daily_open_trades LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("7. Schema: accounts", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_accounts LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("8. Schema: instruments", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_instruments LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("9. Schema: groups", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_groups LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("10. Schema: currencies", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_currencies LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("11. Schema: transfers", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_transfers LIMIT 0")
  |> DataFrame.dtypes()
end)

run.("12. Schema: trade_corrections", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_trade_corrections LIMIT 0")
  |> DataFrame.dtypes()
end)

# === Sample rows ===
run.("13. Sample: all_market_trades (3 rows)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_all_market_trades LIMIT 3")
  |> DataFrame.collect()
end)

run.("14. Sample: daily_statements (3 rows)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_daily_statements LIMIT 3")
  |> DataFrame.collect()
end)

run.("15. Sample: accounts (3 rows)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_accounts LIMIT 3")
  |> DataFrame.collect()
end)

run.("16. Sample: instruments (3 rows)", fn ->
  SparkEx.sql(s, "SELECT * FROM #{landing}.simplefx_core_instruments LIMIT 3")
  |> DataFrame.collect()
end)

# === Row counts ===
run.("17. Count: all_market_trades", fn ->
  SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM #{landing}.simplefx_core_all_market_trades")
  |> DataFrame.collect()
end)

run.("18. Count: daily_statements", fn ->
  SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM #{landing}.simplefx_core_daily_statements")
  |> DataFrame.collect()
end)

run.("19. Count: accounts", fn ->
  SparkEx.sql(s, "SELECT COUNT(*) AS cnt FROM #{landing}.simplefx_core_accounts")
  |> DataFrame.collect()
end)

# === Date ranges ===
run.("20. Date range: all_market_trades", fn ->
  SparkEx.sql(s, """
    SELECT MIN(close_time) AS earliest, MAX(close_time) AS latest
    FROM #{landing}.simplefx_core_all_market_trades
  """) |> DataFrame.collect()
end)

run.("21. Date range: daily_statements", fn ->
  SparkEx.sql(s, """
    SELECT MIN(date) AS earliest, MAX(date) AS latest
    FROM #{landing}.simplefx_core_daily_statements
  """) |> DataFrame.collect()
end)

# === Check report tables schemas ===
run.("22. sfx_dev_report tables schemas", fn ->
  {:ok, tables} = SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_report") |> DataFrame.collect()
  for %{"tableName" => tbl} <- tables do
    IO.puts("  --- sfx.sfx_dev_report.#{tbl} ---")
    {:ok, dtypes} = SparkEx.sql(s, "SELECT * FROM sfx.sfx_dev_report.#{tbl} LIMIT 0") |> DataFrame.dtypes()
    IO.inspect(dtypes, label: "    dtypes", limit: 50)
  end
  :ok
end)

IO.puts("=== Schema discovery complete ===")
