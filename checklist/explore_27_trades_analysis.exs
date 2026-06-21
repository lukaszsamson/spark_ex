# Exploratory: Financial performance analysis — broker trades report
# Using sfx.sfx_dev_report.trades as the primary table
alias SparkEx.{DataFrame, Column}
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

trades = "sfx.sfx_dev_report.trades"

# === 0. Full schema ===
run.("0. Full schema of trades report", fn ->
  SparkEx.sql(s, "SELECT * FROM #{trades} LIMIT 0") |> DataFrame.dtypes()
end)

# === 0b. Sample row ===
run.("0b. Sample 1 row", fn ->
  SparkEx.sql(s, "SELECT * FROM #{trades} LIMIT 1") |> DataFrame.collect()
end)

# === 0c. Date range and total count ===
run.("0c. Date range and count", fn ->
  SparkEx.sql(s, """
    SELECT COUNT(*) AS total_trades,
           MIN(close_time) AS earliest_close,
           MAX(close_time) AS latest_close,
           MIN(open_time) AS earliest_open,
           MAX(open_time) AS latest_open
    FROM #{trades}
  """) |> DataFrame.collect()
end)

# ==========================================
# FINANCIAL PERFORMANCE: Last year (2025)
# ==========================================

# === 1. Monthly revenue (broker P&L = -client P&L) ===
run.("1. Monthly broker P&L (2025)", fn ->
  SparkEx.sql(s, """
    SELECT DATE_TRUNC('month', close_time) AS month,
           COUNT(*) AS num_trades,
           SUM(-profit_usd) AS broker_profit_usd,
           SUM(-swaps_usd) AS broker_swaps_usd,
           SUM(-profit_usd - swaps_usd) AS broker_total_revenue_usd,
           SUM(nominal_value_in_usd) AS total_volume_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY DATE_TRUNC('month', close_time)
    ORDER BY month
  """) |> DataFrame.collect()
end)

# === 2. Quarterly breakdown ===
run.("2. Quarterly broker performance (2025)", fn ->
  SparkEx.sql(s, """
    SELECT QUARTER(close_time) AS quarter,
           COUNT(*) AS num_trades,
           SUM(-profit_usd) AS broker_profit_usd,
           SUM(-swaps_usd) AS broker_swaps_usd,
           SUM(-profit_usd - swaps_usd) AS broker_total_usd,
           SUM(nominal_value_in_usd) AS volume_usd,
           COUNT(DISTINCT account) AS active_accounts
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY QUARTER(close_time)
    ORDER BY quarter
  """) |> DataFrame.collect()
end)

# === 3. Top instruments by volume ===
run.("3. Top 15 instruments by volume (2025)", fn ->
  SparkEx.sql(s, """
    SELECT instrument,
           COUNT(*) AS num_trades,
           SUM(nominal_value_in_usd) AS total_volume_usd,
           SUM(-profit_usd) AS broker_profit_usd,
           SUM(-swaps_usd) AS broker_swaps_usd,
           COUNT(DISTINCT account) AS unique_accounts
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY instrument
    ORDER BY total_volume_usd DESC
    LIMIT 15
  """) |> DataFrame.collect()
end)

# === 4. Revenue by instrument type/category (using SparkEx pipeline) ===
run.("4. Revenue by instrument category (2025) — SparkEx pipeline", fn ->
  instruments = "sfx.sfx_core_dev_db_simplefx_core_dev_live_landing.simplefx_core_instruments"

  trades_2025 = SparkEx.sql(s, """
    SELECT * FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
  """)

  inst = SparkEx.sql(s, "SELECT instrument, type AS instrument_type FROM #{instruments}")

  trades_2025
  |> DataFrame.join(inst, ["instrument"], :left)
  |> DataFrame.group_by([col("instrument_type")])
  |> DataFrame.agg([
    count(lit(1)) |> Column.alias_("num_trades"),
    sum(Column.negate(col("profit_usd"))) |> Column.alias_("broker_profit_usd"),
    sum(Column.negate(col("swaps_usd"))) |> Column.alias_("broker_swaps_usd"),
    sum(col("nominal_value_in_usd")) |> Column.alias_("volume_usd"),
    count_distinct(col("account")) |> Column.alias_("unique_accounts")
  ])
  |> DataFrame.order_by([desc(col("volume_usd"))])
  |> DataFrame.collect()
end)

# === 5. By jurisdiction ===
run.("5. Revenue by jurisdiction (2025)", fn ->
  SparkEx.sql(s, """
    SELECT jurisdiction_id,
           COUNT(*) AS num_trades,
           SUM(-profit_usd) AS broker_profit_usd,
           SUM(-swaps_usd) AS broker_swaps_usd,
           SUM(nominal_value_in_usd) AS volume_usd,
           COUNT(DISTINCT account) AS unique_accounts,
           COUNT(DISTINCT user_id) AS unique_users
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY jurisdiction_id
    ORDER BY volume_usd DESC
  """) |> DataFrame.collect()
end)

# === 6. By account currency ===
run.("6. Revenue by account currency (2025)", fn ->
  SparkEx.sql(s, """
    SELECT account_currency,
           COUNT(*) AS num_trades,
           SUM(-profit_usd) AS broker_profit_usd,
           SUM(-swaps_usd) AS broker_swaps_usd,
           COUNT(DISTINCT account) AS unique_accounts
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY account_currency
    ORDER BY num_trades DESC
  """) |> DataFrame.collect()
end)

# === 7. Trade duration analysis ===
run.("7. Average trade duration by month (2025)", fn ->
  SparkEx.sql(s, """
    SELECT DATE_TRUNC('month', close_time) AS month,
           AVG(UNIX_TIMESTAMP(close_time) - UNIX_TIMESTAMP(open_time)) / 3600 AS avg_duration_hours,
           PERCENTILE_APPROX(UNIX_TIMESTAMP(close_time) - UNIX_TIMESTAMP(open_time), 0.5) / 3600 AS median_duration_hours,
           COUNT(*) AS num_trades
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY DATE_TRUNC('month', close_time)
    ORDER BY month
  """) |> DataFrame.collect()
end)

# === 8. Win rate analysis (client perspective) ===
run.("8. Client win rate by month (2025)", fn ->
  SparkEx.sql(s, """
    SELECT DATE_TRUNC('month', close_time) AS month,
           COUNT(*) AS total_trades,
           SUM(CASE WHEN profit_usd > 0 THEN 1 ELSE 0 END) AS winning_trades,
           SUM(CASE WHEN profit_usd <= 0 THEN 1 ELSE 0 END) AS losing_trades,
           ROUND(SUM(CASE WHEN profit_usd > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct,
           AVG(CASE WHEN profit_usd > 0 THEN profit_usd END) AS avg_win_usd,
           AVG(CASE WHEN profit_usd < 0 THEN profit_usd END) AS avg_loss_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY DATE_TRUNC('month', close_time)
    ORDER BY month
  """) |> DataFrame.collect()
end)

# === 9. Top accounts by volume (SparkEx pipeline with window) ===
run.("9. Top 10 accounts by volume with ranking (2025)", fn ->
  w = SparkEx.Window.order_by([desc(col("volume_usd"))])

  SparkEx.sql(s, """
    SELECT account, account_currency, jurisdiction_id,
           COUNT(*) AS num_trades,
           SUM(nominal_value_in_usd) AS volume_usd,
           SUM(-profit_usd) AS broker_profit_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY account, account_currency, jurisdiction_id
  """)
  |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("rank"), lit(10)))
  |> DataFrame.order_by([asc(col("rank"))])
  |> DataFrame.collect()
end)

# === 10. New vs returning clients (by first_deposit_date) ===
run.("10. New vs returning clients monthly (2025)", fn ->
  SparkEx.sql(s, """
    SELECT DATE_TRUNC('month', close_time) AS month,
           SUM(CASE WHEN first_deposit_date >= TIMESTAMP '2025-01-01' THEN 1 ELSE 0 END) AS new_client_trades,
           SUM(CASE WHEN first_deposit_date < TIMESTAMP '2025-01-01' THEN 1 ELSE 0 END) AS returning_client_trades,
           SUM(CASE WHEN first_deposit_date >= TIMESTAMP '2025-01-01' THEN -profit_usd ELSE 0 END) AS new_client_broker_pnl,
           SUM(CASE WHEN first_deposit_date < TIMESTAMP '2025-01-01' THEN -profit_usd ELSE 0 END) AS returning_client_broker_pnl
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY DATE_TRUNC('month', close_time)
    ORDER BY month
  """) |> DataFrame.collect()
end)

# === 11. Deposit cohort analysis ===
run.("11. Performance by deposit tier (2025)", fn ->
  SparkEx.sql(s, """
    SELECT
      CASE
        WHEN total_deposits_usd IS NULL OR total_deposits_usd = 0 THEN 'No deposits'
        WHEN total_deposits_usd < 100 THEN '< $100'
        WHEN total_deposits_usd < 1000 THEN '$100-$1K'
        WHEN total_deposits_usd < 10000 THEN '$1K-$10K'
        WHEN total_deposits_usd < 100000 THEN '$10K-$100K'
        ELSE '> $100K'
      END AS deposit_tier,
      COUNT(*) AS num_trades,
      COUNT(DISTINCT account) AS accounts,
      SUM(-profit_usd) AS broker_profit_usd,
      SUM(nominal_value_in_usd) AS volume_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY 1
    ORDER BY volume_usd DESC
  """) |> DataFrame.collect()
end)

# === 12. Country breakdown (top 10) ===
run.("12. Top 10 countries by volume (2025)", fn ->
  SparkEx.sql(s, """
    SELECT country_name,
           COUNT(DISTINCT user_id) AS unique_users,
           COUNT(DISTINCT account) AS unique_accounts,
           COUNT(*) AS num_trades,
           SUM(nominal_value_in_usd) AS volume_usd,
           SUM(-profit_usd) AS broker_profit_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY country_name
    ORDER BY volume_usd DESC
    LIMIT 10
  """) |> DataFrame.collect()
end)

# === 13. YoY comparison (2024 vs 2025) ===
run.("13. Year-over-year comparison (2024 vs 2025)", fn ->
  SparkEx.sql(s, """
    SELECT
      YEAR(close_time) AS yr,
      COUNT(*) AS num_trades,
      SUM(-profit_usd) AS broker_profit_usd,
      SUM(-swaps_usd) AS broker_swaps_usd,
      SUM(nominal_value_in_usd) AS volume_usd,
      COUNT(DISTINCT account) AS active_accounts,
      COUNT(DISTINCT user_id) AS active_users
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2024-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY YEAR(close_time)
    ORDER BY yr
  """) |> DataFrame.collect()
end)

# === 14. SparkEx DataFrame pipeline: monthly running total ===
run.("14. Monthly running total (SparkEx window pipeline)", fn ->
  monthly = SparkEx.sql(s, """
    SELECT DATE_TRUNC('month', close_time) AS month,
           SUM(-profit_usd - swaps_usd) AS monthly_revenue_usd
    FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2026-01-01'
    GROUP BY DATE_TRUNC('month', close_time)
  """)

  w = SparkEx.Window.order_by([asc(col("month"))])

  monthly
  |> DataFrame.with_column("cumulative_revenue_usd", sum(col("monthly_revenue_usd")) |> Column.over(w))
  |> DataFrame.with_column("month_rank", row_number() |> Column.over(w))
  |> DataFrame.order_by([asc(col("month"))])
  |> DataFrame.collect()
end)

# === 15. Pivot: monthly revenue by instrument category ===
run.("15. Monthly revenue pivoted by instrument type (Q1 2025)", fn ->
  instruments = "sfx.sfx_core_dev_db_simplefx_core_dev_live_landing.simplefx_core_instruments"

  base = SparkEx.sql(s, """
    SELECT t.*, i.type AS instrument_type
    FROM #{trades} t
    LEFT JOIN #{instruments} i ON t.instrument = i.instrument
    WHERE t.close_time >= TIMESTAMP '2025-01-01' AND t.close_time < TIMESTAMP '2025-04-01'
  """)

  base
  |> DataFrame.with_column("month", date_trunc(lit("month"), col("close_time")))
  |> DataFrame.group_by([col("month")])
  |> DataFrame.pivot(col("instrument_type"), ["Forex", "Indices", "Crypto", "Commodities"])
  |> DataFrame.agg([sum(Column.negate(col("profit_usd")))])
  |> DataFrame.order_by([asc(col("month"))])
  |> DataFrame.collect()
end)

IO.puts("=== Financial analysis complete ===")
