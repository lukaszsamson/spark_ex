# Fix test 15 from explore_27: use correct date_trunc API and test pivot
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
instruments = "sfx.sfx_core_dev_db_simplefx_core_dev_live_landing.simplefx_core_instruments"

# === EX-41: date_trunc requires list for column args ===
run.("1. date_trunc with list arg (workaround)", fn ->
  SparkEx.sql(s, """
    SELECT * FROM #{trades}
    WHERE close_time >= TIMESTAMP '2025-01-01' AND close_time < TIMESTAMP '2025-04-01'
    LIMIT 5
  """)
  |> DataFrame.with_column("month", date_trunc("month", [col("close_time")]))
  |> DataFrame.select([col("month"), col("instrument"), col("profit_usd")])
  |> DataFrame.collect()
end)

# === 2. Pivot with correct date_trunc ===
run.("2. Pivot: monthly revenue by instrument type (Q1 2025)", fn ->
  base = SparkEx.sql(s, """
    SELECT t.*, i.type AS instrument_type
    FROM #{trades} t
    LEFT JOIN #{instruments} i ON t.instrument = i.instrument
    WHERE t.close_time >= TIMESTAMP '2025-01-01' AND t.close_time < TIMESTAMP '2025-04-01'
  """)

  base
  |> DataFrame.with_column("month", date_trunc("month", [col("close_time")]))
  |> DataFrame.group_by([col("month")])
  |> DataFrame.pivot(col("instrument_type"), ["Forex", "Indices", "Crypto", "Commodities"])
  |> DataFrame.agg([sum(Column.negate(col("profit_usd")))])
  |> DataFrame.order_by([asc(col("month"))])
  |> DataFrame.collect()
end)

# === 3. date_trunc positional (PySpark-style) — expect to fail ===
run.("3. date_trunc positional (PySpark-style, should fail)", fn ->
  SparkEx.sql(s, "SELECT TIMESTAMP '2025-06-15 10:30:00' AS ts")
  |> DataFrame.with_column("truncated", date_trunc("month", col("ts")))
  |> DataFrame.collect()
end)

# === 4. Other lit_then_cols functions: concat_ws ===
run.("4. concat_ws with list of cols", fn ->
  SparkEx.sql(s, """
    SELECT * FROM #{trades} LIMIT 3
  """)
  |> DataFrame.select([
    concat_ws("-", [col("jurisdiction_id"), col("account_currency")]) |> Column.alias_("combined")
  ])
  |> DataFrame.collect()
end)

# === 5. Pivot with more instrument types ===
run.("5. Pivot with all instrument types (Q1 2025)", fn ->
  base = SparkEx.sql(s, """
    SELECT t.*, i.type AS instrument_type
    FROM #{trades} t
    LEFT JOIN #{instruments} i ON t.instrument = i.instrument
    WHERE t.close_time >= TIMESTAMP '2025-01-01' AND t.close_time < TIMESTAMP '2025-04-01'
  """)

  base
  |> DataFrame.with_column("month", date_trunc("month", [col("close_time")]))
  |> DataFrame.group_by([col("month")])
  |> DataFrame.pivot(col("instrument_type"))
  |> DataFrame.agg([count(lit(1))])
  |> DataFrame.order_by([asc(col("month"))])
  |> DataFrame.collect()
end)

# === 6. Multi-level aggregation after pivot ===
run.("6. Pivot with multiple aggregations", fn ->
  base = SparkEx.sql(s, """
    SELECT t.*, i.type AS instrument_type
    FROM #{trades} t
    LEFT JOIN #{instruments} i ON t.instrument = i.instrument
    WHERE t.close_time >= TIMESTAMP '2025-01-01' AND t.close_time < TIMESTAMP '2025-04-01'
  """)

  base
  |> DataFrame.with_column("month", date_trunc("month", [col("close_time")]))
  |> DataFrame.group_by([col("month")])
  |> DataFrame.pivot(col("instrument_type"), ["Forex", "Crypto"])
  |> DataFrame.agg([
    sum(Column.negate(col("profit_usd"))),
    count(lit(1))
  ])
  |> DataFrame.order_by([asc(col("month"))])
  |> DataFrame.collect()
end)

IO.puts("=== Pivot and date_trunc tests complete ===")
