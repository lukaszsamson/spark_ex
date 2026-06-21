# Exploratory: advanced window functions, complex aggregations, edge cases
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

sales = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 'East', '2024-01-15', 100),
    (2, 'Bob', 'West', '2024-01-20', 200),
    (3, 'Alice', 'East', '2024-02-01', 150),
    (4, 'Carol', 'East', '2024-02-10', 300),
    (5, 'Bob', 'West', '2024-02-15', 250),
    (6, 'Alice', 'East', '2024-03-01', 175),
    (7, 'Carol', 'East', '2024-03-10', 225),
    (8, 'Dave', 'West', '2024-03-15', 400),
    (9, 'Alice', 'East', '2024-04-01', 125),
    (10, 'Bob', 'West', '2024-04-05', 350)
  AS t(id, rep, region, sale_date, amount)
""")

# 1. ntile window function
run.("1. ntile(3)", fn ->
  w = SparkEx.Window.order_by([desc(col("amount"))])
  sales
  |> DataFrame.with_column("quartile", ntile(lit(3)) |> Column.over(w))
  |> DataFrame.select([col("rep"), col("amount"), col("quartile")])
  |> DataFrame.order_by([desc(col("amount"))])
  |> DataFrame.collect()
end)

# 2. percent_rank
run.("2. percent_rank", fn ->
  w = SparkEx.Window.partition_by([col("region")]) |> SparkEx.Window.order_by([asc(col("amount"))])
  sales
  |> DataFrame.with_column("pct_rank", percent_rank() |> Column.over(w))
  |> DataFrame.select([col("rep"), col("region"), col("amount"), col("pct_rank")])
  |> DataFrame.collect()
end)

# 3. cume_dist
run.("3. cume_dist", fn ->
  w = SparkEx.Window.order_by([asc(col("amount"))])
  sales
  |> DataFrame.with_column("cume", cume_dist() |> Column.over(w))
  |> DataFrame.select([col("rep"), col("amount"), col("cume")])
  |> DataFrame.collect()
end)

# 4. nth_value
run.("4. nth_value", fn ->
  w = SparkEx.Window.partition_by([col("region")])
    |> SparkEx.Window.order_by([asc(col("amount"))])
    |> SparkEx.Window.rows_between(:unbounded, :current_row)
  sales
  |> DataFrame.with_column("second_lowest", nth_value(col("amount"), lit(2)) |> Column.over(w))
  |> DataFrame.select([col("rep"), col("region"), col("amount"), col("second_lowest")])
  |> DataFrame.collect()
end)

# 5. first_value / last_value with ignore nulls
run.("5. first_value / last_value", fn ->
  null_df = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a', NULL), (2, 'a', 10), (3, 'a', NULL), (4, 'a', 20)
    AS t(id, grp, val)
  """)
  w = SparkEx.Window.partition_by([col("grp")]) |> SparkEx.Window.order_by([asc(col("id"))])
    |> SparkEx.Window.rows_between(:unbounded, :unbounded)
  null_df
  |> DataFrame.with_columns([
    first_value(col("val")) |> Column.over(w) |> Column.alias_("first_v"),
    last_value(col("val")) |> Column.over(w) |> Column.alias_("last_v")
  ])
  |> DataFrame.collect()
end)

# 6. Moving average (rows between -2 and 0)
run.("6. Moving average (3-row window)", fn ->
  w = SparkEx.Window.partition_by([col("rep")])
    |> SparkEx.Window.order_by([asc(col("sale_date"))])
    |> SparkEx.Window.rows_between(-2, :current_row)
  sales
  |> DataFrame.with_column("moving_avg", avg(col("amount")) |> Column.over(w))
  |> DataFrame.filter(Column.eq(col("rep"), lit("Alice")))
  |> DataFrame.select([col("sale_date"), col("amount"), col("moving_avg")])
  |> DataFrame.collect()
end)

# 7. Range-based window (amount range)
run.("7. Range-based window", fn ->
  w = SparkEx.Window.order_by([asc(col("amount"))])
    |> SparkEx.Window.range_between(-50, 50)
  sales
  |> DataFrame.with_column("nearby_count", count(lit(1)) |> Column.over(w))
  |> DataFrame.select([col("rep"), col("amount"), col("nearby_count")])
  |> DataFrame.order_by([asc(col("amount"))])
  |> DataFrame.collect()
end)

# 8. collect_list / collect_set in window
run.("8. collect_list in window", fn ->
  w = SparkEx.Window.partition_by([col("region")])
    |> SparkEx.Window.order_by([asc(col("sale_date"))])
    |> SparkEx.Window.rows_between(:unbounded, :current_row)
  sales
  |> DataFrame.with_column("running_reps", collect_list(col("rep")) |> Column.over(w))
  |> DataFrame.filter(Column.eq(col("region"), lit("East")))
  |> DataFrame.select([col("sale_date"), col("rep"), col("running_reps")])
  |> DataFrame.collect()
end)

# 9. Pivot with multiple agg columns
run.("9. Pivot with sum", fn ->
  DataFrame.group_by(sales, [col("region")])
  |> DataFrame.pivot(col("rep"), ["Alice", "Bob", "Carol", "Dave"])
  |> DataFrame.agg([sum(col("amount"))])
  |> DataFrame.collect()
end)

# 10. Rollup with subtotals
run.("10. Rollup", fn ->
  DataFrame.rollup(sales, [col("region"), col("rep")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("region")), asc(col("rep"))])
  |> DataFrame.collect()
end)

# 11. Cube
run.("11. Cube", fn ->
  DataFrame.cube(sales, [col("region"), col("rep")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total")
  ])
  |> DataFrame.order_by([asc(col("region")), asc(col("rep"))])
  |> DataFrame.collect()
end)

# 12. approx_count_distinct
run.("12. approx_count_distinct", fn ->
  DataFrame.agg(sales, [
    approx_count_distinct(col("rep")) |> Column.alias_("approx_distinct_reps"),
    count_distinct(col("rep")) |> Column.alias_("exact_distinct_reps")
  ])
  |> DataFrame.collect()
end)

# 13. Multiple window functions in same query
run.("13. Multiple window functions", fn ->
  w1 = SparkEx.Window.partition_by([col("rep")]) |> SparkEx.Window.order_by([asc(col("sale_date"))])
  w2 = SparkEx.Window.partition_by([col("region")]) |> SparkEx.Window.order_by([desc(col("amount"))])
  sales
  |> DataFrame.with_columns([
    row_number() |> Column.over(w1) |> Column.alias_("rep_sale_num"),
    rank() |> Column.over(w2) |> Column.alias_("region_rank"),
    sum(col("amount")) |> Column.over(w1) |> Column.alias_("rep_running_total"),
    lag(col("amount"), lit(1)) |> Column.over(w1) |> Column.alias_("prev_amount")
  ])
  |> DataFrame.select([col("rep"), col("region"), col("sale_date"), col("amount"),
    col("rep_sale_num"), col("region_rank"), col("rep_running_total"), col("prev_amount")])
  |> DataFrame.order_by([asc(col("rep")), asc(col("sale_date"))])
  |> DataFrame.collect()
end)

# 14. grouping with having-like pattern (filter after agg)
run.("14. Group + filter (HAVING equivalent)", fn ->
  DataFrame.group_by(sales, [col("rep")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.filter(Column.gt(col("total"), lit(500)))
  |> DataFrame.collect()
end)

# 15. percentile_approx
run.("15. percentile_approx", fn ->
  DataFrame.agg(sales, [
    percentile_approx(col("amount"), lit(0.5), lit(100)) |> Column.alias_("median"),
    percentile_approx(col("amount"), lit(0.9), lit(100)) |> Column.alias_("p90")
  ])
  |> DataFrame.collect()
end)

IO.puts("=== Windows/Aggregations exploration complete ===")
