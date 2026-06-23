# Exploratory: Combining DataFrames in complex ways — multi-join, union chains, cross patterns
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 40, printable_limit: 2000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === 1. Three-way join (employees → departments → locations) ===
run.("1. Three-way join chain", fn ->
  employees = SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'Alice',10), (2,'Bob',20), (3,'Carol',10), (4,'Dave',30)
    AS t(emp_id, name, dept_id)
  """)
  departments = SparkEx.sql(s, """
    SELECT * FROM VALUES (10,'Engineering','LOC1'), (20,'Sales','LOC2'), (30,'HR','LOC1')
    AS t(dept_id, dept_name, loc_id)
  """)
  locations = SparkEx.sql(s, """
    SELECT * FROM VALUES ('LOC1','New York'), ('LOC2','San Francisco')
    AS t(loc_id, city)
  """)

  employees
  |> DataFrame.join(departments, ["dept_id"], :inner)
  |> DataFrame.join(locations, ["loc_id"], :inner)
  |> DataFrame.select([col("name"), col("dept_name"), col("city")])
  |> DataFrame.order_by([asc(col("name"))])
  |> DataFrame.collect()
end)

# === 2. Left join chain preserving all left rows ===
run.("2. Left join chain (preserve all left rows)", fn ->
  orders = SparkEx.sql(s, """
    SELECT * FROM VALUES (1,100,'2024-01-01'), (2,200,'2024-02-01'), (3,999,'2024-03-01')
    AS t(order_id, cust_id, order_date)
  """)
  customers = SparkEx.sql(s, """
    SELECT * FROM VALUES (100,'Alice'), (200,'Bob') AS t(cust_id, cust_name)
  """)
  shipping = SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'shipped'), (2,'pending') AS t(order_id, status)
  """)

  orders
  |> DataFrame.join(customers, ["cust_id"], :left)
  |> DataFrame.join(shipping, ["order_id"], :left)
  |> DataFrame.order_by([asc(col("order_id"))])
  |> DataFrame.collect()
end)

# === 3. Anti join then union with different source ===
run.("3. Anti join + union", fn ->
  all_users = SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'Alice'), (2,'Bob'), (3,'Carol'), (4,'Dave') AS t(id, name)
  """)
  active = SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'Alice'), (3,'Carol') AS t(id, name)
  """)
  new_users = SparkEx.sql(s, """
    SELECT * FROM VALUES (5,'Eve'), (6,'Frank') AS t(id, name)
  """)

  # Inactive users + new users
  inactive = DataFrame.join(all_users, active, ["id"], :left_anti)
  DataFrame.union(inactive, new_users)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 4. Semi join followed by aggregation ===
run.("4. Semi join → aggregate", fn ->
  orders = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 100, 50.0), (2, 100, 75.0), (3, 200, 100.0),
      (4, 300, 25.0), (5, 200, 60.0)
    AS t(order_id, cust_id, amount)
  """)
  vip = SparkEx.sql(s, "SELECT * FROM VALUES (100), (200) AS t(cust_id)")

  DataFrame.join(orders, vip, ["cust_id"], :left_semi)
  |> DataFrame.group_by([col("cust_id")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("order_count")
  ])
  |> DataFrame.order_by([asc(col("cust_id"))])
  |> DataFrame.collect()
end)

# === 5. Full outer join with coalesce for merged keys ===
run.("5. Full outer join with coalesce", fn ->
  jan = SparkEx.sql(s, """
    SELECT * FROM VALUES ('Alice', 100), ('Bob', 200), ('Carol', 150)
    AS t(name, jan_sales)
  """)
  feb = SparkEx.sql(s, """
    SELECT * FROM VALUES ('Bob', 250), ('Carol', 175), ('Dave', 300)
    AS t(name, feb_sales)
  """)

  DataFrame.join(jan, feb, ["name"], :outer)
  |> DataFrame.with_column("total",
    Column.plus(
      coalesce([col("jan_sales"), lit(0)]),
      coalesce([col("feb_sales"), lit(0)])
    )
  )
  |> DataFrame.order_by([asc(col("name"))])
  |> DataFrame.collect()
end)

# === 6. Union of 5 DataFrames ===
run.("6. Union of 5 DataFrames", fn ->
  dfs = Enum.map(1..5, fn i ->
    SparkEx.sql(s, "SELECT #{i} AS batch, id FROM range(#{(i-1)*3}, #{i*3})")
  end)

  Enum.reduce(dfs, fn df, acc -> DataFrame.union(acc, df) end)
  |> DataFrame.order_by([asc(col("batch")), asc(col("id"))])
  |> DataFrame.collect()
end)

# === 7. Cross join then filter (Cartesian product pattern) ===
run.("7. Cross join product → filter", fn ->
  colors = SparkEx.sql(s, "SELECT * FROM VALUES ('red'), ('green'), ('blue') AS t(color)")
  sizes = SparkEx.sql(s, "SELECT * FROM VALUES ('S'), ('M'), ('L') AS t(size)")
  prices = SparkEx.sql(s, "SELECT * FROM VALUES ('S', 10), ('M', 15), ('L', 20) AS t(size, price)")

  DataFrame.cross_join(colors, sizes)
  |> DataFrame.join(prices, ["size"], :inner)
  |> DataFrame.filter(Column.gt(col("price"), lit(10)))
  |> DataFrame.order_by([asc(col("color")), asc(col("price"))])
  |> DataFrame.collect()
end)

# === 8. Join with expression (non-equi join) ===
run.("8. Non-equi join (range join)", fn ->
  events = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 5), (2, 15), (3, 25), (4, 35), (5, 45)
    AS t(event_id, event_time)
  """)
  windows = SparkEx.sql(s, """
    SELECT * FROM VALUES ('W1', 0, 10), ('W2', 10, 20), ('W3', 20, 30), ('W4', 30, 50)
    AS t(window_id, start_time, end_time)
  """)

  DataFrame.join(events, windows,
    Column.and_(
      Column.gte(col("event_time"), col("start_time")),
      Column.lt(col("event_time"), col("end_time"))
    ),
    :inner
  )
  |> DataFrame.select([col("event_id"), col("event_time"), col("window_id")])
  |> DataFrame.order_by([asc(col("event_id"))])
  |> DataFrame.collect()
end)

# === 9. Self-join for parent-child hierarchy ===
run.("9. Self-join hierarchy (manager lookup)", fn ->
  emps = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Carol', 1),
      (4, 'Dave', 2), (5, 'Eve', 3)
    AS t(id, name, mgr_id)
  """)

  mgrs = DataFrame.select(emps, [
    Column.alias_(col("id"), "m_id"),
    Column.alias_(col("name"), "manager_name")
  ])

  DataFrame.join(emps, mgrs,
    Column.eq(col("mgr_id"), col("m_id")),
    :left
  )
  |> DataFrame.select([col("id"), col("name"), col("manager_name")])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 10. union_by_name with different column orders ===
run.("10. union_by_name with reordered columns", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name, 100 AS score")
  df2 = SparkEx.sql(s, "SELECT 200 AS score, 'Bob' AS name, 2 AS id")

  DataFrame.union_by_name(df1, df2) |> DataFrame.collect()
end)

# === 11. Accumulating join (running enrichment) ===
run.("11. Sequential enrichment joins", fn ->
  base = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'p1'), (2, 'p2'), (3, 'p3') AS t(id, product)")
  prices = SparkEx.sql(s, "SELECT * FROM VALUES ('p1', 10.0), ('p2', 20.0), ('p3', 30.0) AS t(product, price)")
  categories = SparkEx.sql(s, "SELECT * FROM VALUES ('p1', 'A'), ('p2', 'B'), ('p3', 'A') AS t(product, category)")
  ratings = SparkEx.sql(s, "SELECT * FROM VALUES ('p1', 4.5), ('p2', 3.8), ('p3', 4.2) AS t(product, rating)")

  base
  |> DataFrame.join(prices, ["product"], :left)
  |> DataFrame.join(categories, ["product"], :left)
  |> DataFrame.join(ratings, ["product"], :left)
  |> DataFrame.with_column("value_score",
    Column.multiply(col("rating"), Column.divide(lit(10.0), col("price")))
  )
  |> DataFrame.order_by([desc(col("value_score"))])
  |> DataFrame.collect()
end)

# === 12. except then count (finding missing items) ===
run.("12. Except-based missing detection", fn ->
  expected = SparkEx.sql(s, """
    SELECT id FROM range(1, 11)
  """)
  actual = SparkEx.sql(s, """
    SELECT * FROM VALUES (1),(2),(4),(5),(7),(8),(10) AS t(id)
  """)

  missing = DataFrame.except(expected, actual)
  {:ok, count} = DataFrame.count(missing)
  {:ok, rows} = missing |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
  %{missing_count: count, missing_ids: rows}
end)

# === 13. Multiple aggregation levels with different groupings ===
run.("13. Multiple aggregation passes", fn ->
  data = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('East', 'Alice', 'A', 100), ('East', 'Alice', 'B', 200),
      ('East', 'Bob', 'A', 150), ('West', 'Carol', 'A', 300),
      ('West', 'Carol', 'B', 250), ('West', 'Dave', 'A', 175)
    AS t(region, rep, product, amount)
  """)

  # By region+product
  by_region_product = DataFrame.group_by(data, [col("region"), col("product")])
    |> DataFrame.agg([sum(col("amount")) |> Column.alias_("total")])

  # By region only
  by_region = DataFrame.group_by(data, [col("region")])
    |> DataFrame.agg([sum(col("amount")) |> Column.alias_("region_total")])

  # Join to get percentage
  DataFrame.join(by_region_product, by_region, ["region"], :left)
  |> DataFrame.with_column("pct",
    round(Column.multiply(Column.divide(col("total"), col("region_total")), lit(100)), lit(1))
  )
  |> DataFrame.order_by([asc(col("region")), desc(col("pct"))])
  |> DataFrame.collect()
end)

# === 14. DataFrame aliased and used in subquery-like pattern ===
run.("14. Aliased DataFrames in correlated-like pattern", fn ->
  orders = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 100, 50.0), (2, 100, 75.0), (3, 200, 120.0), (4, 200, 80.0), (5, 300, 200.0)
    AS t(order_id, cust_id, amount)
  """)

  # Per-customer average
  cust_avg = DataFrame.group_by(orders, [col("cust_id")])
    |> DataFrame.agg([avg(col("amount")) |> Column.alias_("avg_amount")])

  # Orders above their customer's average
  DataFrame.join(orders, cust_avg, ["cust_id"], :inner)
  |> DataFrame.filter(Column.gt(col("amount"), col("avg_amount")))
  |> DataFrame.select([col("order_id"), col("cust_id"), col("amount"), col("avg_amount")])
  |> DataFrame.order_by([asc(col("order_id"))])
  |> DataFrame.collect()
end)

# === 15. Complex pipeline combining all: filter + join + window + aggregate ===
run.("15. Full pipeline: filter → join → window → aggregate → collect", fn ->
  transactions = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'A', '2024-01-01', 100), (2, 'A', '2024-01-15', 200),
      (3, 'B', '2024-01-01', 150), (4, 'B', '2024-02-01', 300),
      (5, 'A', '2024-02-01', 250), (6, 'C', '2024-01-15', 175),
      (7, 'C', '2024-02-15', 225), (8, 'A', '2024-03-01', 350)
    AS t(txn_id, account, txn_date, amount)
  """)

  account_info = SparkEx.sql(s, """
    SELECT * FROM VALUES ('A', 'Premium'), ('B', 'Standard'), ('C', 'Premium')
    AS t(account, tier)
  """)

  w = SparkEx.Window.partition_by([col("account")])
    |> SparkEx.Window.order_by([asc(col("txn_date"))])

  transactions
  |> DataFrame.filter(Column.gte(col("amount"), lit(150)))
  |> DataFrame.join(account_info, ["account"], :inner)
  |> DataFrame.with_column("running_total", sum(col("amount")) |> Column.over(w))
  |> DataFrame.with_column("txn_rank", row_number() |> Column.over(w))
  |> DataFrame.group_by([col("account"), col("tier")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("num_txns"),
    max(col("running_total")) |> Column.alias_("max_running")
  ])
  |> DataFrame.order_by([desc(col("total"))])
  |> DataFrame.collect()
end)

IO.puts("=== Combine DataFrames exploration complete ===")
