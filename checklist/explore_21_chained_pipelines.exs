# Exploratory: Deep chained pipelines, multi-step transforms, combining results
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

# === 1. Multi-stage ETL: filter → enrich → aggregate → join back ===
run.("1. Multi-stage ETL pipeline", fn ->
  orders = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 101, 'East', 50.0, '2024-01-15'),
      (2, 102, 'West', 120.0, '2024-01-20'),
      (3, 101, 'East', 75.0, '2024-02-01'),
      (4, 103, 'East', 200.0, '2024-02-15'),
      (5, 102, 'West', 90.0, '2024-03-01'),
      (6, 101, 'East', 30.0, '2024-03-10'),
      (7, 104, 'West', 300.0, '2024-03-15'),
      (8, 103, 'East', 150.0, '2024-04-01')
    AS t(order_id, cust_id, region, amount, order_date)
  """)

  # Step 1: Filter to Q1 only
  q1 = DataFrame.filter(orders, Column.lte(col("order_date"), lit("2024-03-31")))

  # Step 2: Add month column
  q1_enriched = DataFrame.with_column(q1, "month",
    substring(col("order_date"), lit(1), lit(7))
  )

  # Step 3: Per-customer aggregation
  cust_agg = DataFrame.group_by(q1_enriched, [col("cust_id")])
    |> DataFrame.agg([
      sum(col("amount")) |> Column.alias_("total_spend"),
      count(lit(1)) |> Column.alias_("order_count"),
      avg(col("amount")) |> Column.alias_("avg_order")
    ])

  # Step 4: Rank customers
  w = SparkEx.Window.order_by([desc(col("total_spend"))])
  ranked = DataFrame.with_column(cust_agg, "rank", row_number() |> Column.over(w))

  # Step 5: Join back with orders to get region
  region_map = DataFrame.select(q1, [col("cust_id"), col("region")])
    |> DataFrame.distinct()

  result = DataFrame.join(ranked, region_map, ["cust_id"], :left)
    |> DataFrame.order_by([asc(col("rank"))])
    |> DataFrame.collect()

  result
end)

# === 2. Chained window functions on aggregated data ===
run.("2. Aggregate then window over aggregation", fn ->
  sales = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('Alice', 'East', 'Jan', 100), ('Alice', 'East', 'Feb', 150),
      ('Alice', 'East', 'Mar', 200), ('Bob', 'West', 'Jan', 80),
      ('Bob', 'West', 'Feb', 120), ('Bob', 'West', 'Mar', 160),
      ('Carol', 'East', 'Jan', 90), ('Carol', 'East', 'Feb', 110),
      ('Carol', 'East', 'Mar', 130)
    AS t(rep, region, month, amount)
  """)

  # Step 1: Monthly totals per rep
  monthly = DataFrame.group_by(sales, [col("rep"), col("region"), col("month")])
    |> DataFrame.agg([sum(col("amount")) |> Column.alias_("total")])

  # Step 2: Running total per rep
  w_rep = SparkEx.Window.partition_by([col("rep")])
    |> SparkEx.Window.order_by([asc(col("month"))])

  # Step 3: Regional running total
  w_region = SparkEx.Window.partition_by([col("region")])
    |> SparkEx.Window.order_by([asc(col("month")), asc(col("rep"))])

  monthly
  |> DataFrame.with_column("rep_running", sum(col("total")) |> Column.over(w_rep))
  |> DataFrame.with_column("region_running", sum(col("total")) |> Column.over(w_region))
  |> DataFrame.with_column("rep_rank", rank() |> Column.over(
    SparkEx.Window.partition_by([col("month")]) |> SparkEx.Window.order_by([desc(col("total"))])
  ))
  |> DataFrame.order_by([asc(col("month")), desc(col("total"))])
  |> DataFrame.collect()
end)

# === 3. Union multiple transformed DataFrames ===
run.("3. Union of differently-transformed DataFrames", fn ->
  base = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 55000)
    AS t(id, name, salary)
  """)

  high = DataFrame.filter(base, Column.gt(col("salary"), lit(55000)))
    |> DataFrame.with_column("category", lit("high"))
    |> DataFrame.select([col("id"), col("name"), col("salary"), col("category")])

  low = DataFrame.filter(base, Column.lte(col("salary"), lit(55000)))
    |> DataFrame.with_column("category", lit("low"))
    |> DataFrame.select([col("id"), col("name"), col("salary"), col("category")])

  DataFrame.union(high, low)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 4. DataFrame reused as both left and right in different joins ===
run.("4. Same DataFrame joined differently twice", fn ->
  employees = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', 100, NULL), (2, 'Bob', 100, 1), (3, 'Carol', 200, 1),
      (4, 'Dave', 200, 3), (5, 'Eve', 100, 2)
    AS t(id, name, dept_id, manager_id)
  """)

  depts = SparkEx.sql(s, """
    SELECT * FROM VALUES (100, 'Engineering'), (200, 'Sales') AS t(dept_id, dept_name)
  """)

  # Join 1: with departments
  with_dept = DataFrame.join(employees, depts, ["dept_id"], :left)

  # Join 2: self-join for manager name (using aliased employees)
  managers = DataFrame.select(employees, [
    Column.alias_(col("id"), "mgr_id"),
    Column.alias_(col("name"), "manager_name")
  ])

  with_dept
  |> DataFrame.join(managers,
    Column.eq(col("manager_id"), col("mgr_id")),
    :left
  )
  |> DataFrame.select([
    col("id"), col("name"), col("dept_name"), col("manager_name")
  ])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 5. Chained temp views referencing each other ===
run.("5. Chained temp views", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()

  # View 1: base data
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50) AS t(id, val)
  """)
  |> DataFrame.create_or_replace_temp_view("chain_base_#{run_id}")

  # View 2: filtered from view 1
  SparkEx.sql(s, "SELECT * FROM chain_base_#{run_id} WHERE val > 20")
  |> DataFrame.create_or_replace_temp_view("chain_filtered_#{run_id}")

  # View 3: aggregated from view 2
  SparkEx.sql(s, "SELECT SUM(val) AS total, COUNT(*) AS cnt FROM chain_filtered_#{run_id}")
  |> DataFrame.create_or_replace_temp_view("chain_agg_#{run_id}")

  # Query joining view 1 with view 3
  result = SparkEx.sql(s, """
    SELECT b.id, b.val, a.total, a.cnt,
           CAST(b.val AS DOUBLE) / a.total * 100 AS pct_of_total
    FROM chain_base_#{run_id} b
    CROSS JOIN chain_agg_#{run_id} a
    WHERE b.val > 20
    ORDER BY b.id
  """) |> DataFrame.collect()

  # Cleanup
  SparkEx.Catalog.drop_temp_view(s, "chain_base_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "chain_filtered_#{run_id}")
  SparkEx.Catalog.drop_temp_view(s, "chain_agg_#{run_id}")

  result
end)

# === 6. Deeply nested Column expressions ===
run.("6. Complex nested column expression", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 10, 5), (2, 20, 15), (3, 30, 25), (4, 40, 35)
    AS t(id, price, cost)
  """)
  |> DataFrame.with_column("margin",
    Column.divide(
      Column.minus(col("price"), col("cost")),
      col("price")
    )
  )
  |> DataFrame.with_column("margin_pct",
    round(Column.multiply(col("margin"), lit(100)), lit(1))
  )
  |> DataFrame.with_column("tier",
    when_(Column.gt(col("margin_pct"), lit(40.0)), lit("A"))
    |> Column.when_(Column.gt(col("margin_pct"), lit(30.0)), lit("B"))
    |> Column.when_(Column.gt(col("margin_pct"), lit(20.0)), lit("C"))
    |> Column.otherwise(lit("D"))
  )
  |> DataFrame.with_column("score",
    Column.plus(
      Column.multiply(col("margin_pct"), lit(2)),
      when_(Column.eq(col("tier"), lit("A")), lit(100))
      |> Column.when_(Column.eq(col("tier"), lit("B")), lit(50))
      |> Column.otherwise(lit(0))
    )
  )
  |> DataFrame.order_by([desc(col("score"))])
  |> DataFrame.collect()
end)

# === 7. Multi-level group_by: group → agg → group again → agg ===
run.("7. Double aggregation", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('East', 'Alice', 'Jan', 100), ('East', 'Alice', 'Feb', 150),
      ('East', 'Bob', 'Jan', 200), ('East', 'Bob', 'Feb', 250),
      ('West', 'Carol', 'Jan', 120), ('West', 'Carol', 'Feb', 180),
      ('West', 'Dave', 'Jan', 90), ('West', 'Dave', 'Feb', 110)
    AS t(region, rep, month, amount)
  """)
  # First level: sum per region+rep
  |> DataFrame.group_by([col("region"), col("rep")])
  |> DataFrame.agg([sum(col("amount")) |> Column.alias_("rep_total")])
  # Second level: avg of rep totals per region
  |> DataFrame.group_by([col("region")])
  |> DataFrame.agg([
    avg(col("rep_total")) |> Column.alias_("avg_rep_total"),
    max(col("rep_total")) |> Column.alias_("max_rep_total"),
    min(col("rep_total")) |> Column.alias_("min_rep_total"),
    count(lit(1)) |> Column.alias_("num_reps")
  ])
  |> DataFrame.order_by([asc(col("region"))])
  |> DataFrame.collect()
end)

# === 8. Explode → re-aggregate → join back ===
run.("8. Explode then re-aggregate pipeline", fn ->
  base = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', array('eng', 'lead', 'python')),
      (2, 'Bob', array('sales', 'python')),
      (3, 'Carol', array('eng', 'java')),
      (4, 'Dave', array('sales', 'lead'))
    AS t(id, name, skills)
  """)

  # Explode skills
  exploded = DataFrame.select(base, [
    col("id"), col("name"),
    explode(col("skills")) |> Column.alias_("skill")
  ])

  # Count skill frequency
  skill_counts = DataFrame.group_by(exploded, [col("skill")])
    |> DataFrame.agg([count(lit(1)) |> Column.alias_("num_people")])

  # Join back: each person's skills with frequency
  exploded
  |> DataFrame.join(skill_counts, ["skill"], :left)
  |> DataFrame.order_by([asc(col("name")), desc(col("num_people"))])
  |> DataFrame.collect()
end)

# === 9. Chained filter + with_column referencing new columns ===
run.("9. Chain: add column → filter on new column → add another → filter again", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 100, 50), (2, 200, 180), (3, 300, 100),
      (4, 400, 350), (5, 500, 250), (6, 50, 45)
    AS t(id, revenue, cost)
  """)
  |> DataFrame.with_column("profit", Column.minus(col("revenue"), col("cost")))
  |> DataFrame.filter(Column.gt(col("profit"), lit(0)))
  |> DataFrame.with_column("margin_pct",
    Column.multiply(
      Column.divide(col("profit"), col("revenue")),
      lit(100)
    )
  )
  |> DataFrame.filter(Column.gt(col("margin_pct"), lit(30.0)))
  |> DataFrame.with_column("rating",
    when_(Column.gt(col("margin_pct"), lit(60.0)), lit("excellent"))
    |> Column.otherwise(lit("good"))
  )
  |> DataFrame.order_by([desc(col("margin_pct"))])
  |> DataFrame.collect()
end)

# === 10. Multiple except/intersect/union chained ===
run.("10. Set operations chain: union → except → intersect", fn ->
  a = SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3),(4),(5) AS t(id)")
  b = SparkEx.sql(s, "SELECT * FROM VALUES (3),(4),(5),(6),(7) AS t(id)")
  c = SparkEx.sql(s, "SELECT * FROM VALUES (4),(5),(6),(7),(8) AS t(id)")

  # (A union B) except C → should be {1, 2, 3}
  result1 = DataFrame.union(a, b) |> DataFrame.except(c)
    |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()

  # (A intersect B) union (B intersect C) → {3,4,5} union {4,5,6,7} = {3,4,5,6,7}
  result2 = DataFrame.union(
    DataFrame.intersect(a, b),
    DataFrame.intersect(b, c)
  ) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()

  %{union_except: result1, intersect_union: result2}
end)

# === 11. DataFrame created from multiple create_dataframe calls joined ===
run.("11. Join two create_dataframe results", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s,
    [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}],
    schema: "id INT, name STRING"
  )
  {:ok, df2} = SparkEx.create_dataframe(s,
    [%{"id" => 1, "score" => 95}, %{"id" => 2, "score" => 87}],
    schema: "id INT, score INT"
  )
  DataFrame.join(df1, df2, ["id"], :inner)
  |> DataFrame.collect()
end)

# === 12. Complex pipeline with sample + describe ===
run.("12. Sample → transform → describe", fn ->
  SparkEx.sql(s, "SELECT id, id * RAND(42) AS noise FROM range(0, 100)")
  |> DataFrame.sample(0.3, seed: 42)
  |> DataFrame.with_column("category",
    when_(Column.gt(col("noise"), lit(50)), lit("high"))
    |> Column.otherwise(lit("low"))
  )
  |> DataFrame.describe()
  |> DataFrame.collect()
end)

# === 13. Pivot result used in further operations ===
run.("13. Pivot → with_column → filter", fn ->
  sales = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('East', 'Q1', 100), ('East', 'Q2', 200),
      ('West', 'Q1', 150), ('West', 'Q2', 250),
      ('East', 'Q3', 300), ('West', 'Q3', 350)
    AS t(region, quarter, amount)
  """)

  DataFrame.group_by(sales, [col("region")])
  |> DataFrame.pivot(col("quarter"), ["Q1", "Q2", "Q3"])
  |> DataFrame.agg([sum(col("amount"))])
  |> DataFrame.with_column("total", Column.plus(Column.plus(col("Q1"), col("Q2")), col("Q3")))
  |> DataFrame.with_column("growth_pct",
    round(Column.multiply(
      Column.divide(Column.minus(col("Q3"), col("Q1")), col("Q1")),
      lit(100)
    ), lit(1))
  )
  |> DataFrame.order_by([desc(col("growth_pct"))])
  |> DataFrame.collect()
end)

# === 14. unpivot → group_by → having equivalent ===
run.("14. Unpivot → aggregate → filter on aggregate", fn ->
  wide = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('Alice', 90, 85, 92, 88),
      ('Bob', 70, 75, 68, 72),
      ('Carol', 95, 98, 92, 96)
    AS t(name, test1, test2, test3, test4)
  """)

  DataFrame.unpivot(wide, ["name"],
    [{"test1", "test1"}, {"test2", "test2"}, {"test3", "test3"}, {"test4", "test4"}],
    "test", "score"
  )
  |> DataFrame.group_by([col("name")])
  |> DataFrame.agg([
    avg(col("score")) |> Column.alias_("avg_score"),
    min(col("score")) |> Column.alias_("min_score"),
    max(col("score")) |> Column.alias_("max_score"),
    Column.minus(max(col("score")), min(col("score"))) |> Column.alias_("range")
  ])
  |> DataFrame.filter(Column.gte(col("avg_score"), lit(85)))
  |> DataFrame.order_by([desc(col("avg_score"))])
  |> DataFrame.collect()
end)

# === 15. Deeply chained with_columns (10 new columns) ===
run.("15. Add 10 derived columns sequentially", fn ->
  SparkEx.sql(s, "SELECT 100.0 AS base_val")
  |> DataFrame.with_column("c1", Column.multiply(col("base_val"), lit(1.1)))
  |> DataFrame.with_column("c2", Column.multiply(col("c1"), lit(1.1)))
  |> DataFrame.with_column("c3", Column.multiply(col("c2"), lit(1.1)))
  |> DataFrame.with_column("c4", Column.multiply(col("c3"), lit(1.1)))
  |> DataFrame.with_column("c5", Column.multiply(col("c4"), lit(1.1)))
  |> DataFrame.with_column("c6", Column.multiply(col("c5"), lit(1.1)))
  |> DataFrame.with_column("c7", Column.multiply(col("c6"), lit(1.1)))
  |> DataFrame.with_column("c8", Column.multiply(col("c7"), lit(1.1)))
  |> DataFrame.with_column("c9", Column.multiply(col("c8"), lit(1.1)))
  |> DataFrame.with_column("c10", Column.multiply(col("c9"), lit(1.1)))
  |> DataFrame.select([col("base_val"), col("c10")])
  |> DataFrame.collect()
end)

IO.puts("=== Chained pipelines exploration complete ===")
