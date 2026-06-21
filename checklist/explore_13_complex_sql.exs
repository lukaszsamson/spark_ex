# Exploratory: Complex SQL patterns, CTEs, subqueries, lateral views, type constructors
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

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

# === CTE (Common Table Expression) ===
run.("1. CTE via SQL", fn ->
  SparkEx.sql(s, """
    WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
      FROM VALUES
        (1, 'Alice', 'Eng', 80000),
        (2, 'Bob', 'Eng', 90000),
        (3, 'Carol', 'Sales', 70000),
        (4, 'Dave', 'Sales', 85000),
        (5, 'Eve', 'Eng', 95000)
      AS t(id, name, dept, salary)
    )
    SELECT name, dept, salary FROM ranked WHERE rn = 1
  """) |> DataFrame.collect()
end)

# === Correlated subquery ===
run.("2. Correlated subquery via SQL", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', 'Eng', 80000),
      (2, 'Bob', 'Eng', 90000),
      (3, 'Carol', 'Sales', 70000),
      (4, 'Dave', 'Sales', 85000)
    AS emp(id, name, dept, salary)
    WHERE salary > (
      SELECT AVG(salary) FROM VALUES
        (1, 'Alice', 'Eng', 80000),
        (2, 'Bob', 'Eng', 90000),
        (3, 'Carol', 'Sales', 70000),
        (4, 'Dave', 'Sales', 85000)
      AS emp2(id, name, dept, salary)
      WHERE emp2.dept = emp.dept
    )
  """) |> DataFrame.collect()
end)

# === LATERAL VIEW explode ===
run.("3. LATERAL VIEW explode", fn ->
  SparkEx.sql(s, """
    SELECT id, tag
    FROM VALUES (1, array('a','b','c')), (2, array('d','e'))
    AS t(id, tags)
    LATERAL VIEW explode(tags) AS tag
  """) |> DataFrame.collect()
end)

# === LATERAL VIEW OUTER explode (preserves nulls) ===
run.("4. LATERAL VIEW OUTER explode", fn ->
  SparkEx.sql(s, """
    SELECT id, tag
    FROM VALUES (1, array('a','b')), (2, array()), (3, NULL)
    AS t(id, tags)
    LATERAL VIEW OUTER explode(tags) AS tag
  """) |> DataFrame.collect()
end)

# === Multiple CTEs ===
run.("5. Multiple CTEs", fn ->
  SparkEx.sql(s, """
    WITH
      sales AS (
        SELECT * FROM VALUES ('East', 100), ('West', 200), ('East', 300) AS t(region, amount)
      ),
      agg AS (
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region
      )
    SELECT * FROM agg ORDER BY region
  """) |> DataFrame.collect()
end)

# === TABLESAMPLE ===
run.("6. TABLESAMPLE via SQL", fn ->
  SparkEx.sql(s, """
    SELECT * FROM (
      SELECT * FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(id)
    ) TABLESAMPLE (50 PERCENT) REPEATABLE (42)
  """) |> DataFrame.collect()
end)

# === PIVOT via SQL ===
run.("7. PIVOT via SQL", fn ->
  SparkEx.sql(s, """
    SELECT * FROM (
      SELECT region, rep, amount FROM VALUES
        ('East', 'Alice', 100), ('West', 'Bob', 200),
        ('East', 'Alice', 150), ('West', 'Bob', 250)
      AS t(region, rep, amount)
    )
    PIVOT (
      SUM(amount) FOR rep IN ('Alice', 'Bob')
    )
    ORDER BY region
  """) |> DataFrame.collect()
end)

# === UNPIVOT via SQL ===
run.("8. UNPIVOT via SQL", fn ->
  SparkEx.sql(s, """
    SELECT * FROM (
      SELECT * FROM VALUES (1, 10, 20, 30), (2, 40, 50, 60) AS t(id, jan, feb, mar)
    )
    UNPIVOT (
      value FOR month IN (jan, feb, mar)
    )
    ORDER BY id, month
  """) |> DataFrame.collect()
end)

# === Complex date arithmetic ===
run.("9. Complex date arithmetic", fn ->
  SparkEx.sql(s, """
    SELECT
      DATE '2024-01-15' AS start_date,
      DATE '2024-06-30' AS end_date,
      DATEDIFF(DATE '2024-06-30', DATE '2024-01-15') AS day_diff,
      MONTHS_BETWEEN(DATE '2024-06-30', DATE '2024-01-15') AS month_diff,
      ADD_MONTHS(DATE '2024-01-15', 6) AS plus_6mo,
      DATE_ADD(DATE '2024-01-15', 100) AS plus_100d,
      LAST_DAY(DATE '2024-02-15') AS last_feb,
      NEXT_DAY(DATE '2024-01-15', 'Monday') AS next_monday,
      DATE_TRUNC('month', TIMESTAMP '2024-06-15 14:30:00') AS trunc_month,
      EXTRACT(DOW FROM DATE '2024-01-15') AS dow,
      EXTRACT(WEEK FROM DATE '2024-06-15') AS week_num
  """) |> DataFrame.collect()
end)

# === Complex timestamp operations ===
run.("10. Timestamp operations", fn ->
  SparkEx.sql(s, """
    SELECT
      TIMESTAMP '2024-06-15 14:30:45.123' AS ts,
      CAST(TIMESTAMP '2024-06-15 14:30:45.123' AS LONG) AS unix_ts,
      DATE_FORMAT(TIMESTAMP '2024-06-15 14:30:45.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS formatted,
      TO_UTC_TIMESTAMP(TIMESTAMP '2024-06-15 14:30:45', 'America/New_York') AS utc_ts,
      FROM_UTC_TIMESTAMP(TIMESTAMP '2024-06-15 14:30:45', 'America/New_York') AS ny_ts
  """) |> DataFrame.collect()
end)

# === generate_series equivalent ===
run.("11. Generate series via explode(sequence)", fn ->
  SparkEx.sql(s, """
    SELECT id FROM (SELECT explode(sequence(1, 100)) AS id)
    WHERE id % 7 = 0
    ORDER BY id
  """) |> DataFrame.collect()
end)

# === Complex CASE via Column API ===
run.("12. Nested when/otherwise", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40)
    AS t(id, grp, val)
  """)
  |> DataFrame.with_column("category",
    when_(Column.and_(Column.eq(col("grp"), lit("a")), Column.gt(col("val"), lit(15))), lit("a_high"))
    |> Column.when_(Column.eq(col("grp"), lit("a")), lit("a_low"))
    |> Column.when_(Column.eq(col("grp"), lit("b")), lit("b_any"))
    |> Column.otherwise(lit("other"))
  )
  |> DataFrame.collect()
end)

# === Column.substr ===
run.("13. Column.substr", fn ->
  SparkEx.sql(s, "SELECT 'Hello World' AS text")
  |> DataFrame.select([
    Column.substr(col("text"), lit(1), lit(5)) |> Column.alias_("first5"),
    Column.substr(col("text"), lit(7), lit(5)) |> Column.alias_("last5")
  ])
  |> DataFrame.collect()
end)

# === Column.contains / startswith / endswith ===
run.("14. Column.contains / startswith / endswith", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('apple pie'), ('banana bread'), ('apple sauce'), ('cherry pie')
    AS t(item)
  """)
  |> DataFrame.select([
    col("item"),
    Column.contains(col("item"), lit("pie")) |> Column.alias_("has_pie"),
    Column.startswith(col("item"), lit("apple")) |> Column.alias_("starts_apple"),
    Column.endswith(col("item"), lit("bread")) |> Column.alias_("ends_bread")
  ])
  |> DataFrame.collect()
end)

# === Column.isin with mixed types ===
run.("15. Column.isin with strings", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('Alice'), ('Bob'), ('Carol'), ('Dave')
    AS t(name)
  """)
  |> DataFrame.filter(Column.isin(col("name"), ["Alice", "Carol"]))
  |> DataFrame.collect()
end)

# === Multiple orderBy with nulls handling ===
run.("16. Order by with nulls first/last", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL), (5, 'b')
    AS t(id, name)
  """)
  |> DataFrame.order_by([asc_nulls_first(col("name")), desc(col("id"))])
  |> DataFrame.collect()
end)

# === Recursive CTE (not supported on Spark 3.5, expect error) ===
run.("17. Recursive CTE (expect failure on Spark 3.5)", fn ->
  SparkEx.sql(s, """
    WITH RECURSIVE nums AS (
      SELECT 1 AS n
      UNION ALL
      SELECT n + 1 FROM nums WHERE n < 5
    )
    SELECT * FROM nums
  """) |> DataFrame.collect()
end)

# === QUALIFY (Spark 3.5+ via subquery) ===
run.("18. Window filter via subquery (QUALIFY equivalent)", fn ->
  SparkEx.sql(s, """
    SELECT name, dept, salary FROM (
      SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
      FROM VALUES
        (1, 'Alice', 'Eng', 80000),
        (2, 'Bob', 'Eng', 90000),
        (3, 'Carol', 'Sales', 70000),
        (4, 'Dave', 'Sales', 85000)
      AS t(id, name, dept, salary)
    ) WHERE rnk <= 1
  """) |> DataFrame.collect()
end)

# === expr() function ===
run.("19. DataFrame.select_expr with complex expressions", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 55000) AS t(id, name, salary)
  """)
  |> DataFrame.select_expr([
    "id",
    "UPPER(name) AS upper_name",
    "salary * 1.1 AS raised_salary",
    "CASE WHEN salary > 55000 THEN 'high' ELSE 'low' END AS band"
  ])
  |> DataFrame.collect()
end)

# === Table-valued function: range ===
run.("20. Table-valued function: range", fn ->
  SparkEx.sql(s, "SELECT * FROM range(0, 10, 2)") |> DataFrame.collect()
end)

IO.puts("=== Complex SQL exploration complete ===")
