# Round 80: exploratory combinations (queries + transforms + aggregations)
# Read-only: uses VALUES/temp views only; does not create/alter/drop persistent tables.
import SparkEx.Functions
alias SparkEx.{DataFrame, Column, GroupedData}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 50, printable_limit: 2000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 20, printable_limit: 1000)}")
  end
end

orders =
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      (1, 'EU', 'A', DATE '2024-01-01', 100.0, 2),
      (2, 'EU', 'A', DATE '2024-01-02', 120.0, 1),
      (3, 'EU', 'B', DATE '2024-01-02', 90.0, 3),
      (4, 'US', 'A', DATE '2024-01-01', 80.0, 1),
      (5, 'US', 'B', DATE '2024-01-03', 200.0, 4),
      (6, 'US', 'B', DATE '2024-01-03', 150.0, 2),
      (7, 'APAC', 'C', DATE '2024-01-02', 70.0, 2),
      (8, 'APAC', 'C', DATE '2024-01-04', 75.0, 1)
    AS t(order_id, region, product, order_date, price, qty)
    """
  )

products =
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      ('A', 'hardware', map('tier', 'gold', 'family', 'alpha')),
      ('B', 'hardware', map('tier', 'silver', 'family', 'beta')),
      ('C', 'software', map('tier', 'gold', 'family', 'gamma'))
    AS t(product, category, attrs)
    """
  )

run.("1) SQL CTE + join + window + filter", fn ->
  SparkEx.sql(
    s,
    """
    WITH base AS (
      SELECT o.region, o.product, o.order_date, o.price * o.qty AS amount
      FROM VALUES
        (1, 'EU', 'A', DATE '2024-01-01', 100.0, 2),
        (2, 'EU', 'A', DATE '2024-01-02', 120.0, 1),
        (3, 'EU', 'B', DATE '2024-01-02', 90.0, 3),
        (4, 'US', 'A', DATE '2024-01-01', 80.0, 1),
        (5, 'US', 'B', DATE '2024-01-03', 200.0, 4),
        (6, 'US', 'B', DATE '2024-01-03', 150.0, 2),
        (7, 'APAC', 'C', DATE '2024-01-02', 70.0, 2),
        (8, 'APAC', 'C', DATE '2024-01-04', 75.0, 1)
      AS o(order_id, region, product, order_date, price, qty)
    ),
    enriched AS (
      SELECT
        region,
        product,
        order_date,
        amount,
        SUM(amount) OVER (PARTITION BY region ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, order_date DESC) AS rn
      FROM base
    )
    SELECT region, product, order_date, amount, running_amount
    FROM enriched
    WHERE rn <= 2
    ORDER BY region, order_date
    """
  )
  |> DataFrame.collect()
end)

run.("2) DataFrame join + transform + grouped agg + post-filter", fn ->
  joined = DataFrame.join(orders, products, ["product"], :inner)

  joined
  |> DataFrame.with_columns([
    Column.multiply(col("price"), col("qty")) |> Column.alias_("amount"),
    when_(Column.gte(col("qty"), lit(3)), lit("bulk")) |> Column.otherwise(lit("retail")) |> Column.alias_("size_band")
  ])
  |> DataFrame.group_by([col("region"), col("category"), col("size_band")])
  |> GroupedData.agg([
    sum(col("amount")) |> Column.alias_("gross"),
    avg(col("amount")) |> Column.alias_("avg_ticket"),
    count(lit(1)) |> Column.alias_("n")
  ])
  |> DataFrame.filter(Column.gt(col("gross"), lit(100.0)))
  |> DataFrame.order_by([col("region"), desc(col("gross"))])
  |> DataFrame.collect()
end)

run.("3) SQL grouping sets + grouping_id + HAVING", fn ->
  SparkEx.sql(
    s,
    """
    SELECT
      region,
      product,
      grouping_id(region, product) AS gid,
      SUM(price * qty) AS gross,
      COUNT(*) AS n
    FROM VALUES
      (1, 'EU', 'A', DATE '2024-01-01', 100.0, 2),
      (2, 'EU', 'A', DATE '2024-01-02', 120.0, 1),
      (3, 'EU', 'B', DATE '2024-01-02', 90.0, 3),
      (4, 'US', 'A', DATE '2024-01-01', 80.0, 1),
      (5, 'US', 'B', DATE '2024-01-03', 200.0, 4),
      (6, 'US', 'B', DATE '2024-01-03', 150.0, 2),
      (7, 'APAC', 'C', DATE '2024-01-02', 70.0, 2),
      (8, 'APAC', 'C', DATE '2024-01-04', 75.0, 1)
    AS t(order_id, region, product, order_date, price, qty)
    GROUP BY GROUPING SETS ((region, product), (region), ())
    HAVING SUM(price * qty) > 100
    ORDER BY gid, region, product
    """
  )
  |> DataFrame.collect()
end)

run.("4) Map extraction + aggregate + percentile", fn ->
  DataFrame.join(orders, products, ["product"], :inner)
  |> DataFrame.with_columns([
    element_at(col("attrs"), lit("tier")) |> Column.alias_("tier"),
    Column.multiply(col("price"), col("qty")) |> Column.alias_("amount")
  ])
  |> DataFrame.group_by([col("region"), col("tier")])
  |> GroupedData.agg([
    expr("percentile_approx(amount, 0.5)") |> Column.alias_("p50"),
    sum(col("amount")) |> Column.alias_("gross")
  ])
  |> DataFrame.order_by([col("region"), col("tier")])
  |> DataFrame.collect()
end)

run.("5) explode + regroup + sorted collect_list", fn ->
  tags =
    SparkEx.sql(
      s,
      """
      SELECT * FROM VALUES
        ('EU', array('x', 'x', 'y')),
        ('US', array('x', 'z')),
        ('APAC', array('y', 'z', 'z'))
      AS t(region, tags)
      """
    )

  exploded =
    tags
    |> DataFrame.with_column("tag", explode(col("tags")))
    |> DataFrame.drop(["tags"])

  exploded
  |> DataFrame.group_by([col("region")])
  |> GroupedData.agg([
    collect_list(col("tag")) |> Column.alias_("bag"),
    collect_set(col("tag")) |> Column.alias_("uniq")
  ])
  |> DataFrame.with_column("bag_sorted", sort_array(col("bag")))
  |> DataFrame.order_by([col("region")])
  |> DataFrame.collect()
end)

run.("6) Multi-stage SQL with correlated scalar subquery", fn ->
  SparkEx.sql(
    s,
    """
    SELECT
      o.region,
      o.product,
      o.price,
      o.qty,
      (SELECT AVG(i.price) FROM VALUES
          (1, 'EU', 'A', DATE '2024-01-01', 100.0, 2),
          (2, 'EU', 'A', DATE '2024-01-02', 120.0, 1),
          (3, 'EU', 'B', DATE '2024-01-02', 90.0, 3),
          (4, 'US', 'A', DATE '2024-01-01', 80.0, 1),
          (5, 'US', 'B', DATE '2024-01-03', 200.0, 4),
          (6, 'US', 'B', DATE '2024-01-03', 150.0, 2),
          (7, 'APAC', 'C', DATE '2024-01-02', 70.0, 2),
          (8, 'APAC', 'C', DATE '2024-01-04', 75.0, 1)
        AS i(order_id, region, product, order_date, price, qty)
        WHERE i.region = o.region) AS avg_region_price
    FROM VALUES
      (1, 'EU', 'A', DATE '2024-01-01', 100.0, 2),
      (2, 'EU', 'A', DATE '2024-01-02', 120.0, 1),
      (3, 'EU', 'B', DATE '2024-01-02', 90.0, 3),
      (4, 'US', 'A', DATE '2024-01-01', 80.0, 1),
      (5, 'US', 'B', DATE '2024-01-03', 200.0, 4),
      (6, 'US', 'B', DATE '2024-01-03', 150.0, 2),
      (7, 'APAC', 'C', DATE '2024-01-02', 70.0, 2),
      (8, 'APAC', 'C', DATE '2024-01-04', 75.0, 1)
    AS o(order_id, region, product, order_date, price, qty)
    ORDER BY region, product
    """
  )
  |> DataFrame.collect()
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
