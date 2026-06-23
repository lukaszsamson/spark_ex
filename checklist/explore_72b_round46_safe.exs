# Round 46b (safe): Tests that were blocked by EX-112 crash
# NOTE: list_databases(s, catalog: "sfx") CRASHES SESSION (EX-112)
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 80, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. Catalog operations (correct API)
# =============================================

run.("1a. list_databases (no filter)", fn ->
  Catalog.list_databases(s)
end)

run.("1b. list_databases with pattern string", fn ->
  Catalog.list_databases(s, "spark_*")
end)

run.("1c. list_tables", fn ->
  Catalog.list_tables(s, "spark_ex_db")
end)

run.("1d. current catalog and database", fn ->
  cat = Catalog.current_catalog(s)
  db = Catalog.current_database(s)
  %{catalog: cat, database: db}
end)

# =============================================
# 2. Date dimension via SQL
# =============================================

run.("2. date dimension table", fn ->
  SparkEx.sql(s, """
    SELECT
      date_add(DATE '2024-01-01', CAST(id AS INT)) AS date_key,
      year(date_add(DATE '2024-01-01', CAST(id AS INT))) AS yr,
      month(date_add(DATE '2024-01-01', CAST(id AS INT))) AS mo,
      dayofweek(date_add(DATE '2024-01-01', CAST(id AS INT))) AS dow,
      quarter(date_add(DATE '2024-01-01', CAST(id AS INT))) AS qtr
    FROM range(0, 366)
  """)
  |> DataFrame.filter(Column.eq(col("mo"), lit(2)))
  |> DataFrame.group_by([col("dow")])
  |> GroupedData.agg([count(col("date_key")) |> Column.alias_("count")])
  |> DataFrame.sort([asc(col("dow"))])
  |> DataFrame.collect()
end)

# =============================================
# 3. approx_count_distinct at scale
# =============================================

run.("3. approx_count_distinct on 5000 rows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5000, fn i -> %{"grp" => rem(i, 10), "val" => rem(i * 1337, 500)} end),
    schema: "grp INT, val INT")

  df
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    count(col("val")) |> Column.alias_("total"),
    count_distinct(col("val")) |> Column.alias_("exact_distinct"),
    approx_count_distinct(col("val")) |> Column.alias_("approx_distinct")
  ])
  |> DataFrame.sort([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 4. Conditional aggregation
# =============================================

run.("4. conditional aggregation (CASE WHEN inside agg)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)),
        "status" => Enum.at(["active", "inactive", "pending"], rem(i, 3)),
        "salary" => 30000 + rem(i * 997, 70000)}
    end), schema: "dept STRING, status STRING, salary INT")

  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([
    count(col("salary")) |> Column.alias_("total"),
    sum(when_(Column.eq(col("status"), lit("active")), col("salary"))
      |> Column.otherwise(lit(0))) |> Column.alias_("active_salary"),
    count(when_(Column.eq(col("status"), lit("active")), lit(1))) |> Column.alias_("active_count"),
    avg(col("salary")) |> Column.alias_("avg_all"),
    avg(when_(Column.eq(col("status"), lit("active")), col("salary"))) |> Column.alias_("avg_active")
  ])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

# =============================================
# 5. String parsing pipeline
# =============================================

run.("5. string parsing: regex extract fields from log lines", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"log" => "2024-01-15 INFO user=alice action=login ip=1.2.3.4"},
    %{"log" => "2024-01-15 ERROR user=bob action=payment ip=5.6.7.8"},
    %{"log" => "2024-01-16 INFO user=alice action=logout ip=1.2.3.4"},
    %{"log" => "2024-01-16 WARN user=charlie action=login ip=9.10.11.12"}
  ], schema: "log STRING")

  df |> DataFrame.select([
    col("log"),
    regexp_extract(col("log"), lit("^(\\S+)"), lit(1)) |> Column.alias_("date"),
    regexp_extract(col("log"), lit("(INFO|ERROR|WARN)"), lit(1)) |> Column.alias_("level"),
    regexp_extract(col("log"), lit("user=(\\S+)"), lit(1)) |> Column.alias_("user"),
    regexp_extract(col("log"), lit("action=(\\S+)"), lit(1)) |> Column.alias_("action"),
    regexp_extract(col("log"), lit("ip=(\\S+)"), lit(1)) |> Column.alias_("ip")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Stack (SQL UNPIVOT alternative)
# =============================================

run.("6. stack via SQL LATERAL VIEW", fn ->
  SparkEx.sql(s, """
    SELECT id, metric, value
    FROM (
      SELECT 1 AS id, 10 AS q1_sales, 20 AS q2_sales, 30 AS q3_sales, 40 AS q4_sales
      UNION ALL
      SELECT 2 AS id, 15 AS q1_sales, 25 AS q2_sales, 35 AS q3_sales, 45 AS q4_sales
    ) t
    LATERAL VIEW stack(4,
      'Q1', q1_sales, 'Q2', q2_sales, 'Q3', q3_sales, 'Q4', q4_sales
    ) AS metric, value
    ORDER BY id, metric
  """) |> DataFrame.collect()
end)

# =============================================
# 7. Sample with seed for reproducibility
# =============================================

run.("7. sample with seed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i, "grp" => rem(i, 5)} end),
    schema: "id INT, grp INT")

  {:ok, rows1} = df |> DataFrame.sample(fraction: 0.1, seed: 42) |> DataFrame.collect()
  {:ok, rows2} = df |> DataFrame.sample(fraction: 0.1, seed: 42) |> DataFrame.collect()

  %{sample1_count: length(rows1), sample2_count: length(rows2),
    same_results: rows1 == rows2}
end)

# =============================================
# 8. Multiple temp views cross-referenced
# =============================================

run.("8. multiple temp views cross-referenced via SQL", fn ->
  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"oid" => 1, "cid" => 1, "amount" => 100},
    %{"oid" => 2, "cid" => 2, "amount" => 200},
    %{"oid" => 3, "cid" => 1, "amount" => 150}
  ], schema: "oid INT, cid INT, amount INT")

  {:ok, customers} = SparkEx.create_dataframe(s, [
    %{"cid" => 1, "name" => "Alice"},
    %{"cid" => 2, "name" => "Bob"}
  ], schema: "cid INT, name STRING")

  DataFrame.create_temp_view(orders, "tv_orders_#{run_id}")
  DataFrame.create_temp_view(customers, "tv_cust_#{run_id}")

  result = SparkEx.sql(s, """
    SELECT c.name, COUNT(o.oid) AS order_count, SUM(o.amount) AS total
    FROM tv_orders_#{run_id} o
    JOIN tv_cust_#{run_id} c ON o.cid = c.cid
    GROUP BY c.name
    ORDER BY total DESC
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "tv_orders_#{run_id}")
  DataFrame.drop_temp_view(s, "tv_cust_#{run_id}")
  result
end)

# =============================================
# 9. Transform chain with windows
# =============================================

run.("9. transform chain: enrich → window → filter → format", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"emp" => "E#{i}", "dept" => Enum.at(["Eng", "Sales", "Ops"], rem(i, 3)),
        "salary" => 40000 + rem(i * 997, 40000)}
    end), schema: "emp STRING, dept STRING, salary INT")

  result = df
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("tax",
      Column.multiply(Column.cast(col("salary"), "DOUBLE"), lit(0.25)))
  end)
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("net",
      Column.minus(Column.cast(col("salary"), "DOUBLE"), col("tax")))
  end)
  |> DataFrame.transform(fn d ->
    w = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([desc(col("salary"))])
    d |> DataFrame.with_column("dept_rank", dense_rank() |> Column.over(w))
  end)
  |> DataFrame.filter(Column.lte(col("dept_rank"), lit(3)))
  |> DataFrame.sort([asc(col("dept")), asc(col("dept_rank"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  Enum.take(rows, 9)
end)

# =============================================
# 10. Hunt: other Catalog functions with keyword opts that might crash
# =============================================

run.("10a. list_tables with keyword opts (does it crash?)", fn ->
  Catalog.list_tables(s, db_name: "spark_ex_db")
end)

run.("10b. list_functions with keyword (does it crash?)", fn ->
  Catalog.list_functions(s, db_name: "default")
end)

run.("10c. list_catalogs", fn ->
  Catalog.list_catalogs(s)
end)

# =============================================
# 11. Hunt: Catalog operations that might pass opts to protobuf
# =============================================

run.("11a. table_exists?", fn ->
  Catalog.table_exists?(s, "nonexistent_table")
end)

run.("11b. database_exists?", fn ->
  Catalog.database_exists?(s, "spark_ex_db")
end)

run.("11c. function_exists?", fn ->
  Catalog.function_exists?(s, "abs")
end)

IO.puts("=== Round 46b complete ===")
