# Round 46: Iceberg/table operations, WriterV2 create, complex nested data,
# SQL analytics functions, edge cases
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
# 1. WriterV2 create (new table)
# =============================================

run.("1. WriterV2 create table via Iceberg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5},
    %{"id" => 2, "name" => "Bob", "score" => 88.0},
    %{"id" => 3, "name" => "Charlie", "score" => 92.3}
  ], schema: "id INT, name STRING, score DOUBLE")

  table_name = "sfx.spark_ex_db.test_wv2_#{run_id}"

  # Create table via WriterV2
  result = DataFrame.write_v2(df, table_name)
    |> SparkEx.WriterV2.using("iceberg")
    |> SparkEx.WriterV2.table_property("write.format.default", "parquet")
    |> SparkEx.WriterV2.create()

  # Read back
  read_result = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()

  # Clean up
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name}")

  %{create: result, read: read_result}
end)

# =============================================
# 2. WriterV2 append
# =============================================

run.("2. WriterV2 create then append", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")

  table_name = "sfx.spark_ex_db.test_append_#{run_id}"

  # Create
  DataFrame.write_v2(df1, table_name)
    |> SparkEx.WriterV2.using("iceberg")
    |> SparkEx.WriterV2.create()

  # Append
  append_result = DataFrame.write_v2(df2, table_name)
    |> SparkEx.WriterV2.append()

  # Read back
  read_result = SparkEx.Reader.table(s, table_name)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  # Clean up
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name}")

  %{append: append_result, read: read_result}
end)

# =============================================
# 3. WriterV2 overwrite
# =============================================

run.("3. WriterV2 overwrite", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "old"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "new"}
  ], schema: "id INT, val STRING")

  table_name = "sfx.spark_ex_db.test_overwrite_#{run_id}"

  DataFrame.write_v2(df1, table_name)
    |> SparkEx.WriterV2.using("iceberg")
    |> SparkEx.WriterV2.create()

  # Overwrite with condition
  overwrite_result = DataFrame.write_v2(df2, table_name)
    |> SparkEx.WriterV2.overwrite(Column.eq(col("id"), lit(1)))

  read_result = SparkEx.Reader.table(s, table_name)
    |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name}")

  %{overwrite: overwrite_result, read: read_result}
end)

# =============================================
# 4. Complex nested struct processing
# =============================================

run.("4. nested struct: create → access → transform", fn ->
  df = SparkEx.sql(s, """
    SELECT
      named_struct(
        'name', 'Alice',
        'address', named_struct('city', 'NYC', 'state', 'NY', 'zip', '10001'),
        'scores', array(95, 88, 92)
      ) AS person
  """)

  df |> DataFrame.select([
    col("person.name") |> Column.alias_("name"),
    col("person.address.city") |> Column.alias_("city"),
    col("person.address.state") |> Column.alias_("state"),
    size(col("person.scores")) |> Column.alias_("num_scores"),
    element_at(col("person.scores"), lit(1)) |> Column.alias_("first_score"),
    # Add a field to the struct
    Column.with_field(col("person"), "email", lit("alice@example.com"))
    |> Column.alias_("enriched_person")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Map operations chain
# =============================================

run.("5. map operations chain", fn ->
  df = SparkEx.sql(s, """
    SELECT map('a', 1, 'b', 2, 'c', 3) AS m
  """)
  df |> DataFrame.select([
    col("m"),
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values"),
    element_at(col("m"), lit("b")) |> Column.alias_("val_b"),
    size(col("m")) |> Column.alias_("map_size")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Complex: SQL window analytics
# =============================================

run.("6. SQL window analytics (ROWS BETWEEN, RANGE, gaps and islands)", fn ->
  SparkEx.sql(s, """
    WITH sales AS (
      SELECT date_add(DATE '2024-01-01', CAST(id AS INT)) AS dt,
             (id % 7 + 1) * 100 AS amount
      FROM range(0, 90)
    ),
    daily AS (
      SELECT dt, amount,
        SUM(amount) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS week_sum,
        AVG(amount) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS week_avg,
        amount - LAG(amount, 1) OVER (ORDER BY dt) AS daily_change,
        NTILE(4) OVER (ORDER BY amount DESC) AS quartile
      FROM sales
    )
    SELECT * FROM daily
    ORDER BY dt
    LIMIT 10
  """) |> DataFrame.collect()
end)

# =============================================
# 7. Catalog operations
# =============================================

run.("7a. list databases in sfx catalog", fn ->
  Catalog.list_databases(s, catalog: "sfx")
end)

run.("7b. list tables in spark_ex_db", fn ->
  Catalog.list_tables(s, db_name: "spark_ex_db")
end)

run.("7c. current catalog and database", fn ->
  cat = Catalog.current_catalog(s)
  db = Catalog.current_database(s)
  %{catalog: cat, database: db}
end)

# =============================================
# 8. Complex: generate_series-like via SQL + DataFrame
# =============================================

run.("8. date dimension table", fn ->
  SparkEx.sql(s, """
    SELECT
      date_add(DATE '2024-01-01', CAST(id AS INT)) AS date_key,
      year(date_add(DATE '2024-01-01', CAST(id AS INT))) AS year,
      month(date_add(DATE '2024-01-01', CAST(id AS INT))) AS month,
      dayofmonth(date_add(DATE '2024-01-01', CAST(id AS INT))) AS day,
      dayofweek(date_add(DATE '2024-01-01', CAST(id AS INT))) AS dow,
      weekofyear(date_add(DATE '2024-01-01', CAST(id AS INT))) AS week,
      quarter(date_add(DATE '2024-01-01', CAST(id AS INT))) AS quarter
    FROM range(0, 366)
  """)
  |> DataFrame.filter(Column.eq(col("month"), lit(2)))
  |> DataFrame.group_by([col("dow")])
  |> GroupedData.agg([count(col("date_key")) |> Column.alias_("count")])
  |> DataFrame.sort([asc(col("dow"))])
  |> DataFrame.collect()
end)

# =============================================
# 9. Complex: approx_count_distinct at scale
# =============================================

run.("9. approx_count_distinct on large dataset", fn ->
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
# 10. Complex: conditional aggregation
# =============================================

run.("10. conditional aggregation (CASE WHEN inside agg)", fn ->
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
# 11. Complex: string parsing pipeline
# =============================================

run.("11. string parsing: split → explode → regex → aggregate", fn ->
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
# 12. Complex: stack (SQL UNPIVOT alternative)
# =============================================

run.("12. stack (SQL unpivot alternative)", fn ->
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
# 13. Complex: sample + stratified analysis
# =============================================

run.("13. sample with seed for reproducibility", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i, "grp" => rem(i, 5)} end),
    schema: "id INT, grp INT")

  sample1 = df |> DataFrame.sample(fraction: 0.1, seed: 42) |> DataFrame.collect()
  sample2 = df |> DataFrame.sample(fraction: 0.1, seed: 42) |> DataFrame.collect()

  {:ok, rows1} = sample1
  {:ok, rows2} = sample2

  %{sample1_count: length(rows1), sample2_count: length(rows2),
    same_results: rows1 == rows2}
end)

# =============================================
# 14. Complex: multiple temp views cross-referencing
# =============================================

run.("14. multiple temp views cross-referenced via SQL", fn ->
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
# 15. Complex: DataFrame.transform chain with windows
# =============================================

run.("15. transform chain: enrich → window → filter → format", fn ->
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

IO.puts("=== Round 46 complete ===")
