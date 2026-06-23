# Exploratory: verify fixed APIs work + push them harder
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec}
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

sales = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 'East', '2024-01-15', 100),
    (2, 'Bob', 'West', '2024-01-20', 200),
    (3, 'Alice', 'East', '2024-02-01', 150),
    (4, 'Carol', 'East', '2024-02-10', 300),
    (5, 'Bob', 'West', '2024-02-15', 250),
    (6, 'Alice', 'East', '2024-03-01', 175),
    (7, 'Carol', 'East', '2024-03-10', 225),
    (8, 'Dave', 'West', '2024-03-15', 400)
  AS t(id, rep, region, sale_date, amount)
""")

df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 'Engineering', 50000),
    (2, 'Bob', 'Sales', 60000),
    (3, 'Carol', 'Engineering', 55000),
    (4, 'Dave', 'HR', 70000),
    (5, 'Eve', 'Sales', 65000)
  AS t(id, name, dept, salary)
""")

# === EX-2 fix: DataFrame.agg with GroupedData ===
run.("1. group_by |> DataFrame.agg (fixed EX-2)", fn ->
  DataFrame.group_by(df, [col("dept")])
  |> DataFrame.agg([
    sum(col("salary")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("dept"))])
  |> DataFrame.collect()
end)

# === EX-2 fix: rollup |> agg ===
run.("2. rollup |> DataFrame.agg (fixed EX-2)", fn ->
  DataFrame.rollup(sales, [col("region"), col("rep")])
  |> DataFrame.agg([
    sum(col("amount")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("region")), asc(col("rep"))])
  |> DataFrame.collect()
end)

# === EX-2 fix: cube |> agg ===
run.("3. cube |> DataFrame.agg (fixed EX-2)", fn ->
  DataFrame.cube(sales, [col("region"), col("rep")])
  |> DataFrame.agg([sum(col("amount")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("region")), asc(col("rep"))])
  |> DataFrame.collect()
end)

# === EX-3 fix: Column.when_ chaining ===
run.("4. when_ chaining (fixed EX-3)", fn ->
  DataFrame.with_column(df, "level",
    when_(Column.gt(col("salary"), lit(65000)), lit("senior"))
    |> Column.when_(Column.gt(col("salary"), lit(55000)), lit("mid"))
    |> Column.otherwise(lit("junior"))
  )
  |> DataFrame.select([col("name"), col("salary"), col("level")])
  |> DataFrame.collect()
end)

# === EX-3: deeper chaining (4 levels) ===
run.("5. when_ 4-level chain", fn ->
  DataFrame.with_column(df, "band",
    when_(Column.gt(col("salary"), lit(68000)), lit("A"))
    |> Column.when_(Column.gt(col("salary"), lit(60000)), lit("B"))
    |> Column.when_(Column.gt(col("salary"), lit(52000)), lit("C"))
    |> Column.otherwise(lit("D"))
  )
  |> DataFrame.select([col("name"), col("salary"), col("band")])
  |> DataFrame.collect()
end)

# === EX-4 fix: Window chaining ===
run.("6. Window.partition_by |> Window.order_by (fixed EX-4)", fn ->
  w = SparkEx.Window.partition_by([col("region")])
    |> SparkEx.Window.order_by([asc(col("amount"))])
  sales
  |> DataFrame.with_column("pct_rank", percent_rank() |> Column.over(w))
  |> DataFrame.select([col("rep"), col("region"), col("amount"), col("pct_rank")])
  |> DataFrame.collect()
end)

# === EX-4: Window with rows_between chaining ===
run.("7. Window chain with rows_between", fn ->
  w = SparkEx.Window.partition_by([col("rep")])
    |> SparkEx.Window.order_by([asc(col("sale_date"))])
    |> SparkEx.Window.rows_between(-1, :current_row)
  sales
  |> DataFrame.with_column("moving_avg_2", avg(col("amount")) |> Column.over(w))
  |> DataFrame.filter(Column.eq(col("rep"), lit("Alice")))
  |> DataFrame.select([col("sale_date"), col("amount"), col("moving_avg_2")])
  |> DataFrame.collect()
end)

# === EX-4: Window with range_between ===
run.("8. Window chain with range_between", fn ->
  w = SparkEx.Window.order_by([asc(col("amount"))])
    |> SparkEx.Window.range_between(-50, 50)
  sales
  |> DataFrame.with_column("nearby_cnt", count(lit(1)) |> Column.over(w))
  |> DataFrame.select([col("rep"), col("amount"), col("nearby_cnt")])
  |> DataFrame.order_by([asc(col("amount"))])
  |> DataFrame.collect()
end)

# === EX-5 fix: union_by_name with allow_missing_columns ===
run.("9. union_by_name allow_missing_columns (fixed EX-5)", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name")
  df2 = SparkEx.sql(s, "SELECT 2 AS id, 'Bob' AS name, 100 AS score")
  DataFrame.union_by_name(df1, df2, allow_missing_columns: true) |> DataFrame.collect()
end)

# === EX-6 fix: unpivot with tuples ===
run.("10. unpivot with tuple column specs (fixed EX-6)", fn ->
  wide = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 10, 20, 30), (2, 40, 50, 60)
    AS t(id, jan, feb, mar)
  """)
  DataFrame.unpivot(wide, ["id"], [{"jan", "jan"}, {"feb", "feb"}, {"mar", "mar"}], "month", "value")
  |> DataFrame.collect()
end)

# === EX-7 fix: random_split with seed keyword ===
run.("11. random_split with seed: keyword (fixed EX-7)", fn ->
  {:ok, [a, b]} = DataFrame.random_split(df, [0.5, 0.5], seed: 42)
  ca = DataFrame.count(a)
  cb = DataFrame.count(b)
  %{a: ca, b: cb}
end)

# === EX-8 fix: locate ===
run.("12. locate (fixed EX-8)", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    locate(lit("world"), col("text")) |> Column.alias_("pos")
  ])
  |> DataFrame.collect()
end)

# === EX-9 fix: log with base ===
run.("13. log with base (fixed EX-9)", fn ->
  SparkEx.sql(s, "SELECT 100.0 AS val")
  |> DataFrame.select([
    log(col("val")) |> Column.alias_("ln"),
    log(lit(10.0), col("val")) |> Column.alias_("log10")
  ])
  |> DataFrame.collect()
end)

# === EX-10 fix: from_unixtime with format ===
run.("14. from_unixtime with format (fixed EX-10)", fn ->
  SparkEx.sql(s, "SELECT 1718454645 AS epoch")
  |> DataFrame.select([
    from_unixtime(col("epoch")) |> Column.alias_("default_fmt"),
    from_unixtime(col("epoch"), lit("yyyy-MM-dd")) |> Column.alias_("date_only")
  ])
  |> DataFrame.collect()
end)

# === EX-12 fix: replace with subset ===
run.("15. replace with subset (fixed EX-12)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a', 'x'), (2, 'b', 'a') AS t(id, col1, col2)")
  |> DataFrame.replace(%{"a" => "REPLACED"}, subset: ["col1"])
  |> DataFrame.collect()
end)

# === EX-14 fix: DataFrame.pivot ===
run.("16. DataFrame.pivot (fixed EX-14)", fn ->
  DataFrame.group_by(sales, [col("region")])
  |> DataFrame.pivot(col("rep"), ["Alice", "Bob", "Carol", "Dave"])
  |> DataFrame.agg([sum(col("amount"))])
  |> DataFrame.collect()
end)

# === EX-1: array of maps (mitigated) ===
run.("17. array of maps (mitigated EX-1)", fn ->
  SparkEx.sql(s, "SELECT array(map('k1', 1, 'k2', 2), map('k3', 3, 'k4', 4)) AS map_arr")
  |> DataFrame.collect()
end)

# === EX-13 fix: drop_temp_view ===
run.("18. DataFrame.drop_temp_view (fixed EX-13)", fn ->
  df_tmp = SparkEx.sql(s, "SELECT 1 AS id")
  DataFrame.create_temp_view(df_tmp, "test_drop_view_ex13")
  DataFrame.drop_temp_view(s, "test_drop_view_ex13")
end)

IO.puts("=== Fixed APIs verification complete ===")
