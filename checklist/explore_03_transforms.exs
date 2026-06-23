# Exploratory: chained transforms, edge cases, advanced operations
alias SparkEx.{DataFrame, Column, Writer}
import SparkEx.Functions, except: [length: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()
io_root = "/tmp/spark_ex_test_#{run_id}"

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

df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 'Engineering', 50000, DATE '2020-01-15'),
    (2, 'Bob', 'Sales', 60000, DATE '2019-06-01'),
    (3, 'Carol', 'Engineering', 55000, DATE '2021-03-20'),
    (4, 'Dave', 'HR', 70000, DATE '2018-11-10'),
    (5, 'Eve', 'Sales', 65000, DATE '2022-08-05'),
    (6, 'Frank', 'Engineering', 52000, DATE '2023-01-01'),
    (7, 'Grace', 'HR', 48000, DATE '2020-07-15'),
    (8, 'Heidi', 'Sales', 72000, DATE '2017-02-28')
  AS t(id, name, dept, salary, hire_date)
""")

# 1. Long pipeline: filter -> with_column -> group_by -> agg -> order_by -> limit
run.("1. Long chained pipeline", fn ->
  df
  |> DataFrame.filter(Column.gt(col("salary"), lit(50000)))
  |> DataFrame.with_column("bonus", Column.multiply(col("salary"), lit(0.1)))
  |> DataFrame.with_column("total", Column.plus(col("salary"), col("bonus")))
  |> DataFrame.group_by([col("dept")])
  |> DataFrame.agg([
    count(lit(1)) |> Column.alias_("cnt"),
    avg(col("total")) |> Column.alias_("avg_total"),
    max(col("total")) |> Column.alias_("max_total")
  ])
  |> DataFrame.order_by([desc(col("avg_total"))])
  |> DataFrame.collect()
end)

# 2. Multiple with_columns at once
run.("2. Multiple with_columns", fn ->
  DataFrame.with_columns(df, [
    upper(col("name")) |> Column.alias_("upper_name"),
    lower(col("dept")) |> Column.alias_("lower_dept"),
    Column.plus(col("salary"), lit(1000)) |> Column.alias_("raised_salary"),
    year(col("hire_date")) |> Column.alias_("hire_year"),
    datediff(current_date(), col("hire_date")) |> Column.alias_("days_employed")
  ])
  |> DataFrame.select([col("name"), col("upper_name"), col("lower_dept"), col("raised_salary"), col("hire_year"), col("days_employed")])
  |> DataFrame.collect()
end)

# 3. Nested when/otherwise
run.("3. Nested when/otherwise", fn ->
  DataFrame.with_column(df, "level",
    when_(Column.gt(col("salary"), lit(65000)), lit("senior"))
    |> Column.when_(Column.gt(col("salary"), lit(55000)), lit("mid"))
    |> Column.otherwise(lit("junior"))
  )
  |> DataFrame.select([col("name"), col("salary"), col("level")])
  |> DataFrame.collect()
end)

# 4. select_expr with complex SQL expressions
run.("4. select_expr complex", fn ->
  DataFrame.select_expr(df, [
    "name",
    "salary * 12 AS annual_salary",
    "CASE WHEN salary > 60000 THEN 'high' ELSE 'normal' END AS tier",
    "CONCAT(name, ' (', dept, ')') AS name_dept",
    "DATEDIFF(CURRENT_DATE(), hire_date) AS tenure_days"
  ])
  |> DataFrame.collect()
end)

# 5. drop_duplicates with subset
run.("5. drop_duplicates with subset", fn ->
  duped = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a', 'x'), (2, 'a', 'y'), (3, 'b', 'x'), (4, 'b', 'x')
    AS t(id, grp, cat)
  """)
  DataFrame.drop_duplicates(duped, ["grp", "cat"]) |> DataFrame.collect()
end)

# 6. sample with seed
run.("6. sample with seed (reproducible)", fn ->
  r1 = DataFrame.sample(df, 0.5, seed: 42) |> DataFrame.collect()
  r2 = DataFrame.sample(df, 0.5, seed: 42) |> DataFrame.collect()
  %{same_result: r1 == r2, r1_count: Kernel.length(elem(r1, 1)), r2_count: Kernel.length(elem(r2, 1))}
end)

# 7. random_split
run.("7. random_split 3-way", fn ->
  {:ok, [a, b, c]} = DataFrame.random_split(df, [0.3, 0.3, 0.4], seed: 42)
  ca = DataFrame.count(a)
  cb = DataFrame.count(b)
  cc = DataFrame.count(c)
  %{a: ca, b: cb, c: cc}
end)

# 8. union_by_name with mismatched columns
run.("8. union_by_name with extra columns", fn ->
  df1 = SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name")
  df2 = SparkEx.sql(s, "SELECT 2 AS id, 'Bob' AS name, 100 AS score")
  DataFrame.union_by_name(df1, df2, allow_missing_columns: true) |> DataFrame.collect()
end)

# 9. unpivot
run.("9. unpivot multi-column", fn ->
  wide = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 10, 20, 30), (2, 40, 50, 60)
    AS t(id, jan, feb, mar)
  """)
  DataFrame.unpivot(wide, ["id"], [{"jan", "jan"}, {"feb", "feb"}, {"mar", "mar"}], "month", "value")
  |> DataFrame.collect()
end)

# 10. except + intersect chaining
run.("10. Set operations chaining", fn ->
  a = SparkEx.sql(s, "SELECT * FROM VALUES (1), (2), (3), (4) AS t(id)")
  b = SparkEx.sql(s, "SELECT * FROM VALUES (2), (3), (5) AS t(id)")
  c = SparkEx.sql(s, "SELECT * FROM VALUES (3), (4), (6) AS t(id)")
  # (a except b) union (b intersect c)
  left = DataFrame.except(a, b)
  right = DataFrame.intersect(b, c)
  DataFrame.union(left, right) |> DataFrame.collect()
end)

# 11. replace with column subset
run.("11. replace with subset", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a', 'x'), (2, 'b', 'a') AS t(id, col1, col2)")
  |> DataFrame.replace(%{"a" => "REPLACED"}, subset: ["col1"])
  |> DataFrame.collect()
end)

# 12. Write + read round trip with partitioning
run.("12. Partitioned write + read", fn ->
  path = "#{io_root}/explore_partitioned"
  Writer.parquet(df, path, mode: :overwrite, partition_by: ["dept"])
  read_back = SparkEx.Reader.parquet(s, path)
  {:ok, count} = DataFrame.count(read_back)
  {:ok, cols} = DataFrame.columns(read_back)
  %{count: count, columns: cols}
end)

# 13. Coalesce + repartition round trip
run.("13. Coalesce then repartition", fn ->
  df
  |> DataFrame.coalesce(1)
  |> DataFrame.repartition(4)
  |> DataFrame.count()
end)

# 14. to (cast schema) with multiple columns
run.("14. to (cast multiple columns)", fn ->
  schema = "id STRING, name STRING, dept STRING, salary DOUBLE, hire_date STRING"
  DataFrame.to(df, schema) |> DataFrame.dtypes()
end)

# 15. transform with function
run.("15. transform (pipeline function)", fn ->
  add_bonus = fn df_in ->
    DataFrame.with_column(df_in, "bonus", Column.multiply(col("salary"), lit(0.15)))
  end
  DataFrame.transform(df, add_bonus) |> DataFrame.select([col("name"), col("bonus")]) |> DataFrame.collect()
end)

# 16. Multiple orderBy with nulls handling
run.("16. Order with nulls", fn ->
  null_df = SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a', NULL), (2, NULL, 10), (3, 'b', 20), (4, NULL, NULL)
    AS t(id, name, val)
  """)
  null_df
  |> DataFrame.order_by([
    Column.asc_nulls_last(col("name")),
    Column.desc_nulls_first(col("val"))
  ])
  |> DataFrame.collect()
end)

# 17. withColumnsRenamed (map form)
run.("17. with_columns_renamed map", fn ->
  DataFrame.with_columns_renamed(df, %{"name" => "employee_name", "dept" => "department"})
  |> DataFrame.columns()
end)

# 18. col_regex with different patterns
run.("18a. col_regex - salary pattern", fn ->
  DataFrame.select(df, [DataFrame.col_regex(df, "`.*ary`")]) |> DataFrame.collect()
end)

run.("18b. col_regex - starts with pattern", fn ->
  DataFrame.select(df, [DataFrame.col_regex(df, "`(id|name|dept)`")]) |> DataFrame.collect()
end)

# 19. DataFrame.to_df (rename all columns)
run.("19. to_df with new names", fn ->
  small = SparkEx.sql(s, "SELECT 1, 'hello', 3.14")
  DataFrame.to_df(small, ["col_a", "col_b", "col_c"]) |> DataFrame.collect()
end)

# 20. Multiple aggregations on same column
run.("20. Multiple aggs on same column", fn ->
  DataFrame.group_by(df, [col("dept")])
  |> DataFrame.agg([
    min(col("salary")) |> Column.alias_("min_sal"),
    max(col("salary")) |> Column.alias_("max_sal"),
    avg(col("salary")) |> Column.alias_("avg_sal"),
    sum(col("salary")) |> Column.alias_("total_sal"),
    count(col("salary")) |> Column.alias_("cnt"),
    stddev(col("salary")) |> Column.alias_("std_sal")
  ])
  |> DataFrame.order_by([asc(col("dept"))])
  |> DataFrame.collect()
end)

IO.puts("=== Transforms exploration complete ===")
