# Exploratory: complex join patterns
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [abs: 1]

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

emp = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 10, 50000), (2, 'Bob', 20, 60000), (3, 'Carol', 10, 55000),
    (4, 'Dave', 30, 70000), (5, 'Eve', 20, 65000)
  AS t(emp_id, name, dept_id, salary)
""")

dept = SparkEx.sql(s, """
  SELECT * FROM VALUES (10, 'Engineering', 'NYC'), (20, 'Sales', 'LA'), (30, 'HR', 'CHI')
  AS t(dept_id, dept_name, location)
""")

mgr = SparkEx.sql(s, """
  SELECT * FROM VALUES (10, 1), (20, 2), (30, 4)
  AS t(dept_id, manager_id)
""")

# 1. Multi-way join (3 tables)
run.("1. Multi-way join (emp -> dept -> mgr)", fn ->
  emp
  |> DataFrame.join(dept, ["dept_id"], :inner)
  |> DataFrame.join(mgr, ["dept_id"], :inner)
  |> DataFrame.collect()
end)

# 2. Expression join with inequality
run.("2. Expression join with inequality (non-equi join)", fn ->
  low = SparkEx.sql(s, "SELECT * FROM VALUES (40000, 60000, 'low'), (60000, 80000, 'high') AS t(lo, hi, band)")
  DataFrame.join(emp, low,
    Column.and_(
      Column.gte(DataFrame.col(emp, "salary"), DataFrame.col(low, "lo")),
      Column.lt(DataFrame.col(emp, "salary"), DataFrame.col(low, "hi"))
    ), :inner)
  |> DataFrame.collect()
end)

# 3. Self join with expression condition
run.("3. Self join with expression (find pairs earning within 10k)", fn ->
  e1 = DataFrame.alias_(emp, "e1")
  e2 = DataFrame.alias_(emp, "e2")
  DataFrame.join(e1, e2,
    Column.and_(
      Column.lt(DataFrame.col(e1, "emp_id"), DataFrame.col(e2, "emp_id")),
      Column.lt(
        SparkEx.Functions.abs(Column.minus(DataFrame.col(e1, "salary"), DataFrame.col(e2, "salary"))),
        lit(10001)
      )
    ), :inner)
  |> DataFrame.select([
    DataFrame.col(e1, "name") |> Column.alias_("name1"),
    DataFrame.col(e2, "name") |> Column.alias_("name2"),
    DataFrame.col(e1, "salary") |> Column.alias_("sal1"),
    DataFrame.col(e2, "salary") |> Column.alias_("sal2")
  ])
  |> DataFrame.collect()
end)

# 4. Left anti join with expression
run.("4. Left anti join with expression", fn ->
  DataFrame.join(emp, mgr,
    Column.eq(DataFrame.col(emp, "emp_id"), DataFrame.col(mgr, "manager_id")),
    :left_anti)
  |> DataFrame.collect()
end)

# 5. Left semi join with expression
run.("5. Left semi join with expression", fn ->
  DataFrame.join(emp, mgr,
    Column.eq(DataFrame.col(emp, "emp_id"), DataFrame.col(mgr, "manager_id")),
    :left_semi)
  |> DataFrame.collect()
end)

# 6. Cross join followed by filter (theta join pattern)
run.("6. Cross join + filter", fn ->
  DataFrame.cross_join(emp, dept)
  |> DataFrame.filter(Column.eq(col("dept_id"), col("dept_id")))
  |> DataFrame.count()
end)

# 7. Join with aggregated subquery
run.("7. Join with aggregated subquery", fn ->
  avg_sal = DataFrame.group_by(emp, [col("dept_id")])
    |> DataFrame.agg([avg(col("salary")) |> Column.alias_("avg_salary")])
  DataFrame.join(emp, avg_sal, ["dept_id"], :inner)
  |> DataFrame.filter(Column.gt(col("salary"), col("avg_salary")))
  |> DataFrame.collect()
end)

# 8. Full outer join with expression
run.("8. Full outer join with expression", fn ->
  extra_dept = SparkEx.sql(s, "SELECT * FROM VALUES (40, 'Legal', 'SF') AS t(dept_id, dept_name, location)")
  all_dept = DataFrame.union(dept, extra_dept)
  DataFrame.join(emp, all_dept,
    Column.eq(DataFrame.col(emp, "dept_id"), DataFrame.col(all_dept, "dept_id")),
    :full)
  |> DataFrame.collect()
end)

# 9. Join + window function
run.("9. Join + window function (rank within dept)", fn ->
  w = SparkEx.Window.partition_by([col("dept_name")]) |> SparkEx.Window.order_by([desc(col("salary"))])
  emp
  |> DataFrame.join(dept, ["dept_id"], :inner)
  |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  |> DataFrame.collect()
end)

# 10. Multiple self-references in single expression
run.("10. Column arithmetic with bound cols from same df", fn ->
  DataFrame.select(emp, [
    DataFrame.col(emp, "name"),
    Column.plus(DataFrame.col(emp, "salary"), DataFrame.col(emp, "emp_id")) |> Column.alias_("sal_plus_id")
  ])
  |> DataFrame.collect()
end)

IO.puts("=== Joins exploration complete ===")
