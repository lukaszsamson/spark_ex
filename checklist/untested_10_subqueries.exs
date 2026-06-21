# Untested items from section 10: Advanced Joins & Subqueries
# Usage: mix run checklist/untested_10_subqueries.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Untested: Section 10 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 500)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

emp = SparkEx.sql(s, """
  SELECT * FROM VALUES (1, 'Alice', 10, 100), (2, 'Bob', 20, 120), (3, 'Carol', 10, 90), (4, 'Dave', 30, 200)
  AS t(emp_id, name, dept_id, salary)
""")

dept = SparkEx.sql(s, """
  SELECT * FROM VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'Marketing')
  AS t(dept_id, dept_name)
""")

# 10.1 Self join
run.("10.1 Self join (find pairs in same dept)", fn ->
  e1 = DataFrame.alias_(emp, "e1")
  e2 = DataFrame.alias_(emp, "e2")
  DataFrame.join(e1, e2, ["dept_id"], :inner)
  |> DataFrame.filter(Column.lt(DataFrame.col(e1, "emp_id"), DataFrame.col(e2, "emp_id")))
  |> DataFrame.select([
    DataFrame.col(e1, "name") |> Column.alias_("name1"),
    DataFrame.col(e2, "name") |> Column.alias_("name2"),
    DataFrame.col(e1, "dept_id")
  ])
  |> DataFrame.collect()
end)

# 10.2 Multi-column join
run.("10.2 Multi-column join", fn ->
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20) AS t(id, code, val)")
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a', 'x'), (2, 'c', 'y') AS t(id, code, label)")
  DataFrame.join(df1, df2, ["id", "code"], :inner) |> DataFrame.collect()
end)

# 10.3 Subquery via SQL
run.("10.3 Subquery via SQL (correlated)", fn ->
  DataFrame.create_or_replace_temp_view(emp, "emp_sub")
  SparkEx.sql(s, """
    SELECT name, salary FROM emp_sub e
    WHERE salary > (SELECT AVG(salary) FROM emp_sub WHERE dept_id = e.dept_id)
  """) |> DataFrame.collect()
end)

# 10.4 Subquery via temp view + join
run.("10.4 Subquery via temp view + join", fn ->
  avg_df = SparkEx.sql(s, "SELECT dept_id, AVG(salary) AS avg_sal FROM emp_sub GROUP BY dept_id")
  DataFrame.create_or_replace_temp_view(avg_df, "dept_avg")
  SparkEx.sql(s, """
    SELECT e.name, e.salary, d.avg_sal
    FROM emp_sub e JOIN dept_avg d ON e.dept_id = d.dept_id
    WHERE e.salary > d.avg_sal
  """) |> DataFrame.collect()
end)

# 10.5 EXISTS subquery
run.("10.5 EXISTS subquery", fn ->
  DataFrame.create_or_replace_temp_view(dept, "dept_sub")
  SparkEx.sql(s, """
    SELECT * FROM emp_sub e
    WHERE EXISTS (SELECT 1 FROM dept_sub d WHERE d.dept_id = e.dept_id AND d.dept_name = 'Engineering')
  """) |> DataFrame.collect()
end)

# 10.6 IN subquery
run.("10.6 IN subquery", fn ->
  SparkEx.sql(s, """
    SELECT * FROM emp_sub
    WHERE dept_id IN (SELECT dept_id FROM dept_sub WHERE dept_name IN ('Engineering', 'Sales'))
  """) |> DataFrame.collect()
end)

IO.puts("=== Section 10 Complete ===")
