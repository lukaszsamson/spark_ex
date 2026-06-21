# Sections 8 & 9: Aggregations & Joins
# Usage: mix run checklist/section_08_09_agg_joins.exs

alias SparkEx.{DataFrame, Column, GroupedData}
import SparkEx.Functions

IO.puts("=== Section 8: Aggregations ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 300)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

agg_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    ('eng', 'Alice', 100), ('eng', 'Bob', 120),
    ('sales', 'Carol', 90), ('sales', 'Dave', 110),
    ('eng', 'Eve', 130)
  AS t(dept, name, salary)
""")
grouped = DataFrame.group_by(agg_df, ["dept"])

# 8.1 group_by + count
run.("8.1 group_by + count", fn ->
  GroupedData.count(grouped) |> DataFrame.collect()
end)

# 8.2 group_by + sum
run.("8.2 group_by + sum", fn ->
  GroupedData.sum(grouped, ["salary"]) |> DataFrame.collect()
end)

# 8.3 group_by + avg / mean
run.("8.3 group_by + avg", fn ->
  GroupedData.avg(grouped, ["salary"]) |> DataFrame.collect()
end)

# 8.4 group_by + min/max
run.("8.4 group_by + min/max", fn ->
  min_result = GroupedData.min(grouped, ["salary"]) |> DataFrame.collect()
  max_result = GroupedData.max(grouped, ["salary"]) |> DataFrame.collect()
  %{min: min_result, max: max_result}
end)

# 8.5 group_by + agg (expressions)
run.("8.5 group_by + agg (expressions)", fn ->
  GroupedData.agg(grouped, [
    Functions.sum(col("salary")) |> Column.alias_("total"),
    Functions.count(lit(1)) |> Column.alias_("cnt")
  ]) |> DataFrame.collect()
end)

# 8.6 group_by + agg (map)
run.("8.6 group_by + agg (map)", fn ->
  GroupedData.agg(grouped, %{"salary" => "avg", "name" => "count"}) |> DataFrame.collect()
end)

# 8.7 Pivot
run.("8.7 Pivot", fn ->
  grouped |> GroupedData.pivot(col("dept")) |> GroupedData.sum(["salary"]) |> DataFrame.collect()
end)

# 8.8 rollup
run.("8.8 rollup", fn ->
  DataFrame.rollup(agg_df, ["dept"]) |> GroupedData.count() |> DataFrame.collect()
end)

# 8.9 cube
run.("8.9 cube", fn ->
  DataFrame.cube(agg_df, ["dept"]) |> GroupedData.count() |> DataFrame.collect()
end)

# 8.11 Global aggregation
run.("8.11 Global aggregation", fn ->
  agg_df |> DataFrame.select([Functions.sum(col("salary")) |> Column.alias_("total")]) |> DataFrame.collect()
end)

# === Section 9: Joins ===
IO.puts("=== Section 9: Joins ===\n")

emp = SparkEx.sql(s, """
  SELECT * FROM VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 10), (4, 'Dave', NULL)
  AS t(emp_id, name, dept_id)
""")

dept = SparkEx.sql(s, """
  SELECT * FROM VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'Marketing')
  AS t(dept_id, dept_name)
""")

# 9.1 Inner join
run.("9.1 Inner join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :inner) |> DataFrame.collect()
end)

# 9.2 Left join
run.("9.2 Left join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :left) |> DataFrame.collect()
end)

# 9.3 Right join
run.("9.3 Right join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :right) |> DataFrame.collect()
end)

# 9.4 Full outer join
run.("9.4 Full outer join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :full) |> DataFrame.collect()
end)

# 9.5 Cross join
run.("9.5 Cross join", fn ->
  DataFrame.cross_join(emp, dept) |> DataFrame.count()
end)

# 9.6 Left semi join
run.("9.6 Left semi join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :left_semi) |> DataFrame.collect()
end)

# 9.7 Left anti join
run.("9.7 Left anti join", fn ->
  DataFrame.join(emp, dept, ["dept_id"], :left_anti) |> DataFrame.collect()
end)

# 9.8 Join on expression
run.("9.8 Join on expression", fn ->
  DataFrame.join(emp, dept, Column.eq(DataFrame.col(emp, "dept_id"), DataFrame.col(dept, "dept_id")), :inner) |> DataFrame.collect()
end)

IO.puts("=== Sections 8 & 9 Complete ===")
