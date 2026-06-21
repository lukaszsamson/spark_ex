# Retest all bugs from CHECKLIST_PROGRESS.md
# Usage: mix run checklist/retest_bugs.exs

alias SparkEx.{DataFrame, Column, Catalog, GroupedData, Window, WindowSpec, Reader}
import SparkEx.Functions

IO.puts("=== Retest All Bugs ===\n")

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

# ============================================================
# BUG-1: Catalog.list_tables — PARSE_SYNTAX_ERROR
# ============================================================
IO.puts("--- BUG-1: Catalog.list_tables ---")

run.("list_tables (no args)", fn ->
  Catalog.list_tables(s)
end)

run.("list_tables with db_name", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse")
end)

run.("list_tables with pattern", fn ->
  Catalog.list_tables(s, nil, "*")
end)

run.("list_functions", fn ->
  result = Catalog.list_functions(s)
  case result do
    {:ok, fns} when is_list(fns) -> {:ok, "#{Kernel.length(fns)} functions"}
    other -> other
  end
end)

# BUG-4 moved to end (can crash the process)

# ============================================================
# BUG-5: SQL with positional args
# ============================================================
IO.puts("--- BUG-5: SQL positional args ---")

run.("SELECT ? + 1 with args: [42]", fn ->
  SparkEx.sql(s, "SELECT ? + 1 AS result", args: [42]) |> DataFrame.collect()
end)

# ============================================================
# BUG-6: SQL with named args
# ============================================================
IO.puts("--- BUG-6: SQL named args ---")

run.("SELECT :val * 2 with args: [val: 10]", fn ->
  SparkEx.sql(s, "SELECT :val * 2 AS result", args: [val: 10]) |> DataFrame.collect()
end)

# ============================================================
# BUG-7: DataFrame.col_regex crashes
# ============================================================
IO.puts("--- BUG-7: col_regex ---")

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0) AS t(id, name, salary)")

run.("col_regex", fn ->
  DataFrame.col_regex(df, "`(id|name)`") |> DataFrame.collect()
end)

# ============================================================
# BUG-9: with_column_renamed not renaming
# ============================================================
IO.puts("--- BUG-9: with_column_renamed ---")

run.("with_column_renamed", fn ->
  DataFrame.with_column_renamed(df, "name", "full_name") |> DataFrame.collect()
end)

run.("with_columns_renamed", fn ->
  DataFrame.with_columns_renamed(df, %{"id" => "identifier", "name" => "full_name"}) |> DataFrame.collect()
end)

# ============================================================
# BUG-10: lag/lead Keyword.get crash
# ============================================================
IO.puts("--- BUG-10: lag/lead ---")

wdf = SparkEx.sql(s, """
  SELECT * FROM VALUES
    ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('eng', 'Eve', 130),
    ('sales', 'Carol', 90), ('sales', 'Dave', 110)
  AS t(dept, name, salary)
""")
w = Window.partition_by(["dept"]) |> WindowSpec.order_by([col("salary") |> Column.asc()])

run.("lag", fn ->
  wdf |> DataFrame.with_column("prev_sal", lag(col("salary"), lit(1)) |> Column.over(w)) |> DataFrame.collect()
end)

run.("lead", fn ->
  wdf |> DataFrame.with_column("next_sal", lead(col("salary"), lit(1)) |> Column.over(w)) |> DataFrame.collect()
end)

# ============================================================
# BUG-11: to_date 2-arg Keyword.get crash
# ============================================================
IO.puts("--- BUG-11: to_date ---")

run.("to_date with format", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    to_date(lit("2024-01-15"), lit("yyyy-MM-dd")) |> Column.alias_("parsed_date")
  ]) |> DataFrame.collect()
end)

run.("to_timestamp with format", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    to_timestamp(lit("2024-01-15 10:30:00"), lit("yyyy-MM-dd HH:mm:ss")) |> Column.alias_("parsed_ts")
  ]) |> DataFrame.collect()
end)

# ============================================================
# BUG-13: Expression-based join condition
# ============================================================
IO.puts("--- BUG-13: Join on expression ---")

emp = SparkEx.sql(s, """
  SELECT * FROM VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 10)
  AS t(emp_id, name, dept_id)
""")

dept = SparkEx.sql(s, """
  SELECT * FROM VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'Marketing')
  AS t(dept_id, dept_name)
""")

run.("join on expression", fn ->
  DataFrame.join(emp, dept, Column.eq(DataFrame.col(emp, "dept_id"), DataFrame.col(dept, "dept_id")), :inner) |> DataFrame.collect()
end)

# ============================================================
# BUG-14: Decimal -> Explorer type mapping
# ============================================================
IO.puts("--- BUG-14: Decimal in Explorer ---")

run.("to_explorer with decimal column", fn ->
  dec_df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 100.50), (2, 200.75) AS t(id, salary)")
  {:ok, edf} = DataFrame.to_explorer(dec_df)
  IO.puts("  Columns: #{inspect(Explorer.DataFrame.names(edf))}")
  IO.puts("  Dtypes:  #{inspect(Explorer.DataFrame.dtypes(edf))}")
  # Check if salary is numeric or string
  salary_series = Explorer.DataFrame.pull(edf, "salary")
  IO.puts("  Salary dtype: #{inspect(Explorer.Series.dtype(salary_series))}")
  IO.puts("  Salary values: #{inspect(Explorer.Series.to_list(salary_series))}")
  :ok
end)

# ============================================================
# BUG-3: CloneSession (Spark 3.5 — expected to still fail)
# ============================================================
IO.puts("--- BUG-3: CloneSession (expected 3.5 failure) ---")

run.("clone_session", fn ->
  SparkEx.clone_session(s)
end)

# ============================================================
# BUG-12: transpose (Spark 4.x only — expected to still fail)
# ============================================================
IO.puts("--- BUG-12: transpose (expected 3.5 failure) ---")

run.("transpose", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES ('Alice', 90), ('Bob', 80) AS t(name, score)")
  |> DataFrame.transpose()
  |> DataFrame.collect()
end)

# ============================================================
# Additional retests: items that had script issues but likely work
# ============================================================
IO.puts("--- Additional retests ---")

# 7.25 transform (higher-order)
run.("7.25 transform (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn x -> Column.plus(x, lit(10)) end) |> Column.alias_("transformed")
  ]) |> DataFrame.collect()
end)

# 7.26 filter (higher-order)
run.("7.26 filter (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr") |> DataFrame.select([
    SparkEx.Functions.filter(col("arr"), fn x -> Column.gt(x, lit(2)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

# 7.27 aggregate (higher-order)
run.("7.27 aggregate (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    SparkEx.Functions.aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("total")
  ]) |> DataFrame.collect()
end)

# 7.35 expr
run.("7.35 expr", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 100), (2, 200) AS t(id, salary)")
  |> DataFrame.select([SparkEx.Functions.expr("id + salary") |> Column.alias_("combined")])
  |> DataFrame.collect()
end)

# 8.5 group_by + agg (expressions)
run.("8.5 group_by + agg (expressions)", fn ->
  agg_df = SparkEx.sql(s, "SELECT * FROM VALUES ('eng', 100), ('eng', 120), ('sales', 90) AS t(dept, salary)")
  grouped = DataFrame.group_by(agg_df, ["dept"])
  GroupedData.agg(grouped, [
    sum(col("salary")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ]) |> DataFrame.collect()
end)

# 8.11 Global aggregation
run.("8.11 Global aggregation", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (100), (120), (90) AS t(salary)")
  |> DataFrame.select([sum(col("salary")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# 5.13/5.14 order_by with correct asc/desc syntax
run.("5.13 order_by asc (correct syntax)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (3, 'c'), (1, 'a'), (2, 'b') AS t(id, v)")
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("5.14 order_by desc (correct syntax)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (3, 'c'), (1, 'a'), (2, 'b') AS t(id, v)")
  |> DataFrame.order_by([desc(col("id"))])
  |> DataFrame.collect()
end)

# 6.20 when/otherwise
run.("6.20 when/otherwise", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
  |> DataFrame.select([
    when_(col("id") |> Column.eq(lit(1)), lit("one")) |> Column.otherwise(lit("other")) |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

# ============================================================
# BUG-4: Session.release GRPC crash on TLS disconnect
# (Run last, in a Task, since it can crash the process)
# ============================================================
IO.puts("--- BUG-4: Session.release ---")

task = Task.async(fn ->
  {:ok, s2} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
  {:ok, _ver} = SparkEx.spark_version(s2)
  result = SparkEx.Session.release(s2)
  result
end)

case Task.yield(task, 10_000) || Task.shutdown(task) do
  {:ok, result} -> IO.inspect(result, label: "  BUG-4 result")
  {:exit, reason} -> IO.puts("  BUG-4 STILL CRASHES: #{inspect(reason)}")
  nil -> IO.puts("  BUG-4 timed out")
end
IO.puts("")

IO.puts("=== Retest Complete ===")
