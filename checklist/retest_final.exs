# Final retest: all remaining bugs + untested items
# Usage: mix run checklist/retest_final.exs

alias SparkEx.{DataFrame, Column, StreamReader, StreamWriter, StreamingQuery, StreamingQueryManager}
import SparkEx.Functions

IO.puts("=== Final Retest ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run_id = :erlang.system_time(:millisecond) |> to_string()

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

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5) AS t(id, name, salary)")

# ============================================================
# BUG-7: col_regex
# ============================================================
IO.puts("--- BUG-7: col_regex ---")

run.("col_regex", fn ->
  DataFrame.col_regex(df, "`(id|name)`") |> DataFrame.collect()
end)

# ============================================================
# BUG-13: Expression-based join + self join
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

run.("self join (aliased)", fn ->
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

# ============================================================
# 25.1: Streaming trigger (available_now)
# ============================================================
IO.puts("--- 25.1: Streaming trigger ---")

run.("25.1 available_now trigger", fn ->
  {:ok, q} =
    StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("trigger_avail_#{run_id}")
    |> StreamWriter.trigger(available_now: true)
    |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(q)
  IO.puts("  is_active after available_now: #{inspect(active)}")
  # available_now should auto-terminate after processing
  StreamingQuery.stop(q)
  active
end)

# ============================================================
# 27.2: Get query by id
# ============================================================
IO.puts("--- 27.2: Get query by id ---")

try do
  {:ok, q2} =
    StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("getbyid_#{run_id}")
    |> StreamWriter.start()

  Process.sleep(2000)

  run.("27.2 get query by id", fn ->
    id = StreamingQuery.id(q2)
    IO.puts("  query id: #{inspect(id)}")
    # id might be {:ok, id} or just id
    actual_id = case id do
      {:ok, v} -> v
      v -> v
    end
    StreamingQueryManager.get(s, actual_id)
  end)

  StreamingQuery.stop(q2)
rescue
  e -> IO.puts("  STREAMING ERROR: #{Exception.message(e)}")
catch
  kind, reason -> IO.puts("  STREAMING ERROR (#{kind}): #{inspect(reason)}")
end

# ============================================================
# BUG-4: Session.release (run last, isolated)
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

IO.puts("=== Final Retest Complete ===")
