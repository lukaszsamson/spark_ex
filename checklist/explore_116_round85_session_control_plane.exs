# Round 85: session/control-plane exploratory pass
# Read-only operations; no table mutations
import SparkEx.Functions
alias SparkEx.{DataFrame, Session, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = System.system_time(:millisecond)

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 120, printable_limit: 8000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 30, printable_limit: 3000)}")
  end
end

run.("1) tags lifecycle", fn ->
  t1 = "r85_tag_#{run_id}_a"
  t2 = "r85_tag_#{run_id}_b"

  SparkEx.add_tag(s, t1)
  SparkEx.add_tag(s, t2)
  tags1 = SparkEx.get_tags(s)
  SparkEx.remove_tag(s, t1)
  tags2 = SparkEx.get_tags(s)
  SparkEx.clear_tags(s)
  tags3 = SparkEx.get_tags(s)

  %{after_add: tags1, after_remove: tags2, after_clear: tags3}
end)

run.("2) interrupt_tag on missing tag", fn ->
  SparkEx.interrupt_tag(s, "no_such_tag_#{run_id}")
end)

run.("3) interrupt_operation on missing op id", fn ->
  SparkEx.interrupt_operation(s, "no_such_op_#{run_id}")
end)

run.("4) artifact_status edge cases", fn ->
  a0 = SparkEx.artifact_status(s, [])
  a1 = SparkEx.artifact_status(s, ["missing-#{run_id}.jar", "missing-#{run_id}.zip"])
  %{empty: a0, missing: a1}
end)

run.("5) last_execution_metrics before and after action", fn ->
  before = Session.last_execution_metrics(s)

  SparkEx.sql(s, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
  |> DataFrame.filter(Column.gt(col("id"), lit(1)))
  |> DataFrame.collect()

  after_ = Session.last_execution_metrics(s)
  %{before: before, after: after_}
end)

run.("6) register/remove/clear progress handlers", fn ->
  parent = self()

  handler = fn msg ->
    send(parent, {:progress_event, msg})
  end

  :ok = Session.register_progress_handler(s, handler)

  SparkEx.sql(s, "SELECT 1 AS x") |> DataFrame.collect()
  Process.sleep(100)

  ev1 =
    receive do
      {:progress_event, msg} -> {:got_event, msg}
    after
      200 -> :no_event
    end

  :ok = Session.remove_progress_handler(s, handler)
  :ok = Session.clear_progress_handlers(s)

  ev1
end)

run.("7) Session.clone(nil) behavior on Spark 3.5", fn ->
  SparkEx.Session.clone(s)
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
