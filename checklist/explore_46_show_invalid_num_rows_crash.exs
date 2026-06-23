# Exploratory repro: show with invalid num_rows type crashes session
# Usage: mix run checklist/explore_46_show_invalid_num_rows_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: show invalid num_rows type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.show(df, num_rows: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "show invalid-num_rows result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")

