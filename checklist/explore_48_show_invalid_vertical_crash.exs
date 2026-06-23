# Exploratory repro: show with invalid vertical type crashes session
# Usage: mix run checklist/explore_48_show_invalid_vertical_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: show invalid vertical type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.show(df, vertical: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "show invalid-vertical result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
