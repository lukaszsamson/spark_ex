# Exploratory repro: unpersist with invalid blocking type crashes session
# Usage: mix run checklist/explore_45_unpersist_invalid_blocking_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: unpersist invalid blocking type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.unpersist(df, blocking: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "unpersist invalid-blocking result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")

