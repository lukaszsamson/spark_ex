# Exploratory repro: tree_string with invalid level type crashes session
# Usage: mix run checklist/explore_43_tree_string_invalid_level_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: tree_string invalid level type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.tree_string(df, level: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "tree_string invalid-level result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")

