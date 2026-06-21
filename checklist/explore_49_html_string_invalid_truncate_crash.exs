# Exploratory repro: html_string with invalid truncate type crashes session
# Usage: mix run checklist/explore_49_html_string_invalid_truncate_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: html_string invalid truncate type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.html_string(df, truncate: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "html_string invalid-truncate result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
