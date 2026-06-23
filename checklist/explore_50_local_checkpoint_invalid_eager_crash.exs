# Exploratory repro: local_checkpoint with invalid eager type crashes session
# Usage: mix run checklist/explore_50_local_checkpoint_invalid_eager_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: local_checkpoint invalid eager type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.local_checkpoint(df, eager: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "local_checkpoint invalid-eager result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
