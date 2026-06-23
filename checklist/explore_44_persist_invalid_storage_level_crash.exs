# Exploratory repro: persist with invalid storage_level type crashes session
# Usage: mix run checklist/explore_44_persist_invalid_storage_level_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: persist invalid storage_level type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    DataFrame.persist(df, storage_level: "oops")
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "persist invalid-storage-level result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")

