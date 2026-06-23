# Exploratory repro: sample_by with keyword seed crashes session
# Usage: mix run checklist/explore_40_sample_by_keyword_seed_crash.exs

alias SparkEx.DataFrame
import SparkEx.Functions

IO.puts("=== Repro: sample_by keyword seed crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} =
  SparkEx.create_dataframe(
    s,
    [%{"k" => "a", "v" => 1}, %{"k" => "a", "v" => 2}, %{"k" => "b", "v" => 3}],
    schema: "k STRING, v INT"
  )

result =
  try do
    DataFrame.sample_by(df, col("k"), %{"a" => 1.0, "b" => 1.0}, seed: 42)
    |> DataFrame.collect()
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "sample_by keyword result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
