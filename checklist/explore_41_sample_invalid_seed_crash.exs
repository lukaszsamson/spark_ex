# Exploratory repro: sample with invalid seed type crashes session
# Usage: mix run checklist/explore_41_sample_invalid_seed_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: sample invalid seed type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} =
  SparkEx.create_dataframe(
    s,
    [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}],
    schema: "id INT"
  )

result =
  try do
    DataFrame.sample(df, 0.5, seed: "oops")
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "sample invalid-seed result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
