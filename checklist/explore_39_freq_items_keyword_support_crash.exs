# Exploratory repro: freq_items with keyword support crashes session
# Usage: mix run checklist/explore_39_freq_items_keyword_support_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: freq_items keyword support crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} =
  SparkEx.create_dataframe(
    s,
    [%{"a" => "x"}, %{"a" => "y"}],
    schema: "a STRING"
  )

result =
  try do
    DataFrame.freq_items(df, ["a"], support: 0.5)
    |> DataFrame.collect()
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "freq_items keyword result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
