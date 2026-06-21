# Exploratory repro: dropna with invalid thresh type crashes session
# Usage: mix run checklist/explore_42_dropna_invalid_thresh_crash.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: dropna invalid thresh type session crash ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} =
  SparkEx.create_dataframe(
    s,
    [%{"k" => "a", "v" => 1}, %{"k" => "b", "v" => nil}],
    schema: "k STRING, v INT"
  )

result =
  try do
    DataFrame.dropna(df, thresh: 1.5)
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "dropna invalid-thresh result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
