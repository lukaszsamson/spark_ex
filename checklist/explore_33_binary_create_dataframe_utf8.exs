# Exploratory repro: create_dataframe BINARY with valid UTF-8 bytes still fails on collect
# Usage: mix run checklist/explore_33_binary_create_dataframe_utf8.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: create_dataframe BINARY with UTF-8 bytes ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} =
  SparkEx.create_dataframe(
    s,
    [
      %{"id" => 1, "data" => <<65, 66, 67>>}
    ],
    schema: "id INT, data BINARY"
  )

IO.inspect(DataFrame.collect(df), label: "create_dataframe collect")

control =
  SparkEx.sql(s, "SELECT CAST(X'414243' AS BINARY) AS data")
  |> DataFrame.collect()

IO.inspect(control, label: "SQL control collect")

IO.puts("\n=== Done ===")
