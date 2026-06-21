# Exploratory repro: duplicate aliases in SELECT projection
# Usage: mix run checklist/explore_22_duplicate_alias_projection.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: duplicate aliases in projection ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

result =
  SparkEx.sql(s, "SELECT 1 AS dup, 2 AS dup")
  |> DataFrame.collect()

IO.inspect(result, label: "projection result")
IO.inspect(SparkEx.spark_version(s), label: "spark_version after failure")

IO.puts("\n=== Done ===")
