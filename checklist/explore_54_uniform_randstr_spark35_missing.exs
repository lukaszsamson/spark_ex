# Exploratory repro: uniform/randstr wrappers exposed but unavailable on Spark 3.5
# Usage: mix run checklist/explore_54_uniform_randstr_spark35_missing.exs

alias SparkEx.DataFrame
alias SparkEx.Column
import SparkEx.Functions

IO.puts("=== Repro: uniform/randstr on Spark 3.5 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
df = SparkEx.range(s, 1)

uniform_result =
  df
  |> DataFrame.select([uniform(col("id"), 1.0) |> Column.alias_("u")])
  |> DataFrame.collect()

randstr_result =
  df
  |> DataFrame.select([randstr(lit(6)) |> Column.alias_("r")])
  |> DataFrame.collect()

IO.inspect(uniform_result, label: "uniform()/wrapper result")
IO.inspect(randstr_result, label: "randstr()/wrapper result")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
