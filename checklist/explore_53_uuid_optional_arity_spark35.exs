# Exploratory repro: uuid/0 wrapper sends unexpected seed argument on Spark 3.5
# Usage: mix run checklist/explore_53_uuid_optional_arity_spark35.exs

alias SparkEx.DataFrame
alias SparkEx.Column
import SparkEx.Functions

IO.puts("=== Repro: uuid optional arity on Spark 3.5 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
df = SparkEx.range(s, 1)

wrapped = df |> DataFrame.select([uuid() |> Column.alias_("u")]) |> DataFrame.collect()
sql_control = SparkEx.sql(s, "SELECT uuid() AS u") |> DataFrame.collect()

IO.inspect(wrapped, label: "uuid()/wrapper result")
IO.inspect(sql_control, label: "uuid()/sql control")
IO.puts("session_alive?: #{inspect(Process.alive?(s))}")

IO.puts("\n=== Done ===")
