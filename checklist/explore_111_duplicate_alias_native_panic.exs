# Repro: duplicate aliases in DataFrame.select emit native panic (polars-arrow assertion),
# while SparkEx still returns data and keeps the session alive.
import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

df = SparkEx.sql(s, "SELECT 1 AS a, 2 AS b")

res =
  df
  |> DataFrame.select([
    col("b") |> Column.alias_("x"),
    col("a") |> Column.alias_("y"),
    Column.plus(col("a"), col("b")) |> Column.alias_("x")
  ])
  |> DataFrame.collect()

IO.inspect(res, label: "result")
IO.puts("session_alive?: #{Process.alive?(s)}")
