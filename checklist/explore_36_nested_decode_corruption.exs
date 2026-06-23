# Exploratory repro: nested complex decode fallback/coercion corruption
# Usage: mix run checklist/explore_36_nested_decode_corruption.exs

alias SparkEx.{Column, DataFrame}
import SparkEx.Functions

IO.puts("=== Repro: nested decode coercion/corruption ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, sql ->
  IO.puts(label)

  df = SparkEx.sql(s, sql)
  IO.inspect(DataFrame.dtypes(df), label: "  dtypes")
  IO.inspect(DataFrame.collect(df), label: "  collect", limit: 20, printable_limit: 1000)

  json_control =
    DataFrame.select(df, [to_json(col("v")) |> Column.alias_("json_v")])
    |> DataFrame.collect()

  IO.inspect(json_control, label: "  to_json control", limit: 20, printable_limit: 1000)
  IO.puts("")
end

run.(
  "1) array<map<string,array<int>>>",
  "SELECT array(map('a', array(1,2), 'b', array(3,4))) AS v"
)

run.(
  "2) map<string,map<string,int>>",
  "SELECT map('outer', map('inner', 1)) AS v"
)

run.(
  "3) array<map<string,map<string,array<int>>>>",
  "SELECT array(map('a', map('b', array(1,2)))) AS v"
)

IO.puts("=== Done ===")
