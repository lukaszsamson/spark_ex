# Repro: lateral_join with table_function RHS fails with empty relation
import SparkEx.Functions
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

left = SparkEx.sql(s, "SELECT * FROM VALUES (1), (2) AS t(id)")
right = DataFrame.table_function(s, "range", [lit(2)])

# control: table_function alone works
IO.inspect(DataFrame.collect(right), label: "table_function_only")

# repro: lateral join with TVF result as RHS
res =
  DataFrame.lateral_join(left, right, lit(true), :cross)
  |> DataFrame.collect()

IO.inspect(res, label: "lateral_join_with_tvf")
IO.puts("session_alive?: #{Process.alive?(s)}")
