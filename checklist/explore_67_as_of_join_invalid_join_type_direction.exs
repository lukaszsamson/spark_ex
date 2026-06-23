import SparkEx.Functions
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
left = SparkEx.sql(s, "SELECT 1 AS id, TIMESTAMP('2024-01-01 10:05:00') AS ts")
right = SparkEx.sql(s, "SELECT * FROM VALUES (1, TIMESTAMP('2024-01-01 10:00:00')), (1, TIMESTAMP('2024-01-01 10:10:00')) AS t(id, ts2)")

good_df = DataFrame.as_of_join(left, right, col("ts"), col("ts2"), on: ["id"], join_type: "inner", direction: "backward")
weird_result =
  try do
    DataFrame.as_of_join(left, right, col("ts"), col("ts2"), on: ["id"], join_type: 123, direction: "sideways")
  rescue
    e in ArgumentError -> {:caught, Exception.message(e)}
  end

IO.inspect(good_df.plan, label: "good plan")

good = DataFrame.collect(good_df, timeout: 120_000)

IO.inspect(good, label: "good collect")
IO.inspect(weird_result, label: "invalid join_type+direction (should be caught)")
IO.puts("session_alive?: #{Process.alive?(s)}")
