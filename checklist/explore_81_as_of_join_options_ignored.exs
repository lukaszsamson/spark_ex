import SparkEx.Functions
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
left = SparkEx.sql(s, "SELECT * FROM VALUES (1, TIMESTAMP('2024-01-01 10:05:00')), (2, TIMESTAMP('2024-01-01 10:15:00')), (3, TIMESTAMP('2024-01-01 10:05:00')) AS t(id, ts)")
right = SparkEx.sql(s, "SELECT * FROM VALUES (1, TIMESTAMP('2024-01-01 10:00:00')), (1, TIMESTAMP('2024-01-01 10:10:00')), (2, TIMESTAMP('2024-01-01 10:05:00')), (2, TIMESTAMP('2024-01-01 10:20:00')) AS t(id, ts2)")

for direction <- ["backward", "forward", "nearest", "sideways"] do
  result =
    try do
      df = DataFrame.as_of_join(left, right, col("ts"), col("ts2"), on: ["id"], join_type: "inner", direction: direction)
      DataFrame.collect(df, timeout: 120_000)
    rescue
      e in ArgumentError -> {:caught, Exception.message(e)}
    end
  IO.inspect(result, label: "direction=#{direction}")
end

for join_type <- ["inner", "left", "right", "full", 123] do
  result =
    try do
      df = DataFrame.as_of_join(left, right, col("ts"), col("ts2"), on: ["id"], join_type: join_type, direction: "backward")
      DataFrame.collect(df, timeout: 120_000)
    rescue
      e in ArgumentError -> {:caught, Exception.message(e)}
    end
  IO.inspect(result, label: "join_type=#{inspect(join_type)}")
end

IO.puts("session_alive?: #{Process.alive?(s)}")
