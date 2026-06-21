import SparkEx.Functions
alias SparkEx.DataFrame

Process.flag(:trap_exit, true)
{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

left = SparkEx.sql(s, "SELECT 1 AS id, TIMESTAMP('2024-01-01 00:00:00') AS ts")
right = SparkEx.sql(s, "SELECT 1 AS id, TIMESTAMP('2024-01-01 00:00:00') AS ts2")

result =
  try do
    left
    |> DataFrame.as_of_join(right, col("ts"), col("ts2"), on: ["id"], allow_exact_matches: "oops")
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "as_of_join with invalid allow_exact_matches")
IO.puts("session_alive?: #{Process.alive?(s)}")
