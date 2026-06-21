alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df0} = SparkEx.create_dataframe(s, [%{"id" => 3}, %{"id" => 1}, %{"id" => 2}], schema: "id INT")
df = DataFrame.repartition(df0, 1)

true_result = DataFrame.sort_within_partitions(df, ["id"], ascending: [true]) |> DataFrame.collect(timeout: 120_000)
false_result = DataFrame.sort_within_partitions(df, ["id"], ascending: [false]) |> DataFrame.collect(timeout: 120_000)
bad_result =
  try do
    DataFrame.sort_within_partitions(df, ["id"], ascending: ["oops"]) |> DataFrame.collect(timeout: 120_000)
  rescue
    e in ArgumentError -> {:caught, Exception.message(e)}
  end

IO.inspect(true_result, label: "ascending [true]")
IO.inspect(false_result, label: "ascending [false]")
IO.inspect(bad_result, label: "ascending ['oops'] (should be caught)")
IO.puts("session_alive?: #{Process.alive?(s)}")
