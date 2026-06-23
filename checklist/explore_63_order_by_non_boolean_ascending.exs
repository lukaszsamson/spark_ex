alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 2}, %{"id" => 1}], schema: "id INT")

asc_true = DataFrame.order_by(df, ["id"], ascending: [true]) |> DataFrame.collect()
asc_false = DataFrame.order_by(df, ["id"], ascending: [false]) |> DataFrame.collect()
asc_invalid =
  try do
    DataFrame.order_by(df, ["id"], ascending: ["oops"]) |> DataFrame.collect()
  rescue
    e in ArgumentError -> {:caught, Exception.message(e)}
  end

IO.inspect(asc_true, label: "ascending [true]")
IO.inspect(asc_false, label: "ascending [false]")
IO.inspect(asc_invalid, label: "ascending ['oops'] (should be caught)")
IO.puts("session_alive?: #{Process.alive?(s)}")
