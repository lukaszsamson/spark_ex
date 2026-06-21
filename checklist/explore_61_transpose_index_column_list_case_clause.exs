import SparkEx.Functions
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "v" => 10}, %{"id" => 2, "v" => 20}], schema: "id INT, v INT")

string_ok = DataFrame.transpose(df, index_column: "id") |> DataFrame.collect()
column_ok = DataFrame.transpose(df, index_column: col("id")) |> DataFrame.collect()

list_result =
  try do
    DataFrame.transpose(df, index_column: ["id"])
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  end

IO.inspect(string_ok, label: "index_column string")
IO.inspect(column_ok, label: "index_column Column")
IO.inspect(list_result, label: "index_column list")
IO.puts("session_alive?: #{Process.alive?(s)}")
