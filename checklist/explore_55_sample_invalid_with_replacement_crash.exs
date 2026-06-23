alias SparkEx.DataFrame

Process.flag(:trap_exit, true)
{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")

result =
  try do
    df
    |> DataFrame.sample(0.5, with_replacement: "oops")
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "sample with invalid with_replacement")
IO.puts("session_alive?: #{Process.alive?(s)}")
