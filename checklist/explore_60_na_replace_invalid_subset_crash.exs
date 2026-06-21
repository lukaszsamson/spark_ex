alias SparkEx.DataFrame

Process.flag(:trap_exit, true)
{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
{:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "v" => 1}], schema: "id INT, v INT")

result =
  try do
    df
    |> DataFrame.replace(1, 2, subset: [123])
    |> DataFrame.collect()
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(result, label: "replace subset [123]")
IO.puts("session_alive?: #{Process.alive?(s)}")
