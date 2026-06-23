alias SparkEx.DataFrame

url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")

cases = [
  {"parse csv options keyword list",
   fn s ->
     {:ok, df} = SparkEx.create_dataframe(s, [%{"raw" => "1,A"}], schema: "raw STRING")
     DataFrame.parse(df, :csv, "id INT, name STRING", [sep: ","]) |> DataFrame.collect()
   end},
  {"parse json options integer",
   fn s ->
     {:ok, df} = SparkEx.create_dataframe(s, [%{"raw" => ~s({"id":1})}], schema: "raw STRING")
     DataFrame.parse(df, :json, "id INT", 42) |> DataFrame.collect()
   end},
  {"parse csv schema integer",
   fn s ->
     {:ok, df} = SparkEx.create_dataframe(s, [%{"raw" => "1,A"}], schema: "raw STRING")
     DataFrame.parse(df, :csv, 42, %{"sep" => ","}) |> DataFrame.collect()
   end},
  {"parse json schema map",
   fn s ->
     {:ok, df} = SparkEx.create_dataframe(s, [%{"raw" => ~s({"id":1})}], schema: "raw STRING")
     DataFrame.parse(df, :json, %{id: :int}, %{}) |> DataFrame.collect()
   end},
  {"parse control map options",
   fn s ->
     {:ok, df} = SparkEx.create_dataframe(s, [%{"raw" => "1,A"}], schema: "raw STRING")
     DataFrame.parse(df, :csv, "id INT, name STRING", %{"sep" => ","}) |> DataFrame.collect()
   end}
]

for {label, fun} <- cases do
  Process.flag(:trap_exit, true)
  {:ok, s} = SparkEx.connect(url: url)

  result =
    try do
      fun.(s)
    rescue
      e -> {:rescued, e.__struct__, Exception.message(e)}
    catch
      :exit, reason -> {:exit, reason}
    end

  IO.puts("\n=== #{label} ===")
  IO.inspect(result, label: "result", limit: 8)
  IO.puts("session_alive?: #{Process.alive?(s)}")
end
