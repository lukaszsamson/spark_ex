alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

quoted = [%{"id" => 7, "txt" => "a\"b"}]
plain = [%{"id" => 7, "txt" => "ab"}]

{:ok, df_ok} = SparkEx.create_dataframe(s, plain, schema: "id INT, txt STRING")
ok_result = DataFrame.collect(df_ok)

{:ok, df_bad} = SparkEx.create_dataframe(s, quoted, schema: "id INT, txt STRING")
bad_result = DataFrame.collect(df_bad)

{:ok, df_no_schema} = SparkEx.create_dataframe(s, quoted)
no_schema_result = DataFrame.collect(df_no_schema)

IO.inspect(ok_result, label: "schema + plain string (control)")
IO.inspect(bad_result, label: "schema + quoted string")
IO.inspect(no_schema_result, label: "no schema + quoted string (control)")
IO.puts("session_alive?: #{Process.alive?(s)}")
