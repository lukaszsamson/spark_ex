alias SparkEx.{Catalog, DataFrame, StreamReader, StreamWriter, StreamingQuery, UDFRegistration}

url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")
uid = System.system_time(:millisecond)

run = fn label, fun ->
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
  IO.inspect(result, label: "result", limit: 8, printable_limit: 12_000)
  IO.puts("session_alive?: #{Process.alive?(s)}")
end

run.("udf_registration.register_udtf return_type string", fn s ->
  UDFRegistration.register_udtf(s, "udtf_#{uid}", <<1, 2, 3>>, return_type: "id INT")
end)

run.("udf_registration.register_udtf deterministic string", fn s ->
  UDFRegistration.register_udtf(s, "udtf_det_#{uid}", <<1, 2, 3>>, deterministic: "yes")
end)

run.("udf_registration.register_data_source python_ver integer", fn s ->
  UDFRegistration.register_data_source(s, "ds_#{uid}", <<1, 2, 3>>, python_ver: 311)
end)

run.("catalog.create_table options keyword list", fn s ->
  Catalog.create_table(s, "tmp_opts_#{uid}", source: "parquet", schema: "id INT", options: [path: "/tmp/xyz"])
end)

run.("catalog.create_external_table options keyword list", fn s ->
  Catalog.create_external_table(s, "tmp_ext_opts_#{uid}", "/tmp/xyz", source: "parquet", schema: "id INT", options: [a: 1])
end)

run.("streaming_query.explain extended string", fn s ->
  query_name = "q_ex_#{uid}"

  {:ok, q} =
    s
    |> StreamReader.rate(rows_per_second: 1)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name(query_name)
    |> StreamWriter.start()

  result = StreamingQuery.explain(q, extended: "yes")
  _ = StreamingQuery.stop(q)
  result
end)
