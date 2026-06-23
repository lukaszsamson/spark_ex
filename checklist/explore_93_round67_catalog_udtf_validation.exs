alias SparkEx.{Catalog, UDFRegistration}

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
  IO.inspect(result, label: "result", limit: 10, printable_limit: 12_000)
  IO.puts("session_alive?: #{Process.alive?(s)}")
end

# Catalog db_name validation gaps
run.("catalog.table_exists invalid db_name integer", fn s ->
  Catalog.table_exists?(s, "nonexistent_#{uid}", 123)
end)

run.("catalog.get_table invalid db_name integer", fn s ->
  Catalog.get_table(s, "nonexistent_#{uid}", 123)
end)

run.("catalog.list_columns invalid db_name integer", fn s ->
  Catalog.list_columns(s, "nonexistent_#{uid}", 123)
end)

run.("catalog.function_exists invalid db_name integer", fn s ->
  Catalog.function_exists?(s, "upper", 123)
end)

run.("catalog.list_tables invalid db_name integer (control)", fn s ->
  Catalog.list_tables(s, 123)
end)

# UDTF eval_type validation gap
run.("udf_registration.register_udtf eval_type string", fn s ->
  UDFRegistration.register_udtf(s, "udtf_eval_#{uid}", <<1, 2, 3>>, eval_type: "oops")
end)

# Catalog options value type validation gap
run.("catalog.create_table options map non-string value", fn s ->
  Catalog.create_table(s, "tmp_opts_val_#{uid}",
    source: "parquet",
    schema: "id INT",
    options: %{"path" => 123}
  )
end)

run.("catalog.create_external_table options map non-string value", fn s ->
  Catalog.create_external_table(s, "tmp_ext_opts_val_#{uid}", "/tmp/xyz",
    source: "parquet",
    schema: "id INT",
    options: %{"a" => 1}
  )
end)
