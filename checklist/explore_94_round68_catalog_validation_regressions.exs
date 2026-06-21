alias SparkEx.Catalog

url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")

cases = [
  {"list_catalogs keyword", fn s -> Catalog.list_catalogs(s, pattern: "sfx*") end},
  {"list_catalogs integer", fn s -> Catalog.list_catalogs(s, 123) end},
  {"list_tables pattern keyword", fn s -> Catalog.list_tables(s, "sfx_dev_warehouse", pattern: "*") end},
  {"list_tables pattern integer", fn s -> Catalog.list_tables(s, "sfx_dev_warehouse", 123) end},
  {"list_functions pattern keyword", fn s -> Catalog.list_functions(s, "sfx_dev_warehouse", pattern: "*") end},
  {"list_functions pattern integer", fn s -> Catalog.list_functions(s, "sfx_dev_warehouse", 123) end},
  {"table_exists db_name keyword", fn s -> Catalog.table_exists?(s, "foo", db: "sfx") end},
  {"function_exists db_name keyword", fn s -> Catalog.function_exists?(s, "upper", db: "sfx") end}
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
