alias SparkEx.Catalog

checks = [
  {"list_catalogs", fn s -> Catalog.list_catalogs(s, pattern: "sfx*") end},
  {"list_databases", fn s -> Catalog.list_databases(s, pattern: "sfx*") end},
  {"list_tables", fn s -> Catalog.list_tables(s, "sfx_dev_warehouse", pattern: "*") end},
  {"list_functions", fn s -> Catalog.list_functions(s, "sfx_dev_warehouse", pattern: "*") end}
]

for {label, fun} <- checks do
  Process.flag(:trap_exit, true)
  {:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

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
