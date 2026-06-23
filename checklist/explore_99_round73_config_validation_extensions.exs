url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")

cases = [
  {"config_get_option non-list", fn s -> SparkEx.config_get_option(s, 42) end},
  {"config_get_option list non-string", fn s -> SparkEx.config_get_option(s, [42]) end},
  {"config_get_with_default non-list", fn s -> SparkEx.config_get_with_default(s, 42) end},
  {"config_get_with_default key non-string", fn s -> SparkEx.config_get_with_default(s, [{42, "x"}]) end},
  {"config_get_with_default default non-string", fn s -> SparkEx.config_get_with_default(s, [{"k", 42}]) end},
  {"config_get_all prefix non-string", fn s -> SparkEx.config_get_all(s, 42) end},
  {"config_get_all prefix keyword", fn s -> SparkEx.config_get_all(s, prefix: "spark") end}
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
