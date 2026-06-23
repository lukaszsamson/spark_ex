alias SparkEx.StreamingQueryManager
alias SparkEx.UDFRegistration

url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")
suffix = System.unique_integer([:positive])

cases = [
  {"register_java aggregate integer",
   fn s -> UDFRegistration.register_java(s, "f_agg_i_#{suffix}", "com.example.Noop", aggregate: 42) end},
  {"register_java aggregate keyword",
   fn s ->
     UDFRegistration.register_java(s, "f_agg_kw_#{suffix}", "com.example.Noop", aggregate: [x: true])
   end},
  {"register_java control aggregate true",
   fn s -> UDFRegistration.register_java(s, "f_agg_ok_#{suffix}", "com.example.Noop", aggregate: true) end},
  {"session clone integer new_session_id", fn s -> SparkEx.Session.clone(s, 42) end},
  {"session clone keyword new_session_id", fn s -> SparkEx.Session.clone(s, id: "abc") end},
  {"session clone control nil new_session_id", fn s -> SparkEx.Session.clone(s, nil) end},
  {"streaming_manager await_any_termination timeout string",
   fn s -> StreamingQueryManager.await_any_termination(s, timeout: "oops") end}
]

for {label, fun} <- cases do
  Process.flag(:trap_exit, true)
  {:ok, s} = SparkEx.connect(url: url)

  result =
    try do
      value = fun.(s)

      case value do
        {:ok, pid} when is_pid(pid) ->
          SparkEx.stop(pid)
          {:ok, :cloned_session_started_then_stopped}

        other ->
          other
      end
    rescue
      e -> {:rescued, e.__struct__, Exception.message(e)}
    catch
      :exit, reason -> {:exit, reason}
    end

  IO.puts("\n=== #{label} ===")
  IO.inspect(result, label: "result", limit: 8)
  IO.puts("session_alive?: #{Process.alive?(s)}")
end
