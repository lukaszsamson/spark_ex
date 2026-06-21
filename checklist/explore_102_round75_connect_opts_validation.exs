url = System.get_env("SPARK_REMOTE", "sc://localhost:15002")

cases = [
  {"connect user_id integer", [user_id: 42]},
  {"connect user_id keyword", [user_id: [id: "x"]]},
  {"connect client_type integer", [client_type: 42]},
  {"connect session_id integer", [session_id: 42]},
  {"connect server_side_session_id integer", [server_side_session_id: 42]},
  {"connect control valid strings",
   [user_id: "spark_ex_test", client_type: "elixir-test", session_id: "sess-75"]}
]

for {label, extra_opts} <- cases do
  Process.flag(:trap_exit, true)
  connect_result =
    try do
      SparkEx.connect(Keyword.merge([url: url], extra_opts))
    rescue
      e -> {:rescued, e.__struct__, Exception.message(e)}
    catch
      :exit, reason -> {:exit, reason}
    end

  {result, alive?} =
    case connect_result do
      {:ok, s} ->
        op_result =
          try do
            SparkEx.spark_version(s)
          rescue
            e -> {:rescued, e.__struct__, Exception.message(e)}
          catch
            :exit, reason -> {:exit, reason}
          end

        {op_result, Process.alive?(s)}

      other ->
        {other, :not_started}
    end

  IO.puts("\n=== #{label} ===")
  IO.inspect(result, label: "result", limit: 8)
  IO.puts("session_alive?: #{alive?}")
end
