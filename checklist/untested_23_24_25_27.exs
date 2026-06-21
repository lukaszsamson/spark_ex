# Untested items from sections 23, 24, 25, 27
# Usage: mix run checklist/untested_23_24_25_27.exs

alias SparkEx.{DataFrame, Column, StreamReader, StreamWriter, StreamingQuery, StreamingQueryManager}
import SparkEx.Functions

IO.puts("=== Untested: Sections 23, 24, 25, 27 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run_id = :erlang.system_time(:millisecond) |> to_string()

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 500)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# === 23.3 Parse JSON column ===
IO.puts("--- 23.3-23.4 Parse JSON/CSV ---")

run.("23.3 from_json", fn ->
  SparkEx.sql(s, ~s|SELECT '{"name":"Alice","age":30}' AS json_str|)
  |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ])
  |> DataFrame.collect()
end)

run.("23.4 from_csv", fn ->
  SparkEx.sql(s, "SELECT 'Alice,30' AS csv_str")
  |> DataFrame.select([
    from_csv(col("csv_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ])
  |> DataFrame.collect()
end)

# === 24.3 is_streaming on streaming df ===
IO.puts("--- 24.3 is_streaming on streaming df ---")

run.("24.3 is_streaming", fn ->
  sdf = StreamReader.rate(s, rows_per_second: 5)
  result = DataFrame.is_streaming(sdf)
  IO.puts("  is_streaming on rate source: #{inspect(result)}")
  result
end)

# === 24.7-24.8 last_progress, process_all_available ===
IO.puts("--- 24.7-24.8 streaming details ---")

try do
  {:ok, q} =
    StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("test_stream_#{run_id}")
    |> StreamWriter.start()

  Process.sleep(3000)

  run.("24.7 last_progress", fn -> StreamingQuery.last_progress(q) end)

  run.("24.8 process_all_available", fn -> StreamingQuery.process_all_available(q) end)

  # === 25. Streaming triggers ===
  IO.puts("--- 25. Streaming triggers ---")
  run.("25.1 available_now trigger", fn ->
    {:ok, q2} =
      StreamReader.rate(s, rows_per_second: 10)
      |> DataFrame.write_stream()
      |> StreamWriter.format("memory")
      |> StreamWriter.query_name("trigger_test_#{run_id}")
      |> StreamWriter.trigger(:available_now)
      |> StreamWriter.start()

    Process.sleep(3000)
    active = StreamingQuery.is_active?(q2)
    IO.puts("  is_active after available_now: #{inspect(active)}")
    StreamingQuery.stop(q2)
    active
  end)

  # === 27. StreamingQueryManager ===
  IO.puts("--- 27. StreamingQueryManager ---")

  run.("27.2 get query by id", fn ->
    {:ok, id} = StreamingQuery.id(q)
    StreamingQueryManager.get(s, id)
  end)

  run.("27.3 await_any_termination (timeout)", fn ->
    StreamingQueryManager.await_any_termination(s, timeout: 1)
  end)

  run.("27.4 reset_terminated", fn ->
    StreamingQueryManager.reset_terminated(s)
  end)

  # Stop query
  StreamingQuery.stop(q)

rescue
  e -> IO.puts("  STREAMING ERROR: #{Exception.message(e)}")
catch
  kind, reason -> IO.puts("  STREAMING ERROR (#{kind}): #{inspect(reason)}")
end

IO.puts("=== Sections 23, 24, 25, 27 Complete ===")
