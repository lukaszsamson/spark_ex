# Section 24: Streaming — Basic Lifecycle
# Usage: mix run checklist/section_24_streaming.exs

alias SparkEx.{DataFrame, Column, StreamReader, StreamWriter, StreamingQuery, StreamingQueryManager}
import SparkEx.Functions

IO.puts("=== Section 24: Streaming ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run_id = :erlang.system_time(:millisecond) |> to_string()

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 300)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# 24.1 Rate source to memory
IO.puts("24.1 Rate source to memory")
try do
  {:ok, q} =
    StreamReader.rate(s, rows_per_second: 10)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("rate_test_#{run_id}")
    |> StreamWriter.start()
  IO.puts("  Started streaming query")

  # 24.2 is_active
  run.("24.2 is_active", fn -> StreamingQuery.is_active?(q) end)

  # 24.4 status
  run.("24.4 status", fn -> StreamingQuery.status(q) end)

  # 24.5 id / run_id / name
  run.("24.5 id", fn -> StreamingQuery.id(q) end)
  run.("24.5 run_id", fn -> StreamingQuery.run_id(q) end)
  run.("24.5 name", fn -> StreamingQuery.name(q) end)

  # Wait a bit for data
  Process.sleep(5000)

  # 24.6 recent_progress
  run.("24.6 recent_progress", fn -> StreamingQuery.recent_progress(q) end)

  # 24.9 Query memory sink
  run.("24.9 Query memory sink", fn ->
    SparkEx.sql(s, "SELECT count(*) AS cnt FROM rate_test_#{run_id}") |> DataFrame.collect()
  end)

  # 24.10 exception
  run.("24.10 exception", fn -> StreamingQuery.exception(q) end)

  # 27.1 List active queries
  run.("27.1 List active queries", fn -> StreamingQueryManager.active(s) end)

  # 24.11 Stop query
  run.("24.11 Stop", fn -> StreamingQuery.stop(q) end)

rescue
  e -> IO.puts("  STREAMING ERROR: #{Exception.message(e)}")
catch
  kind, reason -> IO.puts("  STREAMING ERROR (#{kind}): #{inspect(reason)}")
end

IO.puts("=== Section 24 Complete ===")
