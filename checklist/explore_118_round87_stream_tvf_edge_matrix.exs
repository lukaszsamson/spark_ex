# Round 87: stream + TVF edge matrix
# Read-only against existing tables; temp streaming paths/sinks only
import SparkEx.Functions
alias SparkEx.{DataFrame, StreamReader, StreamWriter, TableValuedFunction}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = System.system_time(:millisecond)
root = "/tmp/spark_ex_r87_#{run_id}"

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 120, printable_limit: 8000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 30, printable_limit: 3000)}")
  end
end

run.("1) TVF.inline convenience helper", fn ->
  tvf = TableValuedFunction.new(s)

  df =
    SparkEx.sql(
      s,
      """
      SELECT array(named_struct('a',1,'b','x'), named_struct('a',2,'b','y')) AS items
      """
    )

  # inline expects array<struct>
  inline_df = TableValuedFunction.inline(tvf, col("items"))

  # control: project inline() through SQL relation wrapper
  DataFrame.lateral_join(df, inline_df, lit(true), :cross)
  |> DataFrame.collect()
end)

run.("2) TVF.stack num_rows = 0 (validation gap probe)", fn ->
  tvf = TableValuedFunction.new(s)
  ok = TableValuedFunction.stack(tvf, 2, [lit("A"), lit(1), lit("B"), lit(2)]) |> DataFrame.collect()
  bad = TableValuedFunction.stack(tvf, 0, [lit("A"), lit(1)]) |> DataFrame.collect()
  %{control_positive: ok, zero_rows: bad}
end)

run.("3) TVF.stack num_rows = -1 (validation gap probe)", fn ->
  tvf = TableValuedFunction.new(s)
  TableValuedFunction.stack(tvf, -1, [lit("A"), lit(1)]) |> DataFrame.collect()
end)

run.("4) StreamReader.load with empty paths list", fn ->
  reader = StreamReader.new(s) |> StreamReader.format("json") |> StreamReader.option("maxFilesPerTrigger", 1)
  df = StreamReader.load(reader, [])
  DataFrame.is_streaming(df)
end)

run.("5) StreamWriter.start call-time outputMode override", fn ->
  qname = "r87_mem_#{run_id}"

  rate_df = StreamReader.rate(s, rows_per_second: 2)

  {:ok, q} =
    rate_df
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name(qname)
    |> StreamWriter.start(outputMode: "append", trigger: [available_now: true])

  _ = SparkEx.StreamingQuery.await_termination(q, timeout: 30)
  rows = SparkEx.sql(s, "SELECT count(*) AS c FROM #{qname}") |> DataFrame.collect()
  _ = SparkEx.StreamingQuery.stop(q)
  rows
end)

run.("6) StreamWriter.trigger with multiple keys raises", fn ->
  rate_df = StreamReader.rate(s, rows_per_second: 1)

  rate_df
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("r87_bad_trigger_#{run_id}")
  |> StreamWriter.trigger(processing_time: "1 second", available_now: true)
end)

run.("7) StreamReader.schema with Types struct tuple", fn ->
  schema = "id LONG, v STRING"

  reader =
    StreamReader.new(s)
    |> StreamReader.format("json")
    |> StreamReader.schema(schema)
    |> StreamReader.option("maxFilesPerTrigger", 1)

  DataFrame.is_streaming(StreamReader.load(reader, root <> "/dummy"))
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
