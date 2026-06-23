# Round 86: reader/load/binary_file matrix on less-used APIs
# Read-only wrt existing tables; uses temp HDFS paths only
import SparkEx.Functions
alias SparkEx.{DataFrame, Reader, Writer, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = System.system_time(:millisecond)
root = "/tmp/spark_ex_r86_#{run_id}"

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

run.("1) Reader.load(session, format, path)", fn ->
  path = "#{root}/txt"
  src = SparkEx.sql(s, "SELECT * FROM VALUES ('aa'), ('bb') AS t(value)")
  Writer.text(src, path, mode: :overwrite)

  Reader.load(s, "text", path) |> DataFrame.collect()
end)

run.("2) Reader.new |> format |> option |> load(list_of_paths)", fn ->
  p1 = "#{root}/json1"
  p2 = "#{root}/json2"
  Writer.json(SparkEx.sql(s, "SELECT * FROM VALUES (1,'x') AS t(id,v)"), p1, mode: :overwrite)
  Writer.json(SparkEx.sql(s, "SELECT * FROM VALUES (2,'y') AS t(id,v)"), p2, mode: :overwrite)

  r =
    Reader.new(s)
    |> Reader.format("json")
    |> Reader.option("primitivesAsString", false)
    |> Reader.load([p1, p2])

  r
  |> DataFrame.order_by([col("id")])
  |> DataFrame.collect()
end)

run.("3) Reader.binary_file + metadata/path/content length", fn ->
  path = "#{root}/bin_src"
  Writer.text(SparkEx.sql(s, "SELECT * FROM VALUES ('hello'), ('world') AS t(value)"), path, mode: :overwrite)

  Reader.binary_file(s, path)
  |> DataFrame.select([
    col("path"),
    col("length"),
    DataFrame.metadata_column(DataFrame.alias_(Reader.binary_file(s, path), "bf"), "_metadata.file_name")
    |> Column.alias_("meta_name")
  ])
  |> DataFrame.limit(2)
  |> DataFrame.collect()
end)

run.("4) Reader.load(nil, opts) and Reader.load(path, opts)", fn ->
  p = "#{root}/csv1"
  Writer.csv(SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (2,'b') AS t(id,v)"), p, mode: :overwrite, header: true)

  r1 = Reader.new(s) |> Reader.format("csv") |> Reader.load(nil, header: true)
  r2 = Reader.new(s) |> Reader.format("csv") |> Reader.load(p, header: true)

  %{nil_load: DataFrame.collect(r1), path_load: DataFrame.collect(r2)}
end)

run.("5) Reader.jdbc(opts) missing options validation", fn ->
  Reader.jdbc(s, [url: "jdbc:sqlite:/tmp/no.db"]) |> DataFrame.collect()
end)

run.("6) Reader.options(map) key coercion behavior", fn ->
  p = "#{root}/json_opts"
  Writer.json(SparkEx.sql(s, "SELECT * FROM VALUES (1) AS t(id)"), p, mode: :overwrite)

  r =
    Reader.new(s)
    |> Reader.format("json")
    |> Reader.options(%{"allowSingleQuotes" => true, "primitivesAsString" => false})
    |> Reader.load(p)

  DataFrame.collect(r)
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
