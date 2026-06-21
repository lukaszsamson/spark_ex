# Round 84: creative exploratory pass for less-covered APIs
# Read-only: VALUES/temp files/temp views only
import SparkEx.Functions
alias SparkEx.{DataFrame, Column, Reader, Writer}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = System.system_time(:millisecond)
root = "/tmp/spark_ex_r84_#{run_id}"
File.mkdir_p!(root)

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

run.("1) metadata_column on file source", fn ->
  p = "#{root}/text_src"
  src = SparkEx.sql(s, "SELECT * FROM VALUES ('alpha'), ('beta') AS t(value)")
  Writer.text(src, p, mode: :overwrite)

  df = Reader.text(s, p)

  DataFrame.select(df, [
    col("value"),
    DataFrame.metadata_column(df, "_metadata") |> Column.alias_("md")
  ])
  |> DataFrame.collect()
end)

run.("2) metadata nested access from _metadata", fn ->
  p = "#{root}/json_src"
  src = SparkEx.sql(s, "SELECT * FROM VALUES (1,'x'), (2,'y') AS t(id,v)")
  Writer.json(src, p, mode: :overwrite)

  df = Reader.json(s, p)

  DataFrame.select(df, [
    col("id"),
    DataFrame.metadata_column(df, "_metadata.file_path") |> Column.alias_("path"),
    DataFrame.metadata_column(df, "_metadata.file_name") |> Column.alias_("name")
  ])
  |> DataFrame.collect()
end)

run.("3) scalar/exists/in_subquery combination", fn ->
  base = SparkEx.sql(s, "SELECT * FROM VALUES (1,10), (2,20), (3,30) AS t(id,v)")

  avg_sub = base |> DataFrame.select([avg(col("v")) |> Column.alias_("avg_v")])
  allowed = SparkEx.sql(s, "SELECT * FROM VALUES (1), (3) AS t(id)")

  base
  |> DataFrame.filter(Column.gt(col("v"), DataFrame.scalar(avg_sub)))
  |> DataFrame.filter(DataFrame.exists(allowed))
  |> DataFrame.filter(DataFrame.in_subquery(col("id"), allowed))
  |> DataFrame.order_by([col("id")])
  |> DataFrame.collect()
end)

run.("4) DataFrame.in_subquery/2 (df,list-of-cols overload)", fn ->
  sub = SparkEx.sql(s, "SELECT * FROM VALUES (1), (2) AS t(id)")
  left = SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (3,'b') AS t(id,tag)")

  left
  |> DataFrame.filter(DataFrame.in_subquery(sub, [col("id")]))
  |> DataFrame.collect()
end)

run.("5) lateral_join with TVF explode(Column.outer(...))", fn ->
  df =
    SparkEx.sql(
      s,
      """
      SELECT * FROM VALUES
        (1, array('x','y')),
        (2, array('z')),
        (3, CAST(NULL AS ARRAY<STRING>))
      AS t(id, items)
      """
    )

  tvf = SparkEx.tvf(s)
  right = SparkEx.TableValuedFunction.call(tvf, "explode_outer", [Column.outer(col("items"))])

  DataFrame.lateral_join(df, right, nil, :left)
  |> DataFrame.select([col("id"), col("item")])
  |> DataFrame.order_by([col("id"), col("item")])
  |> DataFrame.collect()
end)

run.("6) repartition_by_id by column and num_partitions", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(pid, val)")

  r1 = DataFrame.repartition_by_id(df, 2, col("pid"))
  r2 = DataFrame.repartition_by_id(df, 2, "pid")

  %{
    c1: DataFrame.count(r1),
    c2: DataFrame.count(r2),
    p1: DataFrame.rdd_num_partitions_approx(r1),
    p2: DataFrame.rdd_num_partitions_approx(r2)
  }
end)

run.("7) DataFrame.table_function stack() via API", fn ->
  DataFrame.table_function(s, "stack", [lit(2), lit("A"), lit(10), lit("B"), lit(20)])
  |> DataFrame.collect()
end)

run.("8) to_protobuf/from_protobuf option conflict validation", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1) AS payload")

  try do
    DataFrame.select(df, [
      to_protobuf(col("payload"), "sparkex.test.Simple",
        desc_file_path: "/tmp/nope.desc",
        binary_descriptor_set: <<1, 2, 3>>
      )
    ])
    |> DataFrame.collect()
  rescue
    e -> {:raised, Exception.message(e)}
  end
end)

run.("9) to_protobuf without descriptor (remote behavior)", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1) AS payload")

  DataFrame.select(df, [to_protobuf(col("payload"), "sparkex.test.Simple") |> Column.alias_("bin")])
  |> DataFrame.collect()
end)

run.("10) from_protobuf without descriptor (remote behavior)", fn ->
  df = SparkEx.sql(s, "SELECT CAST('abc' AS BINARY) AS raw")

  DataFrame.select(df, [from_protobuf(col("raw"), "sparkex.test.Simple") |> Column.alias_("decoded")])
  |> DataFrame.collect()
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
