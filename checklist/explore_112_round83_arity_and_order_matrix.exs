# Round 83: additional matrix for uncommon arities + column-order behavior
import SparkEx.Functions
alias SparkEx.{DataFrame, Column, GroupedData}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 100, printable_limit: 5000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 30, printable_limit: 2000)}")
  end
end

run.("1) trim/ltrim/rtrim custom chars arities", fn ->
  d = SparkEx.sql(s, "SELECT '***abc***' AS x")

  d
  |> DataFrame.select([
    trim(col("x")) |> Column.alias_("trim1"),
    trim(col("x"), lit("*")) |> Column.alias_("trim2"),
    ltrim(col("x")) |> Column.alias_("ltrim1"),
    ltrim(col("x"), lit("*")) |> Column.alias_("ltrim2"),
    rtrim(col("x")) |> Column.alias_("rtrim1"),
    rtrim(col("x"), lit("*")) |> Column.alias_("rtrim2")
  ])
  |> DataFrame.collect()
end)

run.("2) lag/lead arities with default", fn ->
  d = SparkEx.sql(s, "SELECT * FROM VALUES (1,10), (2,20), (3,30) AS t(id,v)")
  w = SparkEx.Window.order_by([col("id")])

  d
  |> DataFrame.select([
    col("id"),
    lag(col("v")) |> Column.over(w) |> Column.alias_("lag1"),
    lag(col("v"), 2) |> Column.over(w) |> Column.alias_("lag2"),
    lead(col("v")) |> Column.over(w) |> Column.alias_("lead1"),
    lead(col("v"), 2) |> Column.over(w) |> Column.alias_("lead2")
  ])
  |> DataFrame.collect()
end)

run.("3) sequence arities + explode ordering", fn ->
  d = SparkEx.sql(s, "SELECT DATE '2024-01-01' AS s, DATE '2024-01-05' AS e")

  d
  |> DataFrame.select([
    sequence(col("s"), col("e")) |> Column.alias_("seq1"),
    sequence(col("s"), col("e"), expr("INTERVAL 2 DAYS")) |> Column.alias_("seq2")
  ])
  |> DataFrame.collect()
end)

run.("4) column order through agg map vs agg list", fn ->
  d = SparkEx.sql(s, "SELECT * FROM VALUES ('x',1,10), ('x',2,20), ('y',3,30) AS t(g,a,b)")
  g = DataFrame.group_by(d, [col("g")])

  r1 =
    g
    |> GroupedData.agg([
      sum(col("a")) |> Column.alias_("sum_a"),
      sum(col("b")) |> Column.alias_("sum_b")
    ])

  r2 = GroupedData.agg(g, %{"a" => "sum", "b" => "sum"})

  %{
    cols_list_agg: elem(DataFrame.columns(r1), 1),
    rows_list_agg: elem(DataFrame.collect(r1), 1),
    cols_map_agg: elem(DataFrame.columns(r2), 1),
    rows_map_agg: elem(DataFrame.collect(r2), 1)
  }
end)

run.("5) select_expr column order stability", fn ->
  d = SparkEx.sql(s, "SELECT 1 AS a, 2 AS b, 3 AS c")
  r = DataFrame.select_expr(d, ["c AS x", "a AS y", "b + c AS z"]) 
  %{columns: elem(DataFrame.columns(r), 1), rows: elem(DataFrame.collect(r), 1)}
end)

run.("6) union_by_name with reordered right-side columns", fn ->
  l = SparkEx.sql(s, "SELECT 1 AS c1, 2 AS c2, 3 AS c3")
  r = SparkEx.sql(s, "SELECT 30 AS c3, 20 AS c2, 10 AS c1")
  u = DataFrame.union_by_name(l, r)
  %{columns: elem(DataFrame.columns(u), 1), rows: elem(DataFrame.collect(u), 1)}
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
