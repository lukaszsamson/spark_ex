# Round 82: tricky structure roundtrips + uncommon arity combos + column-order stability
# Read-only against catalog tables (uses VALUES and /tmp files only)
import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = System.system_time(:millisecond)
io_root = "/tmp/spark_ex_round82_#{run_id}"

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 120, printable_limit: 6000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 40, printable_limit: 3000)}")
  end
end

# Struct/map/array-heavy source with null edges
src =
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      (1, named_struct('a', 10, 'b', array(1,2), 'm', map('x', 1, 'y', 2)), array(named_struct('k','u','v',1), named_struct('k','v','v',2))),
      (2, named_struct('a', NULL, 'b', array(), 'm', map()), array(named_struct('k','w','v',3))),
      (3, named_struct('a', 7, 'b', array(9), 'm', map('q', 9)), CAST(NULL AS ARRAY<STRUCT<k:STRING,v:INT>>))
    AS t(id, payload, events)
    """
  )

run.("1) write/read parquet roundtrip for nested struct/map/array", fn ->
  path = "#{io_root}/nested_roundtrip"
  SparkEx.Writer.parquet(src, path, mode: :overwrite)

  back = SparkEx.Reader.parquet(s, path)

  {:ok, schema0} = DataFrame.schema(src)
  {:ok, schema1} = DataFrame.schema(back)
  {:ok, dtypes0} = DataFrame.dtypes(src)
  {:ok, dtypes1} = DataFrame.dtypes(back)
  {:ok, rows0} = DataFrame.collect(src)
  {:ok, rows1} = DataFrame.collect(back)

  %{
    # Raw schema proto equality can differ in metadata/id internals; compare user-visible dtypes too.
    schema_equal_raw: schema0 == schema1,
    dtypes_equal: dtypes0 == dtypes1,
    row_count_equal: Kernel.length(rows0) == Kernel.length(rows1),
    first_src: Enum.at(rows0, 0),
    first_back: Enum.at(rows1, 0)
  }
end)

run.("2) to_json/from_json nested roundtrip + column access", fn ->
  shaped =
    src
    |> DataFrame.with_column("payload_json", to_json(col("payload")))
    |> DataFrame.with_column("payload_back", from_json(col("payload_json"), "a INT, b ARRAY<INT>, m MAP<STRING, INT>"))
    |> DataFrame.select([
      col("id"),
      col("payload.a") |> Column.alias_("a0"),
      col("payload_back.a") |> Column.alias_("a1"),
      element_at(col("payload.m"), lit("x")) |> Column.alias_("mx0"),
      element_at(col("payload_back.m"), lit("x")) |> Column.alias_("mx1")
    ])

  DataFrame.collect(shaped)
end)

run.("3) uncommon arity combos (array_join/split/overlay/locate)", fn ->
  d =
    SparkEx.sql(
      s,
      """
      SELECT * FROM VALUES
        ('ab--cd--ef', array('a', NULL, 'b'), 'spark_ex', 'barbar')
      AS t(txt, arr, base, txt2)
      """
    )

  d
  |> DataFrame.select([
    split(col("txt"), lit("--")) |> Column.alias_("split2"),
    split(col("txt"), lit("--"), lit(2)) |> Column.alias_("split3"),
    array_join(col("arr"), lit("|")) |> Column.alias_("aj2"),
    array_join(col("arr"), lit("|"), lit("NA")) |> Column.alias_("aj3"),
    overlay(col("base"), lit("XX"), lit(3)) |> Column.alias_("ov3"),
    overlay(col("base"), lit("YY"), lit(3), lit(2)) |> Column.alias_("ov4"),
    locate(lit("bar"), col("txt2")) |> Column.alias_("loc2"),
    locate(lit("bar"), col("txt2"), lit(2)) |> Column.alias_("loc3")
  ])
  |> DataFrame.collect()
end)

run.("4) column order after union_by_name allow_missing_columns", fn ->
  l = SparkEx.sql(s, "SELECT 1 AS id, 'a' AS name, 10 AS score")
  r = SparkEx.sql(s, "SELECT 'b' AS name, 2 AS id, true AS flag")

  u = DataFrame.union_by_name(l, r, allow_missing_columns: true)
  {:ok, cols} = DataFrame.columns(u)
  {:ok, rows} = DataFrame.collect(u)
  %{columns: cols, rows: rows}
end)

run.("5) column order after with_columns + to_df rename", fn ->
  d = SparkEx.sql(s, "SELECT 1 AS a, 2 AS b, 3 AS c")

  x =
    d
    |> DataFrame.with_columns([
      Column.plus(col("a"), col("b")) |> Column.alias_("ab"),
      Column.plus(col("b"), col("c")) |> Column.alias_("bc")
    ])

  {:ok, cols1} = DataFrame.columns(x)
  y = DataFrame.to_df(x, ["x1", "x2", "x3", "x4", "x5"])
  {:ok, cols2} = DataFrame.columns(y)
  {:ok, rows} = DataFrame.collect(y)
  %{cols_after_with_columns: cols1, cols_after_to_df: cols2, rows: rows}
end)

run.("6) join column order stability (using columns vs expression)", fn ->
  left = SparkEx.sql(s, "SELECT 1 AS id, 'L1' AS lval UNION ALL SELECT 2 AS id, 'L2' AS lval")
  right = SparkEx.sql(s, "SELECT 1 AS id, 'R1' AS rval UNION ALL SELECT 3 AS id, 'R3' AS rval")

  j1 = DataFrame.join(left, right, ["id"], :left)
  {:ok, c1} = DataFrame.columns(j1)

  j2 = DataFrame.join(left, right, Column.eq(DataFrame.col(left, "id"), DataFrame.col(right, "id")), :left)
  {:ok, c2} = DataFrame.columns(j2)

  %{
    using_columns_order: c1,
    expr_join_order: c2,
    using_rows: elem(DataFrame.collect(j1), 1),
    expr_rows: elem(DataFrame.collect(j2), 1)
  }
end)

run.("7) select order with duplicate-like aliases", fn ->
  d = SparkEx.sql(s, "SELECT 1 AS a, 2 AS b")

  d
  |> DataFrame.select([
    col("b") |> Column.alias_("x"),
    col("a") |> Column.alias_("y"),
    Column.plus(col("a"), col("b")) |> Column.alias_("x")
  ])
  |> DataFrame.collect()
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
