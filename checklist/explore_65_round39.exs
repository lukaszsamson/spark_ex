# Round 39: Protobuf encoding edge cases, UDF correct API, streaming correct pattern,
# edge cases in plan encoding, more creative combos
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, StreamReader, StreamWriter, StreamingQuery}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 80, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. UDF registration with correct keyword API
# =============================================

run.("1a. register_java (correct keyword args)", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_abs_#{run_id}",
    class_name: "java.lang.Math",
    return_type: "INT")
end)

run.("1b. register_java (without return_type)", fn ->
  SparkEx.UDFRegistration.register_java(s, "test_abs2_#{run_id}",
    class_name: "java.lang.Math")
end)

# =============================================
# 2. Streaming with correct destructuring
# =============================================

run.("2. streaming lifecycle (correct pattern)", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("ws39_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(3000)
  active = StreamingQuery.is_active?(query)
  name = StreamingQuery.name(query)
  exc = StreamingQuery.exception(query)
  status = StreamingQuery.status(query)
  progress = StreamingQuery.recent_progress(query)

  {:ok, result} = SparkEx.Reader.table(s, "ws39_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)

  %{
    active: active, name: name, exception: exc,
    status: status, progress_entries: length(progress || []),
    count: length(result)
  }
end)

# =============================================
# 3. Protobuf edge: nested lit() types
# =============================================

run.("3a. lit with Decimal", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(Decimal.new("123.456")) |> Column.alias_("dec")
  ]) |> DataFrame.collect()
end)

run.("3b. lit with Date", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(~D[2024-02-29]) |> Column.alias_("dt")
  ]) |> DataFrame.collect()
end)

run.("3c. lit with NaiveDateTime", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(~N[2024-06-15 10:30:45]) |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("3d. lit with list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit([1, 2, 3]) |> Column.alias_("arr")
  ]) |> DataFrame.collect()
end)

run.("3e. lit with map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(%{"a" => 1, "b" => 2}) |> Column.alias_("mp")
  ]) |> DataFrame.collect()
end)

run.("3f. lit with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit([]) |> Column.alias_("empty_arr")
  ]) |> DataFrame.collect()
end)

run.("3g. lit with empty map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(%{}) |> Column.alias_("empty_map")
  ]) |> DataFrame.collect()
end)

run.("3h. lit with binary", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit({:binary, <<1, 2, 3, 4>>}) |> Column.alias_("bin")
  ]) |> DataFrame.collect()
end)

run.("3i. lit with negative integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(-9_999_999) |> Column.alias_("neg")
  ]) |> DataFrame.collect()
end)

run.("3j. lit with very small float", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(1.0e-300) |> Column.alias_("tiny")
  ]) |> DataFrame.collect()
end)

run.("3k. lit with INT64_MAX (boundary)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(9_223_372_036_854_775_807) |> Column.alias_("max_long")
  ]) |> DataFrame.collect()
end)

run.("3l. lit with INT64_MIN (boundary)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(-9_223_372_036_854_775_808) |> Column.alias_("min_long")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Creative: chained when_/otherwise deeply nested
# =============================================

run.("4. deeply nested when/otherwise (grade ranges)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map([95, 85, 75, 65, 55, 45], fn s -> %{"score" => s} end),
    schema: "score INT")

  df |> DataFrame.with_column("grade",
    when_(Column.gte(col("score"), lit(90)), lit("A"))
    |> Column.otherwise(
      when_(Column.gte(col("score"), lit(80)), lit("B"))
      |> Column.otherwise(
        when_(Column.gte(col("score"), lit(70)), lit("C"))
        |> Column.otherwise(
          when_(Column.gte(col("score"), lit(60)), lit("D"))
          |> Column.otherwise(lit("F"))))))
  |> DataFrame.collect()
end)

# =============================================
# 5. Creative: window with multiple different windows
# =============================================

run.("5. multiple window functions on same DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "A", "name" => "Alice", "salary" => 100},
    %{"dept" => "A", "name" => "Bob", "salary" => 120},
    %{"dept" => "A", "name" => "Charlie", "salary" => 90},
    %{"dept" => "B", "name" => "Dave", "salary" => 110},
    %{"dept" => "B", "name" => "Eve", "salary" => 130}
  ], schema: "dept STRING, name STRING, salary INT")

  w_dept = SparkEx.Window.partition_by([col("dept")])
    |> WindowSpec.order_by([desc(col("salary"))])

  w_all = SparkEx.Window.partition_by([lit(true)])
    |> WindowSpec.order_by([desc(col("salary"))])

  df
  |> DataFrame.with_column("dept_rank", row_number() |> Column.over(w_dept))
  |> DataFrame.with_column("global_rank", row_number() |> Column.over(w_all))
  |> DataFrame.with_column("dept_avg", avg(col("salary")) |> Column.over(
    SparkEx.Window.partition_by([col("dept")])))
  |> DataFrame.sort([asc(col("dept")), asc(col("dept_rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 6. Creative: complex type operations
# =============================================

run.("6a. nested array operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [[1, 2], [3, 4], [5]]}
  ], schema: "arr ARRAY<ARRAY<INT>>")
  df |> DataFrame.select([
    col("arr"),
    flatten(col("arr")) |> Column.alias_("flattened"),
    size(col("arr")) |> Column.alias_("outer_size")
  ]) |> DataFrame.collect()
end)

run.("6b. struct creation and access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30, "city" => "NYC"}
  ], schema: "name STRING, age INT, city STRING")
  df |> DataFrame.select([
    struct([col("name"), col("age")]) |> Column.alias_("person"),
    struct([col("name"), col("age"), col("city")]) |> Column.alias_("full")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Creative: complex filter chains
# =============================================

run.("7. complex filter with AND/OR logic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "val" => rem(i * 7, 100), "category" => Enum.at(["A", "B", "C"], rem(i, 3))}
    end), schema: "id INT, val INT, category STRING")

  df
  |> DataFrame.filter(
    Column.and_(
      Column.or_(
        Column.eq(col("category"), lit("A")),
        Column.eq(col("category"), lit("B"))
      ),
      Column.gt(col("val"), lit(20))
    )
  )
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. Edge: create_dataframe with mixed null types
# =============================================

run.("8. DataFrame with all-null column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil},
    %{"a" => 2, "b" => nil},
    %{"a" => 3, "b" => nil}
  ], schema: "a INT, b STRING")
  df |> DataFrame.select([
    col("a"),
    col("b"),
    isnull(col("b")) |> Column.alias_("b_is_null")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Edge: very deeply nested plan
# =============================================

run.("9. deeply nested plan (20 chained withColumns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"base" => 1}], schema: "base INT")
  result = Enum.reduce(1..20, df, fn i, acc ->
    DataFrame.with_column(acc, "col_#{i}",
      Column.plus(col("base"), lit(i)))
  end)
  {:ok, rows} = DataFrame.collect(result)
  Map.keys(hd(rows)) |> Enum.sort() |> length()
end)

# =============================================
# 10. Edge: DataFrame.distinct with complex types
# =============================================

run.("10. distinct with duplicates", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"},
    %{"a" => 1, "b" => "x"},
    %{"a" => 2, "b" => "y"},
    %{"a" => 2, "b" => "y"},
    %{"a" => 3, "b" => "z"}
  ], schema: "a INT, b STRING")
  df |> DataFrame.distinct() |> DataFrame.sort([asc(col("a"))]) |> DataFrame.collect()
end)

# =============================================
# 11. Creative: window percent_rank / cume_dist / ntile
# =============================================

run.("11. window: percent_rank, cume_dist, ntile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i * 10} end), schema: "val INT")

  w = SparkEx.Window.partition_by([lit(true)])
    |> WindowSpec.order_by([asc(col("val"))])

  df |> DataFrame.select([
    col("val"),
    percent_rank() |> Column.over(w) |> Column.alias_("pct_rank"),
    cume_dist() |> Column.over(w) |> Column.alias_("cume"),
    ntile(4) |> Column.over(w) |> Column.alias_("quartile")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Edge: create_dataframe with many rows
# =============================================

run.("12. create_dataframe with 5000 rows", fn ->
  rows = Enum.map(1..5000, fn i ->
    %{"id" => i, "val" => i * 2.0, "grp" => rem(i, 10)}
  end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "id INT, val DOUBLE, grp INT")

  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    count(col("id")) |> Column.alias_("cnt"),
    avg(col("val")) |> Column.alias_("avg_val")
  ])
  |> DataFrame.sort([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 13. Functions: hash / md5 / sha1 / sha2 / crc32 / xxhash64
# =============================================

run.("13. hash functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    md5(col("txt")) |> Column.alias_("md5"),
    sha1(col("txt")) |> Column.alias_("sha1"),
    sha2(col("txt"), 256) |> Column.alias_("sha256"),
    crc32(col("txt")) |> Column.alias_("crc32"),
    xxhash64(col("txt")) |> Column.alias_("xxh64"),
    hash(col("txt")) |> Column.alias_("murmurhash")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Functions: hex / unhex / conv / bin
# =============================================

run.("14. hex/unhex/conv", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 255}
  ], schema: "val INT")
  df |> DataFrame.select([
    hex(col("val")) |> Column.alias_("hexed"),
    conv(col("val"), 10, 2) |> Column.alias_("binary"),
    bin(col("val")) |> Column.alias_("bin_str")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Functions: cbrt / signum / factorial
# =============================================

run.("15. math functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 27.0, "n" => 5}
  ], schema: "val DOUBLE, n INT")
  df |> DataFrame.select([
    cbrt(col("val")) |> Column.alias_("cbrt"),
    signum(col("val")) |> Column.alias_("sign"),
    factorial(col("n")) |> Column.alias_("fact"),
    degrees(lit(3.14159265)) |> Column.alias_("deg"),
    radians(lit(180.0)) |> Column.alias_("rad")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Edge: create_or_replace_temp_view then SQL then drop
# =============================================

run.("16. temp view lifecycle", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  DataFrame.create_or_replace_temp_view(df, "tv_#{run_id}")
  result = SparkEx.sql(s, "SELECT * FROM tv_#{run_id} ORDER BY id") |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "tv_#{run_id}")

  # Verify it's gone
  gone = SparkEx.sql(s, "SELECT * FROM tv_#{run_id}") |> DataFrame.collect()
  {result, gone}
end)

# =============================================
# 17. Edge: create_dataframe with special characters in values
# =============================================

run.("17. special characters in string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "quote\"inside"},
    %{"txt" => "tab\there"},
    %{"txt" => "backslash\\here"},
    %{"txt" => "null\x00byte"},
    %{"txt" => "single'quote"}
  ], schema: "txt STRING")
  DataFrame.collect(df)
end)

# =============================================
# 18. Functions: concat / concat_ws on different types
# =============================================

run.("18. concat and concat_ws", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "hello", "b" => " ", "c" => "world", "num" => 42}
  ], schema: "a STRING, b STRING, c STRING, num INT")
  df |> DataFrame.select([
    concat([col("a"), col("b"), col("c")]) |> Column.alias_("concatenated"),
    concat_ws(", ", [col("a"), col("c"), Column.cast(col("num"), "STRING")])
      |> Column.alias_("with_sep")
  ]) |> DataFrame.collect()
end)

# =============================================
# 19. Functions: monotonically_increasing_id
# =============================================

run.("19. monotonically_increasing_id", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Charlie"}
  ], schema: "name STRING")
  df |> DataFrame.with_column("row_id", monotonically_increasing_id())
  |> DataFrame.collect()
end)

# =============================================
# 20. Functions: input_file_name / spark_partition_id
# =============================================

run.("20. input_file_name and spark_partition_id", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    col("id"),
    input_file_name() |> Column.alias_("file"),
    spark_partition_id() |> Column.alias_("partition")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 39 complete ===")
