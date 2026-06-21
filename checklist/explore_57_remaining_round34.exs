# Round 34b: Remaining tests after session crash from config_is_modifiable
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, StreamReader, StreamWriter, StreamingQuery}
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
# 1. Config operations (correct API)
# =============================================

run.("1a. config_set / config_get (list of pairs)", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "10"}])
  result = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  # Reset
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "200"}])
  result
end)

run.("1b. config_is_modifiable (list of keys)", fn ->
  SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions"])
end)

run.("1c. config_unset", fn ->
  SparkEx.config_set(s, [{"spark.sql.testCustom12345", "hello"}])
  before = SparkEx.config_get(s, ["spark.sql.testCustom12345"])
  SparkEx.config_unset(s, ["spark.sql.testCustom12345"])
  after_ = SparkEx.config_get(s, ["spark.sql.testCustom12345"])
  {before, after_}
end)

# =============================================
# 2. Tags lifecycle
# =============================================

run.("2. tags: add, get, remove, clear", fn ->
  SparkEx.Session.add_tag(s, "test_tag_1")
  SparkEx.Session.add_tag(s, "test_tag_2")
  tags1 = SparkEx.Session.get_tags(s)

  SparkEx.Session.remove_tag(s, "test_tag_1")
  tags2 = SparkEx.Session.get_tags(s)

  SparkEx.Session.clear_tags(s)
  tags3 = SparkEx.Session.get_tags(s)

  %{after_add: tags1, after_remove: tags2, after_clear: tags3}
end)

# =============================================
# 3. Streaming with windowed aggregation
# =============================================

run.("3. streaming rate → count → memory sink", fn ->
  stream = StreamReader.rate(s, 10)

  query = stream
  |> DataFrame.group_by([Column.mod(col("value"), lit(3)) |> Column.alias_("group")])
  |> GroupedData.agg([count(col("value")) |> Column.alias_("cnt")])
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("stream_test_#{run_id}")
  |> StreamWriter.output_mode("complete")
  |> StreamWriter.start()

  Process.sleep(5000)

  active = StreamingQuery.is_active?(query)
  {:ok, result} = SparkEx.Reader.table(s, "stream_test_#{run_id}") |> DataFrame.collect()
  StreamingQuery.stop(query)

  %{active_before_stop: active, result_count: length(result), sample: Enum.take(result, 3)}
end)

# =============================================
# 4. Cast edge cases
# =============================================

run.("4a. cast string to date/timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"date_str" => "2024-02-29", "ts_str" => "2024-02-29 13:45:30"}
  ], schema: "date_str STRING, ts_str STRING")
  df |> DataFrame.select([
    Column.cast(col("date_str"), "DATE") |> Column.alias_("date"),
    Column.cast(col("ts_str"), "TIMESTAMP") |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("4b. cast numeric overflow (INT to BYTE)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 300}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "BYTE") |> Column.alias_("as_byte"),
    Column.try_cast(col("val"), "SHORT") |> Column.alias_("as_short")
  ]) |> DataFrame.collect()
end)

run.("4c. cast boolean to int and back", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"flag" => true}, %{"flag" => false}], schema: "flag BOOLEAN")
  df |> DataFrame.select([
    col("flag"),
    Column.cast(col("flag"), "INT") |> Column.alias_("as_int"),
    Column.cast(Column.cast(col("flag"), "INT"), "BOOLEAN") |> Column.alias_("round_trip")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Schema introspection
# =============================================

run.("5a. dtypes()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5, "active" => true}
  ], schema: "id INT, name STRING, score DOUBLE, active BOOLEAN")
  DataFrame.dtypes(df)
end)

run.("5b. columns()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.columns(df)
end)

run.("5c. print_schema nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"a" => "hello", "b" => 42}}
  ], schema: "id INT, nested STRUCT<a: STRING, b: INT>")
  DataFrame.print_schema(df)
end)

# =============================================
# 6. explain modes
# =============================================

run.("6a. explain(:simple)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.gt(col("val"), lit(5))) |> DataFrame.explain(:simple)
end)

run.("6b. explain(:cost)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.gt(col("val"), lit(5))) |> DataFrame.explain(:cost)
end)

# =============================================
# 7. Catalog operations
# =============================================

run.("7a. list_tables in dev_warehouse", fn ->
  {:ok, tables} = Catalog.list_tables(s, "sfx_dev_warehouse")
  Enum.map(tables, fn t -> t.name end) |> Enum.take(10)
end)

run.("7b. get_table on existing table", fn ->
  {:ok, tables} = Catalog.list_tables(s, "sfx_dev_warehouse")
  case tables do
    [first | _] ->
      Catalog.get_table(s, "sfx.sfx_dev_warehouse.#{first.name}")
    [] -> :no_tables
  end
end)

run.("7c. list_columns on existing table", fn ->
  {:ok, tables} = Catalog.list_tables(s, "sfx_dev_warehouse")
  case tables do
    [first | _] ->
      Catalog.list_columns(s, "sfx.sfx_dev_warehouse.#{first.name}")
    [] -> :no_tables
  end
end)

# =============================================
# 8. Show / tree_string / html_string
# =============================================

run.("8a. show", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

run.("8b. tree_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.tree_string(df)
end)

run.("8c. html_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, html} = DataFrame.html_string(df)
  String.slice(html, 0..100)
end)

# =============================================
# 9. greatest/least and null functions
# =============================================

run.("9a. greatest / least with many columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 5, "b" => 3, "c" => 8, "d" => 1, "e" => 6}
  ], schema: "a INT, b INT, c INT, d INT, e INT")
  df |> DataFrame.select([
    greatest([col("a"), col("b"), col("c"), col("d"), col("e")]) |> Column.alias_("max"),
    least([col("a"), col("b"), col("c"), col("d"), col("e")]) |> Column.alias_("min")
  ]) |> DataFrame.collect()
end)

run.("9b. nullif / ifnull / nvl / nvl2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 10}, %{"a" => 10, "b" => 20}, %{"a" => nil, "b" => 30}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    nullif(col("a"), col("b")) |> Column.alias_("nullif_ab"),
    ifnull(col("a"), col("b")) |> Column.alias_("ifnull_ab"),
    nvl(col("a"), col("b")) |> Column.alias_("nvl_ab"),
    nvl2(col("a"), lit(999), col("b")) |> Column.alias_("nvl2_ab")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Regex functions
# =============================================

run.("10a. regexp_extract", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "Order #12345 placed on 2024-01-15"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    regexp_extract(col("txt"), lit("#(\\d+)"), lit(1)) |> Column.alias_("order_num"),
    regexp_extract(col("txt"), lit("(\\d{4}-\\d{2}-\\d{2})"), lit(1)) |> Column.alias_("date")
  ]) |> DataFrame.collect()
end)

run.("10b. lpad / rpad / repeat / reverse", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hello"}], schema: "txt STRING")
  df |> DataFrame.select([
    lpad(col("txt"), 10, "*") |> Column.alias_("lpadded"),
    rpad(col("txt"), 10, "-") |> Column.alias_("rpadded"),
    repeat(col("txt"), 3) |> Column.alias_("repeated"),
    reverse(col("txt")) |> Column.alias_("reversed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. DataFrame.na sub-ops
# =============================================

run.("11a. na.fill", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil}, %{"a" => nil, "b" => 2}, %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.na(df) |> SparkEx.DataFrame.NA.fill(%{"a" => 0, "b" => -1}) |> DataFrame.collect()
end)

run.("11b. na.drop (any)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 2}, %{"a" => 1, "b" => nil},
    %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.na(df) |> SparkEx.DataFrame.NA.drop("any") |> DataFrame.collect()
end)

run.("11c. na.drop (all)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 2}, %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.na(df) |> SparkEx.DataFrame.NA.drop("all") |> DataFrame.collect()
end)

# =============================================
# 12. DataFrame.Stat sub-ops
# =============================================

run.("12a. Stat.describe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  DataFrame.Stat.describe(df) |> DataFrame.collect()
end)

run.("12b. Stat.summary with selected stats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  DataFrame.Stat.summary(df, ["count", "min", "50%", "max"]) |> DataFrame.collect()
end)

# =============================================
# 13. Complex: recursive CTE via SQL
# =============================================

run.("13. recursive CTE (Fibonacci via SQL)", fn ->
  SparkEx.sql(s, """
    WITH RECURSIVE fib(n, a, b) AS (
      SELECT 1, 0, 1
      UNION ALL
      SELECT n + 1, b, a + b FROM fib WHERE n < 10
    )
    SELECT n, a AS fib_value FROM fib ORDER BY n
  """) |> DataFrame.collect()
end)

# =============================================
# 14. DataFrame.head / first / take / tail
# =============================================

run.("14a. head / first / take", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  head = DataFrame.head(df, 3)
  first = DataFrame.first(df)
  take = DataFrame.take(df, 3)
  {head, first, take}
end)

run.("14b. tail", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.tail(df, 3)
end)

# =============================================
# 15. DataFrame.same_semantics / semantic_hash
# =============================================

run.("15. same_semantics / semantic_hash", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  same = DataFrame.same_semantics(df1, df2)
  h1 = DataFrame.semantic_hash(df1)
  h2 = DataFrame.semantic_hash(df2)
  {same, h1, h2}
end)

IO.puts("=== Round 34b complete ===")
