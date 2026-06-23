# Round 34: Streaming advanced patterns, DataFrame.observe metrics, Catalog edge cases,
# window session/tumbling, clone_session, config operations, complex type coercions
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
# 1. Streaming with windowed aggregation
# =============================================

run.("1. streaming rate source → windowed aggregation → memory sink", fn ->
  stream = StreamReader.rate(s, 10)
  |> DataFrame.with_column("value_mod", Column.mod(col("value"), lit(5)))

  query = stream
  |> DataFrame.group_by([col("value_mod")])
  |> GroupedData.agg([count(col("value")) |> Column.alias_("cnt")])
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("windowed_stream_#{run_id}")
  |> StreamWriter.output_mode("complete")
  |> StreamWriter.trigger(processing_time: "2 seconds")
  |> StreamWriter.start()

  Process.sleep(5000)

  active = StreamingQuery.is_active?(query)
  status = StreamingQuery.status(query)

  # Read from memory sink
  {:ok, result} = SparkEx.Reader.table(s, "windowed_stream_#{run_id}") |> DataFrame.collect()

  StreamingQuery.stop(query)
  %{active_before_stop: active, status: status, result_count: length(result), sample: Enum.take(result, 3)}
end)

# =============================================
# 2. Streaming with watermark (for late data handling)
# =============================================

run.("2. streaming with watermark", fn ->
  stream = StreamReader.rate(s, 5)
  |> DataFrame.with_watermark("timestamp", "10 seconds")

  query = stream
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("watermark_stream_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.trigger(processing_time: "1 second")
  |> StreamWriter.start()

  Process.sleep(3000)
  status = StreamingQuery.status(query)
  StreamingQuery.stop(query)
  status
end)

# =============================================
# 3. clone_session
# =============================================

run.("3. clone_session and independence", fn ->
  {:ok, s2} = SparkEx.clone_session(s)

  # Create temp view in original session
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.create_or_replace_temp_view(df, "clone_test_#{run_id}")

  # Try to read from cloned session
  try do
    SparkEx.Reader.table(s2, "clone_test_#{run_id}") |> DataFrame.collect()
  rescue
    e -> {:clone_cant_see_original, Exception.message(e)}
  after
    DataFrame.drop_temp_view(s, "clone_test_#{run_id}")
    SparkEx.Session.release(s2)
  end
end)

# =============================================
# 4. Config operations
# =============================================

run.("4a. config set and get", fn ->
  SparkEx.Session.config_set(s, [{"spark.sql.shuffle.partitions", "10"}])
  result = SparkEx.Session.config_get(s, ["spark.sql.shuffle.partitions"])
  # Reset
  SparkEx.Session.config_set(s, [{"spark.sql.shuffle.partitions", "200"}])
  result
end)

run.("4b. config_get_all", fn ->
  {:ok, configs} = SparkEx.Session.config_get_all(s)
  configs
  |> Enum.filter(fn {k, _} -> String.contains?(k, "shuffle.partitions") end)
end)

run.("4c. config_is_modifiable", fn ->
  SparkEx.Session.config_is_modifiable(s, "spark.sql.shuffle.partitions")
end)

run.("4d. config unset", fn ->
  SparkEx.Session.config_set(s, [{"spark.sql.testCustomConfig", "hello"}])
  before = SparkEx.Session.config_get(s, ["spark.sql.testCustomConfig"])
  SparkEx.Session.config_unset(s, ["spark.sql.testCustomConfig"])
  after_ = SparkEx.Session.config_get(s, ["spark.sql.testCustomConfig"])
  {before, after_}
end)

# =============================================
# 5. Tags lifecycle
# =============================================

run.("5. tags: add, get, remove, clear", fn ->
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
# 6. Complex type coercion: cast edge cases
# =============================================

run.("6a. cast string to date/timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"date_str" => "2024-02-29", "ts_str" => "2024-02-29 13:45:30"}
  ], schema: "date_str STRING, ts_str STRING")
  df |> DataFrame.select([
    Column.cast(col("date_str"), "DATE") |> Column.alias_("date"),
    Column.cast(col("ts_str"), "TIMESTAMP") |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("6b. cast numeric overflow (INT to BYTE)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 300}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "BYTE") |> Column.alias_("as_byte"),
    Column.try_cast(col("val"), "SHORT") |> Column.alias_("as_short")
  ]) |> DataFrame.collect()
end)

run.("6c. cast boolean to int and back", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"flag" => true}, %{"flag" => false}], schema: "flag BOOLEAN")
  df |> DataFrame.select([
    col("flag"),
    Column.cast(col("flag"), "INT") |> Column.alias_("as_int"),
    Column.cast(Column.cast(col("flag"), "INT"), "BOOLEAN") |> Column.alias_("round_trip")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.schema in various formats
# =============================================

run.("7a. schema() proto", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"a" => "hello", "b" => 42}}
  ], schema: "id INT, nested STRUCT<a: STRING, b: INT>")
  DataFrame.schema(df)
end)

run.("7b. print_schema()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"a" => "hello", "b" => 42}}
  ], schema: "id INT, nested STRUCT<a: STRING, b: INT>")
  DataFrame.print_schema(df)
end)

run.("7c. dtypes()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.dtypes(df)
end)

run.("7d. columns()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.columns(df)
end)

# =============================================
# 8. DataFrame.explain with different modes
# =============================================

run.("8a. explain(:simple)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end), schema: "id INT, val INT")
  df |> DataFrame.filter(Column.gt(col("val"), lit(50)))
  |> DataFrame.explain(:simple)
end)

run.("8b. explain(:extended)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end), schema: "id INT, val INT")
  df |> DataFrame.filter(Column.gt(col("val"), lit(50)))
  |> DataFrame.explain(:extended)
end)

run.("8c. explain(:formatted)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.explain(:formatted)
end)

# =============================================
# 9. Catalog.list_columns on a real table
# =============================================

run.("9. list_tables in dev_warehouse", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse")
end)

# =============================================
# 10. Complex expressions: greatest/least with many args
# =============================================

run.("10a. greatest / least with 5 columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 5, "b" => 3, "c" => 8, "d" => 1, "e" => 6}
  ], schema: "a INT, b INT, c INT, d INT, e INT")
  df |> DataFrame.select([
    greatest([col("a"), col("b"), col("c"), col("d"), col("e")]) |> Column.alias_("max_val"),
    least([col("a"), col("b"), col("c"), col("d"), col("e")]) |> Column.alias_("min_val")
  ]) |> DataFrame.collect()
end)

run.("10b. nullif / ifnull / nvl / nvl2", fn ->
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
# 11. DataFrame.show (string output)
# =============================================

run.("11. show (prints table)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

# =============================================
# 12. DataFrame.tree_string / html_string
# =============================================

run.("12a. tree_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "test"}], schema: "id INT, val STRING")
  DataFrame.tree_string(df)
end)

run.("12b. html_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, html} = DataFrame.html_string(df)
  String.slice(html, 0..200)
end)

# =============================================
# 13. Catalog.get_table
# =============================================

run.("13. get_table metadata", fn ->
  # First get a table name from list_tables
  {:ok, tables} = Catalog.list_tables(s, "sfx_dev_warehouse")
  case tables do
    [first | _] ->
      table_name = "sfx.sfx_dev_warehouse.#{first.name}"
      Catalog.get_table(s, table_name)
    [] -> :no_tables
  end
end)

# =============================================
# 14. Regex functions
# =============================================

run.("14a. regexp_extract", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "Order #12345 placed on 2024-01-15"},
    %{"txt" => "Order #67890 placed on 2024-02-20"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    regexp_extract(col("txt"), lit("#(\\d+)"), lit(1)) |> Column.alias_("order_num"),
    regexp_extract(col("txt"), lit("(\\d{4}-\\d{2}-\\d{2})"), lit(1)) |> Column.alias_("date")
  ]) |> DataFrame.collect()
end)

run.("14b. regexp_replace with lit wrapping", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "Hello 123 World 456"}], schema: "txt STRING")
  df |> DataFrame.select([
    regexp_replace(col("txt"), lit("\\d+"), lit("NUM")) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. String padding / repeat / reverse
# =============================================

run.("15. lpad / rpad / repeat / reverse", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hello"}], schema: "txt STRING")
  df |> DataFrame.select([
    lpad(col("txt"), 10, "*") |> Column.alias_("lpadded"),
    rpad(col("txt"), 10, "-") |> Column.alias_("rpadded"),
    repeat(col("txt"), 3) |> Column.alias_("repeated"),
    reverse(col("txt")) |> Column.alias_("reversed")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 34 complete ===")
