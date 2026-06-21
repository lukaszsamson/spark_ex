# Round 36b: Repro trim bug, fix test errors from 36, more untested APIs
alias SparkEx.{DataFrame, Column, GroupedData, Observation}
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
# 1. REPRO: ltrim/rtrim/trim with custom chars BUG
# =============================================

run.("1a. ltrim with * (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "***hello***"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    ltrim(col("txt"), "*") |> Column.alias_("ltrimmed")
  ]) |> DataFrame.collect()
end)

run.("1b. SQL equivalent of ltrim with *", fn ->
  SparkEx.sql(s, "SELECT ltrim('*', '***hello***') AS ltrimmed") |> DataFrame.collect()
end)

run.("1c. rtrim with * (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "***hello***"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    rtrim(col("txt"), "*") |> Column.alias_("rtrimmed")
  ]) |> DataFrame.collect()
end)

run.("1d. SQL equivalent of rtrim with *", fn ->
  SparkEx.sql(s, "SELECT rtrim('*', '***hello***') AS rtrimmed") |> DataFrame.collect()
end)

run.("1e. trim with * (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "***hello***"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    trim(col("txt"), "*") |> Column.alias_("trimmed")
  ]) |> DataFrame.collect()
end)

run.("1f. SQL equivalent of trim with *", fn ->
  SparkEx.sql(s, "SELECT trim('*' FROM '***hello***') AS trimmed") |> DataFrame.collect()
end)

run.("1g. ltrim with spaces (default, no trim char)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "   hello   "}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    ltrim(col("txt")) |> Column.alias_("ltrimmed")
  ]) |> DataFrame.collect()
end)

# Check: is the argument order wrong? Spark SQL ltrim(trimChar, string) or ltrim(string, trimChar)?
run.("1h. SQL: ltrim with BOTH arg orderings", fn ->
  SparkEx.sql(s, """
    SELECT
      ltrim('*', '***hello***') AS trim_char_first,
      ltrim('***hello***', '*') AS string_first
  """) |> DataFrame.collect()
end)

# =============================================
# 2. Fixed: locate (correct arg order: substr, col, pos)
# =============================================

run.("2. locate (correct arg order)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world hello"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    locate("world", col("txt")) |> Column.alias_("pos"),
    locate("hello", col("txt"), 2) |> Column.alias_("pos2")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Fixed: make_timestamp_ntz (takes list, not keyword)
# =============================================

run.("3. make_timestamp_ntz (list of columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "m" => 6, "d" => 15, "h" => 10, "mi" => 30, "sc" => 45}
  ], schema: "y INT, m INT, d INT, h INT, mi INT, sc INT")
  df |> DataFrame.select([
    make_timestamp_ntz([col("y"), col("m"), col("d"), col("h"), col("mi"), col("sc")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Fixed: from_xml (schema is plain string, not lit())
# =============================================

run.("4. from_xml (string schema)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"xml" => "<root><name>Alice</name><age>30</age></root>"}
  ], schema: "xml STRING")
  df |> DataFrame.select([
    from_xml(col("xml"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Fixed: uniform (seed should be integer, not lit())
# =============================================

run.("5a. uniform with integer seed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"lo" => 1, "hi" => 100}
  ], schema: "lo INT, hi INT")
  df |> DataFrame.select([
    uniform(col("lo"), col("hi"), 42) |> Column.alias_("rnd")
  ]) |> DataFrame.collect()
end)

run.("5b. uniform with no seed (auto-generate)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"lo" => 1, "hi" => 100}
  ], schema: "lo INT, hi INT")
  df |> DataFrame.select([
    uniform(col("lo"), col("hi")) |> Column.alias_("rnd")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Observation with correct aliases
# =============================================

run.("6. Observation - check if aliases are preserved", fn ->
  obs = Observation.new("obs_alias_test_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  observed = DataFrame.observe(df, obs, [
    count(col("val")) |> Column.alias_("cnt"),
    avg(col("val")) |> Column.alias_("avg_val"),
    max(col("val")) |> Column.alias_("max_val")
  ])

  {:ok, rows} = DataFrame.collect(observed)
  metrics = Observation.get(obs)
  %{rows_count: length(rows), metrics: metrics}
end)

# =============================================
# 7. Fixed: hint broadcast (use different column names)
# =============================================

run.("7. hint broadcast with renamed columns", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 100}, %{"id" => 2, "score" => 200}
  ], schema: "id INT, score INT")

  hinted = DataFrame.hint(df2, "broadcast")
  DataFrame.join(df1, hinted, Column.eq(col("df1.id"), col("df2.id")), :inner) |> DataFrame.collect()
end)

# =============================================
# 8. to_xml
# =============================================

run.("8. to_xml", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30}
  ], schema: "name STRING, age INT")
  # to_xml expects a struct column
  df |> DataFrame.select([
    to_xml(struct([col("name"), col("age")])) |> Column.alias_("xml_str")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Functions: substr_
# =============================================

run.("9. substr_", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    substr_(col("txt"), 1, 5) |> Column.alias_("sub")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Functions: shuffle (array shuffle)
# =============================================

run.("10. shuffle", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    shuffle(col("arr")) |> Column.alias_("shuffled")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Functions: array_sort with comparator
# =============================================

run.("11. array_sort with custom comparator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [5, 3, 1, 4, 2]}
  ], schema: "arr ARRAY<INT>")
  # Descending sort via comparator
  df |> DataFrame.select([
    array_sort(col("arr"), fn left, right ->
      when_(Column.gt(left, right), lit(-1))
      |> Column.otherwise(
        when_(Column.lt(left, right), lit(1))
        |> Column.otherwise(lit(0)))
    end) |> Column.alias_("sorted_desc")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Functions: unix_timestamp
# =============================================

run.("12. unix_timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts_str" => "2024-06-15 10:30:00"}
  ], schema: "ts_str STRING")
  df |> DataFrame.select([
    unix_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss") |> Column.alias_("epoch")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Functions: from_unixtime
# =============================================

run.("13. from_unixtime", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"epoch" => 1718443800}
  ], schema: "epoch LONG")
  df |> DataFrame.select([
    from_unixtime(col("epoch"), "yyyy-MM-dd HH:mm:ss") |> Column.alias_("ts_str")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. DataFrame.collect_as_map with null keys
# =============================================

run.("14. collect_as_map with null key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "value" => 1},
    %{"key" => nil, "value" => 2}
  ], schema: "key STRING, value INT")
  DataFrame.collect_as_map(df)
end)

# =============================================
# 15. DataFrame.is_cached / cache / unpersist
# =============================================

run.("15a. cache then is_cached", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")
  cached = DataFrame.cache(df)
  # Force materialization
  {:ok, _} = DataFrame.collect(cached)
  is_cached = DataFrame.is_cached?(cached)
  DataFrame.unpersist(cached)
  is_cached
end)

# =============================================
# 16. DataFrame.parse with :json FAILFAST mode
# =============================================

run.("16. parse JSON with FAILFAST on malformed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"raw" => ~s({"id": 1, "name": "Alice"})},
    %{"raw" => "not valid json"}
  ], schema: "raw STRING")
  df |> DataFrame.select([col("raw") |> Column.alias_("value")])
  |> DataFrame.parse(:json, "id INT, name STRING", %{"mode" => "FAILFAST"})
  |> DataFrame.collect()
end)

# =============================================
# 17. DataFrame.parse with :csv custom delimiter
# =============================================

run.("17. parse CSV with pipe delimiter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"raw" => "1|Alice|30"},
    %{"raw" => "2|Bob|25"}
  ], schema: "raw STRING")
  df |> DataFrame.select([col("raw") |> Column.alias_("value")])
  |> DataFrame.parse(:csv, "id INT, name STRING, age INT", %{"sep" => "|"})
  |> DataFrame.collect()
end)

# =============================================
# 18. Column.transform (apply function to column)
# =============================================

run.("18. Column.transform on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    transform(col("arr"), fn x -> Column.multiply(x, lit(10)) end)
    |> Column.alias_("times10")
  ]) |> DataFrame.collect()
end)

# =============================================
# 19. DataFrame.grouping_sets with rollup-like behavior
# =============================================

run.("19. grouping_sets with grouping() function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "headcount" => 10},
    %{"dept" => "Eng", "team" => "B", "headcount" => 15},
    %{"dept" => "Sales", "team" => "A", "headcount" => 8},
    %{"dept" => "Sales", "team" => "B", "headcount" => 12}
  ], schema: "dept STRING, team STRING, headcount INT")

  df
  |> DataFrame.grouping_sets([
    [col("dept"), col("team")],
    [col("dept")],
    []
  ])
  |> GroupedData.agg([
    sum(col("headcount")) |> Column.alias_("total"),
    grouping(col("dept")) |> Column.alias_("gd"),
    grouping(col("team")) |> Column.alias_("gt")
  ])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

# =============================================
# 20. DataFrame.to_local_iterator with filter
# =============================================

run.("20. to_local_iterator with transformation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, stream} = df
    |> DataFrame.filter(Column.lte(col("id"), lit(10)))
    |> DataFrame.to_local_iterator()
  rows = Enum.to_list(stream)
  {length(rows), Enum.map(rows, & &1["id"]) |> Enum.sort()}
end)

IO.puts("=== Round 36b complete ===")
