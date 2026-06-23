# Round 36: Untested APIs — collect_as_map, foreach, grouping_sets, parse,
# to_json_rows, rdd_num_partitions, count, transform, observation, to_local_iterator,
# execution_info, hint, assert_true, sequence, json_tuple, locate, overlay, sentences
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, Observation}
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
# 1. DataFrame.collect_as_map
# =============================================

run.("1a. collect_as_map (happy path, 2 columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "value" => 1},
    %{"key" => "b", "value" => 2},
    %{"key" => "c", "value" => 3}
  ], schema: "key STRING, value INT")
  DataFrame.collect_as_map(df)
end)

run.("1b. collect_as_map (error: 3 columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  DataFrame.collect_as_map(df)
end)

run.("1c. collect_as_map (error: 1 column)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1}
  ], schema: "a INT")
  DataFrame.collect_as_map(df)
end)

run.("1d. collect_as_map with duplicate keys (last wins?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "x", "value" => 1},
    %{"key" => "x", "value" => 2},
    %{"key" => "y", "value" => 3}
  ], schema: "key STRING, value INT")
  DataFrame.collect_as_map(df)
end)

# =============================================
# 2. DataFrame.foreach / foreach_partition
# =============================================

run.("2a. foreach (apply function to each row)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")
  acc = :ets.new(:test_foreach, [:set, :public])
  :ets.insert(acc, {:count, 0})
  result = DataFrame.foreach_local(df, fn row ->
    [{:count, n}] = :ets.lookup(acc, :count)
    :ets.insert(acc, {:count, n + 1})
  end)
  [{:count, final}] = :ets.lookup(acc, :count)
  :ets.delete(acc)
  {result, final}
end)

run.("2b. foreach_partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")
  acc = :ets.new(:test_fp, [:set, :public])
  :ets.insert(acc, {:rows, []})
  result = DataFrame.foreach_partition_local(df, fn rows ->
    :ets.insert(acc, {:rows, rows})
  end)
  [{:rows, stored}] = :ets.lookup(acc, :rows)
  :ets.delete(acc)
  {result, length(stored)}
end)

# =============================================
# 3. DataFrame.count
# =============================================

run.("3. DataFrame.count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..25, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.count(df)
end)

# =============================================
# 4. DataFrame.rdd_num_partitions
# =============================================

run.("4. rdd_num_partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.rdd_num_partitions_approx(df)
end)

# =============================================
# 5. DataFrame.to_json_rows
# =============================================

run.("5. to_json_rows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30},
    %{"name" => "Bob", "age" => 25}
  ], schema: "name STRING, age INT")
  df |> DataFrame.to_json_rows() |> DataFrame.collect()
end)

# =============================================
# 6. DataFrame.parse (:csv and :json)
# =============================================

run.("6a. parse CSV string column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"raw" => "1,Alice"},
    %{"raw" => "2,Bob"}
  ], schema: "raw STRING")
  df |> DataFrame.select([col("raw") |> Column.alias_("value")])
  |> DataFrame.parse(:csv, "id INT, name STRING")
  |> DataFrame.collect()
end)

run.("6b. parse JSON string column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"raw" => ~s({"id": 1, "name": "Alice"})},
    %{"raw" => ~s({"id": 2, "name": "Bob"})}
  ], schema: "raw STRING")
  df |> DataFrame.select([col("raw") |> Column.alias_("value")])
  |> DataFrame.parse(:json, "id INT, name STRING")
  |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.grouping_sets
# =============================================

run.("7. grouping_sets", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"region" => "North", "product" => "A", "sales" => 100},
    %{"region" => "North", "product" => "B", "sales" => 200},
    %{"region" => "South", "product" => "A", "sales" => 150},
    %{"region" => "South", "product" => "B", "sales" => 250}
  ], schema: "region STRING, product STRING, sales INT")

  df
  |> DataFrame.grouping_sets([
    [col("region"), col("product")],
    [col("region")],
    []
  ])
  |> GroupedData.agg([
    sum(col("sales")) |> Column.alias_("total_sales")
  ])
  |> DataFrame.sort([asc(col("region")), asc(col("product"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.transform
# =============================================

run.("8a. transform (valid)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 10}, %{"val" => 20}
  ], schema: "val INT")

  df
  |> DataFrame.transform(fn d ->
    d |> DataFrame.with_column("doubled", Column.multiply(col("val"), lit(2)))
  end)
  |> DataFrame.collect()
end)

run.("8b. transform (returns non-DataFrame → should raise)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 1}], schema: "val INT")
  DataFrame.transform(df, fn _d -> :not_a_dataframe end)
end)

# =============================================
# 9. DataFrame.to_local_iterator
# =============================================

run.("9. to_local_iterator (lazy stream)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, stream} = DataFrame.to_local_iterator(df)
  rows = Enum.to_list(stream)
  length(rows)
end)

# =============================================
# 10. DataFrame.hint
# =============================================

run.("10a. hint broadcast", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 100}, %{"id" => 2, "score" => 200}
  ], schema: "id INT, score INT")

  hinted = DataFrame.hint(df2, "broadcast")
  DataFrame.join(df1, hinted, [col("id")], :inner) |> DataFrame.collect()
end)

run.("10b. hint coalesce", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.hint("coalesce", [2]) |> DataFrame.rdd_num_partitions_approx()
end)

# =============================================
# 11. Observation
# =============================================

run.("11a. Observation basic", fn ->
  obs = Observation.new("test_obs_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  observed = DataFrame.observe(df, obs, [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total"),
    avg(col("val")) |> Column.alias_("average")
  ])

  {:ok, _rows} = DataFrame.collect(observed)
  metrics = Observation.get(obs)
  metrics
end)

run.("11b. Observation with auto-generated name", fn ->
  obs = Observation.new()
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}, %{"x" => 3}
  ], schema: "x INT")

  observed = DataFrame.observe(df, obs, [
    count(col("x")) |> Column.alias_("row_count")
  ])
  {:ok, _} = DataFrame.collect(observed)
  Observation.get(obs)
end)

run.("11c. Observation.get before observe (should raise)", fn ->
  obs = Observation.new("never_observed_#{run_id}")
  Observation.get(obs)
end)

# =============================================
# 12. Functions: assert_true
# =============================================

run.("12a. assert_true (passing)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 5}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    assert_true(Column.gt(col("val"), lit(0)))
  ]) |> DataFrame.collect()
end)

run.("12b. assert_true (failing → should error)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -1}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    assert_true(Column.gt(col("val"), lit(0)))
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Functions: sequence
# =============================================

run.("13. sequence (generate array)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"start" => 1, "stop" => 5}
  ], schema: "start INT, stop INT")
  df |> DataFrame.select([
    sequence(col("start"), col("stop"), lit(1)) |> Column.alias_("seq")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Functions: json_tuple
# =============================================

run.("14. json_tuple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ~s({"name": "Alice", "age": 30, "city": "NYC"})}
  ], schema: "json STRING")
  df |> DataFrame.select([
    json_tuple(col("json"), [lit("name"), lit("age"), lit("city")])
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Functions: locate
# =============================================

run.("15. locate (find substring position)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world hello"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    locate(col("txt"), "world") |> Column.alias_("pos"),
    locate(col("txt"), "hello", 2) |> Column.alias_("pos2")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Functions: overlay
# =============================================

run.("16. overlay", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "ABCDEFGH"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    overlay(col("txt"), lit("XYZ"), lit(3), lit(2)) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. Functions: sentences
# =============================================

run.("17. sentences", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "Hello world. How are you? I am fine."}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    sentences(col("txt"), lit("en"), lit("US")) |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. Functions: array_join
# =============================================

run.("18. array_join with null replacement", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [4, nil, 6]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    array_join(col("arr"), ", ", "NULL") |> Column.alias_("joined")
  ]) |> DataFrame.collect()
end)

# =============================================
# 19. Functions: split with limit
# =============================================

run.("19. split with limit", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "a-b-c-d-e"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    split(col("txt"), "-", 3) |> Column.alias_("split3"),
    split(col("txt"), "-") |> Column.alias_("split_all")
  ]) |> DataFrame.collect()
end)

# =============================================
# 20. Functions: replace (3-arg string replace)
# =============================================

run.("20. Functions.replace (not DataFrame.replace)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world hello"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    replace(col("txt"), lit("hello"), lit("hi")) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

# =============================================
# 21. Functions: ltrim/rtrim/trim with custom chars
# =============================================

run.("21. trim with custom characters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "***hello***"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    ltrim(col("txt"), "*") |> Column.alias_("ltrimmed"),
    rtrim(col("txt"), "*") |> Column.alias_("rtrimmed"),
    trim(col("txt"), "*") |> Column.alias_("trimmed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 22. Functions: convert_timezone
# =============================================

run.("22. convert_timezone", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts" => "2024-06-15 10:30:00"}
  ], schema: "ts STRING")
  df |> DataFrame.select([
    convert_timezone(lit("UTC"), lit("America/New_York"),
      Column.cast(col("ts"), "TIMESTAMP")) |> Column.alias_("ny_time")
  ]) |> DataFrame.collect()
end)

# =============================================
# 23. Functions: make_timestamp_ntz
# =============================================

run.("23. make_timestamp_ntz", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "m" => 6, "d" => 15, "h" => 10, "min" => 30, "sec" => 45}
  ], schema: "y INT, m INT, d INT, h INT, min INT, sec INT")
  df |> DataFrame.select([
    make_timestamp_ntz(
      years: col("y"), months: col("m"), days: col("d"),
      hours: col("h"), mins: col("min"), secs: col("sec")
    ) |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

# =============================================
# 24. Functions: make_interval
# =============================================

run.("24. make_interval", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"days_val" => 5, "hours_val" => 3}
  ], schema: "days_val INT, hours_val INT")
  df |> DataFrame.select([
    make_interval(days: col("days_val"), hours: col("hours_val")) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

# =============================================
# 25. Functions: from_xml / schema_of_xml
# =============================================

run.("25a. schema_of_xml", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"xml" => "<root><name>Alice</name><age>30</age></root>"}
  ], schema: "xml STRING")
  schema_of_xml(col("xml"))
end)

run.("25b. from_xml", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"xml" => "<root><name>Alice</name><age>30</age></root>"}
  ], schema: "xml STRING")
  df |> DataFrame.select([
    from_xml(col("xml"), lit("name STRING, age INT")) |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 26. Functions: uuid
# =============================================

run.("26. uuid generation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  df |> DataFrame.select([
    col("id"),
    uuid() |> Column.alias_("uuid")
  ]) |> DataFrame.collect()
end)

# =============================================
# 27. Functions: uniform / randstr
# =============================================

run.("27a. uniform (random number generation)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"lo" => 1, "hi" => 100}
  ], schema: "lo INT, hi INT")
  df |> DataFrame.select([
    uniform(col("lo"), col("hi"), lit(42)) |> Column.alias_("rnd")
  ]) |> DataFrame.collect()
end)

run.("27b. randstr (random string)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"len" => 10}
  ], schema: "len INT")
  df |> DataFrame.select([
    randstr(col("len"), lit(42)) |> Column.alias_("rstr")
  ]) |> DataFrame.collect()
end)

# =============================================
# 28. Functions: approx_count_distinct
# =============================================

run.("28. approx_count_distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => rem(i, 50)} end), schema: "val INT")
  df |> DataFrame.select([
    approx_count_distinct(col("val")) |> Column.alias_("approx_distinct")
  ]) |> DataFrame.collect()
end)

# =============================================
# 29. Functions: any_value / mode
# =============================================

run.("29a. any_value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 1},
    %{"grp" => "a", "val" => 2},
    %{"grp" => "b", "val" => 3}
  ], schema: "grp STRING, val INT")
  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([any_value(col("val")) |> Column.alias_("any")])
  |> DataFrame.collect()
end)

run.("29b. mode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 2},
    %{"val" => 3}, %{"val" => 3}, %{"val" => 3}
  ], schema: "val INT")
  df |> DataFrame.select([
    mode(col("val")) |> Column.alias_("mode_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 30. DataFrame.execution_info
# =============================================

run.("30. execution_info", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, _} = DataFrame.collect(df)
  DataFrame.execution_info(df)
end)

# =============================================
# 31. Functions: parse_url
# =============================================

run.("31. parse_url", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"url" => "https://example.com:8080/path?key=value#fragment"}
  ], schema: "url STRING")
  df |> DataFrame.select([
    parse_url(col("url"), lit("HOST")) |> Column.alias_("host"),
    parse_url(col("url"), lit("PORT")) |> Column.alias_("port"),
    parse_url(col("url"), lit("PATH")) |> Column.alias_("path"),
    parse_url(col("url"), lit("QUERY")) |> Column.alias_("query")
  ]) |> DataFrame.collect()
end)

# =============================================
# 32. Functions: months_between
# =============================================

run.("32. months_between", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"d1" => "2024-01-15", "d2" => "2024-06-20"}
  ], schema: "d1 STRING, d2 STRING")
  df |> DataFrame.select([
    months_between(
      Column.cast(col("d2"), "DATE"),
      Column.cast(col("d1"), "DATE"),
      false
    ) |> Column.alias_("months_diff")
  ]) |> DataFrame.collect()
end)

# =============================================
# 33. Functions: levenshtein with threshold (Elixir form)
# =============================================

run.("33. levenshtein with threshold", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "kitten", "b" => "sitting"}
  ], schema: "a STRING, b STRING")
  df |> DataFrame.select([
    levenshtein(col("a"), col("b")) |> Column.alias_("dist"),
    levenshtein(col("a"), col("b"), 3) |> Column.alias_("dist_thresh3")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 36 complete ===")
