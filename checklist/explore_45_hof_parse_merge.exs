# Round 24: Higher-order functions, parse/serialize, MergeIntoWriter, DataFrame analytics
alias SparkEx.{DataFrame, Column, Writer, MergeIntoWriter}
import SparkEx.Functions, except: [length: 1, abs: 1]

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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r24_#{run_id}"

# =============================================
# 1. Higher-order functions with lambdas
# =============================================

run.("1a. filter (array HOF) with 1-arity fn", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5, 6]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    filter(col("arr"), fn x -> Column.gt(x, lit(3)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

run.("1b. filter (array HOF) with 2-arity fn (value + index)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    filter(col("arr"), fn _x, i -> Column.gt(i, lit(2)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

run.("1c. exists (array HOF)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    col("arr"),
    exists(col("arr"), fn x -> Column.gt(x, lit(5)) end) |> Column.alias_("has_gt5")
  ]) |> DataFrame.collect()
end)

run.("1d. forall (array HOF)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [2, 4, 6]},
    %{"arr" => [1, 2, 3]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    col("arr"),
    forall(col("arr"), fn x -> Column.eq(Column.mod(x, lit(2)), lit(0)) end) |> Column.alias_("all_even")
  ]) |> DataFrame.collect()
end)

run.("1e. transform with 2-arity fn (value + index)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    transform(col("arr"), fn x, i -> Column.plus(x, i) end) |> Column.alias_("indexed")
  ]) |> DataFrame.collect()
end)

run.("1f. reduce (alias for aggregate)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    reduce(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("sum")
  ]) |> DataFrame.collect()
end)

run.("1g. reduce with finish function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    reduce(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end,
      fn total -> Column.multiply(total, lit(2)) end) |> Column.alias_("sum_x2")
  ]) |> DataFrame.collect()
end)

run.("1h. map_filter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2, "c" => 3}}
  ], schema: "m MAP<STRING, INT>")

  df |> DataFrame.select([
    map_filter(col("m"), fn _k, v -> Column.gt(v, lit(1)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

run.("1i. transform_keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"hello" => 1, "world" => 2}}
  ], schema: "m MAP<STRING, INT>")

  df |> DataFrame.select([
    transform_keys(col("m"), fn k, _v -> upper(k) end) |> Column.alias_("upper_keys")
  ]) |> DataFrame.collect()
end)

run.("1j. transform_values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2}}
  ], schema: "m MAP<STRING, INT>")

  df |> DataFrame.select([
    transform_values(col("m"), fn _k, v -> Column.multiply(v, lit(10)) end) |> Column.alias_("scaled")
  ]) |> DataFrame.collect()
end)

run.("1k. zip_with (two arrays)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [10, 20, 30]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")

  df |> DataFrame.select([
    zip_with(col("a"), col("b"), fn x, y -> Column.plus(x, y) end) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

run.("1l. map_zip_with (two maps)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m1" => %{"a" => 1, "b" => 2}, "m2" => %{"a" => 10, "b" => 20}}
  ], schema: "m1 MAP<STRING, INT>, m2 MAP<STRING, INT>")

  df |> DataFrame.select([
    map_zip_with(col("m1"), col("m2"), fn _k, v1, v2 -> Column.plus(v1, v2) end)
    |> Column.alias_("merged")
  ]) |> DataFrame.collect()
end)

run.("1m. array_sort with custom comparator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [3, 1, 4, 1, 5, 9]}
  ], schema: "arr ARRAY<INT>")

  # Sort descending: if a > b return -1, if a < b return 1, else 0
  df |> DataFrame.select([
    array_sort(col("arr"), fn a, b ->
      when_(Column.gt(a, b), lit(-1))
      |> Column.when_(Column.lt(a, b), lit(1))
      |> Column.otherwise(lit(0))
    end) |> Column.alias_("desc_sorted")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Parse/serialize functions
# =============================================

run.("2a. from_json with schema string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => ~s({"name":"Alice","age":30})}
  ], schema: "json_str STRING")

  df |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("2b. from_json with options", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => ~s({"name":"Alice","age":30})}
  ], schema: "json_str STRING")

  df |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT", %{"mode" => "PERMISSIVE"})
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("2c. from_csv with schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"csv_str" => "Alice,30"}
  ], schema: "csv_str STRING")

  df |> DataFrame.select([
    from_csv(col("csv_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("2d. from_csv with options", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"csv_str" => "Alice|30"}
  ], schema: "csv_str STRING")

  df |> DataFrame.select([
    from_csv(col("csv_str"), "name STRING, age INT", %{"sep" => "|"})
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("2e. to_csv (default)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30}}
  ], schema: "info STRUCT<name: STRING, age: INT>")

  df |> DataFrame.select([
    to_csv(col("info")) |> Column.alias_("csv")
  ]) |> DataFrame.collect()
end)

run.("2f. to_csv with options", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30}}
  ], schema: "info STRUCT<name: STRING, age: INT>")

  df |> DataFrame.select([
    to_csv(col("info"), %{"sep" => "|"}) |> Column.alias_("csv")
  ]) |> DataFrame.collect()
end)

run.("2g. schema_of_csv", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dummy" => 1}], schema: "dummy INT")

  df |> DataFrame.select([
    schema_of_csv(lit("Alice,30,true")) |> Column.alias_("schema")
  ]) |> DataFrame.collect()
end)

run.("2h. from_xml with schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"xml_str" => "<row><name>Alice</name><age>30</age></row>"}
  ], schema: "xml_str STRING")

  df |> DataFrame.select([
    from_xml(col("xml_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("2i. to_xml (default)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "age" => 30}}
  ], schema: "info STRUCT<name: STRING, age: INT>")

  df |> DataFrame.select([
    to_xml(col("info")) |> Column.alias_("xml")
  ]) |> DataFrame.collect()
end)

run.("2j. schema_of_xml", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dummy" => 1}], schema: "dummy INT")

  df |> DataFrame.select([
    schema_of_xml(lit("<row><name>Alice</name><age>30</age></row>")) |> Column.alias_("schema")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Timestamp construction functions
# =============================================

run.("3a. make_timestamp with column list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"yr" => 2025, "mo" => 6, "dy" => 15, "hr" => 10, "mn" => 30, "sc" => 45}
  ], schema: "yr INT, mo INT, dy INT, hr INT, mn INT, sc INT")

  df |> DataFrame.select([
    make_timestamp([col("yr"), col("mo"), col("dy"), col("hr"), col("mn"), col("sc")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("3b. make_timestamp_ntz with column list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"yr" => 2025, "mo" => 6, "dy" => 15, "hr" => 10, "mn" => 30, "sc" => 45}
  ], schema: "yr INT, mo INT, dy INT, hr INT, mn INT, sc INT")

  df |> DataFrame.select([
    make_timestamp_ntz([col("yr"), col("mo"), col("dy"), col("hr"), col("mn"), col("sc")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("3c. try_make_timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"yr" => 2025, "mo" => 13, "dy" => 32, "hr" => 10, "mn" => 30, "sc" => 45}
  ], schema: "yr INT, mo INT, dy INT, hr INT, mn INT, sc INT")

  df |> DataFrame.select([
    try_make_timestamp([col("yr"), col("mo"), col("dy"), col("hr"), col("mn"), col("sc")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("3d. try_make_interval", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dummy" => 1}], schema: "dummy INT")

  df |> DataFrame.select([
    try_make_interval(days: 5, hours: 3) |> Column.alias_("interval")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. try_* arithmetic functions
# =============================================

run.("4a. try_divide (normal)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 3}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    try_divide(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4b. try_divide by zero (should return null)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 0}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    try_divide(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4c. try_multiply overflow", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2147483647, "b" => 2}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    try_multiply(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4d. try_add overflow", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2147483647, "b" => 1}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    try_add(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4e. try_subtract underflow", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => -2147483648, "b" => 1}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    try_subtract(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4f. try_avg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => nil}, %{"val" => nil}
  ], schema: "val INT")

  df |> DataFrame.select([
    try_avg(col("val")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4g. try_sum", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 2147483647}, %{"val" => 2147483647}
  ], schema: "val INT")

  df |> DataFrame.select([
    try_sum(col("val")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame analytics
# =============================================

run.("5a. same_semantics (identical plans)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  DataFrame.same_semantics(df1, df2)
end)

run.("5b. semantic_hash", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.semantic_hash(df)
end)

run.("5c. storage_level (unpersisted)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.storage_level(df)
end)

run.("5d. is_local", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.is_local(df)
end)

run.("5e. is_streaming? (batch DataFrame)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.is_streaming?(df)
end)

run.("5f. foreach", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  DataFrame.foreach_local(df, fn row -> IO.inspect(row, label: "    row") end)
end)

run.("5g. foreach_partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  DataFrame.foreach_partition_local(df, fn rows ->
    count = Enum.count(rows)
    IO.puts("    partition with #{count} rows")
  end)
end)

# =============================================
# 6. Misc untested function variants
# =============================================

run.("6a. typeof", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"int_col" => 42, "str_col" => "hello", "dbl_col" => 3.14}
  ], schema: "int_col INT, str_col STRING, dbl_col DOUBLE")

  df |> DataFrame.select([
    typeof(col("int_col")) |> Column.alias_("int_type"),
    typeof(col("str_col")) |> Column.alias_("str_type"),
    typeof(col("dbl_col")) |> Column.alias_("dbl_type")
  ]) |> DataFrame.collect()
end)

run.("6b. input_file_name (on created df — should be empty)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    col("id"),
    input_file_name() |> Column.alias_("file")
  ]) |> DataFrame.collect()
end)

run.("6c. monotonically_increasing_id", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"val" => i * 10} end),
    schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    monotonically_increasing_id() |> Column.alias_("mono_id")
  ]) |> DataFrame.collect()
end)

run.("6d. spark_partition_id", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  df |> DataFrame.select([
    col("id"),
    spark_partition_id() |> Column.alias_("part_id")
  ]) |> DataFrame.collect()
end)

run.("6e. percentile with single percentage", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  df |> DataFrame.select([
    percentile(col("val"), 0.5) |> Column.alias_("p50")
  ]) |> DataFrame.collect()
end)

run.("6f. percentile with array of percentages", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  df |> DataFrame.select([
    percentile(col("val"), [0.25, 0.5, 0.75]) |> Column.alias_("percentiles")
  ]) |> DataFrame.collect()
end)

run.("6g. percentile with frequency", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 10, "freq" => 3},
    %{"val" => 20, "freq" => 1},
    %{"val" => 30, "freq" => 2}
  ], schema: "val INT, freq INT")

  df |> DataFrame.select([
    percentile(col("val"), 0.5, col("freq")) |> Column.alias_("weighted_p50")
  ]) |> DataFrame.collect()
end)

run.("6h. like_ with escape character", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "100% done"},
    %{"name" => "50 percent"},
    %{"name" => "100 items"}
  ], schema: "name STRING")

  df |> DataFrame.filter(
    like_(col("name"), lit("100!% %"), lit("!"))
  ) |> DataFrame.collect()
end)

run.("6i. ilike_ with escape character", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "100% DONE"},
    %{"name" => "50 percent"}
  ], schema: "name STRING")

  df |> DataFrame.filter(
    ilike_(col("name"), lit("100!% %"), lit("!"))
  ) |> DataFrame.collect()
end)

run.("6j. overlay with explicit length", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    overlay(col("text"), lit("Spark"), lit(7), 5) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("6k. overlay without length (default -1)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    overlay(col("text"), lit("Spark"), lit(7)) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("6l. sentences with language/country", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello world. How are you?"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    sentences(col("text")) |> Column.alias_("default"),
    sentences(col("text"), "en", "US") |> Column.alias_("en_us")
  ]) |> DataFrame.collect()
end)

run.("6m. levenshtein with threshold", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "kitten", "b" => "sitting"}
  ], schema: "a STRING, b STRING")

  df |> DataFrame.select([
    levenshtein(col("a"), col("b")) |> Column.alias_("dist"),
    levenshtein(col("a"), col("b"), 2) |> Column.alias_("dist_thresh")
  ]) |> DataFrame.collect()
end)

run.("6n. array_join with null_replacement", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => ["a", nil, "c"]}
  ], schema: "arr ARRAY<STRING>")

  df |> DataFrame.select([
    array_join(col("arr"), ",") |> Column.alias_("without_null"),
    array_join(col("arr"), ",", "NULL") |> Column.alias_("with_null")
  ]) |> DataFrame.collect()
end)

run.("6o. try_parse_url", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"url" => "http://example.com/path?a=1&b=2"},
    %{"url" => "not_a_url"}
  ], schema: "url STRING")

  df |> DataFrame.select([
    col("url"),
    try_parse_url(col("url"), lit("HOST")) |> Column.alias_("host"),
    try_parse_url(col("url"), lit("QUERY"), lit("a")) |> Column.alias_("param_a")
  ]) |> DataFrame.collect()
end)

run.("6p. call_udf", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")

  df |> DataFrame.select([
    call_udf("abs", [col("val")]) |> Column.alias_("abs_val")
  ]) |> DataFrame.collect()
end)

run.("6q. broadcast (DataFrame hint)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "x"}], schema: "id INT, val STRING")

  df_broadcast = broadcast(df2)
  DataFrame.join(df1, df_broadcast, ["id"]) |> DataFrame.collect()
end)

run.("6r. atan2 with mixed column/number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"y" => 1.0, "x" => 1.0}], schema: "y DOUBLE, x DOUBLE")

  df |> DataFrame.select([
    atan2(col("y"), col("x")) |> Column.alias_("angle1"),
    atan2(1.0, col("x")) |> Column.alias_("angle2"),
    atan2(col("y"), 1.0) |> Column.alias_("angle3")
  ]) |> DataFrame.collect()
end)

run.("6s. pow with mixed column/number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"base" => 2.0, "exp" => 10.0}], schema: "base DOUBLE, exp DOUBLE")

  df |> DataFrame.select([
    pow(col("base"), col("exp")) |> Column.alias_("p1"),
    pow(col("base"), 3.0) |> Column.alias_("p2"),
    pow(2.0, col("exp")) |> Column.alias_("p3")
  ]) |> DataFrame.collect()
end)

run.("6t. unix_timestamp/0 and unix_timestamp/2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts_str" => "2025-06-15 10:30:00"}
  ], schema: "ts_str STRING")

  df |> DataFrame.select([
    unix_timestamp() |> Column.alias_("now_ts"),
    unix_timestamp(col("ts_str")) |> Column.alias_("parsed_ts"),
    unix_timestamp(col("ts_str"), format: "yyyy-MM-dd HH:mm:ss") |> Column.alias_("formatted_ts")
  ]) |> DataFrame.collect()
end)

run.("6u. convert_timezone 2-arg and 3-arg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts" => "2025-06-15 10:00:00"}
  ], schema: "ts STRING")

  df |> DataFrame.select([
    convert_timezone(lit("America/New_York"), Column.cast(col("ts"), "TIMESTAMP"))
    |> Column.alias_("ny_time")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. MergeIntoWriter with conditions
# =============================================

run.("7a. MergeIntoWriter: update_all + insert_all (no conditions)", fn ->
  tbl = "#{tbl_base}_merge1"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice', 90), (2, 'Bob', 80)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "score" => 95},
    %{"id" => 3, "name" => "Carol", "score" => 85}
  ], schema: "id INT, name STRING, score INT")

  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_update_all()
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7b. MergeIntoWriter: conditional update + insert", fn ->
  tbl = "#{tbl_base}_merge2"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice', 90), (2, 'Bob', 80)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 50},
    %{"id" => 2, "name" => "Bob", "score" => 95},
    %{"id" => 3, "name" => "Carol", "score" => 85}
  ], schema: "id INT, name STRING, score INT")

  # Only update when new score > old score
  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_update_all(Column.gt(col("score"), col("score")))
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7c. MergeIntoWriter: delete when matched", fn ->
  tbl = "#{tbl_base}_merge3"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")

  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_delete()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7d. MergeIntoWriter: conditional delete", fn ->
  tbl = "#{tbl_base}_merge4"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 90), (2, 50), (3, 70)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 90},
    %{"id" => 2, "score" => 50}
  ], schema: "id INT, score INT")

  # Delete only when score < 60
  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_delete(Column.lt(col("score"), lit(60)))
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7e. MergeIntoWriter: when_matched_update with assignment map", fn ->
  tbl = "#{tbl_base}_merge5"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice', 90), (2, 'Bob', 80)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bobby", "score" => 95}
  ], schema: "id INT, name STRING, score INT")

  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_matched_update(%{"score" => col("score"), "name" => col("name")})
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7f. MergeIntoWriter: when_not_matched_insert with assignments", fn ->
  tbl = "#{tbl_base}_merge6"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING, score INT) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice', 90)") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "score" => 80}
  ], schema: "id INT, name STRING, score INT")

  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_not_matched_insert(%{
    "id" => col("id"),
    "name" => upper(col("name")),
    "score" => Column.plus(col("score"), lit(10))
  })
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7g. MergeIntoWriter: when_not_matched_by_source_delete", fn ->
  tbl = "#{tbl_base}_merge7"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")

  # Delete from target rows not matched by source
  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_not_matched_by_source_delete()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("7h. MergeIntoWriter: with_schema_evolution", fn ->
  tbl = "#{tbl_base}_merge8"

  SparkEx.sql(s, "CREATE TABLE #{tbl} (id INT, name STRING) USING iceberg") |> DataFrame.collect()
  SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, 'Alice')") |> DataFrame.collect()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "extra" => "new_col"}
  ], schema: "id INT, name STRING, extra STRING")

  source
  |> DataFrame.merge_into(tbl, Column.eq(col("id"), col("id")))
  |> MergeIntoWriter.when_not_matched_insert_all()
  |> MergeIntoWriter.with_schema_evolution()
  |> MergeIntoWriter.merge()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

IO.puts("=== HOF, parse/serialize, merge, analytics tests complete ===")
