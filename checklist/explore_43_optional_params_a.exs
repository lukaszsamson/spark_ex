# Round 19: Optional parameter edge cases — less-used arities of functions
# Focus: col_opt functions, date format variants, string trim variants,
# aggregation options, interval construction, window time functions
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1, round: 1]

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

# =============================================
# 1. round/bround with optional scale
# =============================================
run.("1a. round/1 (default scale=0)", fn ->
  SparkEx.sql(s, "SELECT 3.14159 AS val")
  |> DataFrame.select([round(col("val")) |> Column.alias_("rounded")])
  |> DataFrame.collect()
end)

run.("1b. round/2 with scale=2", fn ->
  SparkEx.sql(s, "SELECT 3.14159 AS val")
  |> DataFrame.select([round(col("val"), scale: 2) |> Column.alias_("rounded")])
  |> DataFrame.collect()
end)

run.("1c. bround/2 with scale=1 (banker's rounding)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (2.5), (3.5), (4.5) AS t(val)")
  |> DataFrame.select([
    col("val"),
    bround(col("val"), scale: 0) |> Column.alias_("bround")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 2. to_date / to_timestamp with format
# =============================================
run.("2a. to_date/1 (default format)", fn ->
  SparkEx.sql(s, "SELECT '2025-06-15' AS dt_str")
  |> DataFrame.select([to_date(col("dt_str")) |> Column.alias_("dt")])
  |> DataFrame.collect()
end)

run.("2b. to_date/2 with custom format", fn ->
  SparkEx.sql(s, "SELECT '15/06/2025' AS dt_str")
  |> DataFrame.select([to_date(col("dt_str"), format: "dd/MM/yyyy") |> Column.alias_("dt")])
  |> DataFrame.collect()
end)

run.("2c. to_timestamp/2 with custom format", fn ->
  SparkEx.sql(s, "SELECT '15-Jun-2025 10:30:00' AS ts_str")
  |> DataFrame.select([
    to_timestamp(col("ts_str"), format: "dd-MMM-yyyy HH:mm:ss") |> Column.alias_("ts")
  ])
  |> DataFrame.collect()
end)

run.("2d. try_to_date/2 with invalid value (should return nil)", fn ->
  SparkEx.sql(s, "SELECT 'not-a-date' AS dt_str")
  |> DataFrame.select([
    try_to_date(col("dt_str"), format: "yyyy-MM-dd") |> Column.alias_("dt")
  ])
  |> DataFrame.collect()
end)

run.("2e. to_timestamp_ntz/2 with format", fn ->
  SparkEx.sql(s, "SELECT '2025-06-15 10:30:00' AS ts_str")
  |> DataFrame.select([
    to_timestamp_ntz(col("ts_str"), format: "yyyy-MM-dd HH:mm:ss") |> Column.alias_("ts")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 3. trim variants with trim_string
# =============================================
run.("3a. ltrim/1 (default whitespace)", fn ->
  SparkEx.sql(s, "SELECT '   hello   ' AS text")
  |> DataFrame.select([ltrim(col("text")) |> Column.alias_("trimmed")])
  |> DataFrame.collect()
end)

run.("3b. ltrim/2 with custom trim string", fn ->
  SparkEx.sql(s, "SELECT 'xxxhelloxx' AS text")
  |> DataFrame.select([ltrim(col("text"), lit("x")) |> Column.alias_("trimmed")])
  |> DataFrame.collect()
end)

run.("3c. rtrim/2 with custom trim string", fn ->
  SparkEx.sql(s, "SELECT 'xxxhelloxx' AS text")
  |> DataFrame.select([rtrim(col("text"), lit("x")) |> Column.alias_("trimmed")])
  |> DataFrame.collect()
end)

run.("3d. trim/2 with custom trim string", fn ->
  SparkEx.sql(s, "SELECT '##hello##' AS text")
  |> DataFrame.select([trim(col("text"), lit("#")) |> Column.alias_("trimmed")])
  |> DataFrame.collect()
end)

run.("3e. btrim/2 with custom trim string", fn ->
  SparkEx.sql(s, "SELECT '***hello***' AS text")
  |> DataFrame.select([btrim(col("text"), trim_string: "***") |> Column.alias_("trimmed")])
  |> DataFrame.collect()
end)

# =============================================
# 4. first/last with ignore_nulls
# =============================================
run.("4a. first/1 (default, includes nulls)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (NULL), (10), (20) AS t(val)")
  |> DataFrame.select([first(col("val")) |> Column.alias_("first_val")])
  |> DataFrame.collect()
end)

run.("4b. first/2 with ignore_nulls: true", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (NULL), (10), (20) AS t(val)")
  |> DataFrame.select([first(col("val"), ignore_nulls: true) |> Column.alias_("first_val")])
  |> DataFrame.collect()
end)

run.("4c. last/2 with ignore_nulls: true", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (10), (20), (NULL) AS t(val)")
  |> DataFrame.select([last(col("val"), ignore_nulls: true) |> Column.alias_("last_val")])
  |> DataFrame.collect()
end)

# =============================================
# 5. lag/lead with various options
# =============================================
run.("5a. lag with offset:2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
  df
  |> DataFrame.select([
    col("id"),
    lag(col("val"), offset: 2) |> Column.over(w) |> Column.alias_("lag2")
  ])
  |> DataFrame.collect()
end)

run.("5b. lead with offset:2, default:0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
  df
  |> DataFrame.select([
    col("id"),
    lead(col("val"), offset: 2, default: 0) |> Column.over(w) |> Column.alias_("lead2")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 6. locate with pos parameter
# =============================================
run.("6a. locate/2 (default pos=1)", fn ->
  SparkEx.sql(s, "SELECT 'hello world hello' AS text")
  |> DataFrame.select([
    locate(lit("hello"), col("text")) |> Column.alias_("pos")
  ])
  |> DataFrame.collect()
end)

run.("6b. locate/3 with pos=6 (skip first match)", fn ->
  SparkEx.sql(s, "SELECT 'hello world hello' AS text")
  |> DataFrame.select([
    locate(lit("hello"), col("text"), 6) |> Column.alias_("pos")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 7. approx_count_distinct with rsd
# =============================================
run.("7a. approx_count_distinct/1 (default rsd)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => rem(i, 30)} end),
    schema: "val INT")

  df
  |> DataFrame.select([approx_count_distinct(col("val")) |> Column.alias_("approx")])
  |> DataFrame.collect()
end)

run.("7b. approx_count_distinct/2 with rsd=0.01", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => rem(i, 30)} end),
    schema: "val INT")

  df
  |> DataFrame.select([approx_count_distinct(col("val"), 0.01) |> Column.alias_("approx")])
  |> DataFrame.collect()
end)

# =============================================
# 8. months_between with round_off option
# =============================================
run.("8a. months_between/2 (default round_off=true)", fn ->
  SparkEx.sql(s, "SELECT TIMESTAMP '2025-01-15 10:30:00' AS d1, TIMESTAMP '2025-03-20 14:00:00' AS d2")
  |> DataFrame.select([
    months_between(col("d2"), col("d1")) |> Column.alias_("months")
  ])
  |> DataFrame.collect()
end)

run.("8b. months_between/3 with round_off=false", fn ->
  SparkEx.sql(s, "SELECT TIMESTAMP '2025-01-15 10:30:00' AS d1, TIMESTAMP '2025-03-20 14:00:00' AS d2")
  |> DataFrame.select([
    months_between(col("d2"), col("d1"), false) |> Column.alias_("months")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 9. sort_array with asc option
# =============================================
run.("9a. sort_array/1 (default asc=true)", fn ->
  SparkEx.sql(s, "SELECT array(3, 1, 4, 1, 5) AS arr")
  |> DataFrame.select([sort_array(col("arr")) |> Column.alias_("sorted")])
  |> DataFrame.collect()
end)

run.("9b. sort_array/2 with asc: false", fn ->
  SparkEx.sql(s, "SELECT array(3, 1, 4, 1, 5) AS arr")
  |> DataFrame.select([sort_array(col("arr"), asc: false) |> Column.alias_("sorted")])
  |> DataFrame.collect()
end)

# =============================================
# 10. sequence with step parameter
# =============================================
run.("10a. sequence/2 (no step)", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([sequence(lit(1), lit(10)) |> Column.alias_("seq")])
  |> DataFrame.collect()
end)

run.("10b. sequence/3 with step=2", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([sequence(lit(1), lit(10), lit(2)) |> Column.alias_("seq")])
  |> DataFrame.collect()
end)

run.("10c. sequence/3 with negative step", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([sequence(lit(10), lit(1), lit(-2)) |> Column.alias_("seq")])
  |> DataFrame.collect()
end)

# =============================================
# 11. make_dt_interval / make_interval / make_ym_interval
# =============================================
run.("11a. make_dt_interval/0 (defaults)", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([make_dt_interval() |> Column.alias_("interval")])
  |> DataFrame.collect()
end)

run.("11b. make_dt_interval/1 with options", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([
    make_dt_interval(days: lit(2), hours: lit(3), mins: lit(30), secs: lit(45))
    |> Column.alias_("interval")
  ])
  |> DataFrame.collect()
end)

run.("11c. make_ym_interval/1", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([
    make_ym_interval(years: lit(1), months: lit(6))
    |> Column.alias_("interval")
  ])
  |> DataFrame.collect()
end)

run.("11d. make_interval/1 with mixed params", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([
    make_interval(years: lit(1), days: lit(10), hours: lit(5))
    |> Column.alias_("interval")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 12. from_unixtime with format
# =============================================
run.("12a. from_unixtime/1 (default format)", fn ->
  SparkEx.sql(s, "SELECT 1718441400 AS epoch")
  |> DataFrame.select([from_unixtime(col("epoch")) |> Column.alias_("ts_str")])
  |> DataFrame.collect()
end)

run.("12b. from_unixtime/2 with custom format", fn ->
  SparkEx.sql(s, "SELECT 1718441400 AS epoch")
  |> DataFrame.select([
    from_unixtime(col("epoch"), "dd/MM/yyyy HH:mm") |> Column.alias_("ts_str")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 13. str_to_map with delimiters
# =============================================
run.("13a. str_to_map/1 (default delimiters)", fn ->
  SparkEx.sql(s, "SELECT 'a:1,b:2,c:3' AS text")
  |> DataFrame.select([str_to_map(col("text")) |> Column.alias_("result")])
  |> DataFrame.collect()
end)

run.("13b. str_to_map/2 with custom delimiters", fn ->
  SparkEx.sql(s, "SELECT 'a=1;b=2;c=3' AS text")
  |> DataFrame.select([
    str_to_map(col("text"), pair_delim: ";", key_value_delim: "=")
    |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 14. any_value with ignore_nulls
# =============================================
run.("14a. any_value/1 (default, may return null)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES ('a', NULL), ('a', 10), ('a', 20) AS t(grp, val)")
  |> DataFrame.group_by([col("grp")])
  |> SparkEx.GroupedData.agg([any_value(col("val")) |> Column.alias_("any")])
  |> DataFrame.collect()
end)

run.("14b. any_value/2 with ignore_nulls: true", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES ('a', NULL), ('a', 10), ('a', 20) AS t(grp, val)")
  |> DataFrame.group_by([col("grp")])
  |> SparkEx.GroupedData.agg([any_value(col("val"), true) |> Column.alias_("any")])
  |> DataFrame.collect()
end)

# =============================================
# 15. rand/randn with seed
# =============================================
run.("15a. rand/0 (no seed)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([rand() |> Column.alias_("r")])
  |> DataFrame.collect()
end)

run.("15b. rand/1 with seed", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([rand(42) |> Column.alias_("r")])
  |> DataFrame.collect()
end)

run.("15c. randn/1 with seed", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([randn(42) |> Column.alias_("r")])
  |> DataFrame.collect()
end)

# =============================================
# 16. shuffle with seed
# =============================================
run.("16a. shuffle/1 (no seed)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr")
  |> DataFrame.select([shuffle(col("arr")) |> Column.alias_("shuffled")])
  |> DataFrame.collect()
end)

run.("16b. shuffle/2 with seed", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr")
  |> DataFrame.select([shuffle(col("arr"), 42) |> Column.alias_("shuffled")])
  |> DataFrame.collect()
end)

# =============================================
# 17. replace with optional replacement
# =============================================
run.("17a. replace/3 (explicit replacement)", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    replace(col("text"), lit("world"), lit("spark")) |> Column.alias_("replaced")
  ])
  |> DataFrame.collect()
end)

run.("17b. replace/2 (default replacement = empty string)", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    replace(col("text"), lit("world")) |> Column.alias_("replaced")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 18. split with and without limit
# =============================================
run.("18a. split/2 (no limit)", fn ->
  SparkEx.sql(s, "SELECT 'a,b,c,d' AS csv")
  |> DataFrame.select([split(col("csv"), lit(",")) |> Column.alias_("parts")])
  |> DataFrame.collect()
end)

run.("18b. split/3 with limit=2", fn ->
  SparkEx.sql(s, "SELECT 'a,b,c,d' AS csv")
  |> DataFrame.select([split(col("csv"), lit(","), lit(2)) |> Column.alias_("parts")])
  |> DataFrame.collect()
end)

# =============================================
# 19. assert_true with and without message
# =============================================
run.("19a. assert_true/1 (true condition)", fn ->
  SparkEx.sql(s, "SELECT 10 AS val")
  |> DataFrame.select([
    assert_true(Column.gt(col("val"), lit(5))) |> Column.alias_("check")
  ])
  |> DataFrame.collect()
end)

run.("19b. assert_true/2 with custom error message (failing)", fn ->
  SparkEx.sql(s, "SELECT 3 AS val")
  |> DataFrame.select([
    assert_true(Column.gt(col("val"), lit(5)), lit("Value must be > 5!"))
    |> Column.alias_("check")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 20. aggregate/reduce with finish function
# =============================================
run.("20a. aggregate/3 (no finish)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr")
  |> DataFrame.select([
    aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end)
    |> Column.alias_("sum")
  ])
  |> DataFrame.collect()
end)

run.("20b. aggregate/4 with finish function", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr")
  |> DataFrame.select([
    aggregate(col("arr"), lit(0),
      fn acc, x -> Column.plus(acc, x) end,
      fn total -> Column.multiply(total, lit(10)) end)
    |> Column.alias_("sum_x10")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 21. to_json with options
# =============================================
run.("21a. to_json/1 (default options)", fn ->
  SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 'hello') AS st")
  |> DataFrame.select([to_json(col("st")) |> Column.alias_("json")])
  |> DataFrame.collect()
end)

run.("21b. to_json/2 with options", fn ->
  SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 'hello') AS st")
  |> DataFrame.select([
    to_json(col("st"), %{"pretty" => "true"}) |> Column.alias_("json")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 22. schema_of_json with options
# =============================================
run.("22a. schema_of_json/1 (default options)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([
    schema_of_json(lit("{\"a\":1,\"b\":\"hello\"}")) |> Column.alias_("schema")
  ])
  |> DataFrame.collect()
end)

run.("22b. schema_of_json/2 with options", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([
    schema_of_json(lit("{\"ts\":\"2025-06-15\"}"), %{"timestampFormat" => "yyyy-MM-dd"})
    |> Column.alias_("schema")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 23. mask function with partial options
# =============================================
run.("23a. mask/1 (default masking)", fn ->
  SparkEx.sql(s, "SELECT 'Hello World 123' AS text")
  |> DataFrame.select([mask(col("text")) |> Column.alias_("masked")])
  |> DataFrame.collect()
end)

run.("23b. mask/2 with custom chars", fn ->
  SparkEx.sql(s, "SELECT 'Hello World 123' AS text")
  |> DataFrame.select([
    mask(col("text"), upper_char: "A", lower_char: "a", digit_char: "0")
    |> Column.alias_("masked")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 24. nth_value with ignore_nulls
# =============================================
run.("24a. nth_value/2 (default, includes nulls)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => nil},
    %{"id" => 2, "val" => 10},
    %{"id" => 3, "val" => nil},
    %{"id" => 4, "val" => 20}
  ], schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
  df
  |> DataFrame.select([
    col("id"), col("val"),
    nth_value(col("val"), 2) |> Column.over(w) |> Column.alias_("nth2")
  ])
  |> DataFrame.collect()
end)

run.("24b. nth_value/3 with ignore_nulls: true", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => nil},
    %{"id" => 2, "val" => 10},
    %{"id" => 3, "val" => nil},
    %{"id" => 4, "val" => 20}
  ], schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
  df
  |> DataFrame.select([
    col("id"), col("val"),
    nth_value(col("val"), 2, true) |> Column.over(w) |> Column.alias_("nth2_no_null")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 25. parse_url with key parameter
# =============================================
run.("25a. parse_url/2 (no key)", fn ->
  SparkEx.sql(s, "SELECT 'https://example.com/path?a=1&b=2' AS url")
  |> DataFrame.select([
    parse_url(col("url"), lit("HOST")) |> Column.alias_("host"),
    parse_url(col("url"), lit("PATH")) |> Column.alias_("path"),
    parse_url(col("url"), lit("QUERY")) |> Column.alias_("query")
  ])
  |> DataFrame.collect()
end)

run.("25b. parse_url/3 with key parameter", fn ->
  SparkEx.sql(s, "SELECT 'https://example.com/path?a=1&b=2' AS url")
  |> DataFrame.select([
    parse_url(col("url"), lit("QUERY"), lit("a")) |> Column.alias_("a_val"),
    parse_url(col("url"), lit("QUERY"), lit("b")) |> Column.alias_("b_val")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 26. corr with method parameter
# =============================================
run.("26. DataFrame.corr with method", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => i * 2.0 + 3.0} end),
    schema: "x DOUBLE, y DOUBLE")

  {:ok, pearson} = DataFrame.corr(df, "x", "y", "pearson")
  {pearson}
end)

# =============================================
# 27. window time function with all params
# =============================================
run.("27a. window/2 (duration only)", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (TIMESTAMP '2025-06-15 10:00:00', 1),
      (TIMESTAMP '2025-06-15 10:05:00', 2),
      (TIMESTAMP '2025-06-15 10:12:00', 3),
      (TIMESTAMP '2025-06-15 10:18:00', 4),
      (TIMESTAMP '2025-06-15 10:25:00', 5)
    AS t(ts, val)
  """)
  |> DataFrame.select([
    window(col("ts"), "10 minutes") |> Column.alias_("win"),
    col("val")
  ])
  |> DataFrame.group_by([col("win")])
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("win"))])
  |> DataFrame.collect()
end)

run.("27b. window/4 with slide and start_time", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (TIMESTAMP '2025-06-15 10:00:00', 1),
      (TIMESTAMP '2025-06-15 10:05:00', 2),
      (TIMESTAMP '2025-06-15 10:12:00', 3),
      (TIMESTAMP '2025-06-15 10:18:00', 4),
      (TIMESTAMP '2025-06-15 10:25:00', 5)
    AS t(ts, val)
  """)
  |> DataFrame.select([
    window(col("ts"), "10 minutes", "5 minutes", "2 minutes") |> Column.alias_("win"),
    col("val")
  ])
  |> DataFrame.group_by([col("win")])
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("win"))])
  |> DataFrame.collect()
end)

# =============================================
# 28. DataFrame.show with options
# =============================================
run.("28a. show/1 (defaults)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "long_text" => String.duplicate("abc", 20)}
  ], schema: "id INT, long_text STRING")
  DataFrame.show(df)
end)

run.("28b. show/2 with num_rows and truncate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")
  DataFrame.show(df, num_rows: 3, truncate: 5)
end)

# =============================================
# 29. DataFrame.describe with specific columns
# =============================================
run.("29a. describe/1 (all columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 10.0, "c" => "x"},
    %{"a" => 2, "b" => 20.0, "c" => "y"},
    %{"a" => 3, "b" => 30.0, "c" => "z"}
  ], schema: "a INT, b DOUBLE, c STRING")
  DataFrame.describe(df) |> DataFrame.collect()
end)

run.("29b. describe/2 with specific columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 10.0, "c" => "x"},
    %{"a" => 2, "b" => 20.0, "c" => "y"},
    %{"a" => 3, "b" => 30.0, "c" => "z"}
  ], schema: "a INT, b DOUBLE, c STRING")
  DataFrame.describe(df, ["a", "b"]) |> DataFrame.collect()
end)

# =============================================
# 30. DataFrame.summary with custom statistics
# =============================================
run.("30. summary with custom statistics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 4}, %{"val" => 5}
  ], schema: "val INT")
  DataFrame.summary(df, ["count", "min", "max", "50%"]) |> DataFrame.collect()
end)

IO.puts("=== Optional parameter edge case tests (part A) complete ===")
