# Round 30: Untested API corners — regression, bitwise, array ops, date edge cases, stats
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
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
# 1. Regression functions (regr_*)
# =============================================

run.("1a. regr_slope / regr_intercept / regr_r2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"x" => i, "y" => i * 2.5 + 3.0 + :rand.uniform()} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.select(df, [
    regr_slope(col("y"), col("x")) |> Column.alias_("slope"),
    regr_intercept(col("y"), col("x")) |> Column.alias_("intercept"),
    regr_r2(col("y"), col("x")) |> Column.alias_("r2"),
    regr_count(col("y"), col("x")) |> Column.alias_("cnt")
  ]) |> DataFrame.collect()
end)

run.("1b. regr_avgx / regr_avgy / regr_sxx / regr_syy / regr_sxy", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1.0, "y" => 2.0}, %{"x" => 2.0, "y" => 4.0},
    %{"x" => 3.0, "y" => 5.0}, %{"x" => 4.0, "y" => 4.0},
    %{"x" => 5.0, "y" => 5.0}
  ], schema: "x DOUBLE, y DOUBLE")
  DataFrame.select(df, [
    regr_avgx(col("y"), col("x")) |> Column.alias_("avg_x"),
    regr_avgy(col("y"), col("x")) |> Column.alias_("avg_y"),
    regr_sxx(col("y"), col("x")) |> Column.alias_("sxx"),
    regr_syy(col("y"), col("x")) |> Column.alias_("syy"),
    regr_sxy(col("y"), col("x")) |> Column.alias_("sxy")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Bitwise operations (Column + standalone)
# =============================================

run.("2a. Column bitwise: and, or, xor, not", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 0b1100, "b" => 0b1010}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.bitwise_and(col("a"), col("b")) |> Column.alias_("and"),
    Column.bitwise_or(col("a"), col("b")) |> Column.alias_("or"),
    Column.bitwise_xor(col("a"), col("b")) |> Column.alias_("xor"),
    Column.bitwise_not(col("a")) |> Column.alias_("not_a")
  ]) |> DataFrame.collect()
end)

run.("2b. shiftleft / shiftright / shiftrightunsigned", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 8}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    shiftleft(col("val"), 2) |> Column.alias_("shl_2"),
    shiftright(col("val"), 1) |> Column.alias_("shr_1"),
    shiftrightunsigned(col("val"), 1) |> Column.alias_("shr_unsigned_1")
  ]) |> DataFrame.collect()
end)

run.("2c. bit_count / bit_get / bit_length", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 0b10110}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    bit_count(col("val")) |> Column.alias_("count"),
    bit_get(col("val"), 0) |> Column.alias_("bit_0"),
    bit_get(col("val"), 1) |> Column.alias_("bit_1"),
    bit_get(col("val"), 2) |> Column.alias_("bit_2"),
    bit_length(Column.cast(col("val"), "STRING")) |> Column.alias_("bit_len_str")
  ]) |> DataFrame.collect()
end)

run.("2d. bitwise aggregate: bit_and / bit_or / bit_xor", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => 0b1111},
    %{"grp" => "A", "val" => 0b1010},
    %{"grp" => "B", "val" => 0b0110},
    %{"grp" => "B", "val" => 0b0011}
  ], schema: "grp STRING, val INT")
  DataFrame.group_by(df, [col("grp")])
  |> GroupedData.agg([
    bit_and(col("val")) |> Column.alias_("bit_and"),
    bit_or(col("val")) |> Column.alias_("bit_or"),
    bit_xor(col("val")) |> Column.alias_("bit_xor")
  ])
  |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()
end)

# =============================================
# 3. Boolean aggregate: bool_and / bool_or, count_if, any_value
# =============================================

run.("3a. bool_and / bool_or / count_if", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "flag" => true, "val" => 10},
    %{"grp" => "A", "flag" => true, "val" => 20},
    %{"grp" => "B", "flag" => true, "val" => 30},
    %{"grp" => "B", "flag" => false, "val" => 40}
  ], schema: "grp STRING, flag BOOLEAN, val INT")
  DataFrame.group_by(df, [col("grp")])
  |> GroupedData.agg([
    bool_and(col("flag")) |> Column.alias_("all_true"),
    bool_or(col("flag")) |> Column.alias_("any_true"),
    count_if(Column.gt(col("val"), lit(15))) |> Column.alias_("gt_15_count")
  ])
  |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()
end)

run.("3b. any_value / any_value with ignore_nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => nil}, %{"grp" => "A", "val" => "hello"},
    %{"grp" => "B", "val" => "world"}
  ], schema: "grp STRING, val STRING")
  DataFrame.group_by(df, [col("grp")])
  |> GroupedData.agg([
    any_value(col("val")) |> Column.alias_("any_val"),
    any_value(col("val"), true) |> Column.alias_("any_nonnull")
  ])
  |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()
end)

# =============================================
# 4. Array set operations
# =============================================

run.("4a. array_except / array_intersect / array_union", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3, 4], "b" => [3, 4, 5, 6]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    array_except(col("a"), col("b")) |> Column.alias_("except"),
    array_intersect(col("a"), col("b")) |> Column.alias_("intersect"),
    array_union(col("a"), col("b")) |> Column.alias_("union")
  ]) |> DataFrame.collect()
end)

run.("4b. arrays_overlap / arrays_zip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [3, 4, 5], "names" => ["Alice", "Bob"], "ages" => [30, 25]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>, names ARRAY<STRING>, ages ARRAY<INT>")
  df |> DataFrame.select([
    arrays_overlap(col("a"), col("b")) |> Column.alias_("overlap"),
    arrays_zip([col("names"), col("ages")]) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

run.("4c. flatten / slice / shuffle / reverse / array_distinct / array_compact", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"nested" => [[1, 2], [3, 4], [5]], "arr" => [3, 1, 2, 1, nil, 3]}
  ], schema: "nested ARRAY<ARRAY<INT>>, arr ARRAY<INT>")
  df |> DataFrame.select([
    flatten(col("nested")) |> Column.alias_("flat"),
    slice(col("arr"), 2, 3) |> Column.alias_("sliced"),
    reverse(col("arr")) |> Column.alias_("reversed"),
    array_distinct(col("arr")) |> Column.alias_("distinct"),
    array_compact(col("arr")) |> Column.alias_("compact"),
    array_size(col("arr")) |> Column.alias_("sz"),
    array_min(col("arr")) |> Column.alias_("mn"),
    array_max(col("arr")) |> Column.alias_("mx")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Map operations
# =============================================

run.("5a. map_from_entries / map_from_arrays / map_concat", fn ->
  df = SparkEx.sql(s, """
    SELECT
      array(struct('a', 1), struct('b', 2)) AS entries,
      array('x', 'y') AS keys,
      array(10, 20) AS vals,
      map('a', 1) AS m1,
      map('b', 2) AS m2
  """)
  df |> DataFrame.select([
    map_from_entries(col("entries")) |> Column.alias_("from_entries"),
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("from_arrays"),
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged"),
    map_keys(col("m1")) |> Column.alias_("keys"),
    map_values(col("m1")) |> Column.alias_("values"),
    map_entries(col("m1")) |> Column.alias_("entries_out")
  ]) |> DataFrame.collect()
end)

run.("5b. element_at (array + map)", fn ->
  df = SparkEx.sql(s, "SELECT array(10, 20, 30) AS arr, map('a', 1, 'b', 2) AS mp")
  df |> DataFrame.select([
    element_at(col("arr"), lit(2)) |> Column.alias_("arr_at_2"),
    element_at(col("mp"), lit("b")) |> Column.alias_("map_at_b")
    # element_at is :two_col, needs lit() wrapping for non-column args
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Date/time edge cases
# =============================================

run.("6a. make_date / last_day / next_day / add_months / months_between", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"y" => 2024, "m" => 2, "d" => 29}], schema: "y INT, m INT, d INT")
  df |> DataFrame.select([
    make_date(col("y"), col("m"), col("d")) |> Column.alias_("date"),
    last_day(make_date(col("y"), col("m"), col("d"))) |> Column.alias_("last_d"),
    next_day(make_date(col("y"), col("m"), col("d")), "Monday") |> Column.alias_("next_mon"),
    add_months(make_date(col("y"), col("m"), col("d")), 1) |> Column.alias_("plus_1m")
  ]) |> DataFrame.collect()
end)

run.("6b. date extraction: dayofweek, dayofmonth, dayofyear, weekofyear, hour, minute, second", fn ->
  df = SparkEx.sql(s, "SELECT TIMESTAMP '2024-12-31 23:59:59' AS ts")
  df |> DataFrame.select([
    dayofweek(col("ts")) |> Column.alias_("dow"),
    dayofmonth(col("ts")) |> Column.alias_("dom"),
    dayofyear(col("ts")) |> Column.alias_("doy"),
    weekofyear(col("ts")) |> Column.alias_("woy"),
    hour(col("ts")) |> Column.alias_("hr"),
    minute(col("ts")) |> Column.alias_("min"),
    second(col("ts")) |> Column.alias_("sec")
  ]) |> DataFrame.collect()
end)

run.("6c. date_format / from_unixtime / unix_timestamp", fn ->
  df = SparkEx.sql(s, "SELECT TIMESTAMP '2024-07-04 12:30:45' AS ts")
  df |> DataFrame.select([
    date_format(col("ts"), "yyyy-MM-dd HH:mm") |> Column.alias_("fmt"),
    unix_timestamp(col("ts")) |> Column.alias_("epoch"),
    from_unixtime(unix_timestamp(col("ts")), "yyyy/MM/dd") |> Column.alias_("from_unix")
  ]) |> DataFrame.collect()
end)

run.("6d. months_between / datediff / date_trunc / trunc", fn ->
  df = SparkEx.sql(s, "SELECT DATE '2024-01-15' AS d1, DATE '2024-07-20' AS d2")
  df |> DataFrame.select([
    months_between(col("d2"), col("d1")) |> Column.alias_("months"),
    datediff(col("d2"), col("d1")) |> Column.alias_("days"),
    date_trunc("month", [col("d2")]) |> Column.alias_("trunc_month"),
    trunc(col("d2"), "year") |> Column.alias_("trunc_year")
  ]) |> DataFrame.collect()
end)

run.("6e. from_utc_timestamp / to_utc_timestamp", fn ->
  df = SparkEx.sql(s, "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts")
  df |> DataFrame.select([
    from_utc_timestamp(col("ts"), "America/New_York") |> Column.alias_("ny_time"),
    to_utc_timestamp(col("ts"), "Europe/London") |> Column.alias_("to_utc")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Statistics: crosstab, corr, cov, approx_quantile, sample_by
# =============================================

run.("7a. Stat.crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"color" => "red", "size" => "S"}, %{"color" => "red", "size" => "M"},
    %{"color" => "blue", "size" => "S"}, %{"color" => "blue", "size" => "S"},
    %{"color" => "green", "size" => "L"}, %{"color" => "green", "size" => "M"}
  ], schema: "color STRING, size STRING")
  DataFrame.Stat.crosstab(df, "color", "size") |> DataFrame.collect()
end)

run.("7b. Stat.corr / Stat.cov", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i -> %{"x" => i * 1.0, "y" => i * 2.0 + 1.0} end),
    schema: "x DOUBLE, y DOUBLE")
  corr = DataFrame.Stat.corr(df, "x", "y")
  cov = DataFrame.Stat.cov(df, "x", "y")
  {corr, cov}
end)

run.("7c. Stat.approx_quantile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.Stat.approx_quantile(df, ["val"], [0.25, 0.5, 0.75], 0.01)
end)

run.("7d. Stat.freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    (Enum.map(1..50, fn _ -> %{"color" => "red"} end) ++
     Enum.map(1..30, fn _ -> %{"color" => "blue"} end) ++
     Enum.map(1..20, fn _ -> %{"color" => "green"} end)),
    schema: "color STRING")
  DataFrame.Stat.freq_items(df, ["color"], 0.3) |> DataFrame.collect()
end)

# =============================================
# 8. Formatting: format_number, format_string
# =============================================

run.("8a. format_number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 1234567.891}], schema: "val DOUBLE")
  df |> DataFrame.select([
    format_number(col("val"), lit(2)) |> Column.alias_("formatted")
  ]) |> DataFrame.collect()
end)

run.("8b. format_string (printf-style)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "World", "count" => 42}
  ], schema: "name STRING, count INT")
  df |> DataFrame.select([
    format_string("Hello %s, count=%d", [col("name"), col("count")]) |> Column.alias_("msg")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. approx_count_distinct / percentile_approx / histogram_numeric
# =============================================

run.("9a. approx_count_distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10000, fn i -> %{"val" => rem(i, 500)} end),
    schema: "val INT")
  DataFrame.select(df, [
    approx_count_distinct(col("val")) |> Column.alias_("approx_distinct")
  ]) |> DataFrame.collect()
end)

run.("9b. percentile_approx", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => i} end),
    schema: "val INT")
  DataFrame.select(df, [
    percentile_approx(col("val"), 0.5, 100) |> Column.alias_("median")
  ]) |> DataFrame.collect()
end)

run.("9c. histogram_numeric", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.select(df, [
    histogram_numeric(col("val"), 5) |> Column.alias_("hist")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. isin with mixed types / between edge cases
# =============================================

run.("10a. isin with various types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 4}
  ], schema: "val INT")
  df |> DataFrame.filter(Column.isin(col("val"), [lit(1), lit(3)]))
  |> DataFrame.collect()
end)

run.("10b. between with boundary values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")
  df |> DataFrame.filter(Column.between(col("val"), lit(3), lit(7)))
  |> DataFrame.collect()
end)

run.("10c. between with inverted range (should return empty)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")
  {:ok, result} = df |> DataFrame.filter(Column.between(col("val"), lit(7), lit(3)))
  |> DataFrame.collect()
  length(result)
end)

# =============================================
# 11. sequence / width_bucket
# =============================================

run.("11a. sequence function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"start" => 1, "stop" => 10, "step" => 2}
  ], schema: "start INT, stop INT, step INT")
  df |> DataFrame.select([
    sequence(col("start"), col("stop"), col("step")) |> Column.alias_("seq")
  ]) |> DataFrame.collect()
end)

run.("11b. width_bucket", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 5.0}, %{"val" => 15.0}, %{"val" => 25.0}, %{"val" => 35.0}
  ], schema: "val DOUBLE")
  df |> DataFrame.select([
    col("val"),
    width_bucket(col("val"), 0.0, 40.0, 4) |> Column.alias_("bucket")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. json_tuple / assert_true
# =============================================

run.("12a. json_tuple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json_str" => "{\"name\":\"Alice\",\"age\":30,\"city\":\"NYC\"}"}
  ], schema: "json_str STRING")
  # json_tuple returns multiple columns, use via SQL
  DataFrame.create_or_replace_temp_view(df, "jt_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT jt.* FROM jt_#{run_id}
    LATERAL VIEW json_tuple(json_str, 'name', 'age', 'city') jt AS name, age, city
  """) |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "jt_#{run_id}")
  result
end)

run.("12b. assert_true (passing condition)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 10}], schema: "val INT")
  df |> DataFrame.select([
    assert_true(Column.gt(col("val"), lit(0))) |> Column.alias_("ok"),
    col("val")
  ]) |> DataFrame.collect()
end)

run.("12c. assert_true (failing condition)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -1}], schema: "val INT")
  df |> DataFrame.select([
    assert_true(Column.gt(col("val"), lit(0))) |> Column.alias_("ok")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. mode / nth_value
# =============================================

run.("13a. mode aggregate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 2}, %{"val" => 3}, %{"val" => 3}, %{"val" => 3}
  ], schema: "val INT")
  DataFrame.select(df, [mode(col("val")) |> Column.alias_("mode_val")]) |> DataFrame.collect()
end)

run.("13b. nth_value in window", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  w = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("id"))])
  |> WindowSpec.rows_between(-999999, 999999)
  df |> DataFrame.select([
    col("id"), col("val"),
    nth_value(col("val"), 2) |> Column.over(w) |> Column.alias_("second_val"),
    nth_value(col("val"), 4) |> Column.over(w) |> Column.alias_("fourth_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Catalog: list_catalogs, list_databases, list_functions
# =============================================

run.("14a. list_catalogs", fn ->
  Catalog.list_catalogs(s)
end)

run.("14b. list_databases", fn ->
  Catalog.list_databases(s)
end)

run.("14c. list_functions (first 5)", fn ->
  {:ok, funcs} = Catalog.list_functions(s)
  Enum.take(funcs, 5)
end)

run.("14d. current_catalog / current_database", fn ->
  cat = Catalog.current_catalog(s)
  db = Catalog.current_database(s)
  {cat, db}
end)

# =============================================
# 15. DataFrame.with_metadata
# =============================================

run.("15. with_metadata", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.with_metadata("val", %{"comment" => "answer to everything"})
  |> DataFrame.schema()
end)

# =============================================
# 16. try_cast edge cases
# =============================================

run.("16a. try_cast incompatible types (should return null)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "hello"}, %{"val" => "42"}, %{"val" => "3.14"}
  ], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "INT") |> Column.alias_("as_int"),
    Column.try_cast(col("val"), "DOUBLE") |> Column.alias_("as_double"),
    Column.try_cast(col("val"), "DATE") |> Column.alias_("as_date")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. DataFrame.sample_by (stratified sampling)
# =============================================

run.("17. sample_by (stratified)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    (Enum.map(1..100, fn _ -> %{"grp" => "A"} end) ++
     Enum.map(1..100, fn _ -> %{"grp" => "B"} end)),
    schema: "grp STRING")
  {:ok, result} = DataFrame.Stat.sample_by(df, "grp", %{"A" => 0.1, "B" => 0.5}, 42) |> DataFrame.collect()
  a_count = Enum.count(result, fn r -> r["grp"] == "A" end)
  b_count = Enum.count(result, fn r -> r["grp"] == "B" end)
  %{a_count: a_count, b_count: b_count, total: length(result)}
end)

IO.puts("=== Round 30 complete ===")
