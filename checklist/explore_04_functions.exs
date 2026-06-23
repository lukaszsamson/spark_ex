# Exploratory: built-in functions edge cases, string/date/math functions
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === String functions ===
str_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    ('Hello World', 1), ('  spaces  ', 2), ('UPPERCASE', 3),
    ('camelCase', 4), ('', 5), (NULL, 6)
  AS t(text, id)
""")

run.("1. String: lpad/rpad", fn ->
  DataFrame.select(str_df, [
    col("text"),
    lpad(col("text"), lit(15), lit("*")) |> Column.alias_("lpadded"),
    rpad(col("text"), lit(15), lit("-")) |> Column.alias_("rpadded")
  ]) |> DataFrame.collect()
end)

run.("2. String: repeat/reverse", fn ->
  DataFrame.select(str_df, [
    col("text"),
    repeat(col("text"), lit(2)) |> Column.alias_("repeated"),
    reverse(col("text")) |> Column.alias_("reversed")
  ]) |> DataFrame.collect()
end)

run.("3. String: translate/overlay", fn ->
  DataFrame.select(str_df, [
    col("text"),
    translate(col("text"), lit("aeiou"), lit("12345")) |> Column.alias_("translated")
  ]) |> DataFrame.collect()
end)

run.("4. String: regexp_replace", fn ->
  DataFrame.select(str_df, [
    col("text"),
    regexp_replace(col("text"), lit("[aeiouAEIOU]"), lit("_")) |> Column.alias_("no_vowels")
  ]) |> DataFrame.collect()
end)

run.("5. String: concat_ws", fn ->
  SparkEx.sql(s, "SELECT 'a' AS c1, 'b' AS c2, 'c' AS c3")
  |> DataFrame.select([
    concat_ws(lit(","), [col("c1"), col("c2"), col("c3")]) |> Column.alias_("joined")
  ])
  |> DataFrame.collect()
end)

run.("6. String: initcap/soundex", fn ->
  SparkEx.sql(s, "SELECT 'hello world foo' AS text")
  |> DataFrame.select([
    initcap(col("text")) |> Column.alias_("initcapped"),
    soundex(col("text")) |> Column.alias_("soundex_val")
  ])
  |> DataFrame.collect()
end)

run.("7. String: locate/instr", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    locate(lit("world"), col("text")) |> Column.alias_("pos")
  ])
  |> DataFrame.collect()
end)

# === Math functions ===
run.("8. Math: log/exp/pow", fn ->
  SparkEx.sql(s, "SELECT 100.0 AS val")
  |> DataFrame.select([
    log(col("val")) |> Column.alias_("ln"),
    log(lit(10.0), col("val")) |> Column.alias_("log10"),
    exp(col("val")) |> Column.alias_("exp_val"),
    pow(col("val"), lit(0.5)) |> Column.alias_("sqrt_via_pow")
  ])
  |> DataFrame.collect()
end)

run.("9. Math: trig functions", fn ->
  SparkEx.sql(s, "SELECT 3.14159265 AS pi_val, 1.0 AS one")
  |> DataFrame.select([
    sin(col("pi_val")) |> Column.alias_("sin_pi"),
    cos(col("pi_val")) |> Column.alias_("cos_pi"),
    tan(col("one")) |> Column.alias_("tan_1"),
    atan2(col("one"), col("one")) |> Column.alias_("atan2_1_1")
  ])
  |> DataFrame.collect()
end)

run.("10. Math: sign/signum/cbrt", fn ->
  SparkEx.sql(s, "SELECT -27.0 AS val")
  |> DataFrame.select([
    signum(col("val")) |> Column.alias_("sign"),
    cbrt(Column.negate(col("val"))) |> Column.alias_("cbrt_27")
  ])
  |> DataFrame.collect()
end)

# === Date/Time functions ===
date_df = SparkEx.sql(s, """
  SELECT DATE '2024-06-15' AS dt, TIMESTAMP '2024-06-15 14:30:45' AS ts
""")

run.("11. Date: year/month/day/quarter", fn ->
  DataFrame.select(date_df, [
    year(col("dt")) |> Column.alias_("yr"),
    month(col("dt")) |> Column.alias_("mo"),
    dayofmonth(col("dt")) |> Column.alias_("day"),
    quarter(col("dt")) |> Column.alias_("qtr"),
    dayofweek(col("dt")) |> Column.alias_("dow"),
    dayofyear(col("dt")) |> Column.alias_("doy"),
    weekofyear(col("dt")) |> Column.alias_("woy")
  ])
  |> DataFrame.collect()
end)

run.("12. Date: add/sub months and days", fn ->
  DataFrame.select(date_df, [
    date_add(col("dt"), lit(30)) |> Column.alias_("plus_30d"),
    date_sub(col("dt"), lit(30)) |> Column.alias_("minus_30d"),
    add_months(col("dt"), lit(3)) |> Column.alias_("plus_3m"),
    months_between(col("dt"), lit("2024-01-01") |> Column.cast("DATE")) |> Column.alias_("months_diff")
  ])
  |> DataFrame.collect()
end)

run.("13. Date: last_day/next_day/trunc", fn ->
  DataFrame.select(date_df, [
    last_day(col("dt")) |> Column.alias_("last_d"),
    next_day(col("dt"), lit("Monday")) |> Column.alias_("next_mon"),
    trunc(col("dt"), lit("MM")) |> Column.alias_("month_start"),
    trunc(col("dt"), lit("YY")) |> Column.alias_("year_start")
  ])
  |> DataFrame.collect()
end)

run.("14. Timestamp: hour/minute/second", fn ->
  DataFrame.select(date_df, [
    hour(col("ts")) |> Column.alias_("hr"),
    minute(col("ts")) |> Column.alias_("min"),
    second(col("ts")) |> Column.alias_("sec")
  ])
  |> DataFrame.collect()
end)

run.("15. Timestamp: from_unixtime/unix_timestamp", fn ->
  SparkEx.sql(s, "SELECT 1718454645 AS epoch")
  |> DataFrame.select([
    from_unixtime(col("epoch")) |> Column.alias_("from_epoch"),
    from_unixtime(col("epoch"), lit("yyyy-MM-dd")) |> Column.alias_("date_only")
  ])
  |> DataFrame.collect()
end)

# === Array functions ===
arr_df = SparkEx.sql(s, "SELECT array(3,1,4,1,5,9,2,6) AS arr, array(1,2,3) AS arr2")

run.("16. Array: sort_array", fn ->
  DataFrame.select(arr_df, [
    sort_array(col("arr")) |> Column.alias_("sorted_asc"),
    sort_array(col("arr"), lit(false)) |> Column.alias_("sorted_desc")
  ]) |> DataFrame.collect()
end)

run.("17. Array: array_distinct/array_except/array_intersect/array_union", fn ->
  DataFrame.select(arr_df, [
    array_distinct(col("arr")) |> Column.alias_("distinct"),
    array_except(col("arr"), col("arr2")) |> Column.alias_("except"),
    array_intersect(col("arr"), col("arr2")) |> Column.alias_("intersect"),
    array_union(col("arr"), col("arr2")) |> Column.alias_("union")
  ]) |> DataFrame.collect()
end)

run.("18. Array: flatten/array_repeat", fn ->
  SparkEx.sql(s, "SELECT array(array(1,2), array(3,4)) AS nested_arr")
  |> DataFrame.select([
    flatten(col("nested_arr")) |> Column.alias_("flat"),
    array_repeat(lit("hello"), lit(3)) |> Column.alias_("repeated")
  ]) |> DataFrame.collect()
end)

run.("19. Array: element_at/array_position/array_remove", fn ->
  DataFrame.select(arr_df, [
    element_at(col("arr"), lit(1)) |> Column.alias_("first"),
    array_position(col("arr"), lit(5)) |> Column.alias_("pos_of_5"),
    array_remove(col("arr"), lit(1)) |> Column.alias_("no_ones"),
    size(col("arr")) |> Column.alias_("arr_size")
  ]) |> DataFrame.collect()
end)

# === Conditional functions ===
run.("20. Conditional: coalesce with multiple nulls", fn ->
  SparkEx.sql(s, "SELECT NULL AS a, NULL AS b, 42 AS c, 99 AS d")
  |> DataFrame.select([
    coalesce([col("a"), col("b"), col("c"), col("d")]) |> Column.alias_("first_non_null")
  ]) |> DataFrame.collect()
end)

run.("21. Conditional: nanvl", fn ->
  SparkEx.sql(s, "SELECT CAST('NaN' AS DOUBLE) AS nan_val, 42.0 AS fallback")
  |> DataFrame.select([
    nanvl(col("nan_val"), col("fallback")) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

run.("22. Conditional: greatest/least with nulls", fn ->
  SparkEx.sql(s, "SELECT 10 AS a, NULL AS b, 30 AS c")
  |> DataFrame.select([
    greatest([col("a"), col("b"), col("c")]) |> Column.alias_("max_val"),
    least([col("a"), col("b"), col("c")]) |> Column.alias_("min_val")
  ]) |> DataFrame.collect()
end)

# === Hash functions ===
run.("23. Hash: sha2/crc32", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    sha2(col("text"), lit(256)) |> Column.alias_("sha256"),
    crc32(col("text")) |> Column.alias_("crc")
  ]) |> DataFrame.collect()
end)

# === Map functions ===
run.("24. Map: map_from_arrays/map_concat", fn ->
  SparkEx.sql(s, """
    SELECT array('a', 'b', 'c') AS keys,
           array(1, 2, 3) AS vals,
           map('x', 10) AS m1,
           map('y', 20) AS m2
  """)
  |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("from_arrays"),
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Functions exploration complete ===")
