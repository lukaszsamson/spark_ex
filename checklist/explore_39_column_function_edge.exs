# Round 16: Column operations edge cases, string/date/array function edge cases,
# aggregation edge cases, window frame edge cases
alias SparkEx.{DataFrame, Column}
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

# =============================================
# Column arithmetic edge cases
# =============================================

run.("1. Column.mod (modulo)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 17, "b" => 5},
    %{"a" => -17, "b" => 5},
    %{"a" => 10, "b" => 0}
  ], schema: "a INT, b INT")

  df
  |> DataFrame.select([
    col("a"), col("b"),
    Column.mod(col("a"), col("b")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

run.("2. Column.negate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 42}, %{"val" => -10}, %{"val" => 0}, %{"val" => nil}
  ], schema: "val INT")

  df
  |> DataFrame.select([
    col("val"),
    Column.negate(col("val")) |> Column.alias_("negated")
  ])
  |> DataFrame.collect()
end)

run.("3. Column.pow with edge values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"base" => 2.0, "exp" => 10.0},
    %{"base" => 0.0, "exp" => 0.0},
    %{"base" => -1.0, "exp" => 0.5},
    %{"base" => 10.0, "exp" => 308.0}
  ], schema: "base DOUBLE, exp DOUBLE")

  df
  |> DataFrame.select([
    col("base"), col("exp"),
    Column.pow(col("base"), col("exp")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

run.("4. Column.divide by zero (float)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1.0, "b" => 0.0},
    %{"a" => 0.0, "b" => 0.0},
    %{"a" => -1.0, "b" => 0.0}
  ], schema: "a DOUBLE, b DOUBLE")

  df
  |> DataFrame.select([
    col("a"),
    Column.divide(col("a"), col("b")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

run.("5. Column.divide by zero (int)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 0}
  ], schema: "a INT, b INT")

  df
  |> DataFrame.select([
    Column.divide(col("a"), col("b")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

# =============================================
# String function edge cases
# =============================================

run.("6. substr with negative start", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    Column.substr(col("text"), lit(-5), lit(5)) |> Column.alias_("last5")
  ])
  |> DataFrame.collect()
end)

run.("7. overlay function", fn ->
  SparkEx.sql(s, "SELECT 'hello world' AS text")
  |> DataFrame.select([
    overlay(col("text"), lit("SPARK"), lit(7), lit(5)) |> Column.alias_("replaced")
  ])
  |> DataFrame.collect()
end)

run.("8. sentences function", fn ->
  SparkEx.sql(s, "SELECT 'Hello world. How are you? Fine, thanks!' AS text")
  |> DataFrame.select([
    sentences(col("text"), lit("en"), lit("US")) |> Column.alias_("parsed")
  ])
  |> DataFrame.collect()
end)

run.("9. levenshtein function", fn ->
  SparkEx.sql(s, "SELECT 'kitten' AS a, 'sitting' AS b")
  |> DataFrame.select([
    levenshtein(col("a"), col("b")) |> Column.alias_("dist")
  ])
  |> DataFrame.collect()
end)

run.("10. regexp_replace with groups", fn ->
  SparkEx.sql(s, "SELECT '2025-06-15' AS dt")
  |> DataFrame.select([
    regexp_replace(col("dt"), lit("(\\d{4})-(\\d{2})-(\\d{2})"), lit("$3/$2/$1"))
    |> Column.alias_("reformatted")
  ])
  |> DataFrame.collect()
end)

run.("11. split with limit", fn ->
  SparkEx.sql(s, "SELECT 'a,b,c,d,e' AS csv")
  |> DataFrame.select([
    split(col("csv"), lit(","), lit(3)) |> Column.alias_("parts")
  ])
  |> DataFrame.collect()
end)

run.("12. array_join with null replacement", fn ->
  SparkEx.sql(s, "SELECT array('a', NULL, 'c', NULL, 'e') AS arr")
  |> DataFrame.select([
    array_join(col("arr"), lit(","), lit("NULL")) |> Column.alias_("joined")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Date/Time function edge cases
# =============================================

run.("13. make_timestamp function", fn ->
  SparkEx.sql(s, "SELECT 1 AS dummy")
  |> DataFrame.select([
    make_timestamp([lit(2025), lit(6), lit(15), lit(10), lit(30), lit(0)])
    |> Column.alias_("ts")
  ])
  |> DataFrame.collect()
end)

run.("14. convert_timezone", fn ->
  SparkEx.sql(s, "SELECT TIMESTAMP '2025-06-15 10:30:00' AS ts")
  |> DataFrame.select([
    col("ts"),
    convert_timezone(lit("UTC"), lit("America/New_York"), col("ts"))
    |> Column.alias_("ny_time")
  ])
  |> DataFrame.collect()
end)

run.("15. date arithmetic edge: leap year", fn ->
  SparkEx.sql(s, "SELECT DATE '2024-02-29' AS leap_day")
  |> DataFrame.select([
    col("leap_day"),
    add_months(col("leap_day"), lit(12)) |> Column.alias_("next_year"),
    date_add(col("leap_day"), lit(365)) |> Column.alias_("plus_365"),
    last_day(col("leap_day")) |> Column.alias_("month_end")
  ])
  |> DataFrame.collect()
end)

run.("16. months_between edge cases", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (DATE '2025-01-31', DATE '2025-02-28'),
      (DATE '2024-02-29', DATE '2025-02-28'),
      (DATE '2025-01-01', DATE '2025-01-01')
    AS t(d1, d2)
  """)
  |> DataFrame.select([
    col("d1"), col("d2"),
    months_between(Column.cast(col("d2"), "TIMESTAMP"), Column.cast(col("d1"), "TIMESTAMP"))
    |> Column.alias_("months")
  ])
  |> DataFrame.collect()
end)

run.("17. next_day edge", fn ->
  SparkEx.sql(s, "SELECT DATE '2025-06-15' AS dt")
  |> DataFrame.select([
    next_day(col("dt"), lit("Monday")) |> Column.alias_("next_mon"),
    next_day(col("dt"), lit("Sunday")) |> Column.alias_("next_sun")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Array function edge cases
# =============================================

run.("18. array_sort with comparator", fn ->
  SparkEx.sql(s, "SELECT array(3, 1, 4, 1, 5, 9) AS arr")
  |> DataFrame.select([
    array_sort(col("arr")) |> Column.alias_("sorted")
  ])
  |> DataFrame.collect()
end)

run.("19. array_sort with custom comparator", fn ->
  SparkEx.sql(s, "SELECT array(3, 1, 4, 1, 5, 9) AS arr")
  |> DataFrame.select([
    array_sort(col("arr"), fn a, b -> Column.minus(b, a) end) |> Column.alias_("desc_sorted")
  ])
  |> DataFrame.collect()
end)

run.("20. arrays_zip", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS a, array('x', 'y', 'z') AS b")
  |> DataFrame.select([
    arrays_zip([col("a"), col("b")]) |> Column.alias_("zipped")
  ])
  |> DataFrame.collect()
end)

run.("21. arrays_zip unequal lengths", fn ->
  SparkEx.sql(s, "SELECT array(1, 2) AS short, array('a', 'b', 'c') AS long")
  |> DataFrame.select([
    arrays_zip([col("short"), col("long")]) |> Column.alias_("zipped")
  ])
  |> DataFrame.collect()
end)

run.("22. slice with negative index", fn ->
  SparkEx.sql(s, "SELECT array(10, 20, 30, 40, 50) AS arr")
  |> DataFrame.select([
    slice(col("arr"), lit(-3), lit(2)) |> Column.alias_("sliced")
  ])
  |> DataFrame.collect()
end)

run.("23. array_repeat", fn ->
  SparkEx.sql(s, "SELECT 'hello' AS word")
  |> DataFrame.select([
    array_repeat(col("word"), lit(3)) |> Column.alias_("repeated")
  ])
  |> DataFrame.collect()
end)

run.("24. flatten nested arrays with nulls", fn ->
  SparkEx.sql(s, "SELECT array(array(1, 2), NULL, array(5, 6)) AS nested")
  |> DataFrame.select([
    flatten(col("nested")) |> Column.alias_("flat")
  ])
  |> DataFrame.collect()
end)

run.("25. element_at with out-of-bounds index", fn ->
  SparkEx.sql(s, "SELECT array(10, 20, 30) AS arr")
  |> DataFrame.select([
    element_at(col("arr"), lit(1)) |> Column.alias_("first"),
    element_at(col("arr"), lit(3)) |> Column.alias_("last"),
    element_at(col("arr"), lit(5)) |> Column.alias_("oob"),
    element_at(col("arr"), lit(-1)) |> Column.alias_("neg1")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Aggregation edge cases
# =============================================

run.("26. count_distinct with nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "a"}, %{"val" => "b"}, %{"val" => "a"},
    %{"val" => nil}, %{"val" => nil}, %{"val" => "c"}
  ], schema: "val STRING")

  df
  |> DataFrame.select([
    count(col("val")) |> Column.alias_("cnt"),
    count_distinct(col("val")) |> Column.alias_("cnt_distinct"),
    count(lit(1)) |> Column.alias_("cnt_all")
  ])
  |> DataFrame.collect()
end)

run.("27. any_value aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 10},
    %{"grp" => "a", "val" => 20},
    %{"grp" => "b", "val" => 30}
  ], schema: "grp STRING, val INT")

  df
  |> DataFrame.group_by([col("grp")])
  |> SparkEx.GroupedData.agg([
    any_value(col("val")) |> Column.alias_("any_val")
  ])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

run.("28. mode aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "a"}, %{"val" => "b"}, %{"val" => "a"},
    %{"val" => "c"}, %{"val" => "a"}, %{"val" => "b"}
  ], schema: "val STRING")

  df
  |> DataFrame.select([
    mode(col("val")) |> Column.alias_("most_frequent")
  ])
  |> DataFrame.collect()
end)

run.("29. collect_set vs collect_list with nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 1},
    %{"grp" => "a", "val" => 2},
    %{"grp" => "a", "val" => 1},
    %{"grp" => "a", "val" => nil},
    %{"grp" => "a", "val" => nil}
  ], schema: "grp STRING, val INT")

  df
  |> DataFrame.group_by([col("grp")])
  |> SparkEx.GroupedData.agg([
    collect_list(col("val")) |> Column.alias_("as_list"),
    collect_set(col("val")) |> Column.alias_("as_set")
  ])
  |> DataFrame.collect()
end)

run.("30. percentile_approx with multiple percentiles", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")

  df
  |> DataFrame.select([
    percentile_approx(col("val"), lit(0.5), lit(100)) |> Column.alias_("median"),
    percentile_approx(col("val"), lit(0.95), lit(100)) |> Column.alias_("p95")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Window function edge cases
# =============================================

run.("31. window: rows_between unbounded on both sides", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
    |> SparkEx.WindowSpec.rows_between(:unbounded, :unbounded)

  df
  |> DataFrame.select([
    col("id"), col("val"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("total")
  ])
  |> DataFrame.collect()
end)

run.("32. window: lag/lead with default values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20},
    %{"id" => 3, "val" => 30}
  ], schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])

  df
  |> DataFrame.select([
    col("id"), col("val"),
    lag(col("val"), offset: 1, default: -1) |> Column.over(w) |> Column.alias_("prev"),
    lead(col("val"), offset: 1, default: -1) |> Column.over(w) |> Column.alias_("next")
  ])
  |> DataFrame.collect()
end)

run.("33. window: ntile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  w = SparkEx.Window.order_by([asc(col("id"))])

  df
  |> DataFrame.select([
    col("id"),
    ntile(lit(4)) |> Column.over(w) |> Column.alias_("quartile")
  ])
  |> DataFrame.collect()
end)

run.("34. window: range_between with numeric values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.order_by([asc(col("id"))])
    |> SparkEx.WindowSpec.range_between(-2, 2)

  df
  |> DataFrame.select([
    col("id"), col("val"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("range_sum")
  ])
  |> DataFrame.collect()
end)

run.("35. window: multiple window specs in same query", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..12, fn i ->
      %{"dept" => "d#{rem(i, 3)}", "id" => i, "val" => i * 10}
    end),
    schema: "dept STRING, id INT, val INT")

  w_dept = SparkEx.Window.partition_by([col("dept")])
    |> SparkEx.WindowSpec.order_by([asc(col("id"))])

  w_global = SparkEx.Window.order_by([asc(col("id"))])

  df
  |> DataFrame.select([
    col("dept"), col("id"), col("val"),
    row_number() |> Column.over(w_dept) |> Column.alias_("dept_rank"),
    row_number() |> Column.over(w_global) |> Column.alias_("global_rank"),
    sum(col("val")) |> Column.over(w_dept) |> Column.alias_("dept_running_sum")
  ])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# Conditional expression edge cases
# =============================================

run.("36. coalesce with all nulls", fn ->
  SparkEx.sql(s, "SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS b")
  |> DataFrame.select([
    coalesce([col("a"), col("b"), lit(99)]) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

run.("37. greatest/least with mixed types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 20, "c" => nil}
  ], schema: "a INT, b INT, c INT")

  df
  |> DataFrame.select([
    greatest([col("a"), col("b"), col("c")]) |> Column.alias_("max"),
    least([col("a"), col("b"), col("c")]) |> Column.alias_("min")
  ])
  |> DataFrame.collect()
end)

run.("38. nanvl edge cases", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (CAST('NaN' AS DOUBLE), 42.0),
      (1.5, CAST('NaN' AS DOUBLE)),
      (CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))
    AS t(a, b)
  """)
  |> DataFrame.select([
    col("a"), col("b"),
    nanvl(col("a"), col("b")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Map function edge cases
# =============================================

run.("39. map_concat", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2) AS m1, map('c', 3, 'd', 4) AS m2")
  |> DataFrame.select([
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged")
  ])
  |> DataFrame.collect()
end)

run.("40. map_from_arrays with nulls", fn ->
  SparkEx.sql(s, "SELECT array('a', 'b', 'c') AS keys, array(1, NULL, 3) AS vals")
  |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

run.("41. map_keys and map_values", fn ->
  SparkEx.sql(s, "SELECT map('x', 10, 'y', 20, 'z', 30) AS m")
  |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("vals")
  ])
  |> DataFrame.collect()
end)

run.("42. map_filter", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([
    map_filter(col("m"), fn k, v -> Column.gt(v, lit(1)) end)
    |> Column.alias_("filtered")
  ])
  |> DataFrame.collect()
end)

run.("43. transform_keys and transform_values", fn ->
  SparkEx.sql(s, "SELECT map('hello', 1, 'world', 2) AS m")
  |> DataFrame.select([
    transform_keys(col("m"), fn k, _v -> upper(k) end) |> Column.alias_("upper_keys"),
    transform_values(col("m"), fn _k, v -> Column.multiply(v, lit(10)) end) |> Column.alias_("scaled")
  ])
  |> DataFrame.collect()
end)

# =============================================
# JSON function edge cases
# =============================================

run.("44. get_json_object deeply nested", fn ->
  SparkEx.sql(s, """
    SELECT '{"a":{"b":{"c":{"d":42}}}}' AS json_str
  """)
  |> DataFrame.select([
    get_json_object(col("json_str"), lit("$.a.b.c.d")) |> Column.alias_("deep_val")
  ])
  |> DataFrame.collect()
end)

run.("45. json_tuple multiple keys", fn ->
  SparkEx.sql(s, """
    SELECT '{"name":"Alice","age":30,"city":"NYC"}' AS json_str
  """)
  |> DataFrame.select([
    json_tuple(col("json_str"), ["name", "age", "city"])
  ])
  |> DataFrame.collect()
end)

run.("46. to_json on nested struct", fn ->
  SparkEx.sql(s, """
    SELECT named_struct(
      'name', 'Alice',
      'scores', array(90, 85, 92),
      'addr', named_struct('city', 'NYC', 'zip', '10001')
    ) AS data
  """)
  |> DataFrame.select([
    to_json(col("data")) |> Column.alias_("json_str")
  ])
  |> DataFrame.collect()
end)

run.("47. schema_of_json", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.select([
    schema_of_json(lit("{\"name\":\"Alice\",\"scores\":[1,2,3],\"active\":true}"))
    |> Column.alias_("schema")
  ])
  |> DataFrame.collect()
end)

# =============================================
# Misc edge cases
# =============================================

run.("48. spark_partition_id in create_dataframe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  df
  |> DataFrame.select([
    col("id"),
    spark_partition_id() |> Column.alias_("part"),
    monotonically_increasing_id() |> Column.alias_("mono_id")
  ])
  |> DataFrame.collect()
end)

run.("49. Column.cast chain (double cast)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "123.456"}
  ], schema: "val STRING")

  df
  |> DataFrame.select([
    col("val"),
    col("val") |> Column.cast("DOUBLE") |> Column.cast("INT") |> Column.alias_("as_int"),
    col("val") |> Column.cast("DOUBLE") |> Column.cast("DECIMAL(10,2)") |> Column.alias_("as_dec")
  ])
  |> DataFrame.collect()
end)

run.("50. Column.isin with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")

  df
  |> DataFrame.filter(Column.isin(col("id"), []))
  |> DataFrame.collect()
end)

IO.puts("=== Column/function edge case tests complete ===")
