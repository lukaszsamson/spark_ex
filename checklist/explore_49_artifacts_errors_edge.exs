# Round 27: Artifacts, error edge cases, unusual patterns, Column expressions
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

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. Artifacts API
# =============================================

run.("1a. artifact_status for nonexistent file", fn ->
  SparkEx.artifact_status(s, ["nonexistent.jar"])
end)

run.("1b. artifact_status for multiple nonexistent files", fn ->
  SparkEx.artifact_status(s, ["a.jar", "b.jar", "c.jar"])
end)

run.("1c. add_artifacts with empty data", fn ->
  SparkEx.add_artifacts(s, [{"test_#{run_id}.txt", "hello world"}])
end)

run.("1d. artifact_status after add", fn ->
  SparkEx.add_artifacts(s, [{"test_check_#{run_id}.txt", "content"}])
  SparkEx.artifact_status(s, ["test_check_#{run_id}.txt"])
end)

# =============================================
# 2. Column expression edge cases
# =============================================

run.("2a. Column.alias_ with metadata map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.select([
    Column.alias_(col("id"), "my_id", %{"comment" => "test metadata"})
  ]) |> DataFrame.collect()
end)

run.("2b. Column.cast chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "42"}], schema: "val STRING")

  df |> DataFrame.select([
    col("val")
    |> Column.cast("INT")
    |> Column.cast("DOUBLE")
    |> Column.cast("STRING")
    |> Column.alias_("chain_casted")
  ]) |> DataFrame.collect()
end)

run.("2c. Column.try_cast (invalid cast returns null)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "hello"},
    %{"val" => "42"}
  ], schema: "val STRING")

  df |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "INT") |> Column.alias_("try_int")
  ]) |> DataFrame.collect()
end)

run.("2d. Column.astype (alias for cast)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")

  df |> DataFrame.select([
    Column.astype(col("val"), "STRING") |> Column.alias_("str_val")
  ]) |> DataFrame.collect()
end)

run.("2e. Column.desc_nulls_first / asc_nulls_last", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => 1}, %{"val" => nil}, %{"val" => 2}
  ], schema: "val INT")

  result1 = df |> DataFrame.sort([Column.desc_nulls_first(col("val"))]) |> DataFrame.collect()
  result2 = df |> DataFrame.sort([Column.asc_nulls_last(col("val"))]) |> DataFrame.collect()
  {result1, result2}
end)

run.("2f. Column equality (immutable identity)", fn ->
  c = col("my_col")
  c2 = col("my_col")
  {c, c2, c == c2}
end)

run.("2g. Nested Column arithmetic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 3, "c" => 2}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    Column.plus(Column.multiply(col("a"), col("b")), Column.mod(col("a"), col("c")))
    |> Column.alias_("complex_expr")
  ]) |> DataFrame.collect()
end)

run.("2h. Column.eqNullSafe (null-safe equality)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1},
    %{"a" => nil, "b" => nil},
    %{"a" => 1, "b" => nil}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    col("a"), col("b"),
    Column.eq(col("a"), col("b")) |> Column.alias_("eq"),
    Column.eq_null_safe(col("a"), col("b")) |> Column.alias_("eqns")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Edge case patterns
# =============================================

run.("3a. select with expressions and aliases", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 10}], schema: "x INT")

  df |> DataFrame.select([
    col("x"),
    Column.plus(col("x"), lit(1)) |> Column.alias_("x_plus_1"),
    Column.multiply(col("x"), lit(2)) |> Column.alias_("x_times_2"),
    Column.negate(col("x")) |> Column.alias_("neg_x"),
    Column.not_(Column.gt(col("x"), lit(5))) |> Column.alias_("not_gt5")
  ]) |> DataFrame.collect()
end)

run.("3b. chained filter operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => rem(i, 5)} end),
    schema: "id INT, val INT")

  df
  |> DataFrame.filter(Column.gt(col("id"), lit(5)))
  |> DataFrame.filter(Column.lt(col("id"), lit(15)))
  |> DataFrame.filter(Column.eq(col("val"), lit(0)))
  |> DataFrame.collect()
end)

run.("3c. withColumn (add computed column)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 3, "y" => 4}
  ], schema: "x INT, y INT")

  df
  |> DataFrame.with_column("hyp", sqrt(Column.plus(
    Column.multiply(col("x"), col("x")),
    Column.multiply(col("y"), col("y"))
  )))
  |> DataFrame.collect()
end)

run.("3d. withColumn chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 10}], schema: "val INT")

  df
  |> DataFrame.with_column("doubled", Column.multiply(col("val"), lit(2)))
  |> DataFrame.with_column("tripled", Column.multiply(col("val"), lit(3)))
  |> DataFrame.with_column("label", lit("test"))
  |> DataFrame.collect()
end)

run.("3e. drop column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.drop(["b"]) |> DataFrame.collect()
end)

run.("3f. drop multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4}
  ], schema: "a INT, b INT, c INT, d INT")

  df |> DataFrame.drop(["b", "d"]) |> DataFrame.collect()
end)

run.("3g. select star (all columns) + additional", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")

  df |> DataFrame.select([
    col("*"),
    Column.multiply(col("id"), lit(10)) |> Column.alias_("id_x10")
  ]) |> DataFrame.collect()
end)

run.("3h. DataFrame.limit vs head (collect behavior)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  limited = df |> DataFrame.limit(3) |> DataFrame.collect()
  headed = DataFrame.head(df, 3)
  {limited, headed}
end)

# =============================================
# 4. Error handling edge cases
# =============================================

run.("4a. SQL syntax error", fn ->
  SparkEx.sql(s, "SELCT * FORM invalid") |> DataFrame.collect()
end)

run.("4b. non-existent table", fn ->
  SparkEx.sql(s, "SELECT * FROM nonexistent_table_#{run_id}") |> DataFrame.collect()
end)

run.("4c. division by zero (non-try)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 10, "b" => 0}], schema: "a INT, b INT")
  df |> DataFrame.select([
    Column.divide(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4d. cast overflow (INT -> TINYINT)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 300}], schema: "val INT")
  df |> DataFrame.select([
    Column.cast(col("val"), "TINYINT") |> Column.alias_("tiny")
  ]) |> DataFrame.collect()
end)

run.("4e. invalid column reference", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([col("nonexistent")]) |> DataFrame.collect()
end)

run.("4f. double collect (should work)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  first = DataFrame.collect(df)
  second = DataFrame.collect(df)
  {first, second}
end)

# =============================================
# 5. String function edge cases
# =============================================

run.("5a. concat_ws with multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "hello", "b" => "world", "c" => "!"}
  ], schema: "a STRING, b STRING, c STRING")

  df |> DataFrame.select([
    concat_ws("-", [col("a"), col("b"), col("c")]) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("5b. regexp_extract with group", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "abc-123-def"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    regexp_extract(col("text"), "(\\d+)", 1) |> Column.alias_("digits")
  ]) |> DataFrame.collect()
end)

run.("5c. regexp_replace", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World 123"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    regexp_replace(col("text"), "\\d+", "XXX") |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

run.("5d. lpad and rpad", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "42"}
  ], schema: "val STRING")

  df |> DataFrame.select([
    lpad(col("val"), 5, "0") |> Column.alias_("lpadded"),
    rpad(col("val"), 5, "*") |> Column.alias_("rpadded")
  ]) |> DataFrame.collect()
end)

run.("5e. repeat and reverse", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "abc"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    repeat(col("text"), 3) |> Column.alias_("repeated"),
    reverse(col("text")) |> Column.alias_("reversed")
  ]) |> DataFrame.collect()
end)

run.("5f. translate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "hello"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    translate(col("text"), "helo", "HELO") |> Column.alias_("translated")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Date/time function edge cases
# =============================================

run.("6a. date_add, date_sub, datediff", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2025-06-15"}
  ], schema: "dt STRING")

  df |> DataFrame.select([
    date_add(Column.cast(col("dt"), "DATE"), 10) |> Column.alias_("plus10"),
    date_sub(Column.cast(col("dt"), "DATE"), 5) |> Column.alias_("minus5"),
    datediff(Column.cast(col("dt"), "DATE"), lit(~D[2025-01-01])) |> Column.alias_("diff")
  ]) |> DataFrame.collect()
end)

run.("6b. year, month, dayofmonth, hour, minute, second", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts" => "2025-06-15 14:30:45"}
  ], schema: "ts STRING")

  ts = Column.cast(col("ts"), "TIMESTAMP")
  df |> DataFrame.select([
    year(ts) |> Column.alias_("yr"),
    month(ts) |> Column.alias_("mo"),
    dayofmonth(ts) |> Column.alias_("dy"),
    hour(ts) |> Column.alias_("hr"),
    minute(ts) |> Column.alias_("mn"),
    second(ts) |> Column.alias_("sc")
  ]) |> DataFrame.collect()
end)

run.("6c. dayofweek, dayofyear, weekofyear", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2025-06-15"}
  ], schema: "dt STRING")

  dt = Column.cast(col("dt"), "DATE")
  df |> DataFrame.select([
    dayofweek(dt) |> Column.alias_("dow"),
    dayofyear(dt) |> Column.alias_("doy"),
    weekofyear(dt) |> Column.alias_("woy")
  ]) |> DataFrame.collect()
end)

run.("6d. months_between", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"d1" => "2025-01-15", "d2" => "2025-06-20"}
  ], schema: "d1 STRING, d2 STRING")

  df |> DataFrame.select([
    months_between(
      Column.cast(col("d2"), "TIMESTAMP"),
      Column.cast(col("d1"), "TIMESTAMP")
    ) |> Column.alias_("months")
  ]) |> DataFrame.collect()
end)

run.("6e. next_day and last_day", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2025-06-15"}
  ], schema: "dt STRING")

  dt = Column.cast(col("dt"), "DATE")
  df |> DataFrame.select([
    next_day(dt, "Monday") |> Column.alias_("next_mon"),
    last_day(dt) |> Column.alias_("last_day")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Array function edge cases
# =============================================

run.("7a. array_contains", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_contains(col("arr"), 3) |> Column.alias_("has3"),
    array_contains(col("arr"), 99) |> Column.alias_("has99")
  ]) |> DataFrame.collect()
end)

run.("7b. array_distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 2, 3, 3, 3]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_distinct(col("arr")) |> Column.alias_("distinct_arr")
  ]) |> DataFrame.collect()
end)

run.("7c. array_intersect, array_union, array_except", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [2, 3, 4]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")

  df |> DataFrame.select([
    array_intersect(col("a"), col("b")) |> Column.alias_("intersect"),
    array_union(col("a"), col("b")) |> Column.alias_("union"),
    array_except(col("a"), col("b")) |> Column.alias_("except")
  ]) |> DataFrame.collect()
end)

run.("7d. array_position and element_at", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_position(col("arr"), 30) |> Column.alias_("pos"),
    element_at(col("arr"), 2) |> Column.alias_("second")
  ]) |> DataFrame.collect()
end)

run.("7e. array_repeat and array_zip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 42, "arr1" => [1, 2], "arr2" => ["a", "b"]}
  ], schema: "val INT, arr1 ARRAY<INT>, arr2 ARRAY<STRING>")

  df |> DataFrame.select([
    array_repeat(col("val"), 3) |> Column.alias_("repeated"),
    arrays_zip([col("arr1"), col("arr2")]) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

run.("7f. flatten and array_compact", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"nested" => [[1, 2], [3, 4], [5]]}
  ], schema: "nested ARRAY<ARRAY<INT>>")

  df |> DataFrame.select([
    flatten(col("nested")) |> Column.alias_("flat")
  ]) |> DataFrame.collect()
end)

run.("7g. array_compact (remove nulls)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, nil, 3, nil, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_compact(col("arr")) |> Column.alias_("compacted")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Map function edge cases
# =============================================

run.("8a. map_keys, map_values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2, "c" => 3}}
  ], schema: "m MAP<STRING, INT>")

  df |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values")
  ]) |> DataFrame.collect()
end)

run.("8b. map_from_arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"keys" => ["x", "y", "z"], "vals" => [1, 2, 3]}
  ], schema: "keys ARRAY<STRING>, vals ARRAY<INT>")

  df |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("map")
  ]) |> DataFrame.collect()
end)

run.("8c. map_concat", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m1" => %{"a" => 1}, "m2" => %{"b" => 2}}
  ], schema: "m1 MAP<STRING, INT>, m2 MAP<STRING, INT>")

  df |> DataFrame.select([
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Conditional/null handling
# =============================================

run.("9a. coalesce function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil, "c" => 42}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    coalesce([col("a"), col("b"), col("c")]) |> Column.alias_("first_nonnull")
  ]) |> DataFrame.collect()
end)

run.("9b. nanvl", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "NaN"}
  ], schema: "val STRING")

  df |> DataFrame.select([
    nanvl(Column.cast(col("val"), "DOUBLE"), lit(0.0)) |> Column.alias_("safe")
  ]) |> DataFrame.collect()
end)

run.("9c. nullif and ifnull / nvl", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1},
    %{"a" => 1, "b" => 2}
  ], schema: "a INT, b INT")

  df |> DataFrame.select([
    col("a"), col("b"),
    nullif(col("a"), col("b")) |> Column.alias_("nullif_ab"),
    ifnull(col("a"), col("b")) |> Column.alias_("ifnull_ab")
  ]) |> DataFrame.collect()
end)

run.("9d. greatest and least with multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 20, "c" => 5}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    greatest([col("a"), col("b"), col("c")]) |> Column.alias_("max_val"),
    least([col("a"), col("b"), col("c")]) |> Column.alias_("min_val")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 27 tests complete ===")
