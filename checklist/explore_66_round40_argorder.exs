# Round 40: Search for argument-order bugs similar to EX-108 (ltrim/rtrim/trim)
# Focus on functions that take mixed string+column args where order matters
alias SparkEx.{DataFrame, Column, GroupedData}
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
# 1. Verify lpad/rpad arg order (known to work from round 34b)
# =============================================

run.("1a. lpad (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hi"}], schema: "txt STRING")
  df |> DataFrame.select([
    lpad(col("txt"), 10, "*") |> Column.alias_("padded")
  ]) |> DataFrame.collect()
end)

run.("1b. lpad (SQL for comparison)", fn ->
  SparkEx.sql(s, "SELECT lpad('hi', 10, '*') AS padded") |> DataFrame.collect()
end)

# =============================================
# 2. regexp_replace arg order
# =============================================

run.("2a. regexp_replace (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hello123world456"}], schema: "txt STRING")
  df |> DataFrame.select([
    regexp_replace(col("txt"), lit("\\d+"), lit("NUM")) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

run.("2b. regexp_replace (SQL for comparison)", fn ->
  SparkEx.sql(s, "SELECT regexp_replace('hello123world456', '\\\\d+', 'NUM') AS replaced") |> DataFrame.collect()
end)

# =============================================
# 3. translate arg order
# =============================================

run.("3a. translate (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hello"}], schema: "txt STRING")
  df |> DataFrame.select([
    translate(col("txt"), lit("helo"), lit("HELO")) |> Column.alias_("translated")
  ]) |> DataFrame.collect()
end)

run.("3b. translate (SQL)", fn ->
  SparkEx.sql(s, "SELECT translate('hello', 'helo', 'HELO') AS translated") |> DataFrame.collect()
end)

# =============================================
# 4. instr arg order
# =============================================

run.("4a. instr (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "hello world"}], schema: "txt STRING")
  df |> DataFrame.select([
    instr(col("txt"), "world") |> Column.alias_("pos")
  ]) |> DataFrame.collect()
end)

run.("4b. instr (SQL)", fn ->
  SparkEx.sql(s, "SELECT instr('hello world', 'world') AS pos") |> DataFrame.collect()
end)

# =============================================
# 5. date_format arg order
# =============================================

run.("5a. date_format (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"dt" => "2024-06-15"}], schema: "dt STRING")
  df |> DataFrame.select([
    date_format(col("dt"), "MM/dd/yyyy") |> Column.alias_("formatted")
  ]) |> DataFrame.collect()
end)

run.("5b. date_format (SQL)", fn ->
  SparkEx.sql(s, "SELECT date_format('2024-06-15', 'MM/dd/yyyy') AS formatted") |> DataFrame.collect()
end)

# =============================================
# 6. from_unixtime arg order
# =============================================

run.("6a. from_unixtime (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"epoch" => 1718443800}], schema: "epoch LONG")
  df |> DataFrame.select([
    from_unixtime(col("epoch"), "yyyy-MM-dd") |> Column.alias_("dt")
  ]) |> DataFrame.collect()
end)

run.("6b. from_unixtime (SQL)", fn ->
  SparkEx.sql(s, "SELECT from_unixtime(1718443800, 'yyyy-MM-dd') AS dt") |> DataFrame.collect()
end)

# =============================================
# 7. Test all two-arg string functions for arg order
# =============================================

run.("7a. substring_index (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "a.b.c.d"}], schema: "txt STRING")
  df |> DataFrame.select([
    substring_index(col("txt"), ".", 2) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("7b. substring_index (SQL)", fn ->
  SparkEx.sql(s, "SELECT substring_index('a.b.c.d', '.', 2) AS result") |> DataFrame.collect()
end)

# =============================================
# 8. Like and rlike comparison
# =============================================

run.("8a. like_ (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello"}, %{"txt" => "world"}, %{"txt" => "help"}
  ], schema: "txt STRING")
  df |> DataFrame.filter(like_(col("txt"), lit("hel%"))) |> DataFrame.collect()
end)

run.("8b. rlike (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello"}, %{"txt" => "world"}, %{"txt" => "help"}
  ], schema: "txt STRING")
  df |> DataFrame.filter(Column.rlike(col("txt"), "hel.*")) |> DataFrame.collect()
end)

# =============================================
# 9. split arg order
# =============================================

run.("9a. split (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "a-b-c"}], schema: "txt STRING")
  df |> DataFrame.select([
    split(col("txt"), "-") |> Column.alias_("parts")
  ]) |> DataFrame.collect()
end)

run.("9b. split (SQL)", fn ->
  SparkEx.sql(s, "SELECT split('a-b-c', '-') AS parts") |> DataFrame.collect()
end)

# =============================================
# 10. Verify width_bucket arg order
# =============================================

run.("10a. width_bucket (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 5.5}, %{"val" => 15.5}, %{"val" => 25.5}
  ], schema: "val DOUBLE")
  df |> DataFrame.select([
    col("val"),
    width_bucket(col("val"), 0, 30, 3) |> Column.alias_("bucket")
  ]) |> DataFrame.collect()
end)

run.("10b. width_bucket (SQL)", fn ->
  SparkEx.sql(s, """
    SELECT val, width_bucket(val, 0, 30, 3) AS bucket
    FROM VALUES (5.5), (15.5), (25.5) AS t(val)
  """) |> DataFrame.collect()
end)

# =============================================
# 11. Verify percentile_approx
# =============================================

run.("11. percentile_approx (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  df |> DataFrame.select([
    percentile_approx(col("val"), 0.5, 100) |> Column.alias_("median")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Verify concat_ws arg order ({:lit_then_cols, 1})
# =============================================

run.("12a. concat_ws (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "hello", "b" => "world", "c" => "foo"}
  ], schema: "a STRING, b STRING, c STRING")
  df |> DataFrame.select([
    concat_ws("-", [col("a"), col("b"), col("c")]) |> Column.alias_("joined")
  ]) |> DataFrame.collect()
end)

run.("12b. concat_ws (SQL)", fn ->
  SparkEx.sql(s, "SELECT concat_ws('-', 'hello', 'world', 'foo') AS joined") |> DataFrame.collect()
end)

# =============================================
# 13. Verify next_day ({:col_lit, 1})
# =============================================

run.("13a. next_day (SparkEx)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2024-06-15"}
  ], schema: "dt STRING")
  df |> DataFrame.select([
    next_day(col("dt"), "Monday") |> Column.alias_("next_monday")
  ]) |> DataFrame.collect()
end)

run.("13b. next_day (SQL)", fn ->
  SparkEx.sql(s, "SELECT next_day('2024-06-15', 'Monday') AS next_monday") |> DataFrame.collect()
end)

# =============================================
# 14. Edge: deeply nested struct access vs getField
# =============================================

run.("14a. dot notation vs getField", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('inner', named_struct('val', 42)) AS outer
  """)
  r1 = df |> DataFrame.select([
    col("outer.inner.val") |> Column.alias_("dot_access")
  ]) |> DataFrame.collect()

  r2 = df |> DataFrame.select([
    Column.get_field(Column.get_field(col("outer"), "inner"), "val") |> Column.alias_("get_field_access")
  ]) |> DataFrame.collect()

  {r1, r2}
end)

# =============================================
# 15. Edge: Column.contains / startswith / endswith
# =============================================

run.("15. contains / startswith / endswith", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world"}, %{"txt" => "goodbye world"}, %{"txt" => "hello there"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    col("txt"),
    Column.contains(col("txt"), lit("hello")) |> Column.alias_("has_hello"),
    Column.startswith(col("txt"), lit("hello")) |> Column.alias_("starts_hello"),
    Column.endswith(col("txt"), lit("world")) |> Column.alias_("ends_world")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Edge: Column.eq_null_safe
# =============================================

run.("16. eq_null_safe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1},
    %{"a" => nil, "b" => nil},
    %{"a" => 1, "b" => nil},
    %{"a" => nil, "b" => 1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.eq(col("a"), col("b")) |> Column.alias_("eq"),
    Column.eq_null_safe(col("a"), col("b")) |> Column.alias_("eq_safe")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. Edge: Column.cast chain (cast → cast → cast)
# =============================================

run.("17. triple cast chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "123.456"}], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    col("val")
    |> Column.cast("DOUBLE")
    |> Column.cast("INT")
    |> Column.cast("STRING")
    |> Column.alias_("roundtripped")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. Edge: drop_duplicates with subset
# =============================================

run.("18. drop_duplicates with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x", "c" => 100},
    %{"a" => 1, "b" => "y", "c" => 200},
    %{"a" => 2, "b" => "x", "c" => 300}
  ], schema: "a INT, b STRING, c INT")
  df |> DataFrame.drop_duplicates(["a"]) |> DataFrame.sort([asc(col("a"))]) |> DataFrame.collect()
end)

IO.puts("=== Round 40 complete ===")
