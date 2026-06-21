# Round 28: Functions with literal args via lit() vs bare values, remaining edge cases
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1]

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
# 1. Verify to_expr workarounds with lit()
# =============================================

run.("1a. array_contains with lit(3)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_contains(col("arr"), lit(3)) |> Column.alias_("has3")
  ]) |> DataFrame.collect()
end)

run.("1b. array_position with lit(30)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    array_position(col("arr"), lit(30)) |> Column.alias_("pos")
  ]) |> DataFrame.collect()
end)

run.("1c. regexp_replace with lit() args", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World 123"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    regexp_replace(col("text"), lit("\\d+"), lit("XXX")) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

run.("1d. translate with lit() args", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "hello"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    translate(col("text"), lit("helo"), lit("HELO")) |> Column.alias_("translated")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Functions with potential to_expr issues
# =============================================

run.("2a. instr with lit() string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    instr(col("text"), lit("World")) |> Column.alias_("pos")
  ]) |> DataFrame.collect()
end)

run.("2b. instr with bare string (should fail?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    instr(col("text"), "World") |> Column.alias_("pos")
  ]) |> DataFrame.collect()
end)

run.("2c. split with lit() pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "a-b-c-d"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    split(col("text"), lit("-")) |> Column.alias_("parts")
  ]) |> DataFrame.collect()
end)

run.("2d. split with bare string pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "a-b-c-d"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    split(col("text"), "-") |> Column.alias_("parts")
  ]) |> DataFrame.collect()
end)

run.("2e. locate with lit() args", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World Hello"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    locate(lit("Hello"), col("text")) |> Column.alias_("pos1"),
    locate(lit("Hello"), col("text"), 3) |> Column.alias_("pos2")
  ]) |> DataFrame.collect()
end)

run.("2f. locate with bare string (should work or fail?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World Hello"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    locate("Hello", col("text")) |> Column.alias_("pos")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. col_opt functions with keyword options
# =============================================

run.("3a. round with scale (col_opt pattern)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 3.14159}], schema: "val DOUBLE")
  df |> DataFrame.select([
    SparkEx.Functions.round(col("val"), scale: 2) |> Column.alias_("rounded")
  ]) |> DataFrame.collect()
end)

run.("3b. bround with scale", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 2.5}], schema: "val DOUBLE")
  df |> DataFrame.select([
    bround(col("val"), scale: 0) |> Column.alias_("rounded")
  ]) |> DataFrame.collect()
end)

run.("3c. first with ignore_nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => nil}, %{"val" => 42}, %{"val" => 99}
  ], schema: "val INT")

  df |> DataFrame.select([
    first(col("val")) |> Column.alias_("with_nulls"),
    first(col("val"), ignore_nulls: true) |> Column.alias_("skip_nulls")
  ]) |> DataFrame.collect()
end)

run.("3d. last with ignore_nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 42}, %{"val" => 99}, %{"val" => nil}
  ], schema: "val INT")

  df |> DataFrame.select([
    last(col("val")) |> Column.alias_("with_nulls"),
    last(col("val"), ignore_nulls: true) |> Column.alias_("skip_nulls")
  ]) |> DataFrame.collect()
end)

run.("3e. lag with offset and default", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])

  df |> DataFrame.select([
    col("id"), col("val"),
    lag(col("val"), offset: 1) |> Column.over(w) |> Column.alias_("prev"),
    lag(col("val"), offset: 2, default: 0) |> Column.over(w) |> Column.alias_("prev2")
  ]) |> DataFrame.collect()
end)

run.("3f. lead with offset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])

  df |> DataFrame.select([
    col("id"), col("val"),
    lead(col("val"), offset: 1) |> Column.over(w) |> Column.alias_("next"),
    lead(col("val"), offset: 2, default: 0) |> Column.over(w) |> Column.alias_("next2")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. n_col functions (list of columns)
# =============================================

run.("4a. concat with list of columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "Hello", "b" => " ", "c" => "World"}
  ], schema: "a STRING, b STRING, c STRING")

  df |> DataFrame.select([
    concat([col("a"), col("b"), col("c")]) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

run.("4b. struct from columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30}
  ], schema: "name STRING, age INT")

  df |> DataFrame.select([
    SparkEx.Functions.struct([col("name"), col("age")]) |> Column.alias_("person")
  ]) |> DataFrame.collect()
end)

run.("4c. array with list of columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    array([col("a"), col("b"), col("c")]) |> Column.alias_("arr")
  ]) |> DataFrame.collect()
end)

run.("4d. create_map from key-value pairs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"k1" => "name", "v1" => "Alice", "k2" => "city", "v2" => "NY"}
  ], schema: "k1 STRING, v1 STRING, k2 STRING, v2 STRING")

  df |> DataFrame.select([
    create_map([col("k1"), col("v1"), col("k2"), col("v2")]) |> Column.alias_("map")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Math function edge cases
# =============================================

run.("5a. cbrt, signum, factorial", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 27}], schema: "val INT")
  df |> DataFrame.select([
    cbrt(col("val")) |> Column.alias_("cbrt"),
    signum(Column.cast(col("val"), "DOUBLE")) |> Column.alias_("sign"),
    factorial(col("val")) |> Column.alias_("fact")
  ]) |> DataFrame.collect()
end)

run.("5b. log with explicit base", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 100.0}], schema: "val DOUBLE")
  df |> DataFrame.select([
    log(col("val")) |> Column.alias_("ln"),
    log(col("val"), base: 10.0) |> Column.alias_("log10"),
    log2(col("val")) |> Column.alias_("log2")
  ]) |> DataFrame.collect()
end)

run.("5c. hex, unhex, conv", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 255}], schema: "val INT")
  df |> DataFrame.select([
    hex(col("val")) |> Column.alias_("hex_val"),
    conv(lit("FF"), 16, 10) |> Column.alias_("dec_val")
  ]) |> DataFrame.collect()
end)

run.("5d. abs, negative values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -42}], schema: "val INT")
  df |> DataFrame.select([
    SparkEx.Functions.abs(col("val")) |> Column.alias_("abs_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Hash functions
# =============================================

run.("6a. md5, sha1, sha2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"text" => "hello"}], schema: "text STRING")
  df |> DataFrame.select([
    md5(col("text")) |> Column.alias_("md5"),
    sha1(col("text")) |> Column.alias_("sha1"),
    sha2(col("text"), 256) |> Column.alias_("sha256")
  ]) |> DataFrame.collect()
end)

run.("6b. hash and xxhash64", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "hello"}
  ], schema: "a INT, b STRING")

  df |> DataFrame.select([
    hash([col("a"), col("b")]) |> Column.alias_("hash"),
    xxhash64([col("a"), col("b")]) |> Column.alias_("xxhash")
  ]) |> DataFrame.collect()
end)

run.("6c. crc32", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"text" => "hello"}], schema: "text STRING")
  df |> DataFrame.select([
    crc32(col("text")) |> Column.alias_("crc")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Complex pipeline patterns
# =============================================

run.("7a. multi-step ETL: filter → group → agg → sort → limit", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"dept" => Enum.random(["Eng", "HR", "Sales"]),
        "salary" => 50_000 + :rand.uniform(50_000),
        "active" => rem(i, 7) != 0}
    end),
    schema: "dept STRING, salary INT, active BOOLEAN")

  df
  |> DataFrame.filter(Column.eq(col("active"), lit(true)))
  |> DataFrame.group_by([col("dept")])
  |> SparkEx.GroupedData.agg([
    count(col("dept")) |> Column.alias_("count"),
    avg(col("salary")) |> Column.alias_("avg_salary"),
    max(col("salary")) |> Column.alias_("max_salary")
  ])
  |> DataFrame.sort([desc(col("avg_salary"))])
  |> DataFrame.collect()
end)

run.("7b. multi-join pipeline", fn ->
  {:ok, employees} = SparkEx.create_dataframe(s, [
    %{"emp_id" => 1, "name" => "Alice", "dept_id" => 10},
    %{"emp_id" => 2, "name" => "Bob", "dept_id" => 20},
    %{"emp_id" => 3, "name" => "Carol", "dept_id" => 10}
  ], schema: "emp_id INT, name STRING, dept_id INT")

  {:ok, depts} = SparkEx.create_dataframe(s, [
    %{"dept_id" => 10, "dept_name" => "Engineering"},
    %{"dept_id" => 20, "dept_name" => "HR"}
  ], schema: "dept_id INT, dept_name STRING")

  {:ok, salaries} = SparkEx.create_dataframe(s, [
    %{"emp_id" => 1, "salary" => 100000},
    %{"emp_id" => 2, "salary" => 90000},
    %{"emp_id" => 3, "salary" => 110000}
  ], schema: "emp_id INT, salary INT")

  employees
  |> DataFrame.join(depts, ["dept_id"])
  |> DataFrame.join(salaries, ["emp_id"])
  |> DataFrame.select([
    col("name"), col("dept_name"), col("salary")
  ])
  |> DataFrame.sort([desc(col("salary"))])
  |> DataFrame.collect()
end)

run.("7c. union + dedup pipeline", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "b"},
    %{"id" => 3, "val" => "c"}
  ], schema: "id INT, val STRING")

  DataFrame.union(df1, df2)
  |> DataFrame.distinct()
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("7d. window with group and filter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "name" => "Alice", "salary" => 100},
    %{"dept" => "Eng", "name" => "Bob", "salary" => 120},
    %{"dept" => "Eng", "name" => "Carol", "salary" => 110},
    %{"dept" => "HR", "name" => "Dave", "salary" => 90},
    %{"dept" => "HR", "name" => "Eve", "salary" => 95}
  ], schema: "dept STRING, name STRING, salary INT")

  w = SparkEx.Window.partition_by([col("dept")])
  |> SparkEx.WindowSpec.order_by([desc(col("salary"))])

  # Get top 2 per department
  df
  |> DataFrame.with_column("rank", rank() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("rank"), lit(2)))
  |> DataFrame.select([col("dept"), col("name"), col("salary"), col("rank")])
  |> DataFrame.sort([asc(col("dept")), asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.to_local_iterator
# =============================================

run.("8a. to_local_iterator basic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, iter} = DataFrame.to_local_iterator(df)
  rows = Enum.to_list(iter)
  {:ok, length(rows), rows}
end)

run.("8b. to_local_iterator with transformation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, iter} = df |> DataFrame.filter(Column.gt(col("id"), lit(5))) |> DataFrame.to_local_iterator()
  rows = Enum.to_list(iter)
  {:ok, length(rows)}
end)

# =============================================
# 9. DataFrame.collect_as_map
# =============================================

run.("9. collect_as_map with 2-column df", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "val" => 1},
    %{"key" => "b", "val" => 2},
    %{"key" => "c", "val" => 3}
  ], schema: "key STRING, val INT")

  DataFrame.collect_as_map(df)
end)

IO.puts("=== Round 28 tests complete ===")
