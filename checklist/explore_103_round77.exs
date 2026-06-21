# Round 77: SparkEx.Functions edge cases — wrong argument types to generated functions
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1]

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
# 1. Functions with n_col calling convention (take list of columns)
# What happens with non-Column/non-string in the list?
# =============================================

run.("1a. concat function with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "a", "y" => "b"}], schema: "x STRING, y STRING")
  df |> DataFrame.select([concat([42, col("y")]) |> Column.alias_("res")]) |> DataFrame.collect()
end)

run.("1b. greatest with nil in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  df |> DataFrame.select([greatest([nil, col("y")]) |> Column.alias_("res")]) |> DataFrame.collect()
end)

run.("1c. least with map in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  df |> DataFrame.select([least([%{"a" => 1}, col("y")]) |> Column.alias_("res")]) |> DataFrame.collect()
end)

# =============================================
# 2. Functions that take string arguments (format, pattern etc.)
# =============================================

run.("2a. date_format with integer format", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"d" => "2024-01-15"}], schema: "d STRING")
  df |> DataFrame.select([
    date_format(to_date(col("d")), 42) |> Column.alias_("formatted")
  ]) |> DataFrame.collect()
end)

run.("2b. regexp_extract with integer pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "abc123"}], schema: "x STRING")
  df |> DataFrame.select([
    regexp_extract(col("x"), 42, 0) |> Column.alias_("matched")
  ]) |> DataFrame.collect()
end)

run.("2c. regexp_replace with integer replacement", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "abc123"}], schema: "x STRING")
  df |> DataFrame.select([
    regexp_replace(col("x"), "[0-9]+", 42) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. to_json/from_json edge cases
# =============================================

run.("3a. from_json with integer schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"j" => "{\"x\": 1}"}], schema: "j STRING")
  df |> DataFrame.select([
    from_json(col("j"), 42) |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("3b. from_json with options non-map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"j" => "{\"x\": 1}"}], schema: "j STRING")
  df |> DataFrame.select([
    from_json(col("j"), "x INT", "not_a_map") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. HOF with wrong arity callbacks
# =============================================

run.("4a. Column.transform with 0-arity function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"arr" => [1, 2, 3]}], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn -> lit(42) end) |> Column.alias_("res")
  ]) |> DataFrame.collect()
end)

run.("4b. Column.transform with 3-arity function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"arr" => [1, 2, 3]}], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn x, y, z -> Column.plus(x, Column.plus(y, z)) end) |> Column.alias_("res")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. lit() with exotic types
# =============================================

run.("5a. lit with PID", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([lit(self()) |> Column.alias_("pid_val")]) |> DataFrame.collect()
end)

run.("5b. lit with reference", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([lit(make_ref()) |> Column.alias_("ref_val")]) |> DataFrame.collect()
end)

run.("5c. lit with tuple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([lit({1, 2, 3}) |> Column.alias_("tuple_val")]) |> DataFrame.collect()
end)

run.("5d. lit with function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([lit(fn -> 42 end) |> Column.alias_("fn_val")]) |> DataFrame.collect()
end)

# =============================================
# 6. Column.when_ without otherwise
# =============================================

run.("6. when_ chain without otherwise", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}], schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    when_(col("x") |> Column.eq(lit(1)), lit("one"))
    |> Column.when_(col("x") |> Column.eq(lit(2)), lit("two"))
    |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. SparkEx.sql with very long query
# =============================================

run.("7. sql with 10K char query", fn ->
  # Build a long UNION ALL query
  parts = Enum.map(1..200, fn i -> "SELECT #{i} AS x" end) |> Enum.join(" UNION ALL ")
  SparkEx.sql(s, parts) |> DataFrame.count()
end)

# =============================================
# 8. create_dataframe with very wide schema (100 columns)
# =============================================

run.("8. 100-column create_dataframe", fn ->
  schema = Enum.map(1..100, fn i -> "c#{i} INT" end) |> Enum.join(", ")
  data = [Map.new(Enum.map(1..100, fn i -> {"c#{i}", i} end))]
  {:ok, df} = SparkEx.create_dataframe(s, data, schema: schema)
  {:ok, count} = DataFrame.count(df)
  {:ok, cols} = DataFrame.columns(df)
  {count, length(cols)}
end)

IO.puts("=== Round 77 complete ===")
