# Round 75: Creative edge cases and less-explored paths
# Focus on: observe with bad exprs, pivot edge cases, hint params,
# sql args validation, GroupedData edge cases, nested operations
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
# 1. DataFrame.observe with non-Column expressions
# =============================================

run.("1a. observe with integer in exprs list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.observe(df, "my_obs", [42]) |> DataFrame.collect()
end)

run.("1b. observe with string in exprs list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.observe(df, "my_obs", ["x"]) |> DataFrame.collect()
end)

# =============================================
# 2. GroupedData.pivot with non-list values
# =============================================

run.("2a. pivot with string values (should be list)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "year" => 2020, "sal" => 100},
    %{"dept" => "eng", "year" => 2021, "sal" => 110},
    %{"dept" => "sales", "year" => 2020, "sal" => 90}
  ], schema: "dept STRING, year INT, sal INT")
  DataFrame.group_by(df, ["dept"])
  |> GroupedData.pivot("year", "2020")
  |> GroupedData.sum(["sal"])
  |> DataFrame.collect()
end)

run.("2b. pivot with nil values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "year" => 2020, "sal" => 100}
  ], schema: "dept STRING, year INT, sal INT")
  DataFrame.group_by(df, ["dept"])
  |> GroupedData.pivot("year", nil)
  |> GroupedData.sum(["sal"])
  |> DataFrame.collect()
end)

# =============================================
# 3. DataFrame.hint with edge case parameters
# =============================================

run.("3a. hint with nil parameter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, "coalesce", [nil]) |> DataFrame.collect()
end)

run.("3b. hint with map parameter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, "coalesce", [%{"a" => 1}]) |> DataFrame.collect()
end)

run.("3c. hint with list parameter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, "coalesce", [[1, 2]]) |> DataFrame.collect()
end)

# =============================================
# 4. SparkEx.sql with args edge cases
# =============================================

run.("4a. sql with args keyword list (correct form?)", fn ->
  SparkEx.sql(s, "SELECT :x AS val", x: lit(42)) |> DataFrame.collect()
end)

run.("4b. sql with args nil", fn ->
  SparkEx.sql(s, "SELECT 1 AS x", nil) |> DataFrame.collect()
end)

# =============================================
# 5. GroupedData.agg with empty list
# =============================================

run.("5. GroupedData.agg with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.group_by(df, ["x"]) |> GroupedData.agg([]) |> DataFrame.collect()
end)

# =============================================
# 6. Multiple operations on same DataFrame
# =============================================

run.("6. Multiple operations reusing same df reference", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}], schema: "x INT")

  filtered = DataFrame.filter(df, col("x") |> Column.gt(lit(1)))
  with_col = DataFrame.with_column(df, "y", col("x") |> Column.multiply(lit(2)))
  sorted = DataFrame.sort(df, [desc(col("x"))])

  {:ok, r1} = DataFrame.collect(filtered)
  {:ok, r2} = DataFrame.collect(with_col)
  {:ok, r3} = DataFrame.collect(sorted)
  {r1, r2, r3}
end)

# =============================================
# 7. DataFrame.drop_duplicates edge cases
# =============================================

run.("7a. drop_duplicates with non-string subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.drop_duplicates(df, [42]) |> DataFrame.collect()
end)

run.("7b. drop_duplicates with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}, %{"x" => 1}], schema: "x INT")
  DataFrame.drop_duplicates(df, []) |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.random_split edge cases
# =============================================

run.("8a. random_split with negative weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.random_split(df, [-0.5, 0.5])
end)

run.("8b. random_split with zero weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.random_split(df, [0.0, 1.0])
end)

run.("8c. random_split with empty weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.random_split(df, [])
end)

run.("8d. random_split with non-numeric weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.random_split(df, ["a", "b"])
end)

# =============================================
# 9. Chained collect calls (idempotency)
# =============================================

run.("9. Double collect on same DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, r1} = DataFrame.collect(df)
  {:ok, r2} = DataFrame.collect(df)
  {r1, r2, r1 == r2}
end)

# =============================================
# 10. DataFrame.grouping_sets
# =============================================

run.("10. grouping_sets basic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => "x", "b" => "y", "c" => 1},
    %{"a" => "x", "b" => "z", "c" => 2},
    %{"a" => "w", "b" => "y", "c" => 3}
  ], schema: "a STRING, b STRING, c INT")
  DataFrame.grouping_sets(df, [[col("a")], [col("b")], [col("a"), col("b")]])
  |> GroupedData.agg([sum(col("c")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

IO.puts("=== Round 75 complete ===")
