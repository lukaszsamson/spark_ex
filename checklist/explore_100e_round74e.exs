# Round 74e: select_expr, WindowSpec, and more crash vector hunting
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
# 10. select_expr with non-string elements
# =============================================

run.("10a. select_expr with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select_expr(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== 10a done ===")

run.("10b. select_expr with atom in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select_expr(df, [:x]) |> DataFrame.collect()
end)

IO.puts("=== 10b done ===")

# =============================================
# 11. DataFrame.drop with non-string/non-Column elements
# =============================================

run.("11a. drop with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.drop(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== 11a done ===")

# =============================================
# 12. DataFrame.to_df with non-string column names
# =============================================

run.("12a. to_df with integer column name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to_df(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== 12a done ===")

# =============================================
# 13. DataFrame.sort with non-Column/non-string elements
# =============================================

run.("13a. sort with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.sort(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== 13a done ===")

# =============================================
# 14. DataFrame.group_by with non-Column elements
# =============================================

run.("14a. group_by with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.group_by(df, [42]) |> GroupedData.count() |> DataFrame.collect()
end)

IO.puts("=== 14a done ===")

# =============================================
# 15. DataFrame.select with non-Column/non-string elements
# =============================================

run.("15a. select with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== 15a done ===")

# =============================================
# 16. DataFrame.with_column with non-string column name
# =============================================

run.("16a. with_column integer name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_column(df, 42, col("x")) |> DataFrame.collect()
end)

IO.puts("=== 16a done ===")

# =============================================
# 17. DataFrame.with_columns with non-map/non-list
# =============================================

run.("17a. with_columns with map non-string keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns(df, %{42 => col("x")}) |> DataFrame.collect()
end)

IO.puts("=== 17a done ===")

# =============================================
# 18. DataFrame.filter with non-Column
# =============================================

run.("18a. filter with string expression", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.filter(df, "x > 0") |> DataFrame.collect()
end)

run.("18b. filter with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.filter(df, 42) |> DataFrame.collect()
end)

IO.puts("=== Round 74e complete ===")
