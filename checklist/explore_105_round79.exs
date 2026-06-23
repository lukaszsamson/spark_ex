# Round 79: Join using_columns and other remaining repeated string paths
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
# 1. Join with using_columns list containing non-strings
# =============================================

run.("1a. join with list of column names (valid)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 10}], schema: "x INT, y INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "z" => 20}], schema: "x INT, z INT")
  DataFrame.join(df1, df2, ["x"]) |> DataFrame.collect()
end)

run.("1b. join with integer in column name list", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 10}], schema: "x INT, y INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "z" => 20}], schema: "x INT, z INT")
  DataFrame.join(df1, df2, [42]) |> DataFrame.collect()
end)

run.("1c. join with mixed types in column name list", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 10}], schema: "x INT, y INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "z" => 20}], schema: "x INT, z INT")
  DataFrame.join(df1, df2, ["x", 42]) |> DataFrame.collect()
end)

run.("1d. join with nil in column name list", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 10}], schema: "x INT, y INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "z" => 20}], schema: "x INT, z INT")
  DataFrame.join(df1, df2, [nil]) |> DataFrame.collect()
end)

# =============================================
# 2. Reader.load with non-string path
# =============================================

run.("2a. Reader with non-string path", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.format("parquet") |> SparkEx.Reader.load(42)
end)

run.("2b. Reader with integer in paths list", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.format("parquet") |> SparkEx.Reader.load([42])
end)

# =============================================
# 3. Writer.save with non-string path
# =============================================

run.("3. Writer.save with integer path", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.save(42)
end)

# =============================================
# 4. describe with empty list (should still work)
# =============================================

run.("4a. describe with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.describe(df, []) |> DataFrame.collect()
end)

run.("4b. describe with no args (default)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.describe(df) |> DataFrame.collect()
end)

# =============================================
# 5. summary with empty list
# =============================================

run.("5. summary with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.summary(df, []) |> DataFrame.collect()
end)

# =============================================
# 6. select_expr with empty list
# =============================================

run.("6. select_expr with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select_expr(df, []) |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.join with various join types (non-string)
# =============================================

run.("7a. join with integer join type", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.join(df1, df2, "x", 42) |> DataFrame.collect()
end)

run.("7b. join with invalid string join type", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.join(df1, df2, "x", "invalid_join") |> DataFrame.collect()
end)

IO.puts("=== Round 79 complete ===")
