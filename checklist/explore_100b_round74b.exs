# Round 74b: Continue after freq_items crash — test remaining vectors
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
# 2. with_columns_renamed map with non-string keys/values
# =============================================

run.("2a. with_columns_renamed integer key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{42 => "new_x"}) |> DataFrame.collect()
end)

run.("2b. with_columns_renamed integer value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{"x" => 42}) |> DataFrame.collect()
end)

run.("2c. with_columns_renamed atom key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{x: "new_x"}) |> DataFrame.collect()
end)

run.("2d. with_columns_renamed nil key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{nil => "new_x"}) |> DataFrame.collect()
end)

run.("2e. with_columns_renamed list value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{"x" => ["a", "b"]}) |> DataFrame.collect()
end)

IO.puts("=== Round 74b part 2 done ===")
