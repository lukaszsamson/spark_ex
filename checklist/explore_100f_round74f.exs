# Round 74f: Test remaining functions one at a time to avoid cascade crashes
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

# 11a. drop with integer
run.("11a. drop with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.drop(df, [42]) |> DataFrame.collect()
end)

# 12a. to_df with integer column name
run.("12a. to_df with integer column name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to_df(df, [42]) |> DataFrame.collect()
end)

# 13a. sort with integer in list
run.("13a. sort with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.sort(df, [42]) |> DataFrame.collect()
end)

# 14a. group_by with integer in list
run.("14a. group_by with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.group_by(df, [42]) |> GroupedData.count() |> DataFrame.collect()
end)

# 15a. select with integer in list
run.("15a. select with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select(df, [42]) |> DataFrame.collect()
end)

# 16a. with_column integer name
run.("16a. with_column integer name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_column(df, 42, col("x")) |> DataFrame.collect()
end)

# 17a. with_columns with map non-string keys
run.("17a. with_columns with map non-string keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns(df, %{42 => col("x")}) |> DataFrame.collect()
end)

# 18a. filter with string expression
run.("18a. filter with string expression", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.filter(df, "x > 0") |> DataFrame.collect()
end)

# 18b. filter with integer
run.("18b. filter with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.filter(df, 42) |> DataFrame.collect()
end)

IO.puts("=== Round 74f complete ===")
