# Round 74h: GroupedData paths and remaining edge cases
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

# 1. GroupedData.agg with map containing non-string keys
run.("1. GroupedData.agg with map non-string keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.group_by(df, ["x"]) |> GroupedData.agg(%{42 => "sum"}) |> DataFrame.collect()
end)

# 2. GroupedData.agg with map non-string values
run.("2. GroupedData.agg with map non-string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.group_by(df, ["x"]) |> GroupedData.agg(%{"y" => 42}) |> DataFrame.collect()
end)

# 3. DataFrame.cube with integers
run.("3. DataFrame.cube with integer columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.cube(df, [42]) |> GroupedData.count() |> DataFrame.collect()
end)

# 4. DataFrame.rollup with integers
run.("4. DataFrame.rollup with integer columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.rollup(df, [42]) |> GroupedData.count() |> DataFrame.collect()
end)

# 5. DataFrame.repartition with non-Column partitions
run.("5. repartition with integer partition col", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.repartition(df, 2, [42]) |> DataFrame.collect()
end)

# 6. DataFrame.coalesce with non-integer
run.("6. coalesce with string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.coalesce(df, "2") |> DataFrame.collect()
end)

# 7. DataFrame.sample with non-float fraction
run.("7. sample with string fraction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, "0.5") |> DataFrame.collect()
end)

# 8. DataFrame.join with non-Column condition
run.("8a. join with string condition", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.join(df1, df2, "x") |> DataFrame.collect()
end)

run.("8b. join with integer condition", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.join(df1, df2, 42) |> DataFrame.collect()
end)

# 9. DataFrame.union_by_name with non-boolean allow_missing_columns
run.("9. union_by_name with string allow_missing", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 2, "y" => 3}], schema: "x INT, y INT")
  DataFrame.union_by_name(df1, df2, allow_missing_columns: "yes") |> DataFrame.collect()
end)

# 10. DataFrame.cross_join basic validation
run.("10. cross_join with non-DataFrame", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.cross_join(df1, 42)
end)

# 11. DataFrame.intersect/except with non-DataFrame
run.("11a. intersect with non-DataFrame", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.intersect(df1, 42)
end)

run.("11b. except with non-DataFrame", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.except(df1, 42)
end)

# 12. DataFrame.distinct on nil
run.("12. distinct works", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}, %{"x" => 1}, %{"x" => 2}], schema: "x INT")
  DataFrame.distinct(df) |> DataFrame.collect()
end)

# 13. DataFrame.columns with non-DataFrame
run.("13. columns with non-DataFrame", fn ->
  DataFrame.columns(42)
end)

IO.puts("=== Round 74h complete ===")
