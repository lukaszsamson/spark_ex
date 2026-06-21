# Section 3: Creating DataFrames from Local Data
# Usage: mix run checklist/section_03_create_df.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Section 3: Creating DataFrames from Local Data ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Connected!\n")

# 3.1 From SQL VALUES
IO.puts("3.1 From SQL VALUES")
df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, letter)")
result = DataFrame.collect(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 3.2 SQL with positional args
IO.puts("3.2 SQL with positional args")
try do
  result = SparkEx.sql(s, "SELECT ? + 1 AS result", args: [42]) |> DataFrame.collect()
  IO.inspect(result, label: "  result")
rescue
  e -> IO.puts("  ERROR: #{inspect(e)}")
catch
  kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
end
IO.puts("")

# 3.3 SQL with named args
IO.puts("3.3 SQL with named args")
try do
  result = SparkEx.sql(s, "SELECT :val * 2 AS result", args: [val: 10]) |> DataFrame.collect()
  IO.inspect(result, label: "  result")
rescue
  e -> IO.puts("  ERROR: #{inspect(e)}")
catch
  kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
end
IO.puts("")

# 3.4 create_dataframe from list of maps
IO.puts("3.4 create_dataframe from list of maps")
try do
  result = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => "hello"}, %{"x" => 2, "y" => "world"}])
  IO.inspect(result, label: "  create result")
  case result do
    {:ok, df} ->
      collect_result = DataFrame.collect(df)
      IO.inspect(collect_result, label: "  collect result")
    _ -> :ok
  end
rescue
  e -> IO.puts("  ERROR: #{Exception.message(e)}")
end
IO.puts("")

# 3.5 create_dataframe with explicit schema
IO.puts("3.5 create_dataframe with explicit schema")
try do
  result = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => "hello"}], schema: "x INT, y STRING")
  IO.inspect(result, label: "  create result")
  case result do
    {:ok, df} ->
      collect_result = DataFrame.collect(df)
      IO.inspect(collect_result, label: "  collect result")
    _ -> :ok
  end
rescue
  e -> IO.puts("  ERROR: #{Exception.message(e)}")
end
IO.puts("")

# 3.6 Range
IO.puts("3.6 Range")
result = SparkEx.range(s, 10) |> DataFrame.count()
IO.inspect(result, label: "  result")
IO.puts("")

# 3.7 Range with start/step
IO.puts("3.7 Range with start/step")
result = SparkEx.range(s, 0, 100, 10) |> DataFrame.collect()
IO.inspect(result, label: "  result")
IO.puts("")

IO.puts("=== Section 3 Complete ===")
