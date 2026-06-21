# Round 69b: Continue from 9b after session crash on 9a (config_set non-string key)
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
# 8. Writer v1 validation (re-run)
# =============================================

run.("8a. Writer.mode with non-string (integer)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.mode(42)
end)

run.("8b. Writer.partition_by with non-list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.partition_by(42)
end)

run.("8c. Writer.bucket_by with non-integer num_buckets", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.bucket_by("five", ["x"])
end)

run.("8d. Writer.option with non-string key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.option(42, "value")
end)

run.("8e. Writer.format with non-string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.format(42)
end)

# =============================================
# 9b-d. config API validation (skip 9a which crashes)
# =============================================

run.("9b. config_set with non-string value", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", 42}])
end)

run.("9c. config_get with non-string key", fn ->
  SparkEx.config_get(s, [42])
end)

run.("9d. config_unset with non-string key", fn ->
  SparkEx.config_unset(s, 42)
end)

# =============================================
# 10. View name validation
# =============================================

run.("10a. create_or_replace_temp_view with non-string name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_temp_view(df, 42)
end)

run.("10b. create_or_replace_global_temp_view with non-string name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_global_temp_view(df, 42)
end)

# =============================================
# 11. SparkEx.sql with various invalid inputs
# =============================================

run.("11a. sql with non-string query", fn ->
  SparkEx.sql(s, 42)
end)

run.("11b. sql with nil query", fn ->
  SparkEx.sql(s, nil)
end)

# =============================================
# 12. create_dataframe invalid inputs
# =============================================

run.("12a. create_dataframe with non-list rows", fn ->
  SparkEx.create_dataframe(s, "not a list", schema: "x INT")
end)

run.("12b. create_dataframe with non-map rows", fn ->
  SparkEx.create_dataframe(s, [1, 2, 3], schema: "x INT")
end)

run.("12c. create_dataframe without schema", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}])
end)

run.("12d. create_dataframe with invalid schema string", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "NOT VALID SCHEMA !!!")
end)

# =============================================
# 13. Type mismatch in create_dataframe
# =============================================

run.("13a. String value for INT column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => "not_a_number"}
  ], schema: "x INT")
  df |> DataFrame.collect()
end)

run.("13b. Integer value for STRING column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 42}
  ], schema: "x STRING")
  df |> DataFrame.collect()
end)

run.("13c. Float value for INT column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 3.14}
  ], schema: "x INT")
  df |> DataFrame.collect()
end)

run.("13d. List value for STRING column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => [1, 2, 3]}
  ], schema: "x STRING")
  df |> DataFrame.collect()
end)

run.("13e. Map value for STRING column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => %{"a" => 1}}
  ], schema: "x STRING")
  df |> DataFrame.collect()
end)

# =============================================
# 14. Reader validation
# =============================================

run.("14a. Reader.table with non-string", fn ->
  SparkEx.Reader.table(s, 42)
end)

run.("14b. Reader.format with non-string", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.format(42)
end)

run.("14c. Reader.option with non-string key", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.option(42, "val")
end)

run.("14d. Reader.schema with non-string", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.schema(42)
end)

# =============================================
# 15. SparkEx.range validation
# =============================================

run.("15a. range with non-integer start", fn ->
  SparkEx.range(s, "zero", 100) |> DataFrame.count()
end)

run.("15b. range with non-integer end", fn ->
  SparkEx.range(s, 0, "hundred") |> DataFrame.count()
end)

run.("15c. range with non-integer step", fn ->
  SparkEx.range(s, 0, 100, "two") |> DataFrame.count()
end)

# =============================================
# 16. More protobuf validation gaps: DataFrame ops
# =============================================

run.("16a. DataFrame.with_columns_renamed with non-map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, "not a map") |> DataFrame.collect()
end)

run.("16b. DataFrame.with_columns_renamed with non-string keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{42 => "y"}) |> DataFrame.collect()
end)

run.("16c. DataFrame.drop with non-Column/non-string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.drop(df, [42]) |> DataFrame.collect()
end)

run.("16d. DataFrame.sort with non-Column list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.sort(df, [42]) |> DataFrame.collect()
end)

IO.puts("=== Round 69b complete ===")
