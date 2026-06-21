# Round 69c: Continue from test 10 (after config_set crashes in 69b)
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
# 11. SparkEx.sql with invalid inputs
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
# 16. DataFrame ops validation
# =============================================

run.("16a. with_columns_renamed with non-map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, "not a map") |> DataFrame.collect()
end)

run.("16b. with_columns_renamed with non-string keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_columns_renamed(df, %{42 => "y"}) |> DataFrame.collect()
end)

run.("16c. drop with non-Column/non-string element", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.drop(df, [42]) |> DataFrame.collect()
end)

run.("16d. sort with non-Column in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.sort(df, [42]) |> DataFrame.collect()
end)

run.("16e. DataFrame.alias_ with non-string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.alias_(df, 42)
end)

# =============================================
# 17. config_get/unset/is_modifiable validation
# =============================================

run.("17a. config_get with non-string in list", fn ->
  SparkEx.config_get(s, [42])
end)

run.("17b. config_unset with non-string", fn ->
  SparkEx.config_unset(s, 42)
end)

run.("17c. config_is_modifiable with non-list (bare string)", fn ->
  SparkEx.config_is_modifiable(s, "spark.sql.shuffle.partitions")
end)

run.("17d. config_get_option with non-string", fn ->
  SparkEx.config_get_option(s, 42)
end)

run.("17e. config_get_with_default with non-list input", fn ->
  SparkEx.config_get_with_default(s, 42)
end)

run.("17f. config_get_with_default with non-string key in pair", fn ->
  SparkEx.config_get_with_default(s, [{42, "default"}])
end)

IO.puts("=== Round 69c complete ===")
