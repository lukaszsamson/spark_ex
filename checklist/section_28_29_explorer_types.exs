# Sections 28-29: Explorer & Arrow Integration, Data Type Coverage
# Usage: mix run checklist/section_28_29_explorer_types.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Sections 28-29: Explorer & Data Types ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: :infinity, printable_limit: 2000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# === Section 28: Explorer ===
IO.puts("--- Section 28: Explorer ---\n")

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5) AS t(id, name, salary)")

run.("28.1 to_explorer", fn ->
  result = DataFrame.to_explorer(df)
  case result do
    {:ok, edf} ->
      IO.puts("  Type: #{inspect(edf.__struct__)}")
      IO.puts("  Shape: #{inspect(Explorer.DataFrame.shape(edf))}")
      {:ok, edf}
    other -> other
  end
end)

run.("28.2 create_dataframe from Explorer", fn ->
  edf = Explorer.DataFrame.new(a: [1, 2, 3], b: ["x", "y", "z"])
  {:ok, sdf} = SparkEx.create_dataframe(s, edf)
  DataFrame.collect(sdf)
end)

run.("28.3 to_arrow", fn ->
  result = DataFrame.to_arrow(df)
  case result do
    {:ok, binary} when is_binary(binary) ->
      IO.puts("  Arrow binary size: #{byte_size(binary)} bytes")
      {:ok, :binary_received}
    other -> other
  end
end)

# === Section 29: Data Type Coverage ===
IO.puts("--- Section 29: Data Type Coverage ---\n")

typed_df = SparkEx.sql(s, """
  SELECT
    CAST(1 AS TINYINT) AS byte_val,
    CAST(100 AS SMALLINT) AS short_val,
    42 AS int_val,
    CAST(9999999999 AS BIGINT) AS long_val,
    CAST(3.14 AS FLOAT) AS float_val,
    CAST(2.718281828 AS DOUBLE) AS double_val,
    CAST(12345.67 AS DECIMAL(10,2)) AS decimal_val,
    'hello' AS string_val,
    CAST('2024-06-15' AS DATE) AS date_val,
    CAST('2024-06-15 10:30:00' AS TIMESTAMP) AS ts_val,
    true AS bool_val,
    X'DEADBEEF' AS binary_val,
    array(1, 2, 3) AS array_val,
    map('key', 'value') AS map_val,
    named_struct('a', 1, 'b', 'x') AS struct_val
""")

run.("29.1 Collect typed data", fn ->
  DataFrame.collect(typed_df)
end)

run.("29.2 Verify schema types", fn ->
  DataFrame.dtypes(typed_df)
end)

# 29.3 Null handling
run.("29.3 Null handling", fn ->
  null_df = SparkEx.sql(s, "SELECT CAST(NULL AS INT) AS null_int, CAST(NULL AS STRING) AS null_str")
  DataFrame.collect(null_df)
end)

# Section 30: Plan Comparison
IO.puts("--- Section 30: Plan Comparison ---\n")

df2 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5) AS t(id, name, salary)")

run.("30.1 same_semantics (same)", fn ->
  DataFrame.same_semantics(df, df)
end)

run.("30.2 same_semantics (different)", fn ->
  DataFrame.same_semantics(df, DataFrame.limit(df, 1))
end)

run.("30.3 semantic_hash", fn ->
  DataFrame.semantic_hash(df)
end)

# Section 36: Error Handling
IO.puts("--- Section 36: Error Handling ---\n")

run.("36.1 Invalid SQL", fn ->
  SparkEx.sql(s, "SELEC INVALID") |> DataFrame.collect()
end)

run.("36.2 Division by zero", fn ->
  df |> DataFrame.select([col("id") |> Column.divide(lit(0)) |> Column.alias_("div_zero")]) |> DataFrame.collect()
end)

run.("36.4 Table not found", fn ->
  SparkEx.sql(s, "SELECT * FROM nonexistent_table_xyz_999") |> DataFrame.collect()
end)

run.("36.5 Column not found", fn ->
  df |> DataFrame.select([col("nonexistent_column")]) |> DataFrame.collect()
end)

IO.puts("=== Sections 28-30, 36 Complete ===")
