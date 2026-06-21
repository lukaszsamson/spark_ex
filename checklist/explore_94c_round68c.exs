# Round 68c: Investigate 10K string issue + more edge cases
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
# 1. Long string investigation
# =============================================

run.("1a. 1000-char string", fn ->
  str = String.duplicate("a", 1_000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => str}
  ], schema: "id INT, val STRING")
  df |> DataFrame.select([col("id"), length(col("val")) |> Column.alias_("len")]) |> DataFrame.collect()
end)

run.("1b. 5000-char string", fn ->
  str = String.duplicate("b", 5_000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => str}
  ], schema: "id INT, val STRING")
  df |> DataFrame.select([col("id"), length(col("val")) |> Column.alias_("len")]) |> DataFrame.collect()
end)

run.("1c. 10000-char string", fn ->
  str = String.duplicate("c", 10_000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => str}
  ], schema: "id INT, val STRING")
  df |> DataFrame.select([col("id"), length(col("val")) |> Column.alias_("len")]) |> DataFrame.collect()
end)

run.("1d. 10000-char string - just collect, no length", fn ->
  str = String.duplicate("d", 10_000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => str}
  ], schema: "id INT, val STRING")
  {:ok, rows} = df |> DataFrame.collect()
  # Check the length of the returned value
  [row] = rows
  {row["id"], String.length(row["val"])}
end)

# =============================================
# 2. Nested map in struct via SQL (bypass create_dataframe)
# =============================================

run.("2a. SQL: struct with map field", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('x', 1, 'm', map('a', 10, 'b', 20)) AS rec
  """)
  df |> DataFrame.collect()
end)

run.("2b. SQL: struct with map field - explode map", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('x', 1, 'm', map('a', 10, 'b', 20)) AS rec
  """)
  # Access map via struct field, then explode
  df |> DataFrame.select([
    Column.get_field(col("rec"), "x") |> Column.alias_("x"),
    Column.get_field(col("rec"), "m") |> Column.alias_("m")
  ]) |> DataFrame.collect()
end)

run.("2c. SQL: struct with map - access map field only", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('x', 1, 'm', map('a', 10, 'b', 20)) AS rec
  """)
  # Only access the map field, don't collect the struct
  df |> DataFrame.select([
    Column.get_field(col("rec"), "m") |> Column.alias_("m")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. MAP decode patterns (which work, which don't)
# =============================================

run.("3a. Top-level map (works)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.collect()
end)

run.("3b. Top-level map + other column (works?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "m" => %{"a" => 1}}
  ], schema: "id INT, m MAP<STRING, INT>")
  df |> DataFrame.collect()
end)

run.("3c. Two top-level maps", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m1" => %{"a" => 1}, "m2" => %{"b" => 2}}
  ], schema: "m1 MAP<STRING, INT>, m2 MAP<STRING, INT>")
  df |> DataFrame.collect()
end)

run.("3d. Array of ints inside struct (works)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"arr" => [1, 2, 3]}}
  ], schema: "rec STRUCT<arr: ARRAY<INT>>")
  df |> DataFrame.collect()
end)

run.("3e. MAP<INT, STRING> (non-string keys)", fn ->
  df = SparkEx.sql(s, "SELECT map(1, 'a', 2, 'b') AS m")
  df |> DataFrame.collect()
end)

# =============================================
# 4. More validation gap hunting (session-crashers)
# =============================================

run.("4a. DataFrame.sample with non-float fraction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.sample(df, fraction: "half") |> DataFrame.collect()
end)

run.("4b. DataFrame.random_split with non-float weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.random_split(df, ["one", "two"])
end)

run.("4c. GroupedData.agg with non-list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"g" => "A", "v" => 1}], schema: "g STRING, v INT")
  gd = DataFrame.group_by(df, [col("g")])
  GroupedData.agg(gd, "not a list")
end)

run.("4d. DataFrame.join with non-string join type", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.join(df1, df2, col("x"), 42) |> DataFrame.collect()
end)

run.("4e. Column.when_ with non-boolean condition (literal string)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([
    when_(lit("not_a_bool"), lit(1)) |> Column.otherwise(lit(0)) |> Column.alias_("res")
  ]) |> DataFrame.collect()
end)

run.("4f. DataFrame.with_column with non-Column value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_column(df, "new_col", "not_a_column") |> DataFrame.collect()
end)

run.("4g. DataFrame.filter with non-Column condition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.filter(df, "x > 0") |> DataFrame.collect()
end)

run.("4h. DataFrame.select with mixed list (Column + string)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.select(df, [col("x"), "y"]) |> DataFrame.collect()
end)

# =============================================
# 5. Edge: Decimal/Date/Timestamp in struct
# =============================================

run.("5a. Struct with date field", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('id', 1, 'dt', DATE '2024-01-15') AS rec
  """)
  df |> DataFrame.collect()
end)

run.("5b. Struct with timestamp field", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('id', 1, 'ts', TIMESTAMP '2024-01-15 10:30:00') AS rec
  """)
  df |> DataFrame.collect()
end)

run.("5c. Struct with decimal field", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('id', 1, 'amount', CAST(123.45 AS DECIMAL(10,2))) AS rec
  """)
  df |> DataFrame.collect()
end)

# =============================================
# 6. Edge: union of DataFrames with complex types
# =============================================

run.("6. Union of DataFrames with struct columns", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice"}}
  ], schema: "id INT, info STRUCT<name: STRING>")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "info" => %{"name" => "Bob"}}
  ], schema: "id INT, info STRUCT<name: STRING>")
  DataFrame.union(df1, df2) |> DataFrame.collect()
end)

# =============================================
# 7. Edge: pivot with complex value column
# =============================================

run.("7. Pivot with struct access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"cat" => "A", "sub" => "x", "val" => 10},
    %{"cat" => "A", "sub" => "y", "val" => 20},
    %{"cat" => "B", "sub" => "x", "val" => 30},
    %{"cat" => "B", "sub" => "y", "val" => 40}
  ], schema: "cat STRING, sub STRING, val INT")
  df
  |> DataFrame.group_by([col("cat")])
  |> GroupedData.pivot("sub", ["x", "y"])
  |> GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# =============================================
# 8. Edge: createOrReplaceTempView then read back complex types
# =============================================

run.("8. Temp view with struct → read back", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"a" => 10, "b" => "hello"}},
    %{"id" => 2, "info" => %{"a" => nil, "b" => nil}},
    %{"id" => 3, "info" => nil}
  ], schema: "id INT, info STRUCT<a: INT, b: STRING>")
  DataFrame.create_or_replace_temp_view(df, "tmp_complex_r68")
  SparkEx.sql(s, "SELECT * FROM tmp_complex_r68") |> DataFrame.collect()
end)

IO.puts("=== Round 68c complete ===")
