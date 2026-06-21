# Round 10: Re-verify previously broken create_dataframe scenarios (EX-32..38, EX-40, EX-41, EX-43)
# and push further with complex data structures
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

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
# RE-VERIFY: Previously broken create_dataframe
# =============================================

# EX-32 re-verify: nested maps
run.("1. [EX-32] create_dataframe with nested maps", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice", "age" => 30}},
    %{"id" => 2, "info" => %{"name" => "Bob", "age" => 25}}
  ])
  DataFrame.collect(df)
end)

# EX-33 re-verify: lists as arrays
run.("2. [EX-33] create_dataframe with lists", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["eng", "lead"]},
    %{"id" => 2, "tags" => ["sales"]}
  ])
  DataFrame.collect(df)
end)

# EX-34 re-verify: STRUCT schema ordering
run.("3. [EX-34] create_dataframe STRUCT schema ordering", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "person" => %{"name" => "Alice", "age" => 30}}
  ], schema: "id INT, person STRUCT<name: STRING, age: INT>")
  DataFrame.collect(df)
end)

# EX-35 re-verify: MAP with varying keys
run.("4. [EX-35] create_dataframe MAP varying keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "meta" => %{"k1" => "v1", "k2" => "v2"}},
    %{"id" => 2, "meta" => %{"k3" => "v3"}}
  ], schema: "id INT, meta MAP<STRING, STRING>")
  DataFrame.collect(df)
end)

# EX-36 re-verify: roundtrip column ordering
run.("5. [EX-36] create_dataframe roundtrip column ordering", fn ->
  {:ok, rows} = SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name, true AS active")
    |> DataFrame.collect()
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "id INT, name STRING, active BOOLEAN")
  DataFrame.collect(df)
end)

# EX-37 re-verify: Date + schema ordering
run.("6. [EX-37] create_dataframe Date values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "dt" => ~D[2024-06-15]},
    %{"id" => 2, "dt" => ~D[2025-01-01]}
  ], schema: "id INT, dt DATE")
  DataFrame.collect(df)
end)

# EX-38 re-verify: Decimal values
run.("7. [EX-38] create_dataframe Decimal values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "amount" => Decimal.new("12345.67")},
    %{"id" => 2, "amount" => Decimal.new("99999.99")}
  ], schema: "id INT, amount DECIMAL(10,2)")
  DataFrame.collect(df)
end)

# EX-40 re-verify: lit() with list and map
run.("8. [EX-40] lit() with list", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("arr", lit([1, 2, 3]))
  |> DataFrame.collect()
end)

run.("9. [EX-40] lit() with map", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("m", lit(%{"k" => "v"}))
  |> DataFrame.collect()
end)

# EX-41 re-verify: date_trunc positional
run.("10. [EX-41] date_trunc positional (no list)", fn ->
  SparkEx.sql(s, "SELECT TIMESTAMP '2025-06-15 10:30:00' AS ts")
  |> DataFrame.with_column("truncated", date_trunc("month", col("ts")))
  |> DataFrame.collect()
end)

# =============================================
# NEW: Push further with complex data
# =============================================

# Nested map of maps
run.("11. create_dataframe: map of maps (2 levels deep)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"level1" => %{"a" => 10, "b" => 20}}},
    %{"id" => 2, "nested" => %{"level1" => %{"c" => 30}}}
  ])
  DataFrame.collect(df)
end)

# Lists of maps
run.("12. create_dataframe: list of maps", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [%{"name" => "x", "qty" => 5}, %{"name" => "y", "qty" => 3}]},
    %{"id" => 2, "items" => [%{"name" => "z", "qty" => 1}]}
  ])
  DataFrame.collect(df)
end)

# Maps with integer keys
run.("13. create_dataframe: maps with integer keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "scores" => %{1 => 100, 2 => 95}},
    %{"id" => 2, "scores" => %{1 => 80, 3 => 70}}
  ], schema: "id INT, scores MAP<INT, INT>")
  DataFrame.collect(df)
end)

# Mixed types in nested structures
run.("14. create_dataframe: struct with array and map fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "profile" => %{"tags" => ["a", "b"], "attrs" => %{"x" => "1"}}}
  ], schema: "id INT, profile STRUCT<tags: ARRAY<STRING>, attrs: MAP<STRING, STRING>>")
  DataFrame.collect(df)
end)

# Array of arrays
run.("15. create_dataframe: array of arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "matrix" => [[1, 2], [3, 4]]},
    %{"id" => 2, "matrix" => [[5, 6, 7]]}
  ], schema: "id INT, matrix ARRAY<ARRAY<INT>>")
  DataFrame.collect(df)
end)

# 4 columns all with complex types
run.("16. create_dataframe: 4 complex columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{
      "arr_col" => [1, 2, 3],
      "map_col" => %{"a" => 1, "b" => 2},
      "struct_col" => %{"name" => "Alice", "age" => 30},
      "nested_arr" => [[10, 20], [30]]
    }
  ], schema: "arr_col ARRAY<INT>, map_col MAP<STRING, INT>, struct_col STRUCT<name: STRING, age: INT>, nested_arr ARRAY<ARRAY<INT>>")
  DataFrame.collect(df)
end)

# lit() with nested list
run.("17. lit() with nested list (array of arrays)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("nested", lit([[1, 2], [3, 4]]))
  |> DataFrame.collect()
end)

# lit() with list of strings
run.("18. lit() with list of strings", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("tags", lit(["alpha", "beta", "gamma"]))
  |> DataFrame.collect()
end)

# lit() with map of ints
run.("19. lit() with map of ints", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("scores", lit(%{1 => 100, 2 => 200}))
  |> DataFrame.collect()
end)

# create_dataframe with 100 rows of complex data
run.("20. create_dataframe: 100 rows with array+map columns", fn ->
  rows = for i <- 1..100 do
    %{
      "id" => i,
      "tags" => ["tag_#{rem(i, 5)}", "group_#{rem(i, 3)}"],
      "meta" => %{"score" => i * 10}
    }
  end
  {:ok, df} = SparkEx.create_dataframe(s, rows,
    schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, INT>")
  # Verify count and sample
  {:ok, count_rows} = df |> DataFrame.select([count(lit(1)) |> Column.alias_("cnt")]) |> DataFrame.collect()
  {:ok, sample} = df |> DataFrame.filter(Column.eq(col("id"), lit(1))) |> DataFrame.collect()
  {count_rows, sample}
end)

# create_dataframe with DateTime precision
run.("21. create_dataframe: DateTime microsecond precision", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "ts" => ~U[2024-06-15 10:30:45.123456Z]},
    %{"id" => 2, "ts" => ~U[2025-01-01 00:00:00.000001Z]}
  ], schema: "id INT, ts TIMESTAMP")
  DataFrame.collect(df)
end)

# create_dataframe with nil in complex columns
run.("22. create_dataframe: nil in array and map columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b"], "meta" => %{"k" => "v"}},
    %{"id" => 2, "tags" => nil, "meta" => nil}
  ], schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>")
  DataFrame.collect(df)
end)

# create_dataframe then use in pipeline
run.("23. create_dataframe → filter → agg pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "salary" => 100000},
    %{"dept" => "eng", "salary" => 120000},
    %{"dept" => "sales", "salary" => 90000},
    %{"dept" => "sales", "salary" => 95000},
    %{"dept" => "hr", "salary" => 80000}
  ], schema: "dept STRING, salary INT")

  df
  |> DataFrame.filter(Column.gte(col("salary"), lit(90000)))
  |> DataFrame.group_by([col("dept")])
  |> DataFrame.agg([
    count(lit(1)) |> Column.alias_("cnt"),
    avg(col("salary")) |> Column.alias_("avg_salary")
  ])
  |> DataFrame.order_by([desc(col("avg_salary"))])
  |> DataFrame.collect()
end)

# create_dataframe then join with SQL result
run.("24. create_dataframe → join with SQL table", fn ->
  {:ok, lookup} = SparkEx.create_dataframe(s, [
    %{"code" => "US", "region" => "North America"},
    %{"code" => "UK", "region" => "Europe"},
    %{"code" => "JP", "region" => "Asia"}
  ], schema: "code STRING, region STRING")

  facts = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('US', 100), ('UK', 200), ('JP', 150), ('DE', 120)
    AS t(country_code, revenue)
  """)

  facts
  |> DataFrame.join(lookup, Column.eq(col("country_code"), col("code")), :left)
  |> DataFrame.select([col("country_code"), col("revenue"), col("region")])
  |> DataFrame.order_by([asc(col("country_code"))])
  |> DataFrame.collect()
end)

# Large Decimal precision
run.("25. create_dataframe: high-precision Decimal", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => Decimal.new("123456789.123456789")},
    %{"id" => 2, "val" => Decimal.new("-0.000000001")}
  ], schema: "id INT, val DECIMAL(28, 9)")
  DataFrame.collect(df)
end)

# Boolean arrays
run.("26. create_dataframe: boolean arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "flags" => [true, false, true]},
    %{"id" => 2, "flags" => [false, false]}
  ], schema: "id INT, flags ARRAY<BOOLEAN>")
  DataFrame.collect(df)
end)

# Empty containers
run.("27. create_dataframe: empty array and empty map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [], "m" => %{}},
    %{"id" => 2, "arr" => [1, 2], "m" => %{"k" => 1}}
  ], schema: "id INT, arr ARRAY<INT>, m MAP<STRING, INT>")
  DataFrame.collect(df)
end)

# Write complex create_dataframe to Iceberg table and read back
run.("28. create_dataframe → save_as_table → read back", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_cd_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => Decimal.new("95.5")},
    %{"id" => 2, "name" => "Bob", "score" => Decimal.new("87.3")},
    %{"id" => 3, "name" => "Carol", "score" => Decimal.new("92.1")}
  ], schema: "id INT, name STRING, score DECIMAL(5,1)")

  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

IO.puts("=== Re-verify and complex data tests complete ===")
