# Exploratory: Complex data structure roundtrips — send data via create_dataframe, collect back, verify fidelity
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 50, printable_limit: 3000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === 1. create_dataframe with various primitive types ===
run.("1. create_dataframe with integers, floats, strings, booleans, nil", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5, "active" => true, "notes" => nil},
    %{"id" => 2, "name" => "Bob", "score" => 87.3, "active" => false, "notes" => "good"},
    %{"id" => 3, "name" => nil, "score" => nil, "active" => nil, "notes" => ""}
  ])
  DataFrame.collect(df)
end)

# === 2. create_dataframe with nested maps ===
run.("2. create_dataframe with nested maps (simulating structs)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice", "age" => 30}},
    %{"id" => 2, "info" => %{"name" => "Bob", "age" => 25}}
  ])
  DataFrame.collect(df)
end)

# === 3. create_dataframe with lists (arrays) ===
run.("3. create_dataframe with lists", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["eng", "lead"]},
    %{"id" => 2, "tags" => ["sales"]},
    %{"id" => 3, "tags" => []}
  ])
  DataFrame.collect(df)
end)

# === 4. create_dataframe with explicit complex schema ===
run.("4. create_dataframe with ARRAY schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "vals" => [10, 20, 30]},
    %{"id" => 2, "vals" => [40, 50]}
  ], schema: "id INT, vals ARRAY<INT>")
  DataFrame.collect(df)
end)

# === 5. create_dataframe with STRUCT schema ===
run.("5. create_dataframe with STRUCT schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "person" => %{"name" => "Alice", "age" => 30}},
    %{"id" => 2, "person" => %{"name" => "Bob", "age" => 25}}
  ], schema: "id INT, person STRUCT<name: STRING, age: INT>")
  DataFrame.collect(df)
end)

# === 6. create_dataframe with MAP schema ===
run.("6. create_dataframe with MAP schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "meta" => %{"k1" => "v1", "k2" => "v2"}},
    %{"id" => 2, "meta" => %{"k3" => "v3"}}
  ], schema: "id INT, meta MAP<STRING, STRING>")
  DataFrame.collect(df)
end)

# === 7. Roundtrip: SQL create → collect → create_dataframe → collect ===
run.("7. Roundtrip: SQL → collect → create_dataframe → collect", fn ->
  {:ok, rows} = SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1, 'Alice', 50000.50, true),
      (2, 'Bob', 60000.75, false)
    AS t(id, name, salary, active)
  """) |> DataFrame.collect()

  IO.inspect(rows, label: "  Step1 rows")

  {:ok, df2} = SparkEx.create_dataframe(s, rows,
    schema: "id INT, name STRING, salary DECIMAL(8,2), active BOOLEAN"
  )
  DataFrame.collect(df2)
end)

# === 8. create_dataframe with Date values ===
run.("8. create_dataframe with Date values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "dt" => ~D[2024-06-15]},
    %{"id" => 2, "dt" => ~D[2024-12-31]}
  ], schema: "id INT, dt DATE")
  DataFrame.collect(df)
end)

# === 9. create_dataframe with DateTime/Timestamp values ===
run.("9. create_dataframe with DateTime values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "ts" => ~U[2024-06-15 14:30:00Z]},
    %{"id" => 2, "ts" => ~U[2024-12-31 23:59:59Z]}
  ], schema: "id INT, ts TIMESTAMP")
  DataFrame.collect(df)
end)

# === 10. create_dataframe with Decimal values ===
run.("10. create_dataframe with Decimal values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "amount" => Decimal.new("12345.67")},
    %{"id" => 2, "amount" => Decimal.new("99999.99")}
  ], schema: "id INT, amount DECIMAL(10,2)")
  DataFrame.collect(df)
end)

# === 11. create_dataframe with large row count ===
run.("11. create_dataframe with 100 rows", fn ->
  rows = Enum.map(1..100, fn i ->
    %{"id" => i, "val" => i * 10, "label" => "row_#{i}"}
  end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "id INT, val INT, label STRING")
  {:ok, cnt} = DataFrame.count(df)
  {:ok, first} = DataFrame.order_by(df, [asc(col("id"))]) |> DataFrame.head(3)
  {:ok, last} = DataFrame.order_by(df, [desc(col("id"))]) |> DataFrame.head(3)
  %{count: cnt, first: first, last: last}
end)

# === 12. SQL types decoded → various Elixir types ===
run.("12. All SQL types decoded to Elixir", fn ->
  SparkEx.sql(s, """
    SELECT
      CAST(1 AS TINYINT) AS tiny,
      CAST(2 AS SMALLINT) AS small,
      CAST(3 AS INT) AS int_val,
      CAST(4 AS BIGINT) AS big,
      CAST(5.5 AS FLOAT) AS float_val,
      CAST(6.6 AS DOUBLE) AS double_val,
      CAST(7.77 AS DECIMAL(5,2)) AS decimal_val,
      CAST('hello' AS STRING) AS string_val,
      CAST(true AS BOOLEAN) AS bool_val,
      CAST('2024-06-15' AS DATE) AS date_val,
      CAST('2024-06-15 14:30:00' AS TIMESTAMP) AS ts_val,
      CAST(X'DEADBEEF' AS BINARY) AS binary_val
  """) |> DataFrame.collect()
end)

# === 13. Nested struct roundtrip via SQL ===
run.("13. Deeply nested struct (3 levels) roundtrip", fn ->
  SparkEx.sql(s, """
    SELECT named_struct(
      'level1', named_struct(
        'level2', named_struct(
          'value', 'deep',
          'num', 42
        ),
        'other', 'mid'
      ),
      'top', 'surface'
    ) AS nested
  """) |> DataFrame.collect()
end)

# === 14. Array of structs roundtrip ===
run.("14. Array of structs", fn ->
  SparkEx.sql(s, """
    SELECT array(
      named_struct('name', 'Alice', 'age', 30),
      named_struct('name', 'Bob', 'age', 25),
      named_struct('name', 'Carol', 'age', 35)
    ) AS people
  """) |> DataFrame.collect()
end)

# === 15. Map with various key/value types ===
run.("15. Map<STRING, INT>", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m") |> DataFrame.collect()
end)

run.("16. Map<INT, STRING>", fn ->
  SparkEx.sql(s, "SELECT map(1, 'one', 2, 'two', 3, 'three') AS m") |> DataFrame.collect()
end)

run.("17. Map<STRING, ARRAY<INT>>", fn ->
  SparkEx.sql(s, "SELECT map('x', array(1,2,3), 'y', array(4,5)) AS m") |> DataFrame.collect()
end)

run.("18. Map<STRING, STRUCT<a: INT, b: STRING>>", fn ->
  SparkEx.sql(s, """
    SELECT map('k1', named_struct('a', 1, 'b', 'one'),
               'k2', named_struct('a', 2, 'b', 'two')) AS m
  """) |> DataFrame.collect()
end)

# === 16. Array of various element types ===
run.("19. Array<DOUBLE>", fn ->
  SparkEx.sql(s, "SELECT array(1.1, 2.2, 3.3, 4.4) AS arr") |> DataFrame.collect()
end)

run.("20. Array<BOOLEAN>", fn ->
  SparkEx.sql(s, "SELECT array(true, false, true, NULL) AS arr") |> DataFrame.collect()
end)

run.("21. Array<DATE>", fn ->
  SparkEx.sql(s, "SELECT array(DATE '2024-01-01', DATE '2024-06-15', DATE '2024-12-31') AS arr")
  |> DataFrame.collect()
end)

run.("22. Array<TIMESTAMP>", fn ->
  SparkEx.sql(s, """
    SELECT array(
      TIMESTAMP '2024-01-01 00:00:00',
      TIMESTAMP '2024-06-15 12:30:00',
      TIMESTAMP '2024-12-31 23:59:59'
    ) AS arr
  """) |> DataFrame.collect()
end)

# === 17. Null in complex types ===
run.("23. Null elements in array", fn ->
  SparkEx.sql(s, "SELECT array(1, NULL, 3, NULL, 5) AS arr") |> DataFrame.collect()
end)

run.("24. Null values in map", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', CAST(NULL AS INT), 'c', 3) AS m") |> DataFrame.collect()
end)

run.("25. Null struct field", fn ->
  SparkEx.sql(s, """
    SELECT named_struct('name', 'Alice', 'age', CAST(NULL AS INT)) AS person
  """) |> DataFrame.collect()
end)

run.("26. Entirely null complex column", fn ->
  SparkEx.sql(s, "SELECT CAST(NULL AS ARRAY<INT>) AS arr, CAST(NULL AS MAP<STRING,INT>) AS m")
  |> DataFrame.collect()
end)

# === 18. Mixed row types with nulls in create_dataframe ===
run.("27. create_dataframe with nil in various positions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x", "c" => 10.0},
    %{"a" => nil, "b" => nil, "c" => nil},
    %{"a" => 3, "b" => "z", "c" => 30.0}
  ])
  DataFrame.collect(df)
end)

# === 19. Very wide row (30 columns) via create_dataframe ===
run.("28. create_dataframe with 30 columns", fn ->
  row = Enum.reduce(1..30, %{}, fn i, acc -> Map.put(acc, "col_#{i}", i * 10) end)
  {:ok, df} = SparkEx.create_dataframe(s, [row, row])
  {:ok, cols} = DataFrame.columns(df)
  {:ok, cnt} = DataFrame.count(df)
  %{num_columns: Kernel.length(cols), row_count: cnt}
end)

# === 20. Binary data roundtrip ===
run.("29. Binary data encode/decode", fn ->
  SparkEx.sql(s, "SELECT X'48656C6C6F' AS bin_data, CAST('Hello' AS BINARY) AS str_bin")
  |> DataFrame.collect()
end)

# === 21. Special float values roundtrip ===
run.("30. NaN, Infinity, -Infinity in arrays", fn ->
  SparkEx.sql(s, """
    SELECT array(
      CAST('NaN' AS DOUBLE),
      CAST('Infinity' AS DOUBLE),
      CAST('-Infinity' AS DOUBLE),
      1.0,
      0.0
    ) AS special_floats
  """) |> DataFrame.collect()
end)

# === 22. Empty containers ===
run.("31. Empty array and empty map", fn ->
  SparkEx.sql(s, """
    SELECT
      array() AS empty_arr,
      map() AS empty_map
  """) |> DataFrame.collect()
end)

# === 23. Very large map (50 entries) ===
run.("32. Large map (50 entries)", fn ->
  entries = Enum.map(1..50, fn i -> "'k#{i}', #{i}" end) |> Enum.join(", ")
  SparkEx.sql(s, "SELECT map(#{entries}) AS big_map")
  |> DataFrame.select([size(col("big_map")) |> Column.alias_("map_size")])
  |> DataFrame.collect()
end)

# === 24. Struct with many fields (15) ===
run.("33. Struct with 15 fields", fn ->
  fields = Enum.map(1..15, fn i -> "'f#{i}', #{i}" end) |> Enum.join(", ")
  SparkEx.sql(s, "SELECT named_struct(#{fields}) AS wide_struct")
  |> DataFrame.collect()
end)

# === 25. create_dataframe without schema (auto-infer) ===
run.("34. create_dataframe auto-infer schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5},
    %{"id" => 2, "name" => "Bob", "score" => 87.3}
  ])
  dtypes = DataFrame.dtypes(df)
  rows = DataFrame.collect(df)
  %{dtypes: dtypes, rows: rows}
end)

# === 26. create_dataframe with single row ===
run.("35. create_dataframe single row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 42}])
  DataFrame.collect(df)
end)

# === 27. create_dataframe with empty list ===
run.("36. create_dataframe with empty list", fn ->
  SparkEx.create_dataframe(s, [])
end)

# === 28. Extreme string values ===
run.("37. Strings with special chars", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('line1\\nline2'),
      ('tab\\there'),
      ('quote\\'s'),
      ('backslash\\\\path'),
      ('emoji: 🎉🚀'),
      ('null char: \\0end'),
      ('')
    AS t(text)
  """) |> DataFrame.collect()
end)

# === 29. Timestamp with microsecond precision ===
run.("38. Timestamp microsecond precision", fn ->
  SparkEx.sql(s, """
    SELECT
      TIMESTAMP '2024-06-15 14:30:45.123456' AS micro_ts,
      TIMESTAMP '2024-06-15 14:30:45.000001' AS one_micro,
      TIMESTAMP '2024-06-15 14:30:45.999999' AS max_micro
  """) |> DataFrame.collect()
end)

# === 30. Decimal edge cases ===
run.("39. Decimal edge cases", fn ->
  SparkEx.sql(s, """
    SELECT
      CAST(0.0 AS DECIMAL(10,2)) AS zero,
      CAST(-99999999.99 AS DECIMAL(10,2)) AS neg_max,
      CAST(99999999.99 AS DECIMAL(10,2)) AS pos_max,
      CAST(0.01 AS DECIMAL(10,2)) AS smallest,
      CAST(-0.01 AS DECIMAL(10,2)) AS neg_smallest
  """) |> DataFrame.collect()
end)

IO.puts("=== Complex data roundtrip exploration complete ===")
