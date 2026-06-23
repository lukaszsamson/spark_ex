# Exploratory: edge case data types and complex nested structures
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# 1. Special float values: NaN, Infinity, -Infinity
run.("1. Special floats (NaN, Inf, -Inf)", fn ->
  SparkEx.sql(s, """
    SELECT CAST('NaN' AS DOUBLE) AS nan_val,
           CAST('Infinity' AS DOUBLE) AS pos_inf,
           CAST('-Infinity' AS DOUBLE) AS neg_inf
  """) |> DataFrame.collect()
end)

# 2. Very large numbers
run.("2. Very large numbers", fn ->
  SparkEx.sql(s, """
    SELECT CAST(9223372036854775807 AS LONG) AS max_long,
           CAST(-9223372036854775808 AS LONG) AS min_long,
           CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) AS big_decimal
  """) |> DataFrame.collect()
end)

# 3. Empty string vs null
run.("3. Empty string vs null", fn ->
  SparkEx.sql(s, """
    SELECT '' AS empty_str, NULL AS null_val, ' ' AS space_str
  """) |> DataFrame.collect()
end)

# 4. Unicode and special characters
run.("4. Unicode strings", fn ->
  SparkEx.sql(s, """
    SELECT '日本語' AS japanese, 'émojis 🎉🔥' AS emoji, 'café' AS french,
           'line1\\nline2' AS newline, 'tab\\there' AS tab_char
  """) |> DataFrame.collect()
end)

# 5. Deeply nested struct
run.("5. Deeply nested struct", fn ->
  SparkEx.sql(s, """
    SELECT named_struct(
      'level1', named_struct(
        'level2', named_struct(
          'level3', 'deep_value'
        ),
        'sibling', 42
      ),
      'top_field', 'hello'
    ) AS nested
  """) |> DataFrame.collect()
end)

# 6. Array of structs
run.("6. Array of structs", fn ->
  SparkEx.sql(s, """
    SELECT array(
      named_struct('name', 'Alice', 'age', 30),
      named_struct('name', 'Bob', 'age', 25)
    ) AS people
  """) |> DataFrame.collect()
end)

# 7. Map of arrays
run.("7. Map of arrays", fn ->
  SparkEx.sql(s, """
    SELECT map('fruits', array('apple', 'banana'), 'vegs', array('carrot', 'pea')) AS food_map
  """) |> DataFrame.collect()
end)

# 8. Array of maps
run.("8. Array of maps", fn ->
  SparkEx.sql(s, """
    SELECT array(map('k1', 1, 'k2', 2), map('k3', 3, 'k4', 4)) AS map_arr
  """) |> DataFrame.collect()
end)

# 9. Null in arrays and maps
run.("9. Nulls in complex types", fn ->
  SparkEx.sql(s, """
    SELECT array(1, NULL, 3) AS arr_with_null,
           map('a', 1, 'b', NULL) AS map_with_null
  """) |> DataFrame.collect()
end)

# 10. Binary data
run.("10. Binary data round-trip", fn ->
  SparkEx.sql(s, """
    SELECT X'DEADBEEF' AS hex_bin, X'00FF' AS short_bin, X'' AS empty_bin
  """) |> DataFrame.collect()
end)

# 11. Date edge cases
run.("11. Date edge cases", fn ->
  SparkEx.sql(s, """
    SELECT DATE '1970-01-01' AS epoch,
           DATE '2000-02-29' AS leap_day,
           DATE '9999-12-31' AS max_date,
           DATE '0001-01-01' AS min_date
  """) |> DataFrame.collect()
end)

# 12. Timestamp edge cases
run.("12. Timestamp edge cases", fn ->
  SparkEx.sql(s, """
    SELECT TIMESTAMP '1970-01-01 00:00:00' AS epoch_ts,
           TIMESTAMP '2024-06-15 23:59:59.999999' AS micro_ts,
           TIMESTAMP '2024-01-01 00:00:00+05:30' AS tz_ts
  """) |> DataFrame.collect()
end)

# 13. Interval types
run.("13. Interval types", fn ->
  SparkEx.sql(s, """
    SELECT INTERVAL '1' YEAR AS year_interval,
           INTERVAL '2' MONTH AS month_interval,
           INTERVAL '3' DAY AS day_interval,
           INTERVAL '4' HOUR AS hour_interval
  """) |> DataFrame.collect()
end)

# 14. create_dataframe with complex types
run.("14. create_dataframe with nested maps", fn ->
  {:ok, nested_df} =
    SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => %{"color" => "red", "size" => "large"}},
    %{"id" => 2, "tags" => %{"color" => "blue", "size" => "small"}}
    ])

  DataFrame.collect(nested_df)
end)

# 15. create_dataframe with arrays
run.("15. create_dataframe with arrays", fn ->
  {:ok, array_df} =
    SparkEx.create_dataframe(s, [
    %{"id" => 1, "scores" => [90, 85, 92]},
    %{"id" => 2, "scores" => [78, 88]}
    ])

  DataFrame.collect(array_df)
end)

# 16. create_dataframe with nil values in different positions
run.("16. create_dataframe with nils", fn ->
  {:ok, nils_df} =
    SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "val" => nil},
    %{"id" => nil, "name" => nil, "val" => 42},
    %{"id" => 3, "name" => "Carol", "val" => 99}
    ])

  DataFrame.collect(nils_df)
end)

# 17. create_dataframe with booleans and floats
run.("17. create_dataframe with bools and floats", fn ->
  {:ok, bools_df} =
    SparkEx.create_dataframe(s, [
    %{"flag" => true, "ratio" => 3.14, "name" => "pi"},
    %{"flag" => false, "ratio" => 2.718, "name" => "e"}
    ])

  DataFrame.collect(bools_df)
end)

# 18. Empty DataFrame
run.("18. Empty DataFrame operations", fn ->
  empty = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, val) WHERE false")
  schema = DataFrame.schema(empty)
  count = DataFrame.count(empty)
  collected = DataFrame.collect(empty)
  %{schema: schema, count: count, collected: collected}
end)

IO.puts("=== Types exploration complete ===")
