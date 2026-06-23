# Round 11: Edge cases in collect/decode — large results, special values in containers,
# type coercion during collect, column naming edge cases
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

# === 1. Null elements inside arrays ===
run.("1. Array with null elements", fn ->
  SparkEx.sql(s, "SELECT array(1, null, 3, null, 5) AS arr")
  |> DataFrame.collect()
end)

# === 2. Map with null values ===
run.("2. Map with null values", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', CAST(NULL AS INT), 'c', 3) AS m")
  |> DataFrame.collect()
end)

# === 3. Struct with null field ===
run.("3. Struct with null field", fn ->
  SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', CAST(NULL AS INT)) AS person")
  |> DataFrame.collect()
end)

# === 4. NaN in arrays ===
run.("4. NaN and Infinity in arrays", fn ->
  SparkEx.sql(s, """
    SELECT array(
      CAST('NaN' AS DOUBLE),
      CAST('Infinity' AS DOUBLE),
      CAST('-Infinity' AS DOUBLE),
      1.5
    ) AS special_arr
  """)
  |> DataFrame.collect()
end)

# === 5. Very large array (1000 elements) ===
run.("5. Large array (1000 elements)", fn ->
  SparkEx.sql(s, "SELECT sequence(1, 1000) AS big_arr")
  |> DataFrame.select([
    array_size(col("big_arr")) |> Column.alias_("sz"),
    element_at(col("big_arr"), lit(1)) |> Column.alias_("first"),
    element_at(col("big_arr"), lit(1000)) |> Column.alias_("last")
  ])
  |> DataFrame.collect()
end)

# === 6. Large map (100 entries) ===
run.("6. Large map (100 entries)", fn ->
  # Build a map with 100 entries via SQL
  SparkEx.sql(s, """
    SELECT map_from_arrays(
      transform(sequence(1, 100), x -> concat('key_', CAST(x AS STRING))),
      sequence(1, 100)
    ) AS big_map
  """)
  |> DataFrame.select([
    size(col("big_map")) |> Column.alias_("sz"),
    element_at(col("big_map"), lit("key_1")) |> Column.alias_("first_val"),
    element_at(col("big_map"), lit("key_100")) |> Column.alias_("last_val")
  ])
  |> DataFrame.collect()
end)

# === 7. Struct with 20 fields ===
run.("7. Struct with 20 fields", fn ->
  fields = Enum.map(1..20, fn i -> "'f#{i}', #{i}" end) |> Enum.join(", ")
  SparkEx.sql(s, "SELECT named_struct(#{fields}) AS wide_struct")
  |> DataFrame.collect()
end)

# === 8. Array of different-length arrays ===
run.("8. Array of variable-length arrays", fn ->
  SparkEx.sql(s, """
    SELECT array(
      array(1),
      array(2, 3),
      array(4, 5, 6),
      array(7, 8, 9, 10)
    ) AS ragged
  """)
  |> DataFrame.collect()
end)

# === 9. Unicode in map keys and struct field values ===
run.("9. Unicode in map keys and values", fn ->
  SparkEx.sql(s, """
    SELECT map('日本語', 1, 'émoji🎉', 2, 'ñoño', 3) AS unicode_map
  """)
  |> DataFrame.collect()
end)

# === 10. Empty string keys in map ===
run.("10. Empty string key in map", fn ->
  SparkEx.sql(s, "SELECT map('', 0, 'a', 1) AS m")
  |> DataFrame.collect()
end)

# === 11. SQL LATERAL VIEW explode on create_dataframe result ===
run.("11. create_dataframe → register → LATERAL VIEW explode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["x", "y", "z"]},
    %{"id" => 2, "tags" => ["a"]}
  ], schema: "id INT, tags ARRAY<STRING>")

  DataFrame.create_or_replace_temp_view(df, "tagged_items")

  SparkEx.sql(s, """
    SELECT id, tag
    FROM tagged_items
    LATERAL VIEW explode(tags) t AS tag
    ORDER BY id, tag
  """)
  |> DataFrame.collect()
end)

# === 12. collect_as_map from create_dataframe ===
run.("12. create_dataframe → collect_as_map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "value" => 10},
    %{"key" => "b", "value" => 20},
    %{"key" => "c", "value" => 30}
  ], schema: "key STRING, value INT")

  DataFrame.collect_as_map(df)
end)

# === 13. Multiple collect formats: collect vs first vs head ===
run.("13. create_dataframe → first", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "one"},
    %{"id" => 2, "val" => "two"},
    %{"id" => 3, "val" => "three"}
  ], schema: "id INT, val STRING")
  DataFrame.first(df)
end)

run.("14. create_dataframe → head(2)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "one"},
    %{"id" => 2, "val" => "two"},
    %{"id" => 3, "val" => "three"}
  ], schema: "id INT, val STRING")
  DataFrame.head(df, 2)
end)

run.("15. create_dataframe → tail(1)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "one"},
    %{"id" => 2, "val" => "two"},
    %{"id" => 3, "val" => "three"}
  ], schema: "id INT, val STRING")
  DataFrame.tail(df, 1)
end)

# === 16. dtypes on create_dataframe with complex schema ===
run.("16. create_dataframe complex → dtypes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a"], "meta" => %{"k" => "v"}, "info" => %{"name" => "test"}}
  ], schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>, info STRUCT<name: STRING>")
  DataFrame.dtypes(df)
end)

# === 17. columns on create_dataframe ===
run.("17. create_dataframe → columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"z_col" => 1, "a_col" => 2, "m_col" => 3}
  ], schema: "z_col INT, a_col INT, m_col INT")
  DataFrame.columns(df)
end)

# === 18. describe on create_dataframe ===
run.("18. create_dataframe → describe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10.5},
    %{"id" => 2, "val" => 20.3},
    %{"id" => 3, "val" => 30.1}
  ], schema: "id INT, val DOUBLE")
  DataFrame.describe(df) |> DataFrame.collect()
end)

# === 19. create_dataframe with BIGINT values ===
run.("19. create_dataframe: BIGINT edge values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "big" => 9_223_372_036_854_775_807},  # Long.MAX_VALUE
    %{"id" => 2, "big" => -9_223_372_036_854_775_808}   # Long.MIN_VALUE
  ], schema: "id INT, big BIGINT")
  DataFrame.collect(df)
end)

# === 20. create_dataframe with FLOAT vs DOUBLE precision ===
run.("20. create_dataframe: FLOAT precision", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "flt" => 3.14159265358979, "dbl" => 3.14159265358979}
  ], schema: "id INT, flt FLOAT, dbl DOUBLE")
  DataFrame.collect(df)
end)

# === 21. create_dataframe with binary/bytes ===
run.("21. create_dataframe: binary data", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "data" => <<0xDE, 0xAD, 0xBE, 0xEF>>}
  ], schema: "id INT, data BINARY")
  DataFrame.collect(df)
end)

# === 22. Multiple create_dataframe in same session, different schemas ===
run.("22. Two create_dataframes, different schemas, then join", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")

  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 95.5},
    %{"id" => 2, "score" => 87.3}
  ], schema: "id INT, score DOUBLE")

  DataFrame.join(left, right, ["id"])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 23. create_dataframe → to_explorer conversion ===
run.("23. create_dataframe → to_explorer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  explorer_df = DataFrame.to_explorer(df)
  IO.inspect(explorer_df, label: "  Explorer DF")
end)

# === 24. Chained create_dataframe → multiple transforms → collect ===
run.("24. create_dataframe → 5-step pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"id" => i, "group" => "g#{rem(i, 3)}", "value" => i * 10}
    end),
    schema: "id INT, group STRING, value INT")

  df
  |> DataFrame.filter(Column.gt(col("value"), lit(50)))
  |> DataFrame.with_column("doubled", Column.multiply(col("value"), lit(2)))
  |> DataFrame.group_by([col("group")])
  |> DataFrame.agg([
    count(lit(1)) |> Column.alias_("cnt"),
    sum(col("doubled")) |> Column.alias_("total"),
    avg(col("value")) |> Column.alias_("avg_val")
  ])
  |> DataFrame.order_by([asc(col("group"))])
  |> DataFrame.collect()
end)

# === 25. create_dataframe with single column ===
run.("25. create_dataframe: single column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"value" => 1}, %{"value" => 2}, %{"value" => 3}
  ], schema: "value INT")
  DataFrame.collect(df)
end)

# === 26. create_dataframe with 1 row, many types ===
run.("26. create_dataframe: 1 row, 8 different types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{
      "a_int" => 42,
      "b_long" => 9999999999,
      "c_float" => 3.14,
      "d_string" => "hello",
      "e_bool" => true,
      "f_decimal" => Decimal.new("123.45"),
      "g_date" => ~D[2025-06-15],
      "h_timestamp" => ~U[2025-06-15 10:30:00Z]
    }
  ], schema: "a_int INT, b_long BIGINT, c_float DOUBLE, d_string STRING, e_bool BOOLEAN, f_decimal DECIMAL(10,2), g_date DATE, h_timestamp TIMESTAMP")
  DataFrame.collect(df)
end)

IO.puts("=== Collect/decode edge case tests complete ===")
