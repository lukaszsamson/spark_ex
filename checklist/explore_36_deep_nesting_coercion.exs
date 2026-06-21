# Round 14: Deep nesting, type coercion, schema edge cases, wide schemas
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

# === 1. Type coercion: INT data with BIGINT schema ===
run.("1. create_dataframe: INT data, BIGINT schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id BIGINT, val BIGINT")
  DataFrame.dtypes(df)
end)

# === 2. Type coercion: integer data with DOUBLE schema ===
run.("2. create_dataframe: int data, DOUBLE schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id INT, val DOUBLE")
  DataFrame.collect(df)
end)

# === 3. Type coercion: float data with DECIMAL schema ===
run.("3. create_dataframe: float data, DECIMAL schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 3.14}
  ], schema: "id INT, val DECIMAL(10,2)")
  DataFrame.collect(df)
end)

# === 4. Type coercion: string data with INT schema ===
run.("4. create_dataframe: string '42' with INT schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "42"}
  ], schema: "id INT, val INT")
  DataFrame.collect(df)
end)

# === 5. TINYINT and SMALLINT ===
run.("5. create_dataframe: TINYINT and SMALLINT", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"tiny" => 127, "small" => 32767}
  ], schema: "tiny TINYINT, small SMALLINT")
  {:ok, rows} = DataFrame.collect(df)
  {:ok, dtypes} = DataFrame.dtypes(df)
  {rows, dtypes}
end)

# === 6. FLOAT vs DOUBLE precision ===
run.("6. create_dataframe: FLOAT schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "flt" => 3.14159265358979}
  ], schema: "id INT, flt FLOAT")
  DataFrame.collect(df)
end)

# === 7. Wide schema: 30 columns of mixed types ===
run.("7. create_dataframe: 30 columns mixed types", fn ->
  row = Map.new(1..30, fn i ->
    case rem(i, 5) do
      0 -> {"col_#{i}", true}
      1 -> {"col_#{i}", i}
      2 -> {"col_#{i}", i * 1.5}
      3 -> {"col_#{i}", "str_#{i}"}
      4 -> {"col_#{i}", Decimal.new("#{i}.99")}
    end
  end)

  schema_parts = Enum.map(1..30, fn i ->
    case rem(i, 5) do
      0 -> "col_#{i} BOOLEAN"
      1 -> "col_#{i} INT"
      2 -> "col_#{i} DOUBLE"
      3 -> "col_#{i} STRING"
      4 -> "col_#{i} DECIMAL(10,2)"
    end
  end)

  {:ok, df} = SparkEx.create_dataframe(s, [row], schema: Enum.join(schema_parts, ", "))
  {:ok, dtypes} = DataFrame.dtypes(df)
  {:ok, cols} = DataFrame.columns(df)
  {length(dtypes), length(cols), Enum.take(dtypes, 5)}
end)

# === 8. Duplicate column names in schema ===
run.("8. create_dataframe: duplicate column names", fn ->
  SparkEx.create_dataframe(s, [
    %{"id" => 1, "id" => 2}
  ], schema: "id INT, id INT")
end)

# === 9. Very deep nesting: 5-level struct ===
run.("9. SQL: 5-level nested struct", fn ->
  SparkEx.sql(s, """
    SELECT named_struct(
      'l1', named_struct(
        'l2', named_struct(
          'l3', named_struct(
            'l4', named_struct(
              'value', 'deep'
            )
          )
        )
      )
    ) AS deep5
  """)
  |> DataFrame.collect()
end)

# === 10. 5-level nesting via create_dataframe ===
run.("10. create_dataframe: 5-level nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "l1" => %{"l2" => %{"l3" => %{"l4" => %{"val" => "deep"}}}}}
  ], schema: "id INT, l1 STRUCT<l2: STRUCT<l3: STRUCT<l4: STRUCT<val: STRING>>>>")
  DataFrame.collect(df)
end)

# === 11. Array of array of array (3-level nested arrays) ===
run.("11. create_dataframe: ARRAY<ARRAY<ARRAY<INT>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "cube" => [[[1, 2], [3, 4]], [[5, 6]]]}
  ], schema: "id INT, cube ARRAY<ARRAY<ARRAY<INT>>>")
  DataFrame.collect(df)
end)

# === 12. MAP<STRING, MAP<STRING, INT>> ===
run.("12. create_dataframe: MAP<STRING, MAP<STRING, INT>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested_map" => %{"outer" => %{"inner_a" => 10, "inner_b" => 20}}}
  ], schema: "id INT, nested_map MAP<STRING, MAP<STRING, INT>>")
  DataFrame.collect(df)
end)

# === 13. STRUCT containing ARRAY<STRUCT<...>> ===
run.("13. create_dataframe: STRUCT<items: ARRAY<STRUCT<k: STRING, v: INT>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "data" => %{"items" => [%{"k" => "a", "v" => 1}, %{"k" => "b", "v" => 2}]}}
  ], schema: "id INT, data STRUCT<items: ARRAY<STRUCT<k: STRING, v: INT>>>")
  DataFrame.collect(df)
end)

# === 14. create_dataframe with nil in nested struct ===
run.("14. create_dataframe: nil at various nesting levels", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "outer" => %{"inner" => %{"val" => 42}}},
    %{"id" => 2, "outer" => %{"inner" => nil}},
    %{"id" => 3, "outer" => nil}
  ], schema: "id INT, outer STRUCT<inner: STRUCT<val: INT>>")
  DataFrame.collect(df)
end)

# === 15. create_dataframe with nil elements in nested array ===
run.("15. create_dataframe: nil in nested arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "matrix" => [[1, nil], [nil, 4]]},
    %{"id" => 2, "matrix" => [nil, [5, 6]]}
  ], schema: "id INT, matrix ARRAY<ARRAY<INT>>")
  DataFrame.collect(df)
end)

# === 16. create_dataframe roundtrip: collect complex → re-create ===
run.("16. Roundtrip: SQL complex → collect → create_dataframe", fn ->
  {:ok, rows} = SparkEx.sql(s, """
    SELECT 1 AS id, array(10, 20) AS arr, 'hello' AS name
  """) |> DataFrame.collect()

  # Re-create from collected rows
  {:ok, df2} = SparkEx.create_dataframe(s, rows,
    schema: "id INT, arr ARRAY<INT>, name STRING")
  DataFrame.collect(df2)
end)

# === 17. create_dataframe with array of maps (previously EX-1 family) ===
run.("17. create_dataframe: ARRAY<MAP<STRING, STRING>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [%{"a" => "1"}, %{"b" => "2"}]}
  ], schema: "id INT, items ARRAY<MAP<STRING, STRING>>")
  DataFrame.collect(df)
end)

# === 18. create_dataframe → Iceberg with complex types → read back → to_explorer ===
run.("18. Complex roundtrip: create → Iceberg → read → to_explorer", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_crt_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b"], "score" => Decimal.new("95.5")},
    %{"id" => 2, "tags" => ["c"], "score" => Decimal.new("87.3")}
  ], schema: "id INT, tags ARRAY<STRING>, score DECIMAL(5,1)")

  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  read_df = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id")
  explorer_result = DataFrame.to_explorer(read_df)
  collect_result = DataFrame.collect(read_df)

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  {explorer_result, collect_result}
end)

# === 19. create_dataframe with map where values are Decimals ===
run.("19. create_dataframe: MAP<STRING, DECIMAL> with various precisions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "prices" => %{
      "item_a" => Decimal.new("0.01"),
      "item_b" => Decimal.new("99999.99"),
      "item_c" => Decimal.new("-123.456")
    }}
  ], schema: "id INT, prices MAP<STRING, DECIMAL(10,3)>")
  DataFrame.collect(df)
end)

# === 20. create_dataframe then use with_columns (multiple) ===
run.("20. create_dataframe → with_columns (batch add)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "base" => 100},
    %{"id" => 2, "base" => 200}
  ], schema: "id INT, base INT")

  df
  |> DataFrame.with_columns([
    {"doubled", Column.multiply(col("base"), lit(2))},
    {"tripled", Column.multiply(col("base"), lit(3))},
    {"label", concat_ws("", [lit("item_"), Column.cast(col("id"), "STRING")])}
  ])
  |> DataFrame.collect()
end)

# === 21. create_dataframe → crosstab ===
run.("21. create_dataframe → crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"dept" => "dept_#{rem(i, 3)}", "status" => if(rem(i, 2) == 0, do: "active", else: "inactive")}
    end),
    schema: "dept STRING, status STRING")

  DataFrame.crosstab(df, "dept", "status") |> DataFrame.collect()
end)

# === 22. create_dataframe → freq_items ===
run.("22. create_dataframe → freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"color" => Enum.at(["red", "blue", "red", "green", "red"], rem(i, 5))}
    end),
    schema: "color STRING")

  DataFrame.freq_items(df, ["color"]) |> DataFrame.collect()
end)

# === 23. create_dataframe with very many rows (2000) ===
run.("23. create_dataframe: 2000 rows", fn ->
  rows = Enum.map(1..2000, fn i -> %{"id" => i, "val" => rem(i * 7, 100)} end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "id INT, val INT")

  df
  |> DataFrame.select([
    count(lit(1)) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total"),
    min(col("id")) |> Column.alias_("min_id"),
    max(col("id")) |> Column.alias_("max_id")
  ])
  |> DataFrame.collect()
end)

# === 24. create_dataframe → approx_quantile ===
run.("24. create_dataframe → approx_quantile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")

  DataFrame.approx_quantile(df, ["val"], [0.25, 0.5, 0.75], 0.01)
end)

# === 25. create_dataframe → corr / cov ===
run.("25. create_dataframe → corr and cov", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => i * 2.0 + 5.0} end),
    schema: "x DOUBLE, y DOUBLE")

  {:ok, corr} = DataFrame.corr(df, "x", "y")
  {:ok, cov} = DataFrame.cov(df, "x", "y")
  {corr, cov}
end)

IO.puts("=== Deep nesting, coercion, and schema edge case tests complete ===")
