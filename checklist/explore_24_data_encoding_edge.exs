# Exploratory: Data encoding edge cases — create_dataframe with tricky inputs,
# large payloads, type mismatches, schema conflicts
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

# === 1. create_dataframe with mismatched column keys across rows ===
run.("1. create_dataframe: rows with different keys", fn ->
  SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "score" => 100},
    %{"name" => "Carol", "score" => 90}
  ])
end)

# === 2. create_dataframe with integer that overflows INT ===
run.("2. create_dataframe: integer > INT max (auto schema)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "big" => 9_999_999_999},
    %{"id" => 2, "big" => -9_999_999_999}
  ])
  DataFrame.collect(df)
end)

# === 3. create_dataframe with float special values ===
run.("3. create_dataframe: NaN and Infinity floats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => :nan},
    %{"id" => 2, "val" => :infinity},
    %{"id" => 3, "val" => :neg_infinity}
  ])
  DataFrame.collect(df)
end)

# === 4. create_dataframe with atom values ===
run.("4. create_dataframe: atoms as values", fn ->
  SparkEx.create_dataframe(s, [
    %{"id" => 1, "status" => :active},
    %{"id" => 2, "status" => :inactive}
  ])
end)

# === 5. create_dataframe with tuple values ===
run.("5. create_dataframe: tuples as values", fn ->
  SparkEx.create_dataframe(s, [
    %{"id" => 1, "pair" => {10, 20}},
    %{"id" => 2, "pair" => {30, 40}}
  ])
end)

# === 6. create_dataframe where schema disagrees with data ===
run.("6. create_dataframe: schema says INT but data is string", fn ->
  SparkEx.create_dataframe(s, [
    %{"id" => "not_an_int", "name" => "Alice"}
  ], schema: "id INT, name STRING")
end)

# === 7. lit() with various Elixir types ===
run.("7. lit() with integer", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(42))
  |> DataFrame.collect()
end)

run.("8. lit() with float", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(3.14))
  |> DataFrame.collect()
end)

run.("9. lit() with string", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit("hello"))
  |> DataFrame.collect()
end)

run.("10. lit() with boolean", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(true))
  |> DataFrame.collect()
end)

run.("11. lit() with nil", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(nil))
  |> DataFrame.collect()
end)

run.("12. lit() with Decimal", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(Decimal.new("123.45")))
  |> DataFrame.collect()
end)

run.("13. lit() with Date", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(~D[2024-06-15]))
  |> DataFrame.collect()
end)

run.("14. lit() with DateTime", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(~U[2024-06-15 14:30:00Z]))
  |> DataFrame.collect()
end)

run.("15. lit() with negative integer", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(-999))
  |> DataFrame.collect()
end)

run.("16. lit() with zero", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(0))
  |> DataFrame.collect()
end)

run.("17. lit() with very large integer", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(9_999_999_999_999))
  |> DataFrame.collect()
end)

run.("18. lit() with empty string", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(""))
  |> DataFrame.collect()
end)

run.("19. lit() with unicode string", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit("日本語テスト 🎉"))
  |> DataFrame.collect()
end)

run.("20. lit() with list (expect error or array)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit([1, 2, 3]))
  |> DataFrame.collect()
end)

run.("21. lit() with map (expect error or map column)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("val", lit(%{"k" => "v"}))
  |> DataFrame.collect()
end)

# === 8. Column.cast roundtrip edge cases ===
run.("22. Cast BIGINT to STRING and back", fn ->
  SparkEx.sql(s, "SELECT 9999999999999 AS big")
  |> DataFrame.select([
    Column.cast(col("big"), "STRING") |> Column.alias_("as_str")
  ])
  |> DataFrame.select([
    Column.cast(col("as_str"), "BIGINT") |> Column.alias_("back_to_big")
  ])
  |> DataFrame.collect()
end)

run.("23. Cast DOUBLE to DECIMAL and back", fn ->
  SparkEx.sql(s, "SELECT 3.14159265358979 AS pi")
  |> DataFrame.select([
    Column.cast(col("pi"), "DECIMAL(10,5)") |> Column.alias_("as_dec")
  ])
  |> DataFrame.select([
    Column.cast(col("as_dec"), "DOUBLE") |> Column.alias_("back_to_double")
  ])
  |> DataFrame.collect()
end)

run.("24. Cast BOOLEAN to INT and back", fn ->
  SparkEx.sql(s, "SELECT true AS flag")
  |> DataFrame.select([Column.cast(col("flag"), "INT") |> Column.alias_("as_int")])
  |> DataFrame.select([Column.cast(col("as_int"), "BOOLEAN") |> Column.alias_("back")])
  |> DataFrame.collect()
end)

run.("25. Cast DATE to STRING to TIMESTAMP", fn ->
  SparkEx.sql(s, "SELECT DATE '2024-06-15' AS dt")
  |> DataFrame.select([Column.cast(col("dt"), "STRING") |> Column.alias_("as_str")])
  |> DataFrame.select([Column.cast(col("as_str"), "TIMESTAMP") |> Column.alias_("as_ts")])
  |> DataFrame.collect()
end)

# === 9. collect_as_map / first / head edge cases ===
run.("26. DataFrame.first on multi-row df", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(id, name)")
  |> DataFrame.first()
end)

run.("27. DataFrame.head with n=0", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3) AS t(id)")
  |> DataFrame.head(0)
end)

run.("28. DataFrame.head with n larger than row count", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2) AS t(id)")
  |> DataFrame.head(100)
end)

# === 10. Very large literal in expression ===
run.("29. Very long string literal in expression", fn ->
  long = String.duplicate("abcdefghij", 1000)  # 10000 chars
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("long_val", lit(long))
  |> DataFrame.select([SparkEx.Functions.length(col("long_val")) |> Column.alias_("len")])
  |> DataFrame.collect()
end)

# === 11. save_as_table then read back complex types ===
run.("30. Write & read back struct+array via table", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_rt_#{run_id}"

  SparkEx.sql(s, """
    SELECT
      1 AS id,
      named_struct('name', 'Alice', 'scores', array(90, 85, 92)) AS profile,
      map('level', 'senior', 'team', 'alpha') AS meta
  """)
  |> DataFrame.write()
  |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

IO.puts("=== Data encoding edge cases complete ===")
