# Round 11: Stress-test struct/map/array combinations after all fixes
# Focus on: re-verify EX-44..47, deeper nesting, create_dataframe→pipeline→collect roundtrips,
# mixed types in containers, schema edge cases
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
# RE-VERIFY: EX-44..47 (just fixed)
# =============================================

run.("1. [EX-44] nested maps auto-infer heterogeneous keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"level1" => %{"a" => 10, "b" => 20}}},
    %{"id" => 2, "nested" => %{"level1" => %{"c" => 30}}}
  ])
  DataFrame.collect(df)
end)

run.("2. [EX-45] create_dataframe MAP<INT, INT> keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "scores" => %{1 => 100, 2 => 95}},
    %{"id" => 2, "scores" => %{1 => 80, 3 => 70}}
  ], schema: "id INT, scores MAP<INT, INT>")
  DataFrame.collect(df)
end)

run.("3. [EX-46] struct containing MAP field", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "profile" => %{"tags" => ["a", "b"], "attrs" => %{"x" => "1"}}}
  ], schema: "id INT, profile STRUCT<tags: ARRAY<STRING>, attrs: MAP<STRING, STRING>>")
  DataFrame.collect(df)
end)

run.("4. [EX-47] lit() nested arrays", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("nested", lit([[1, 2], [3, 4]]))
  |> DataFrame.collect()
end)

# =============================================
# NEW: Push combinations harder
# =============================================

# Struct of struct of array of map
run.("5. create_dataframe: struct<struct<array<map>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "data" => %{"inner" => %{"items" => [%{"k" => "v1"}, %{"k" => "v2"}]}}}
  ], schema: "id INT, data STRUCT<inner: STRUCT<items: ARRAY<MAP<STRING, STRING>>>>")
  DataFrame.collect(df)
end)

# Array of structs with nested arrays
run.("6. create_dataframe: ARRAY<STRUCT<name: STRING, scores: ARRAY<INT>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "students" => [
      %{"name" => "Alice", "scores" => [90, 85]},
      %{"name" => "Bob", "scores" => [70, 80, 95]}
    ]}
  ], schema: "id INT, students ARRAY<STRUCT<name: STRING, scores: ARRAY<INT>>>")
  DataFrame.collect(df)
end)

# MAP<STRING, ARRAY<INT>>
run.("7. create_dataframe: MAP<STRING, ARRAY<INT>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grouped_scores" => %{"math" => [90, 85], "eng" => [70, 80]}}
  ], schema: "id INT, grouped_scores MAP<STRING, ARRAY<INT>>")
  DataFrame.collect(df)
end)

# MAP<STRING, STRUCT<...>>
run.("8. create_dataframe: MAP<STRING, STRUCT<age: INT, active: BOOLEAN>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "people" => %{"alice" => %{"age" => 30, "active" => true}}}
  ], schema: "id INT, people MAP<STRING, STRUCT<age: INT, active: BOOLEAN>>")
  DataFrame.collect(df)
end)

# 5 columns, each a different complex type
run.("9. create_dataframe: 5 heterogeneous complex columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{
      "c1_arr" => [1, 2, 3],
      "c2_map" => %{"a" => "x"},
      "c3_struct" => %{"name" => "test", "val" => 42},
      "c4_nested_arr" => [[10], [20, 30]],
      "c5_arr_struct" => [%{"k" => 1}, %{"k" => 2}]
    }
  ], schema: """
    c1_arr ARRAY<INT>,
    c2_map MAP<STRING, STRING>,
    c3_struct STRUCT<name: STRING, val: INT>,
    c4_nested_arr ARRAY<ARRAY<INT>>,
    c5_arr_struct ARRAY<STRUCT<k: INT>>
  """)
  DataFrame.collect(df)
end)

# create_dataframe with struct where field names have special chars
run.("10. create_dataframe: struct field names with underscores/numbers", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "meta" => %{"field_1" => "a", "field_2" => "b", "count_99" => 42}}
  ], schema: "id INT, meta STRUCT<field_1: STRING, field_2: STRING, count_99: INT>")
  DataFrame.collect(df)
end)

# create_dataframe → explode → re-aggregate roundtrip
run.("11. create_dataframe → explode → collect_list roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => ["a", "b", "c"]},
    %{"id" => 2, "items" => ["d", "e"]}
  ], schema: "id INT, items ARRAY<STRING>")

  df
  |> DataFrame.select([col("id"), explode(col("items")) |> Column.alias_("item")])
  |> DataFrame.group_by([col("id")])
  |> DataFrame.agg([
    collect_list(col("item")) |> Column.alias_("back"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# create_dataframe → transform array with higher-order function
run.("12. create_dataframe → transform higher-order", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "vals" => [10, 20, 30]},
    %{"id" => 2, "vals" => [5, 15]}
  ], schema: "id INT, vals ARRAY<INT>")

  df
  |> DataFrame.with_column("doubled",
    transform(col("vals"), fn x -> Column.multiply(x, lit(2)) end))
  |> DataFrame.with_column("sum_vals",
    aggregate(col("vals"), lit(0), fn acc, x -> Column.plus(acc, x) end))
  |> DataFrame.collect()
end)

# create_dataframe → save to Iceberg → read back with complex types
run.("13. create_dataframe complex → Iceberg roundtrip", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_cplx_rt_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b"], "attrs" => %{"x" => "1", "y" => "2"}},
    %{"id" => 2, "tags" => ["c"], "attrs" => %{"z" => "3"}}
  ], schema: "id INT, tags ARRAY<STRING>, attrs MAP<STRING, STRING>")

  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# create_dataframe with many rows then window function
run.("14. create_dataframe 50 rows → window ranking", fn ->
  rows = for i <- 1..50 do
    %{"dept" => "dept_#{rem(i, 5)}", "salary" => 50000 + i * 1000}
  end
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "dept STRING, salary INT")

  w = SparkEx.Window.partition_by([col("dept")]) |> SparkEx.WindowSpec.order_by([desc(col("salary"))])

  df
  |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("rank"), lit(3)))
  |> DataFrame.order_by([asc(col("dept")), asc(col("rank"))])
  |> DataFrame.collect()
end)

# create_dataframe → union with SQL result
run.("15. create_dataframe union with SQL result", fn ->
  {:ok, local} = SparkEx.create_dataframe(s, [
    %{"id" => 100, "label" => "from_create"},
    %{"id" => 101, "label" => "from_create"}
  ], schema: "id INT, label STRING")

  remote = SparkEx.sql(s, """
    SELECT * FROM VALUES (200, 'from_sql'), (201, 'from_sql') AS t(id, label)
  """)

  DataFrame.union(local, remote)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# create_dataframe with all-nil complex columns
run.("16. create_dataframe: all nil values in complex columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => nil, "m" => nil, "st" => nil},
    %{"id" => 2, "arr" => nil, "m" => nil, "st" => nil}
  ], schema: "id INT, arr ARRAY<INT>, m MAP<STRING, STRING>, st STRUCT<name: STRING>")
  DataFrame.collect(df)
end)

# create_dataframe with mixed nil/non-nil in struct
run.("17. create_dataframe: nil struct fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice", "age" => nil}},
    %{"id" => 2, "info" => %{"name" => nil, "age" => 25}}
  ], schema: "id INT, info STRUCT<name: STRING, age: INT>")
  DataFrame.collect(df)
end)

# create_dataframe → create_or_replace_temp_view → SQL query
run.("18. create_dataframe → temp view → SQL join", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"code" => "A", "desc" => "Alpha"},
    %{"code" => "B", "desc" => "Beta"},
    %{"code" => "C", "desc" => "Gamma"}
  ], schema: "code STRING, desc STRING")

  DataFrame.create_or_replace_temp_view(df, "lookup_codes")

  SparkEx.sql(s, """
    SELECT t.id, t.code, l.desc
    FROM (SELECT * FROM VALUES (1, 'A'), (2, 'B'), (3, 'D') AS t(id, code)) t
    LEFT JOIN lookup_codes l ON t.code = l.code
    ORDER BY t.id
  """)
  |> DataFrame.collect()
end)

# Decimal array
run.("19. create_dataframe: ARRAY<DECIMAL(10,2)>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "prices" => [Decimal.new("10.50"), Decimal.new("20.75")]},
    %{"id" => 2, "prices" => [Decimal.new("99.99")]}
  ], schema: "id INT, prices ARRAY<DECIMAL(10,2)>")
  DataFrame.collect(df)
end)

# Date array
run.("20. create_dataframe: ARRAY<DATE>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "dates" => [~D[2024-01-01], ~D[2024-06-15]]},
    %{"id" => 2, "dates" => [~D[2025-12-31]]}
  ], schema: "id INT, dates ARRAY<DATE>")
  DataFrame.collect(df)
end)

# Timestamp array
run.("21. create_dataframe: ARRAY<TIMESTAMP>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "events" => [~U[2024-01-01 00:00:00Z], ~U[2024-06-15 12:30:00Z]]},
    %{"id" => 2, "events" => [~U[2025-12-31 23:59:59Z]]}
  ], schema: "id INT, events ARRAY<TIMESTAMP>")
  DataFrame.collect(df)
end)

# MAP<STRING, DECIMAL>
run.("22. create_dataframe: MAP<STRING, DECIMAL(10,2)>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "balances" => %{"USD" => Decimal.new("1234.56"), "EUR" => Decimal.new("987.65")}}
  ], schema: "id INT, balances MAP<STRING, DECIMAL(10,2)>")
  DataFrame.collect(df)
end)

# Struct with Decimal and Date fields
run.("23. create_dataframe: STRUCT<amount: DECIMAL, dt: DATE, active: BOOLEAN>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "rec" => %{"amount" => Decimal.new("999.99"), "dt" => ~D[2025-01-15], "active" => true}},
    %{"id" => 2, "rec" => %{"amount" => Decimal.new("0.01"), "dt" => ~D[2024-12-31], "active" => false}}
  ], schema: "id INT, rec STRUCT<amount: DECIMAL(10,2), dt: DATE, active: BOOLEAN>")
  DataFrame.collect(df)
end)

# Large create_dataframe with structs (200 rows)
run.("24. create_dataframe: 200 rows with STRUCT column", fn ->
  rows = for i <- 1..200 do
    %{"id" => i, "info" => %{"name" => "user_#{i}", "score" => rem(i * 7, 100)}}
  end
  {:ok, df} = SparkEx.create_dataframe(s, rows,
    schema: "id INT, info STRUCT<name: STRING, score: INT>")

  df
  |> DataFrame.filter(Column.gt(
    col("info") |> Column.get_field("score"), lit(90)))
  |> DataFrame.select([col("id"), col("info") |> Column.get_field("score") |> Column.alias_("score")])
  |> DataFrame.order_by([desc(col("score"))])
  |> DataFrame.collect()
end)

# Empty struct
run.("25. create_dataframe: empty struct fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "meta" => %{}}
  ])
  DataFrame.collect(df)
end)

# Deeply nested lit() expressions
run.("26. lit() with map of lists", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("data", lit(%{"scores" => [1, 2, 3], "tags" => ["a", "b"]}))
  |> DataFrame.collect()
end)

# lit() with struct-like nested map
run.("27. lit() with nested map (struct-like)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("profile", lit(%{"name" => "Alice", "addr" => %{"city" => "NYC"}}))
  |> DataFrame.collect()
end)

# create_dataframe then multiple collect calls (idempotency)
run.("28. create_dataframe → collect twice (idempotency)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "first"},
    %{"id" => 2, "val" => "second"}
  ], schema: "id INT, val STRING")

  {:ok, r1} = DataFrame.collect(df)
  {:ok, r2} = DataFrame.collect(df)
  {r1 == r2, length(r1), length(r2)}
end)

IO.puts("=== Struct/map/array combination tests complete ===")
