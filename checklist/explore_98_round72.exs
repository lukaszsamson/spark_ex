# Round 72: Exotic combinations and remaining validation gaps
# - HOF (higher-order functions) with edge cases
# - Complex SQL mixing with DataFrame API
# - Multiple simultaneous temp views
# - select_expr edge cases
# - DataFrame.transform with complex functions
# - Large expression trees
# - Column arithmetic overflow
# - Stack overflow with deeply nested plans
# - NA API with correct module path
# - create_dataframe with 0, 1, 1000+ rows
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
# 1. NA API via DataFrame (correct path)
# =============================================

run.("1a. dropna (via DataFrame.dropna)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => 1, "y" => nil},
    %{"x" => 1, "y" => "a"},
    %{"x" => nil, "y" => "b"}
  ], schema: "x INT, y STRING")
  DataFrame.dropna(df) |> DataFrame.collect()
end)

run.("1b. dropna with how: any", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => 1, "y" => nil},
    %{"x" => 1, "y" => "a"}
  ], schema: "x INT, y STRING")
  DataFrame.dropna(df, how: "any") |> DataFrame.collect()
end)

run.("1c. dropna with how: all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => 1, "y" => nil},
    %{"x" => 1, "y" => "a"}
  ], schema: "x INT, y STRING")
  DataFrame.dropna(df, how: "all") |> DataFrame.collect()
end)

run.("1d. fillna with column-specific values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil, "z" => nil},
    %{"x" => 1, "y" => "hello", "z" => 3.14}
  ], schema: "x INT, y STRING, z DOUBLE")
  DataFrame.fillna(df, %{"x" => 0, "y" => "default", "z" => 0.0}) |> DataFrame.collect()
end)

run.("1e. replace values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}, %{"x" => 3}
  ], schema: "x INT")
  DataFrame.replace(df, ["x"], %{1 => 10, 2 => 20}) |> DataFrame.collect()
end)

# =============================================
# 2. HOF edge cases
# =============================================

run.("2a. Column.transform on empty array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => []},
    %{"arr" => [1, 2, 3]},
    %{"arr" => nil}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    SparkEx.Functions.transform(col("arr"), fn x -> Column.multiply(x, lit(2)) end) |> Column.alias_("doubled")
  ]) |> DataFrame.collect()
end)

run.("2b. HOF array_sort with custom comparator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [3, 1, 4, 1, 5, 9]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    # Sort descending via comparator
    array_sort(col("arr"), fn left, right ->
      when_(Column.lt(left, right), lit(1))
      |> Column.when_(Column.gt(left, right), lit(-1))
      |> Column.otherwise(lit(0))
    end) |> Column.alias_("desc_sorted")
  ]) |> DataFrame.collect()
end)

run.("2c. HOF aggregate (fold) on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("sum_manual")
  ]) |> DataFrame.collect()
end)

run.("2d. HOF exists on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [4, 5, 6]},
    %{"arr" => nil}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    exists(col("arr"), fn x -> Column.gt(x, lit(5)) end) |> Column.alias_("has_gt_5")
  ]) |> DataFrame.collect()
end)

run.("2e. HOF forall on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]},
    %{"arr" => [10, 20, 30]},
    %{"arr" => nil}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    forall(col("arr"), fn x -> Column.gt(x, lit(5)) end) |> Column.alias_("all_gt_5")
  ]) |> DataFrame.collect()
end)

run.("2f. HOF zip_with", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [10, 20, 30]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    zip_with(col("a"), col("b"), fn x, y -> Column.plus(x, y) end) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. SQL ↔ DataFrame mixing
# =============================================

run.("3. SQL → DataFrame → temp view → SQL → collect", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 100},
    %{"id" => 2, "val" => 200},
    %{"id" => 3, "val" => 300}
  ], schema: "id INT, val INT")
  DataFrame.create_or_replace_temp_view(df1, "tmp_r72_src")

  # SQL query on temp view
  df2 = SparkEx.sql(s, "SELECT id, val, val * 2 AS doubled FROM tmp_r72_src WHERE val > 100")
  DataFrame.create_or_replace_temp_view(df2, "tmp_r72_mid")

  # Another SQL on top
  df3 = SparkEx.sql(s, "SELECT *, doubled + 50 AS final FROM tmp_r72_mid")
  df3 |> DataFrame.collect()
end)

# =============================================
# 4. select_expr edge cases
# =============================================

run.("4a. select_expr basic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1, "y" => 2}
  ], schema: "x INT, y INT")
  DataFrame.select_expr(df, ["x", "y", "x + y AS sum_xy", "x * y AS prod_xy"]) |> DataFrame.collect()
end)

run.("4b. select_expr with aggregation (should fail)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  DataFrame.select_expr(df, ["sum(x)"]) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.transform
# =============================================

run.("5. DataFrame.transform chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}, %{"x" => 3}
  ], schema: "x INT")

  add_doubled = fn df -> DataFrame.with_column(df, "doubled", col("x") |> Column.multiply(lit(2))) end
  add_squared = fn df -> DataFrame.with_column(df, "squared", col("x") |> Column.multiply(col("x"))) end
  filter_big = fn df -> DataFrame.filter(df, col("x") |> Column.gt(lit(1))) end

  df
  |> DataFrame.transform(add_doubled)
  |> DataFrame.transform(add_squared)
  |> DataFrame.transform(filter_big)
  |> DataFrame.collect()
end)

# =============================================
# 6. Large expression: 20 columns
# =============================================

run.("6. 20-column computed select", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")
  cols = Enum.map(1..20, fn i ->
    (col("x") |> Column.multiply(lit(i))) |> Column.alias_("c_#{i}")
  end)
  df |> DataFrame.select([col("x") | cols]) |> DataFrame.collect()
end)

# =============================================
# 7. Column arithmetic edge cases
# =============================================

run.("7a. Division by zero (integer)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 10}], schema: "x INT")
  df |> DataFrame.select([
    col("x") |> Column.divide(lit(0)) |> Column.alias_("div_zero")
  ]) |> DataFrame.collect()
end)

run.("7b. Division by zero (double)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 10.0}], schema: "x DOUBLE")
  df |> DataFrame.select([
    col("x") |> Column.divide(lit(0.0)) |> Column.alias_("div_zero")
  ]) |> DataFrame.collect()
end)

run.("7c. Integer overflow via multiplication", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 2_147_483_647}
  ], schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    (col("x") |> Column.multiply(lit(2))) |> Column.alias_("overflowed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Deeply chained plan (10 filters)
# =============================================

run.("8. 10 chained filters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"x" => i} end), schema: "x INT")
  result = Enum.reduce(1..10, df, fn i, acc ->
    DataFrame.filter(acc, col("x") |> Column.gt(lit(i)))
  end)
  {:ok, count} = DataFrame.count(result)
  count
end)

# =============================================
# 9. create_dataframe scale tests
# =============================================

run.("9a. create_dataframe 0 rows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "x INT, y STRING")
  {:ok, count} = DataFrame.count(df)
  count
end)

run.("9b. create_dataframe 1 row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.collect()
end)

run.("9c. create_dataframe 2000 rows", fn ->
  rows = Enum.map(1..2000, fn i -> %{"x" => i, "y" => "val_#{i}"} end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "x INT, y STRING")
  {:ok, count} = DataFrame.count(df)
  {:ok, first_rows} = df |> DataFrame.limit(3) |> DataFrame.collect()
  {:ok, last_rows} = DataFrame.tail(df, 3)
  {count, first_rows, last_rows}
end)

# =============================================
# 10. Creative: ETL pipeline with error handling
# =============================================

run.("10. ETL: parse JSON → validate → aggregate → output", fn ->
  # Simulate JSON input with mixed quality
  {:ok, raw} = SparkEx.create_dataframe(s, [
    %{"json" => "{\"product\": \"A\", \"qty\": 10, \"price\": 5.0}"},
    %{"json" => "{\"product\": \"B\", \"qty\": 20, \"price\": 3.5}"},
    %{"json" => "{\"product\": \"A\", \"qty\": 5, \"price\": 5.0}"},
    %{"json" => "{\"product\": \"C\", \"qty\": 15, \"price\": 8.0}"},
    %{"json" => "{\"product\": \"B\", \"qty\": 8, \"price\": 3.5}"},
    %{"json" => "{broken json}"},
    %{"json" => nil}
  ], schema: "json STRING")

  schema = "STRUCT<product: STRING, qty: INT, price: DOUBLE>"

  raw
  |> DataFrame.select([
    from_json(col("json"), schema) |> Column.alias_("parsed")
  ])
  |> DataFrame.select([
    Column.get_field(col("parsed"), "product") |> Column.alias_("product"),
    Column.get_field(col("parsed"), "qty") |> Column.alias_("qty"),
    Column.get_field(col("parsed"), "price") |> Column.alias_("price")
  ])
  |> DataFrame.filter(Column.is_not_null(col("product")))
  |> DataFrame.with_column("revenue",
    Column.cast(col("qty"), "DOUBLE") |> Column.multiply(col("price")))
  |> DataFrame.group_by([col("product")])
  |> GroupedData.agg([
    sum(col("revenue")) |> Column.alias_("total_revenue"),
    sum(col("qty")) |> Column.alias_("total_qty"),
    count(col("product")) |> Column.alias_("order_count")
  ])
  |> DataFrame.sort([desc(col("total_revenue"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 72 complete ===")
