# Round 53: Creative corner cases — SparkEx.Reader options, DataFrame.explain,
# Column.cast to all types, create_dataframe with all type combinations,
# complex expressions, error boundaries, Writer options
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

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

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. DataFrame.explain
# =============================================

run.("1a. explain simple plan", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.explain(df)
end)

run.("1b. explain with extended mode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  complex = df
    |> DataFrame.filter(Column.gt(col("val"), lit(30)))
    |> DataFrame.with_column("doubled", Column.multiply(col("val"), lit(2)))
    |> DataFrame.sort([desc(col("doubled"))])
  DataFrame.explain(complex, "extended")
end)

# =============================================
# 2. DataFrame.schema / dtypes / columns
# =============================================

run.("2a. schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "test", "score" => 1.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.schema(df)
end)

run.("2b. dtypes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "test", "score" => 1.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.dtypes(df)
end)

run.("2c. columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "test", "score" => 1.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.columns(df)
end)

# =============================================
# 3. Column.cast to various types
# =============================================

run.("3. Column.cast type roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "42"}
  ], schema: "val STRING")

  df |> DataFrame.select([
    col("val"),
    Column.cast(col("val"), "INT") |> Column.alias_("as_int"),
    Column.cast(col("val"), "LONG") |> Column.alias_("as_long"),
    Column.cast(col("val"), "DOUBLE") |> Column.alias_("as_double"),
    Column.cast(col("val"), "FLOAT") |> Column.alias_("as_float"),
    Column.cast(col("val"), "BOOLEAN") |> Column.alias_("as_bool"),
    Column.cast(col("val"), "BINARY") |> Column.alias_("as_binary")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. SparkEx.Reader options: json, csv, parquet via SQL
# =============================================

run.("4a. Reader.table with options", fn ->
  # Read existing table with reader
  SparkEx.Reader.table(s, "sfx.sfx_dev_warehouse.instruments")
  |> DataFrame.select([col("name"), col("type")])
  |> DataFrame.limit(3)
  |> DataFrame.collect()
end)

# =============================================
# 5. create_dataframe with complex types
# =============================================

run.("5a. create_dataframe with nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "Alice", "age" => 30}},
    %{"id" => 2, "info" => %{"name" => "Bob", "age" => 25}}
  ], schema: "id INT, info STRUCT<name: STRING, age: INT>")
  DataFrame.collect(df)
end)

run.("5b. create_dataframe with array type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b", "c"]},
    %{"id" => 2, "tags" => ["x", "y"]}
  ], schema: "id INT, tags ARRAY<STRING>")
  DataFrame.collect(df)
end)

run.("5c. create_dataframe with map type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "props" => %{"color" => "red", "size" => "large"}},
    %{"id" => 2, "props" => %{"color" => "blue"}}
  ], schema: "id INT, props MAP<STRING, STRING>")
  DataFrame.collect(df)
end)

run.("5d. create_dataframe with date and timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"event" => "login", "date" => "2024-01-15", "ts" => "2024-01-15 10:30:00"}
  ], schema: "event STRING, date DATE, ts TIMESTAMP")
  DataFrame.collect(df)
end)

run.("5e. create_dataframe with decimal", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"price" => "99.99", "tax" => "7.50"}
  ], schema: "price DECIMAL(10,2), tax DECIMAL(10,2)")
  DataFrame.collect(df)
end)

# =============================================
# 6. Error boundaries: what happens with type mismatches
# =============================================

run.("6a. INT overflow (value > INT max)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 2147483648}  # INT max is 2147483647
  ], schema: "val INT")
  DataFrame.collect(df)
end)

run.("6b. Cast impossible type (STRING 'hello' to INT)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "hello"}], schema: "val STRING")
  df |> DataFrame.select([Column.cast(col("val"), "INT") |> Column.alias_("casted")])
    |> DataFrame.collect()
end)

run.("6c. Division by zero", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 10, "b" => 0}], schema: "a INT, b INT")
  df |> DataFrame.select([
    Column.divide(col("a"), col("b")) |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Complex: user-defined aggregation via HOF
# =============================================

run.("7. Custom aggregation: geometric mean via aggregate + log/exp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"vals" => [2.0, 4.0, 8.0]},
    %{"vals" => [1.0, 10.0, 100.0, 1000.0]}
  ], schema: "vals ARRAY<DOUBLE>")

  df |> DataFrame.select([
    col("vals"),
    # Geometric mean = exp(mean(log(vals)))
    # Step 1: transform to log values
    # Step 2: aggregate to sum
    # Step 3: divide by size, then exp
    exp(
      Column.divide(
        aggregate(
          SparkEx.Functions.transform(col("vals"), fn x -> log(x) end),
          lit(0.0),
          fn acc, x -> Column.plus(acc, x) end
        ),
        Column.cast(size(col("vals")), "DOUBLE")
      )
    ) |> Column.alias_("geometric_mean")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Complex: pivot with multiple agg functions
# =============================================

run.("8. Pivot with count and sum", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..60, fn i ->
      %{"dept" => Enum.at(["Eng", "Sales", "Ops"], rem(i, 3)),
        "quarter" => "Q#{rem(i, 4) + 1}",
        "revenue" => (rem(i * 997, 100) + 1) * 1.0}
    end), schema: "dept STRING, quarter STRING, revenue DOUBLE")

  # Pivot on quarter, aggregate sum of revenue
  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.pivot(col("quarter"))
  |> GroupedData.agg([sum(col("revenue")) |> Column.alias_("total")])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

# =============================================
# 9. Complex: window lead/lag with defaults
# =============================================

run.("9. lead/lag with default values across partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..12, fn i ->
      %{"month" => i, "region" => Enum.at(["East", "West"], rem(i, 2)),
        "sales" => (rem(i * 1337, 100) + 1) * 1.0}
    end), schema: "month INT, region STRING, sales DOUBLE")

  w = SparkEx.Window.partition_by([col("region")]) |> WindowSpec.order_by([asc(col("month"))])

  df
  |> DataFrame.with_column("prev_sales", lag(col("sales"), offset: 1, default: lit(0.0)) |> Column.over(w))
  |> DataFrame.with_column("next_sales", lead(col("sales"), offset: 1, default: lit(-1.0)) |> Column.over(w))
  |> DataFrame.with_column("sales_change",
    Column.minus(col("sales"), col("prev_sales")))
  |> DataFrame.sort([asc(col("region")), asc(col("month"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. Complex: multi-table Iceberg write + cross-table join
# =============================================

run.("10. Multi-table write + cross-table join", fn ->
  t1 = "sfx.sfx_dev_warehouse.spark_ex_r53_t1_#{run_id}"
  t2 = "sfx.sfx_dev_warehouse.spark_ex_r53_t2_#{run_id}"

  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"order_id" => 1, "customer" => "Alice", "amount" => 100},
    %{"order_id" => 2, "customer" => "Bob", "amount" => 200},
    %{"order_id" => 3, "customer" => "Alice", "amount" => 150}
  ], schema: "order_id INT, customer STRING, amount INT")

  {:ok, discounts} = SparkEx.create_dataframe(s, [
    %{"customer" => "Alice", "discount_pct" => 10},
    %{"customer" => "Bob", "discount_pct" => 5}
  ], schema: "customer STRING, discount_pct INT")

  # Write both tables
  DataFrame.write_v2(orders, t1) |> SparkEx.WriterV2.create()
  DataFrame.write_v2(discounts, t2) |> SparkEx.WriterV2.create()

  # Join across tables
  {:ok, result} = SparkEx.sql(s, """
    SELECT o.order_id, o.customer, o.amount,
           d.discount_pct,
           o.amount * (1 - d.discount_pct / 100.0) AS final_amount
    FROM #{t1} o
    JOIN #{t2} d ON o.customer = d.customer
    ORDER BY o.order_id
  """) |> DataFrame.collect()

  # Cleanup
  Catalog.drop_table(s, t1, if_exists: true, purge: true)
  Catalog.drop_table(s, t2, if_exists: true, purge: true)
  result
end)

# =============================================
# 11. Edge: very long string column name
# =============================================

run.("11. 200-char column name", fn ->
  long_name = String.duplicate("a", 200)
  SparkEx.sql(s, "SELECT 42 AS `#{long_name}`") |> DataFrame.collect()
end)

# =============================================
# 12. Edge: column name with special chars
# =============================================

run.("12. Column names with special characters", fn ->
  SparkEx.sql(s, """
    SELECT
      1 AS `column with spaces`,
      2 AS `col-with-dashes`,
      3 AS `col.with.dots`,
      4 AS `col/with/slashes`,
      5 AS `col(with)parens`
  """) |> DataFrame.collect()
end)

# =============================================
# 13. Complex: DataFrame.unpivot with real data patterns
# =============================================

run.("13. unpivot quarterly revenue", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"company" => "ACME", "q1" => 100, "q2" => 120, "q3" => 90, "q4" => 150},
    %{"company" => "Beta", "q1" => 80, "q2" => 85, "q3" => 95, "q4" => 110}
  ], schema: "company STRING, q1 INT, q2 INT, q3 INT, q4 INT")

  DataFrame.unpivot(df,
    [col("company")],
    [col("q1"), col("q2"), col("q3"), col("q4")],
    "quarter", "revenue")
  |> DataFrame.sort([asc(col("company")), asc(col("quarter"))])
  |> DataFrame.collect()
end)

# =============================================
# 14. Complex: summary statistics
# =============================================

run.("14. summary with custom stats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  DataFrame.summary(df, ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"])
  |> DataFrame.collect()
end)

IO.puts("=== Round 53 complete ===")
