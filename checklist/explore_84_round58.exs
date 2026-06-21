# Round 58: UDFRegistration with proper UDF class, Reader.schema with Types,
# StreamWriter.foreach_batch, DataFrame.with_columns_renamed (various forms),
# complex error recovery, Writer v1 format/save, lateral subquery,
# DataFrame.transform chained, Column.isin with subquery, approx_count_distinct
alias SparkEx.{DataFrame, Column, GroupedData, Catalog}
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

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. DataFrame.with_columns_renamed variations
# =============================================

run.("1a. with_columns_renamed map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2}], schema: "a INT, b INT")
  df |> DataFrame.with_columns_renamed(%{"a" => "alpha", "b" => "beta"})
  |> DataFrame.collect()
end)

run.("1b. with_columns_renamed function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"old_name" => 1, "other_col" => 2}],
    schema: "old_name INT, other_col INT")
  df |> DataFrame.with_columns_renamed(fn name -> "prefix_#{name}" end)
  |> DataFrame.collect()
end)

# =============================================
# 2. Column.isin
# =============================================

run.("2a. Column.isin with list of literals", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  df
  |> DataFrame.filter(Column.isin(col("id"), [2, 5, 8]))
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("2b. Column.isin with string list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Charlie"}, %{"name" => "Diana"}
  ], schema: "name STRING")
  df
  |> DataFrame.filter(Column.isin(col("name"), ["Alice", "Charlie"]))
  |> DataFrame.collect()
end)

# =============================================
# 3. Functions: approx_count_distinct, collect_list, collect_set
# =============================================

run.("3a. approx_count_distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => rem(i, 50)} end), schema: "id INT")
  df |> DataFrame.select([
    approx_count_distinct(col("id")) |> Column.alias_("approx_distinct")
  ]) |> DataFrame.collect()
end)

run.("3b. collect_list / collect_set", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => 1}, %{"grp" => "A", "val" => 2}, %{"grp" => "A", "val" => 1},
    %{"grp" => "B", "val" => 3}, %{"grp" => "B", "val" => 3}
  ], schema: "grp STRING, val INT")
  df
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    collect_list(col("val")) |> Column.alias_("list_vals"),
    collect_set(col("val")) |> Column.alias_("set_vals")
  ])
  |> DataFrame.sort([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 4. Functions: array_contains, array_sort, array_distinct, array_union
# =============================================

run.("4. Array functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [3, 1, 2, 1, 4]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    array_contains(col("arr"), lit(3)) |> Column.alias_("has_3"),
    array_sort(col("arr")) |> Column.alias_("sorted"),
    array_distinct(col("arr")) |> Column.alias_("distinct"),
    size(col("arr")) |> Column.alias_("size")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Functions: map_keys, map_values, map_from_arrays
# =============================================

run.("5. Map functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2, "c" => 3}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    col("m"),
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("vals"),
    size(col("m")) |> Column.alias_("size")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Functions: regexp_extract, regexp_replace
# =============================================

run.("6. Regex functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Order #12345 placed on 2024-01-15"},
    %{"text" => "Invoice #98765 generated"}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    regexp_extract(col("text"), "#(\\d+)", 1) |> Column.alias_("number"),
    regexp_replace(col("text"), "#\\d+", "#XXX") |> Column.alias_("redacted")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Functions: date/timestamp functions
# =============================================

run.("7. Date/timestamp functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts" => "2024-06-15 14:30:00"}
  ], schema: "ts TIMESTAMP")
  df |> DataFrame.select([
    col("ts"),
    year(col("ts")) |> Column.alias_("year"),
    month(col("ts")) |> Column.alias_("month"),
    dayofmonth(col("ts")) |> Column.alias_("day"),
    hour(col("ts")) |> Column.alias_("hour"),
    minute(col("ts")) |> Column.alias_("minute"),
    dayofweek(col("ts")) |> Column.alias_("dow"),
    quarter(col("ts")) |> Column.alias_("quarter"),
    date_format(col("ts"), lit("yyyy/MM/dd")) |> Column.alias_("formatted"),
    date_add(Column.cast(col("ts"), "DATE"), lit(7)) |> Column.alias_("plus_7_days"),
    datediff(lit("2024-12-31"), Column.cast(col("ts"), "DATE")) |> Column.alias_("days_to_eoy")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Functions: string functions
# =============================================

run.("8. String functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "  Hello World  ", "val" => "foo_bar_baz"}
  ], schema: "name STRING, val STRING")
  df |> DataFrame.select([
    trim(col("name")) |> Column.alias_("trimmed"),
    upper(col("name")) |> Column.alias_("upper"),
    lower(col("name")) |> Column.alias_("lower"),
    reverse(col("name")) |> Column.alias_("reversed"),
    Column.substr(col("val"), lit(5), lit(3)) |> Column.alias_("substr"),
    split(col("val"), lit("_")) |> Column.alias_("split"),
    # concat/concat_ws not available as SparkEx.Functions
    Column.plus(Column.plus(trim(col("name")), lit("-")), col("val")) |> Column.alias_("joined"),
    lpad(col("val"), lit(20), lit("*")) |> Column.alias_("padded"),
    initcap(col("name")) |> Column.alias_("initcap")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Functions: math functions
# =============================================

run.("9. Math functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 3.14159, "y" => -42.7}
  ], schema: "x DOUBLE, y DOUBLE")
  df |> DataFrame.select([
    SparkEx.Functions.ceil(col("x")) |> Column.alias_("ceil"),
    SparkEx.Functions.floor(col("x")) |> Column.alias_("floor"),
    bround(col("x"), lit(2)) |> Column.alias_("bround"),
    SparkEx.Functions.abs(col("y")) |> Column.alias_("abs"),
    sqrt(col("x")) |> Column.alias_("sqrt"),
    cbrt(col("x")) |> Column.alias_("cbrt"),
    pow(col("x"), lit(2)) |> Column.alias_("pow2"),
    log2(col("x")) |> Column.alias_("log2"),
    log10(col("x")) |> Column.alias_("log10"),
    sin(col("x")) |> Column.alias_("sin"),
    cos(col("x")) |> Column.alias_("cos"),
    degrees(col("x")) |> Column.alias_("degrees"),
    signum(col("y")) |> Column.alias_("signum")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Functions: conditional / null handling
# =============================================

run.("10. Conditional and null functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c INT")
  df |> DataFrame.select([
    col("a"), col("b"), col("c"),
    coalesce([col("a"), col("b"), col("c")]) |> Column.alias_("first_non_null"),
    nanvl(Column.cast(col("a"), "DOUBLE"), lit(999.0)) |> Column.alias_("nanvl"),
    # greatest/least might use different arity or not be imported
    when_(Column.is_null(col("a")), lit("null_a"))
    |> Column.when_(Column.is_null(col("b")), lit("null_b"))
    |> Column.otherwise(lit("both_set"))
    |> Column.alias_("case_result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Complex: ETL with all function types combined
# =============================================

run.("11. ETL with diverse function types", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r58_etl_#{run_id}"

  {:ok, raw} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"user_id" => rem(i, 20) + 1,
        "event" => Enum.at(["click", "view", "purchase"], rem(i, 3)),
        "timestamp" => "2024-#{String.pad_leading(to_string(rem(i, 12) + 1), 2, "0")}-15 10:00:00",
        "amount" => (rem(i * 997, 500) + 1) * 0.01}
    end), schema: "user_id INT, event STRING, timestamp TIMESTAMP, amount DOUBLE")

  result = raw
  |> DataFrame.with_column("month", month(col("timestamp")))
  |> DataFrame.with_column("quarter", quarter(col("timestamp")))
  |> DataFrame.filter(Column.isin(col("event"), ["click", "purchase"]))
  |> DataFrame.group_by([col("event"), col("quarter")])
  |> GroupedData.agg([
    count(col("user_id")) |> Column.alias_("event_count"),
    sum(col("amount")) |> Column.alias_("total_amount"),
    approx_count_distinct(col("user_id")) |> Column.alias_("unique_users"),
    avg(col("amount")) |> Column.alias_("avg_amount")
  ])
  |> DataFrame.sort([asc(col("event")), asc(col("quarter"))])
  |> DataFrame.collect()

  result
end)

# =============================================
# 12. Complex: DataFrame.subtract (set difference)
# =============================================

run.("12. DataFrame.subtract", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s,
    [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 4}, %{"id" => 5}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s,
    [%{"id" => 2}, %{"id" => 4}], schema: "id INT")
  DataFrame.subtract(df1, df2)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 13. Complex: nested aggregate → having → window
# =============================================

run.("13. Nested agg → having → window", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"dept" => Enum.at(["Eng", "Sales", "Ops", "HR"], rem(i, 4)),
        "salary" => (rem(i * 1337, 100) + 30) * 1000}
    end), schema: "dept STRING, salary INT")

  # First: aggregate per dept
  agg = df
    |> DataFrame.group_by([col("dept")])
    |> GroupedData.agg([
      count(col("salary")) |> Column.alias_("count"),
      avg(col("salary")) |> Column.alias_("avg_salary"),
      sum(col("salary")) |> Column.alias_("total_salary")
    ])

  # Then: rank departments by total_salary
  w = SparkEx.Window.order_by([desc(col("total_salary"))])

  agg
  |> DataFrame.with_column("rank", dense_rank() |> Column.over(w))
  |> DataFrame.with_column("pct_of_total",
    Column.divide(
      Column.cast(col("total_salary"), "DOUBLE"),
      Column.cast(sum(col("total_salary")) |> Column.over(%SparkEx.WindowSpec{}), "DOUBLE"))
    |> Column.multiply(lit(100.0)))
  |> DataFrame.sort([asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 14. Edge: DataFrame.drop multiple columns
# =============================================

run.("14. DataFrame.drop multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5}
  ], schema: "a INT, b INT, c INT, d INT, e INT")
  df |> DataFrame.drop(["b", "d"]) |> DataFrame.collect()
end)

# =============================================
# 15. Edge: DataFrame.union by position vs name
# =============================================

run.("15a. union (by position)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 10}], schema: "x INT, y INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"x" => 2, "y" => 20}], schema: "x INT, y INT")
  DataFrame.union(df1, df2) |> DataFrame.collect()
end)

run.("15b. union_by_name with missing column", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 10}], schema: "a INT, b INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"a" => 2, "c" => 20}], schema: "a INT, c INT")
  DataFrame.union_by_name(df1, df2, allow_missing_columns: true) |> DataFrame.collect()
end)

# =============================================
# 16. Edge: Window.unspecified / Window.current_row
# =============================================

run.("16. Window.unspecified for global aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end), schema: "id INT, val INT")
  w = %SparkEx.WindowSpec{}
  df
  |> DataFrame.with_column("total", sum(col("val")) |> Column.over(w))
  |> DataFrame.with_column("pct", Column.divide(
    Column.cast(col("val"), "DOUBLE"),
    Column.cast(sum(col("val")) |> Column.over(w), "DOUBLE")) |> Column.multiply(lit(100.0)))
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 17. Edge: Column.between with various types
# =============================================

run.("17. Column.between with strings and dates", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 25, "date" => "2024-03-15"},
    %{"name" => "Bob", "age" => 35, "date" => "2024-07-20"},
    %{"name" => "Charlie", "age" => 45, "date" => "2024-11-01"},
    %{"name" => "Diana", "age" => 55, "date" => "2024-01-05"}
  ], schema: "name STRING, age INT, date DATE")
  df
  |> DataFrame.filter(Column.between(col("age"), lit(30), lit(50)))
  |> DataFrame.with_column("mid_year",
    Column.between(col("date"), lit("2024-04-01"), lit("2024-09-30")))
  |> DataFrame.sort([asc(col("name"))])
  |> DataFrame.collect()
end)

# =============================================
# 18. Edge: multiple aliases on same column expression
# =============================================

run.("18. Multiple select with computed columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"price" => 100.0, "tax_rate" => 0.08, "quantity" => 5}
  ], schema: "price DOUBLE, tax_rate DOUBLE, quantity INT")
  df |> DataFrame.select([
    col("price"),
    col("quantity"),
    Column.multiply(col("price"), col("tax_rate")) |> Column.alias_("tax"),
    Column.multiply(col("price"), Column.plus(lit(1.0), col("tax_rate")))
      |> Column.alias_("price_with_tax"),
    Column.multiply(
      Column.multiply(col("price"), Column.plus(lit(1.0), col("tax_rate"))),
      Column.cast(col("quantity"), "DOUBLE"))
      |> Column.alias_("total")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 58 complete ===")
