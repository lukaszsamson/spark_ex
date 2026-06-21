# Exploratory: Edge cases, large data, column name conflicts, type limits
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

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

# === Column names with special characters ===
run.("1. Column names with spaces", fn ->
  SparkEx.sql(s, "SELECT 1 AS `my column`, 2 AS `another col`")
  |> DataFrame.collect()
end)

run.("2. Column names with dots", fn ->
  SparkEx.sql(s, "SELECT 1 AS `a.b.c`, 2 AS `x.y`")
  |> DataFrame.collect()
end)

run.("3. Column names with unicode", fn ->
  SparkEx.sql(s, "SELECT 1 AS `名前`, 2 AS `값`")
  |> DataFrame.collect()
end)

# === Very long column name ===
run.("4. Very long column name (200 chars)", fn ->
  long_name = String.duplicate("a", 200)
  SparkEx.sql(s, "SELECT 1 AS `#{long_name}`")
  |> DataFrame.collect()
end)

# === Many columns (50) ===
run.("5. 50-column DataFrame", fn ->
  cols = Enum.map(1..50, fn i -> "#{i} AS c#{i}" end) |> Enum.join(", ")
  df = SparkEx.sql(s, "SELECT #{cols}")
  {columns, dtypes} = {DataFrame.columns(df), DataFrame.dtypes(df)}
  %{num_columns: Kernel.length(elem(columns, 1)), dtypes_count: Kernel.length(elem(dtypes, 1))}
end)

# === Large-ish result set ===
run.("6. Collect 1000 rows", fn ->
  df = SparkEx.sql(s, "SELECT id, id * 2 AS doubled FROM range(0, 1000)")
  {:ok, rows} = DataFrame.collect(df)
  %{count: Kernel.length(rows), first: hd(rows), last: List.last(rows)}
end)

# === 10000 rows ===
run.("7. Collect 10000 rows", fn ->
  df = SparkEx.sql(s, "SELECT id FROM range(0, 10000)")
  {:ok, rows} = DataFrame.collect(df)
  Kernel.length(rows)
end)

# === Duplicate column names (from self-join without alias) ===
run.("8. Duplicate column names handling", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS id, 'Alice' AS name")
  a = DataFrame.alias_(df, "a")
  b = DataFrame.alias_(df, "b")
  DataFrame.cross_join(a, b) |> DataFrame.collect()
end)

# === Column cast edge cases ===
run.("9. Cast string to various types", fn ->
  SparkEx.sql(s, """
    SELECT '123' AS str_int, '3.14' AS str_double, 'true' AS str_bool,
           '2024-06-15' AS str_date, '2024-06-15 14:30:00' AS str_ts
  """)
  |> DataFrame.select([
    Column.cast(col("str_int"), "INT") |> Column.alias_("as_int"),
    Column.cast(col("str_double"), "DOUBLE") |> Column.alias_("as_double"),
    Column.cast(col("str_bool"), "BOOLEAN") |> Column.alias_("as_bool"),
    Column.cast(col("str_date"), "DATE") |> Column.alias_("as_date"),
    Column.cast(col("str_ts"), "TIMESTAMP") |> Column.alias_("as_ts")
  ])
  |> DataFrame.collect()
end)

# === Overflow behavior ===
run.("10. Integer overflow (INT max + 1)", fn ->
  SparkEx.sql(s, "SELECT CAST(2147483647 AS INT) + 1 AS overflow")
  |> DataFrame.collect()
end)

# === Empty string vs null distinction ===
run.("11. Empty string vs null in operations", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('', 1), (NULL, 2), (' ', 3), ('a', 4) AS t(val, id)
  """)
  |> DataFrame.select([
    col("id"), col("val"),
    Column.is_null(col("val")) |> Column.alias_("is_null"),
    Column.eq(col("val"), lit("")) |> Column.alias_("is_empty"),
    SparkEx.Functions.length(col("val")) |> Column.alias_("len")
  ])
  |> DataFrame.collect()
end)

# Oops, length conflicts. Use SparkEx.Functions.length explicitly
run.("12. Empty string vs null (fixed length call)", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('', 1), (NULL, 2), (' ', 3), ('a', 4) AS t(val, id)
  """)
  |> DataFrame.select([
    col("id"), col("val"),
    Column.is_null(col("val")) |> Column.alias_("is_null"),
    Column.eq(col("val"), lit("")) |> Column.alias_("is_empty"),
    SparkEx.Functions.length(col("val")) |> Column.alias_("len")
  ])
  |> DataFrame.collect()
end)

# === Very deeply nested struct (5 levels) ===
run.("13. 5-level nested struct", fn ->
  SparkEx.sql(s, """
    SELECT named_struct(
      'l1', named_struct(
        'l2', named_struct(
          'l3', named_struct(
            'l4', named_struct(
              'l5', 'deep_value'
            )
          )
        )
      )
    ) AS nested
  """) |> DataFrame.collect()
end)

# === Array of arrays ===
run.("14. Array of arrays", fn ->
  SparkEx.sql(s, """
    SELECT array(array(1,2,3), array(4,5,6), array(7,8,9)) AS matrix
  """) |> DataFrame.collect()
end)

# === Map with complex values ===
run.("15. Map with struct values", fn ->
  SparkEx.sql(s, """
    SELECT map('alice', named_struct('age', 30, 'city', 'NYC'),
               'bob', named_struct('age', 25, 'city', 'LA')) AS people_map
  """) |> DataFrame.collect()
end)

# === Map with array values ===
run.("16. Map with array values", fn ->
  SparkEx.sql(s, """
    SELECT map('fruits', array('apple', 'banana'), 'vegs', array('carrot', 'pea')) AS food_map
  """) |> DataFrame.collect()
end)

# === Very large string ===
run.("17. Large string (10000 chars)", fn ->
  SparkEx.sql(s, "SELECT REPEAT('x', 10000) AS big_str")
  |> DataFrame.select([
    SparkEx.Functions.length(col("big_str")) |> Column.alias_("len"),
    substring(col("big_str"), lit(1), lit(5)) |> Column.alias_("first5")
  ])
  |> DataFrame.collect()
end)

# === Multiple aggregations with different filters ===
run.("18. Filtered aggregations (FILTER WHERE)", fn ->
  SparkEx.sql(s, """
    SELECT
      COUNT(*) AS total,
      COUNT(*) FILTER (WHERE region = 'East') AS east_cnt,
      SUM(amount) FILTER (WHERE region = 'West') AS west_total
    FROM VALUES
      ('East', 100), ('West', 200), ('East', 150), ('West', 250), ('East', 300)
    AS t(region, amount)
  """) |> DataFrame.collect()
end)

# === DataFrame.to_df (rename columns) ===
run.("19. DataFrame.to_df with new names", fn ->
  SparkEx.sql(s, "SELECT 1 AS a, 2 AS b, 3 AS c")
  |> DataFrame.to_df(["x", "y", "z"])
  |> DataFrame.collect()
end)

# === DataFrame.transform (custom function) ===
run.("20. DataFrame.transform with custom function", fn ->
  df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id, val)")
  DataFrame.transform(df, fn d ->
    DataFrame.with_column(d, "doubled", Column.multiply(col("val"), lit(2)))
  end)
  |> DataFrame.collect()
end)

# === offset in order_by (Spark 3.4+) ===
run.("21. LIMIT with OFFSET via SQL", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(id)
    ORDER BY id
    LIMIT 3 OFFSET 5
  """) |> DataFrame.collect()
end)

# === DataFrame.offset ===
run.("22. DataFrame.offset", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(id)")
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.offset(5)
  |> DataFrame.limit(3)
  |> DataFrame.collect()
end)

# === DataFrame.tail ===
run.("23. DataFrame.tail", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3),(4),(5) AS t(id)")
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.tail(2)
end)

# === DataFrame.input_files ===
run.("24. DataFrame.input_files (from SQL query, should be empty)", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.input_files()
end)

# === DataFrame.is_streaming ===
run.("25. DataFrame.is_streaming on batch df", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.is_streaming?()
end)

# === DataFrame.is_empty ===
run.("26. DataFrame.is_empty on non-empty df", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.is_empty?()
end)

run.("27. DataFrame.is_empty on empty df", fn ->
  SparkEx.sql(s, "SELECT 1 AS id WHERE false")
  |> DataFrame.is_empty?()
end)

# === Very wide aggregation (aggregate all 20 columns) ===
run.("28. Aggregate 20 columns", fn ->
  cols = Enum.map(1..20, fn i -> "#{i * 10} AS c#{i}" end) |> Enum.join(", ")
  agg_exprs = Enum.map(1..20, fn i ->
    sum(col("c#{i}")) |> Column.alias_("sum_c#{i}")
  end)
  SparkEx.sql(s, "SELECT #{cols}")
  |> DataFrame.agg(agg_exprs)
  |> DataFrame.collect()
end)

IO.puts("=== Edge cases exploration complete ===")
