# Exploratory: advanced patterns, higher-order funcs, schema manipulation, edge cases
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec}
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

# === Higher-order functions ===
arr_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, array(10, 20, 30, 40)),
    (2, array(5, 15, 25, 35)),
    (3, array(100, 200))
  AS t(id, nums)
""")

run.("1. transform HOF: double each element", fn ->
  DataFrame.select(arr_df, [
    col("id"),
    transform(col("nums"), fn x -> Column.multiply(x, lit(2)) end) |> Column.alias_("doubled")
  ]) |> DataFrame.collect()
end)

run.("2. filter HOF: keep even numbers", fn ->
  DataFrame.select(arr_df, [
    col("id"),
    filter(col("nums"), fn x -> Column.eq(Column.mod(x, lit(2)), lit(0)) end) |> Column.alias_("evens")
  ]) |> DataFrame.collect()
end)

run.("3. aggregate HOF: sum + finish (multiply by 2)", fn ->
  DataFrame.select(arr_df, [
    col("id"),
    aggregate(col("nums"), lit(0),
      fn acc, x -> Column.plus(acc, x) end,
      fn acc -> Column.multiply(acc, lit(2)) end
    ) |> Column.alias_("double_sum")
  ]) |> DataFrame.collect()
end)

run.("4. exists HOF: any element > 100", fn ->
  DataFrame.select(arr_df, [
    col("id"),
    exists(col("nums"), fn x -> Column.gt(x, lit(100)) end) |> Column.alias_("has_big")
  ]) |> DataFrame.collect()
end)

run.("5. forall HOF: all elements > 0", fn ->
  DataFrame.select(arr_df, [
    col("id"),
    forall(col("nums"), fn x -> Column.gt(x, lit(0)) end) |> Column.alias_("all_positive")
  ]) |> DataFrame.collect()
end)

run.("6. zip_with HOF", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS a, array(10, 20, 30) AS b")
  |> DataFrame.select([
    zip_with(col("a"), col("b"), fn x, y -> Column.plus(x, y) end) |> Column.alias_("zipped")
  ]) |> DataFrame.collect()
end)

run.("7. transform_keys / transform_values HOF on map", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([
    transform_keys(col("m"), fn k, _v -> upper(k) end) |> Column.alias_("upper_keys"),
    transform_values(col("m"), fn _k, v -> Column.multiply(v, lit(10)) end) |> Column.alias_("scaled_vals")
  ]) |> DataFrame.collect()
end)

run.("8. map_filter HOF", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([
    map_filter(col("m"), fn _k, v -> Column.gt(v, lit(1)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

# === Complex struct manipulation ===
struct_df = SparkEx.sql(s, """
  SELECT named_struct('name', 'Alice', 'address',
    named_struct('street', '123 Main', 'city', 'NYC', 'zip', '10001')
  ) AS person
""")

run.("9. get_field on nested struct", fn ->
  DataFrame.select(struct_df, [
    Column.get_field(col("person"), "name") |> Column.alias_("name"),
    Column.get_field(Column.get_field(col("person"), "address"), "city") |> Column.alias_("city"),
    Column.get_field(Column.get_field(col("person"), "address"), "zip") |> Column.alias_("zip")
  ]) |> DataFrame.collect()
end)

run.("10. with_field to update nested struct", fn ->
  DataFrame.select(struct_df, [
    Column.with_field(col("person"), "age", lit(30)) |> Column.alias_("person_with_age")
  ]) |> DataFrame.collect()
end)

# === JSON parsing with complex schemas ===
run.("11. from_json with nested schema", fn ->
  SparkEx.sql(s, """
    SELECT '{"name": "Alice", "scores": [90, 85, 92], "meta": {"level": "senior"}}' AS json_str
  """)
  |> DataFrame.select([
    from_json(col("json_str"), "name STRING, scores ARRAY<INT>, meta STRUCT<level: STRING>")
    |> Column.alias_("parsed")
  ])
  |> DataFrame.collect()
end)

run.("12. from_json + explode pipeline", fn ->
  SparkEx.sql(s, """
    SELECT '{"items": [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]}' AS json_str
  """)
  |> DataFrame.select([
    from_json(col("json_str"), "items ARRAY<STRUCT<id: INT, val: STRING>>") |> Column.alias_("parsed")
  ])
  |> DataFrame.select([explode(Column.get_field(col("parsed"), "items")) |> Column.alias_("item")])
  |> DataFrame.select([
    Column.get_field(col("item"), "id") |> Column.alias_("id"),
    Column.get_field(col("item"), "val") |> Column.alias_("val")
  ])
  |> DataFrame.collect()
end)

run.("13. to_json on struct column", fn ->
  SparkEx.sql(s, """
    SELECT named_struct('a', 1, 'b', array(2,3,4)) AS data
  """)
  |> DataFrame.select([to_json(col("data")) |> Column.alias_("json_out")])
  |> DataFrame.collect()
end)

# === Chained col_regex ===
run.("14. col_regex in complex pipeline", fn ->
  wide = SparkEx.sql(s, """
    SELECT 1 AS id, 'Alice' AS name, 100 AS salary_base, 20 AS salary_bonus, 120 AS salary_total
  """)
  DataFrame.select(wide, [DataFrame.col_regex(wide, "`salary.*`")])
  |> DataFrame.collect()
end)

# === Multiple aliases + cross-reference ===
run.("15. Triple self-join with aliases", fn ->
  nums = SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3) AS t(n)")
  a = DataFrame.alias_(nums, "a")
  b = DataFrame.alias_(nums, "b")
  c = DataFrame.alias_(nums, "c")
  DataFrame.cross_join(a, b)
  |> DataFrame.cross_join(c)
  |> DataFrame.filter(
    Column.and_(
      Column.lt(DataFrame.col(a, "n"), DataFrame.col(b, "n")),
      Column.lt(DataFrame.col(b, "n"), DataFrame.col(c, "n"))
    )
  )
  |> DataFrame.select([
    DataFrame.col(a, "n") |> Column.alias_("x"),
    DataFrame.col(b, "n") |> Column.alias_("y"),
    DataFrame.col(c, "n") |> Column.alias_("z")
  ])
  |> DataFrame.collect()
end)

# === Decimal precision ===
run.("16. Decimal arithmetic precision", fn ->
  SparkEx.sql(s, """
    SELECT CAST(1.0 AS DECIMAL(10,2)) / CAST(3.0 AS DECIMAL(10,2)) AS div_result,
           CAST(999999999.99 AS DECIMAL(12,2)) * CAST(1.005 AS DECIMAL(4,3)) AS mul_result,
           CAST(0.1 AS DECIMAL(2,1)) + CAST(0.2 AS DECIMAL(2,1)) AS add_result
  """) |> DataFrame.collect()
end)

# === Null propagation edge cases ===
run.("17. Null in arithmetic", fn ->
  SparkEx.sql(s, "SELECT NULL AS a, 5 AS b")
  |> DataFrame.select([
    Column.plus(col("a"), col("b")) |> Column.alias_("null_plus"),
    Column.multiply(col("a"), col("b")) |> Column.alias_("null_mul"),
    greatest([col("a"), col("b")]) |> Column.alias_("greatest_null"),
    coalesce([col("a"), col("b")]) |> Column.alias_("coalesce_null")
  ])
  |> DataFrame.collect()
end)

# === Same-column multi-window ===
run.("18. Multiple window functions on same column", fn ->
  w1 = SparkEx.Window.partition_by([col("region")]) |> SparkEx.Window.order_by([asc(col("amount"))])
  w2 = SparkEx.Window.order_by([asc(col("amount"))])

  sales = SparkEx.sql(s, """
    SELECT * FROM VALUES ('Alice','East',100), ('Bob','West',200), ('Carol','East',300)
    AS t(rep, region, amount)
  """)

  sales
  |> DataFrame.with_columns([
    row_number() |> Column.over(w1) |> Column.alias_("regional_rank"),
    row_number() |> Column.over(w2) |> Column.alias_("global_rank"),
    sum(col("amount")) |> Column.over(w1) |> Column.alias_("regional_running"),
    sum(col("amount")) |> Column.over(w2) |> Column.alias_("global_running")
  ])
  |> DataFrame.collect()
end)

# === create_dataframe with explicit schema ===
run.("19. create_dataframe with explicit schema string", fn ->
  SparkEx.create_dataframe(s,
    [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}],
    schema: "id INT, name STRING"
  )
  |> DataFrame.dtypes()
end)

# === to_explorer with various types ===
run.("20. to_explorer with dates and nulls", fn ->
  SparkEx.sql(s, """
    SELECT DATE '2024-06-15' AS dt, 42 AS num, NULL AS maybe_null, true AS flag
  """)
  |> DataFrame.to_explorer()
end)

# === DataFrame.describe ===
run.("21. describe on mixed types", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 'a', 10.5), (2, 'b', 20.3), (3, 'c', 30.1)
    AS t(id, name, val)
  """)
  |> DataFrame.describe()
  |> DataFrame.collect()
end)

# === Subquery + join pattern ===
run.("22. Subquery (temp view) + expression join", fn ->
  emp = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 55000) AS t(id, name, salary)")
  avg_df = DataFrame.agg(emp, [avg(col("salary")) |> Column.alias_("avg_sal")])
  DataFrame.cross_join(emp, avg_df)
  |> DataFrame.filter(Column.gt(col("salary"), col("avg_sal")))
  |> DataFrame.select([col("name"), col("salary"), col("avg_sal")])
  |> DataFrame.collect()
end)

# === Grouping sets via SQL ===
run.("23. Grouping sets via SQL", fn ->
  SparkEx.sql(s, """
    SELECT region, rep, SUM(amount) AS total
    FROM VALUES ('East', 'Alice', 100), ('East', 'Bob', 200), ('West', 'Alice', 150), ('West', 'Bob', 250)
    AS t(region, rep, amount)
    GROUP BY GROUPING SETS ((region, rep), (region), ())
    ORDER BY region, rep
  """) |> DataFrame.collect()
end)

# === Extreme chaining (10+ operations) ===
run.("24. 10-step pipeline", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (1,'a','x',10), (2,'b','y',20), (3,'a','x',30), (4,'b','z',40),
      (5,'a','y',50), (6,'c','x',60), (7,'c','z',70), (8,'a','x',80)
    AS t(id, grp1, grp2, val)
  """)
  |> DataFrame.filter(Column.gt(col("val"), lit(15)))          # 1. filter
  |> DataFrame.with_column("val2", Column.multiply(col("val"), lit(2)))  # 2. add col
  |> DataFrame.with_column("tag", upper(col("grp2")))          # 3. add col
  |> DataFrame.drop(["grp2"])                                    # 4. drop
  |> DataFrame.with_column_renamed("grp1", "group")            # 5. rename
  |> DataFrame.distinct()                                        # 6. distinct
  |> DataFrame.order_by([desc(col("val2"))])                    # 7. order
  |> DataFrame.limit(5)                                          # 8. limit
  |> DataFrame.select([col("group"), col("val"), col("val2"), col("tag")])  # 9. select
  |> DataFrame.collect()                                         # 10. collect
end)

# === Empty results handling ===
run.("25. Filter that returns 0 rows", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3) AS t(id)")
  |> DataFrame.filter(Column.gt(col("id"), lit(999)))
  |> DataFrame.collect()
end)

run.("26. Group by with no matching groups", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a') AS t(id, grp) WHERE false")
  |> DataFrame.group_by([col("grp")])
  |> DataFrame.agg([count(lit(1)) |> Column.alias_("cnt")])
  |> DataFrame.collect()
end)

# === Type coercion in expressions ===
run.("27. Mixed type arithmetic (int + double)", fn ->
  SparkEx.sql(s, "SELECT 1 AS int_val, 2.5 AS double_val")
  |> DataFrame.select([
    Column.plus(col("int_val"), col("double_val")) |> Column.alias_("sum"),
    Column.multiply(col("int_val"), col("double_val")) |> Column.alias_("product")
  ])
  |> DataFrame.collect()
end)

# === Large literal lists ===
run.("28. isin with many values", fn ->
  vals = Enum.to_list(1..100)
  SparkEx.sql(s, "SELECT * FROM VALUES (50),(99),(101),(1),(200) AS t(id)")
  |> DataFrame.filter(Column.isin(col("id"), vals))
  |> DataFrame.collect()
end)

IO.puts("=== Advanced exploration complete ===")
