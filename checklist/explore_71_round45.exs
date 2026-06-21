# Round 45: Wild creative combinations — HOF, complex joins, SQL↔DataFrame mixing,
# unusual function combos, stress testing
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, StreamReader, StreamWriter, StreamingQuery}
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
# 1. Higher-order: Column.transform on arrays
# =============================================

run.("1a. Column.transform (double each element)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    SparkEx.Functions.transform(col("arr"), fn x -> Column.multiply(x, lit(2)) end)
    |> Column.alias_("doubled")
  ]) |> DataFrame.collect()
end)

run.("1b. Column.transform with conditional", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn x ->
      when_(Column.gt(x, lit(5)), Column.multiply(x, lit(10)))
      |> Column.otherwise(x)
    end) |> Column.alias_("conditionally_scaled")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Higher-order: array_sort with comparator
# =============================================

run.("2. array_sort with custom comparator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [5, 3, 8, 1, 9, 2]}
  ], schema: "arr ARRAY<INT>")
  # Sort descending via comparator
  df |> DataFrame.select([
    col("arr"),
    array_sort(col("arr"), fn a, b -> Column.minus(b, a) end)
    |> Column.alias_("desc_sorted")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Higher-order: filter on arrays
# =============================================

run.("3. filter array elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    filter(col("arr"), fn x -> Column.eq(Column.mod(x, lit(2)), lit(0)) end)
    |> Column.alias_("evens")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Higher-order: aggregate on arrays
# =============================================

run.("4. aggregate array to compute product", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    aggregate(col("arr"), lit(1), fn acc, x -> Column.multiply(acc, x) end)
    |> Column.alias_("product")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. SQL → DataFrame → SQL mixing
# =============================================

run.("5. SQL → DataFrame transform → temp view → SQL query", fn ->
  # Start with SQL
  df = SparkEx.sql(s, "SELECT id, id * id AS squared FROM range(1, 21)")

  # Transform with DataFrame API
  enriched = df
    |> DataFrame.with_column("cubed",
      Column.multiply(col("id"), Column.multiply(col("id"), col("id"))))
    |> DataFrame.filter(Column.eq(Column.mod(col("id"), lit(3)), lit(0)))

  # Save as temp view and query with SQL again
  DataFrame.create_temp_view(enriched, "enriched_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT id, squared, cubed,
           cubed - squared AS diff,
           RANK() OVER (ORDER BY cubed DESC) as rank
    FROM enriched_#{run_id}
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "enriched_#{run_id}")
  result
end)

# =============================================
# 6. Three-way join
# =============================================

run.("6. three-way join", fn ->
  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"order_id" => 1, "customer_id" => 101, "product_id" => 201, "qty" => 3},
    %{"order_id" => 2, "customer_id" => 102, "product_id" => 202, "qty" => 1},
    %{"order_id" => 3, "customer_id" => 101, "product_id" => 203, "qty" => 5}
  ], schema: "order_id INT, customer_id INT, product_id INT, qty INT")

  {:ok, customers} = SparkEx.create_dataframe(s, [
    %{"customer_id" => 101, "name" => "Alice"},
    %{"customer_id" => 102, "name" => "Bob"}
  ], schema: "customer_id INT, name STRING")

  {:ok, products} = SparkEx.create_dataframe(s, [
    %{"product_id" => 201, "product_name" => "Widget", "price" => 10.0},
    %{"product_id" => 202, "product_name" => "Gadget", "price" => 25.0},
    %{"product_id" => 203, "product_name" => "Doohickey", "price" => 5.0}
  ], schema: "product_id INT, product_name STRING, price DOUBLE")

  orders_alias = DataFrame.alias_(orders, "o")
  customers_alias = DataFrame.alias_(customers, "c")
  products_alias = DataFrame.alias_(products, "p")

  DataFrame.join(orders_alias, customers_alias,
    Column.eq(col("o.customer_id"), col("c.customer_id")), "inner")
  |> DataFrame.join(products_alias,
    Column.eq(col("o.product_id"), col("p.product_id")), "inner")
  |> DataFrame.select([
    col("o.order_id"),
    col("c.name") |> Column.alias_("customer"),
    col("p.product_name") |> Column.alias_("product"),
    col("o.qty"),
    Column.multiply(Column.cast(col("o.qty"), "DOUBLE"), col("p.price"))
    |> Column.alias_("total_price")
  ])
  |> DataFrame.sort([asc(col("o.order_id"))])
  |> DataFrame.collect()
end)

# =============================================
# 7. Complex: histogram bucket distribution
# =============================================

run.("7. histogram with width_bucket", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => :math.sin(i / 100) * 50 + 50} end),
    schema: "val DOUBLE")

  df
  |> DataFrame.with_column("bucket", width_bucket(col("val"), 0.0, 100.0, 10))
  |> DataFrame.group_by([col("bucket")])
  |> GroupedData.agg([
    count(col("val")) |> Column.alias_("count"),
    avg(col("val")) |> Column.alias_("avg_val")
  ])
  |> DataFrame.sort([asc(col("bucket"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. Complex: anti-join for finding missing records
# =============================================

run.("8. left_anti join for missing records", fn ->
  {:ok, all_ids} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end), schema: "id INT")

  {:ok, existing} = SparkEx.create_dataframe(s,
    Enum.map([1, 3, 5, 7, 11, 13, 17, 19], fn i -> %{"id" => i} end),
    schema: "id INT")

  all_alias = DataFrame.alias_(all_ids, "all")
  existing_alias = DataFrame.alias_(existing, "ex")

  DataFrame.join(all_alias, existing_alias,
    Column.eq(col("all.id"), col("ex.id")), "left_anti")
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 9. Complex: date series generation and analysis
# =============================================

run.("9. date series with business day logic", fn ->
  SparkEx.sql(s, """
    WITH dates AS (
      SELECT date_add(DATE '2024-01-01', id) AS dt
      FROM range(0, 365)
    )
    SELECT
      dayofweek(dt) AS dow,
      CASE dayofweek(dt)
        WHEN 1 THEN 'Sun'
        WHEN 2 THEN 'Mon'
        WHEN 3 THEN 'Tue'
        WHEN 4 THEN 'Wed'
        WHEN 5 THEN 'Thu'
        WHEN 6 THEN 'Fri'
        WHEN 7 THEN 'Sat'
      END AS day_name,
      COUNT(*) AS count,
      CASE WHEN dayofweek(dt) BETWEEN 2 AND 6 THEN 'Business' ELSE 'Weekend' END AS type
    FROM dates
    GROUP BY dow, type
    ORDER BY dow
  """) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: collect_set + size for unique counting
# =============================================

run.("10. collect_set + size for unique counting per group", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)),
        "skill" => Enum.at(["Python", "Java", "Scala", "Elixir", "Rust"], rem(i, 5))}
    end), schema: "dept STRING, skill STRING")

  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([
    collect_set(col("skill")) |> Column.alias_("unique_skills"),
    size(collect_set(col("skill"))) |> Column.alias_("skill_count"),
    count(col("skill")) |> Column.alias_("total_entries")
  ])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

# =============================================
# 11. Complex: from_csv / to_csv roundtrip
# =============================================

run.("11. to_csv → from_csv roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30},
    %{"name" => "Bob", "age" => 25}
  ], schema: "name STRING, age INT")

  csv_df = df |> DataFrame.select([
    to_csv(SparkEx.Functions.struct([col("name"), col("age")])) |> Column.alias_("csv_str")
  ])

  csv_df |> DataFrame.select([
    col("csv_str"),
    from_csv(col("csv_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Complex: schema_of_json / schema_of_csv
# =============================================

run.("12a. schema_of_json", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    schema_of_json(lit("{\"name\":\"Alice\",\"age\":30,\"scores\":[1,2,3]}"))
    |> Column.alias_("inferred_schema")
  ]) |> DataFrame.collect()
end)

run.("12b. schema_of_csv", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    schema_of_csv(lit("Alice,30,true")) |> Column.alias_("inferred_schema")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Complex: multi-window with frame specs
# =============================================

run.("13. multiple window frames on same partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => rem(i * 7, 20)} end),
    schema: "id INT, val INT")

  w_all = SparkEx.Window.order_by([asc(col("id"))])
    |> WindowSpec.rows_between(:unbounded, :unbounded)

  w_3 = SparkEx.Window.order_by([asc(col("id"))])
    |> WindowSpec.rows_between(-1, 1)

  w_cum = SparkEx.Window.order_by([asc(col("id"))])
    |> WindowSpec.rows_between(:unbounded, :current_row)

  df |> DataFrame.select([
    col("id"), col("val"),
    avg(col("val")) |> Column.over(w_all) |> Column.alias_("global_avg"),
    avg(col("val")) |> Column.over(w_3) |> Column.alias_("moving_avg_3"),
    sum(col("val")) |> Column.over(w_cum) |> Column.alias_("cumulative_sum"),
    min(col("val")) |> Column.over(w_cum) |> Column.alias_("running_min"),
    max(col("val")) |> Column.over(w_cum) |> Column.alias_("running_max")
  ]) |> DataFrame.limit(10) |> DataFrame.collect()
end)

# =============================================
# 14. Complex: UDF-like pattern via SQL functions
# =============================================

run.("14. chained string transformations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"raw" => "  Hello, World!  "},
    %{"raw" => "  foo BAR baz  "},
    %{"raw" => "  123-abc-DEF  "}
  ], schema: "raw STRING")
  df |> DataFrame.select([
    col("raw"),
    trim(col("raw")) |> Column.alias_("trimmed"),
    lower(trim(col("raw"))) |> Column.alias_("lowered"),
    regexp_replace(lower(trim(col("raw"))), lit("[^a-z]"), lit("")) |> Column.alias_("alpha_only"),
    SparkEx.Functions.length(regexp_replace(lower(trim(col("raw"))), lit("[^a-z]"), lit("")))
    |> Column.alias_("alpha_len")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Complex: case sensitivity in column references
# =============================================

run.("15. case sensitivity in column names", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS lowercase, 2 AS UPPERCASE, 3 AS MixedCase")
  df |> DataFrame.select([
    col("lowercase"),
    col("UPPERCASE"),
    col("MixedCase")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 45 complete ===")
