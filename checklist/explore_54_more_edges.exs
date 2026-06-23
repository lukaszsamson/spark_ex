# Round 32: More edge cases — lateral_join API, WriterV2, to_xml/from_xml,
# session window, time-based window, call_udf, unwrap_udt, try_element_at,
# raise_error, spark_partition_id, negative indexing
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
# 1. lateral_join via DataFrame API (correct usage)
# =============================================

run.("1a. lateral_join with explode - TVF approach", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [10, 20, 30]},
    %{"id" => 2, "items" => [40, 50]}
  ], schema: "id INT, items ARRAY<INT>")

  tvf = SparkEx.tvf(s)
  exploded = SparkEx.TableValuedFunction.call(tvf, "explode", [Column.outer(col("items"))])
  DataFrame.lateral_join(df, exploded)
  |> DataFrame.collect()
end)

# =============================================
# 2. try_element_at (returns null instead of error for out of bounds)
# =============================================

run.("2a. try_element_at in bounds", fn ->
  df = SparkEx.sql(s, "SELECT array(10, 20, 30) AS arr, map('a', 1, 'b', 2) AS mp")
  df |> DataFrame.select([
    try_element_at(col("arr"), lit(2)) |> Column.alias_("arr_at_2"),
    try_element_at(col("mp"), lit("a")) |> Column.alias_("map_at_a")
  ]) |> DataFrame.collect()
end)

run.("2b. try_element_at out of bounds (should return null)", fn ->
  df = SparkEx.sql(s, "SELECT array(10, 20, 30) AS arr")
  df |> DataFrame.select([
    try_element_at(col("arr"), lit(99)) |> Column.alias_("out_of_bounds"),
    try_element_at(col("arr"), lit(-1)) |> Column.alias_("negative")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. raise_error (explicit error raising)
# =============================================

run.("3a. raise_error", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -1}], schema: "val INT")
  df |> DataFrame.select([
    when_(Column.lt(col("val"), lit(0)), raise_error(lit("Value cannot be negative!")))
    |> Column.otherwise(col("val"))
    |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. spark_partition_id / monotonically_increasing_id / input_file_name
# =============================================

run.("4. partition-related functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    spark_partition_id() |> Column.alias_("partition"),
    monotonically_increasing_id() |> Column.alias_("mono_id")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Complex Column expressions: nested get_item, get_field
# =============================================

run.("5a. get_item on array and map", fn ->
  df = SparkEx.sql(s, "SELECT array(10, 20, 30) AS arr, map('x', 100, 'y', 200) AS mp")
  df |> DataFrame.select([
    Column.get_item(col("arr"), lit(1)) |> Column.alias_("arr_1"),
    Column.get_item(col("mp"), lit("y")) |> Column.alias_("mp_y")
  ]) |> DataFrame.collect()
end)

run.("5b. get_field on struct", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  df |> DataFrame.select([
    Column.get_field(col("person"), "name") |> Column.alias_("name"),
    Column.get_field(col("person"), "age") |> Column.alias_("age")
  ]) |> DataFrame.collect()
end)

run.("5c. with_field and drop_fields on struct", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS data")
  df |> DataFrame.select([
    Column.with_field(col("data"), "d", lit(4)) |> Column.alias_("added"),
    Column.drop_fields(col("data"), ["b", "c"]) |> Column.alias_("dropped")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Column.like_ / ilike_ / rlike
# =============================================

run.("6. like_ / ilike_ / rlike pattern matching", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "ALICE"},
    %{"name" => "charlie"}, %{"name" => "alice_99"}
  ], schema: "name STRING")
  df |> DataFrame.select([
    col("name"),
    Column.like(col("name"), "Ali%") |> Column.alias_("like_Ali"),
    Column.ilike(col("name"), "ali%") |> Column.alias_("ilike_ali"),
    Column.rlike(col("name"), "^[a-z]+$") |> Column.alias_("rlike_lowercase")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Column cast chain: INT → STRING → DOUBLE → LONG
# =============================================

run.("7. chained casts", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    col("val") |> Column.cast("STRING") |> Column.cast("DOUBLE") |> Column.cast("LONG")
    |> Column.alias_("chained")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Negative array indexing
# =============================================

run.("8. negative array index via element_at", fn ->
  df = SparkEx.sql(s, "SELECT array('a', 'b', 'c', 'd') AS arr")
  df |> DataFrame.select([
    element_at(col("arr"), lit(-1)) |> Column.alias_("last"),
    element_at(col("arr"), lit(-2)) |> Column.alias_("second_last")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. WriterV2 — create, append, replace
# =============================================

run.("9a. WriterV2 create table", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_writerv2_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  # Create new table
  result = SparkEx.WriterV2.create(df, table_name)
  # Verify
  read_back = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()
  # Cleanup
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  {result, read_back}
end)

run.("9b. WriterV2 append to table", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_writerv2_app_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")

  SparkEx.WriterV2.create(df1, table_name)
  SparkEx.WriterV2.append(df2, table_name)
  result = SparkEx.Reader.table(s, table_name) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  result
end)

run.("9c. WriterV2 overwrite", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_writerv2_ow_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")

  SparkEx.WriterV2.create(df1, table_name)
  SparkEx.WriterV2.overwrite(df2, table_name)
  result = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  result
end)

# =============================================
# 10. Catalog.list_columns
# =============================================

run.("10. list_columns for existing table", fn ->
  # Use one of the known warehouse tables
  Catalog.list_columns(s, "sfx.sfx_dev_warehouse.spark_ex_test")
end)

# =============================================
# 11. Complex DataFrame pipeline with multiple joins
# =============================================

run.("11. multi-join pipeline", fn ->
  {:ok, users} = SparkEx.create_dataframe(s, [
    %{"uid" => 1, "name" => "Alice"}, %{"uid" => 2, "name" => "Bob"},
    %{"uid" => 3, "name" => "Charlie"}
  ], schema: "uid INT, name STRING")

  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"oid" => 1, "uid" => 1, "product" => "Widget", "qty" => 5},
    %{"oid" => 2, "uid" => 1, "product" => "Gadget", "qty" => 2},
    %{"oid" => 3, "uid" => 2, "product" => "Widget", "qty" => 1},
    %{"oid" => 4, "uid" => 3, "product" => "Doohickey", "qty" => 10}
  ], schema: "oid INT, uid INT, product STRING, qty INT")

  {:ok, products} = SparkEx.create_dataframe(s, [
    %{"product" => "Widget", "price" => 10.0},
    %{"product" => "Gadget", "price" => 25.0},
    %{"product" => "Doohickey", "price" => 5.0}
  ], schema: "product STRING, price DOUBLE")

  # Join all three and compute totals
  users_alias = DataFrame.alias_(users, "u")
  orders_alias = DataFrame.alias_(orders, "o")
  products_alias = DataFrame.alias_(products, "p")

  joined = DataFrame.join(users_alias, orders_alias,
    Column.eq(col("u.uid"), col("o.uid")), "inner")
  |> DataFrame.join(products_alias,
    Column.eq(col("o.product"), col("p.product")), "inner")
  |> DataFrame.with_column("total", Column.multiply(col("o.qty"), col("p.price")))
  |> DataFrame.group_by([col("u.name")])
  |> GroupedData.agg([
    sum(col("total")) |> Column.alias_("revenue"),
    count(col("o.oid")) |> Column.alias_("order_count")
  ])
  |> DataFrame.sort([desc(col("revenue"))])
  |> DataFrame.collect()
end)

# =============================================
# 12. typeof / input_file_name / current_date / current_timestamp
# =============================================

run.("12. utility functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42, "txt" => "hello", "f" => 3.14}],
    schema: "val INT, txt STRING, f DOUBLE")
  df |> DataFrame.select([
    typeof(col("val")) |> Column.alias_("type_val"),
    typeof(col("txt")) |> Column.alias_("type_txt"),
    typeof(col("f")) |> Column.alias_("type_f"),
    current_date() |> Column.alias_("today"),
    current_timestamp() |> Column.alias_("now"),
    input_file_name() |> Column.alias_("input_file")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. xxhash64 / crc32 / md5 / sha2 on various types
# =============================================

run.("13. hash functions on compound expression", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "test_string_123"}], schema: "val STRING")
  df |> DataFrame.select([
    md5(col("val")) |> Column.alias_("md5"),
    sha1(col("val")) |> Column.alias_("sha1"),
    sha2(col("val"), 256) |> Column.alias_("sha256"),
    xxhash64([col("val")]) |> Column.alias_("xxhash"),
    crc32(col("val")) |> Column.alias_("crc32"),
    hash([col("val")]) |> Column.alias_("spark_hash")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Window: percent_rank / cume_dist / ntile
# =============================================

run.("14. percent_rank / cume_dist / ntile window functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i * 10} end),
    schema: "val INT")
  w = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("val"))])
  df |> DataFrame.select([
    col("val"),
    percent_rank() |> Column.over(w) |> Column.alias_("pct_rank"),
    cume_dist() |> Column.over(w) |> Column.alias_("cume"),
    ntile(4) |> Column.over(w) |> Column.alias_("quartile")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. DataFrame.drop_duplicates with subset
# =============================================

run.("15a. drop_duplicates (all columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"}, %{"a" => 1, "b" => "x"},
    %{"a" => 2, "b" => "y"}, %{"a" => 1, "b" => "y"}
  ], schema: "a INT, b STRING")
  DataFrame.drop_duplicates(df) |> DataFrame.sort([asc(col("a")), asc(col("b"))]) |> DataFrame.collect()
end)

run.("15b. drop_duplicates with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"}, %{"a" => 1, "b" => "y"},
    %{"a" => 2, "b" => "x"}, %{"a" => 2, "b" => "y"}
  ], schema: "a INT, b STRING")
  DataFrame.drop_duplicates(df, ["a"]) |> DataFrame.sort([asc(col("a"))]) |> DataFrame.collect()
end)

# =============================================
# 16. rand / randn (seeded)
# =============================================

run.("16. rand / randn seeded", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    rand(42) |> Column.alias_("uniform"),
    randn(42) |> Column.alias_("normal")
  ]) |> DataFrame.collect()
end)

# =============================================
# 17. DataFrame.exceptAll / intersectAll (with duplicates)
# =============================================

run.("17a. exceptAll preserves duplicates", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")
  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}
  ], schema: "val INT")
  DataFrame.except_all(a, b) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

run.("17b. intersectAll preserves duplicates", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}
  ], schema: "val INT")
  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 1}
  ], schema: "val INT")
  DataFrame.intersect_all(a, b) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

# =============================================
# 18. DataFrame.unpivot
# =============================================

run.("18. unpivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "jan" => 100, "feb" => 200, "mar" => 300},
    %{"id" => 2, "jan" => 400, "feb" => 500, "mar" => 600}
  ], schema: "id INT, jan INT, feb INT, mar INT")
  DataFrame.unpivot(df, [col("id")], [col("jan"), col("feb"), col("mar")], "month", "revenue")
  |> DataFrame.sort([asc(col("id")), asc(col("month"))])
  |> DataFrame.collect()
end)

# =============================================
# 19. DataFrame.to_local_iterator / collect_as_map
# =============================================

run.("19a. to_local_iterator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, iter} = DataFrame.to_local_iterator(df)
  rows = Enum.to_list(iter)
  length(rows)
end)

run.("19b. collect_as_map (key-value pairs)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "val" => 1}, %{"key" => "b", "val" => 2}, %{"key" => "c", "val" => 3}
  ], schema: "key STRING, val INT")
  DataFrame.collect_as_map(df)
end)

# =============================================
# 20. Complex type coercion edge cases
# =============================================

run.("20a. lit with Decimal", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([
    lit(Decimal.new("123456789.123456789")) |> Column.alias_("dec")
  ]) |> DataFrame.collect()
end)

run.("20b. lit with NaN and Infinity (float)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([
    lit(:nan) |> Column.alias_("nan_val"),
    lit(:infinity) |> Column.alias_("inf_val"),
    lit(:neg_infinity) |> Column.alias_("neg_inf_val")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 32 complete ===")
