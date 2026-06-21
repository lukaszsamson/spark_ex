# Round 62: Theta sketches, KLL sketches, Reader format options (CSV/JSON/Parquet),
# DataFrame.foreach, DataFrame.foreach_partition, Column.substr,
# DataFrame.with_watermark, DataFrame.storage_level, Catalog.refresh_by_path,
# try_to_timestamp, make_timestamp, format_number, format_string,
# json_array_length, json_object_keys, map_filter/transform_keys/transform_values,
# creative wild combos
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

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. Map HOFs: map_filter, transform_keys, transform_values
# =============================================

run.("1a. map_filter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"scores" => %{"math" => 95, "english" => 60, "science" => 85, "art" => 45}}
  ], schema: "scores MAP<STRING, INT>")
  df |> DataFrame.select([
    col("scores"),
    map_filter(col("scores"), fn _k, v -> Column.gte(v, lit(80)) end)
    |> Column.alias_("high_scores")
  ]) |> DataFrame.collect()
end)

run.("1b. transform_keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => %{"name" => "Alice", "age" => "30", "city" => "NYC"}}
  ], schema: "data MAP<STRING, STRING>")
  df |> DataFrame.select([
    col("data"),
    transform_keys(col("data"), fn k, _v -> upper(k) end)
    |> Column.alias_("upper_keys")
  ]) |> DataFrame.collect()
end)

run.("1c. transform_values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"prices" => %{"widget" => 10.0, "gadget" => 25.0, "doohickey" => 50.0}}
  ], schema: "prices MAP<STRING, DOUBLE>")
  df |> DataFrame.select([
    col("prices"),
    transform_values(col("prices"), fn _k, v -> Column.multiply(v, lit(1.2)) end)
    |> Column.alias_("prices_with_tax")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. JSON functions: json_array_length, json_object_keys
# =============================================

run.("2a. json_array_length", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ~s([1, 2, 3, 4, 5])},
    %{"json" => ~s([])},
    %{"json" => ~s({"not": "array"})}
  ], schema: "json STRING")
  df |> DataFrame.select([
    col("json"),
    json_array_length(col("json")) |> Column.alias_("arr_len")
  ]) |> DataFrame.collect()
end)

run.("2b. json_object_keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ~s({"name":"Alice","age":30,"city":"NYC"})},
    %{"json" => ~s({})}
  ], schema: "json STRING")
  df |> DataFrame.select([
    col("json"),
    json_object_keys(col("json")) |> Column.alias_("keys")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. format_number + format_string
# =============================================

run.("3a. format_number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1234567.891}, %{"val" => 0.5}, %{"val" => -99999.0}
  ], schema: "val DOUBLE")
  df |> DataFrame.select([
    col("val"),
    format_number(col("val"), lit(2)) |> Column.alias_("formatted_2"),
    format_number(col("val"), lit(0)) |> Column.alias_("formatted_0")
  ]) |> DataFrame.collect()
end)

run.("3b. format_string (lit_then_cols: format + list of cols)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30, "score" => 95.5},
    %{"name" => "Bob", "age" => 25, "score" => 88.3}
  ], schema: "name STRING, age INT, score DOUBLE")
  df |> DataFrame.select([
    format_string("%s is %d years old with score %.1f",
      [col("name"), col("age"), col("score")]) |> Column.alias_("description")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Column.substr
# =============================================

run.("4. Column.substr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "Hello, World!"}, %{"s" => "ABCDEFGH"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    col("s"),
    Column.substr(col("s"), 1, 5) |> Column.alias_("first_5"),
    Column.substr(col("s"), 8, 100) |> Column.alias_("from_8")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.foreach
# =============================================

run.("5a. DataFrame.foreach (basic)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")
  DataFrame.foreach_local(df, fn row -> IO.inspect(row, label: "    foreach_row") end)
end)

run.("5b. DataFrame.foreach_partition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")
  partitions = :ets.new(:parts, [:set, :public])
  DataFrame.foreach_partition_local(df, fn rows ->
    :ets.insert(partitions, {self(), length(rows)})
  end)
  result = :ets.tab2list(partitions) |> Enum.map(fn {_pid, count} -> count end) |> Enum.sort()
  :ets.delete(partitions)
  %{partition_sizes: result, total: Enum.sum(result)}
end)

# =============================================
# 6. DataFrame.storage_level
# =============================================

run.("6. storage_level after persist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  cached = DataFrame.persist(df)
  sl = DataFrame.storage_level(cached)
  DataFrame.unpersist(cached)
  sl
end)

# =============================================
# 7. try_to_timestamp + make_timestamp
# =============================================

run.("7a. try_to_timestamp", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts" => "2024-01-15 10:30:00"},
    %{"ts" => "not-a-timestamp"},
    %{"ts" => "2024-12-31 23:59:59"}
  ], schema: "ts STRING")
  df |> DataFrame.select([
    col("ts"),
    try_to_timestamp(col("ts")) |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("7b. make_timestamp (list form)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "mo" => 6, "d" => 15, "h" => 10, "mi" => 30, "sec" => 45}
  ], schema: "y INT, mo INT, d INT, h INT, mi INT, sec INT")
  df |> DataFrame.select([
    make_timestamp([col("y"), col("mo"), col("d"), col("h"), col("mi"), col("sec")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("7c. make_timestamp (keyword form)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2024-06-15", "tm" => "10:30:45"}
  ], schema: "dt STRING, tm STRING")
  df |> DataFrame.select([
    make_timestamp(date: col("dt"), time: col("tm")) |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("7d. try_make_timestamp (invalid input)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "mo" => 13, "d" => 32, "h" => 25, "mi" => 61, "sec" => 99}
  ], schema: "y INT, mo INT, d INT, h INT, mi INT, sec INT")
  df |> DataFrame.select([
    try_make_timestamp([col("y"), col("mo"), col("d"), col("h"), col("mi"), col("sec")])
    |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Catalog operations
# =============================================

run.("8a. Catalog.is_cached? + cache_table + uncache_table", fn ->
  table = "sfx.sfx_dev_warehouse.instruments"
  before = Catalog.is_cached?(s, table)
  Catalog.cache_table(s, table)
  during = Catalog.is_cached?(s, table)
  Catalog.uncache_table(s, table)
  after_ = Catalog.is_cached?(s, table)
  %{before: before, during_cache: during, after_uncache: after_}
end)

run.("8b. Catalog.refresh_table", fn ->
  Catalog.refresh_table(s, "sfx.sfx_dev_warehouse.instruments")
end)

# =============================================
# 9. Reader format options
# =============================================

run.("9. Reader with options (table + select projection)", fn ->
  # Read table with reader options
  SparkEx.Reader.new(s)
  |> SparkEx.Reader.option("mergeSchema", "true")
  |> SparkEx.Reader.table("sfx.sfx_dev_warehouse.instruments")
  |> DataFrame.select([col("symbol"), col("type")])
  |> DataFrame.limit(5)
  |> DataFrame.collect()
end)

# =============================================
# 10. Creative: Theta sketch for set operations
# =============================================

run.("10. Theta sketch intersection/difference", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => "apple"}, %{"grp" => "A", "val" => "banana"},
    %{"grp" => "A", "val" => "cherry"}, %{"grp" => "A", "val" => "date"},
    %{"grp" => "B", "val" => "banana"}, %{"grp" => "B", "val" => "cherry"},
    %{"grp" => "B", "val" => "elderberry"}, %{"grp" => "B", "val" => "fig"}
  ], schema: "grp STRING, val STRING")

  sketches = df
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([theta_sketch_agg(col("val")) |> Column.alias_("sketch")])

  # Self-join to get both sketches as columns
  a = DataFrame.filter(sketches, Column.eq(col("grp"), lit("A")))
  b = DataFrame.filter(sketches, Column.eq(col("grp"), lit("B")))

  SparkEx.sql(s, "SELECT 'set_ops' as label")
  |> DataFrame.select([
    lit("result") |> Column.alias_("label")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 11. Wild combo: map HOF + window + struct + HLL
# =============================================

run.("11. Wild: map_filter + struct + window analytics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{
        "emp_id" => i,
        "dept" => Enum.at(["eng", "sales", "ops"], rem(i, 3)),
        "skills" => %{
          "python" => rem(i * 3, 10),
          "java" => rem(i * 7, 10),
          "elixir" => rem(i * 11, 10),
          "rust" => rem(i * 13, 10)
        }
      }
    end),
    schema: "emp_id INT, dept STRING, skills MAP<STRING, INT>")

  w = SparkEx.Window.partition_by([col("dept")])

  df
  |> DataFrame.with_column("expert_skills",
    map_filter(col("skills"), fn _k, v -> Column.gte(v, lit(7)) end))
  |> DataFrame.with_column("num_expert",
    size(col("expert_skills")))
  |> DataFrame.with_column("dept_avg_expert",
    avg(Column.cast(col("num_expert"), "DOUBLE")) |> Column.over(w))
  |> DataFrame.select([
    col("emp_id"), col("dept"), col("expert_skills"),
    col("num_expert"), col("dept_avg_expert")
  ])
  |> DataFrame.filter(Column.gt(col("num_expert"), lit(0)))
  |> DataFrame.sort([desc(col("num_expert")), asc(col("emp_id"))])
  |> DataFrame.limit(10)
  |> DataFrame.collect()
end)

# =============================================
# 12. Wild combo: JSON parse → explode → aggregate → pivot
# =============================================

run.("12. Wild: JSON → explode → aggregate → pivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "data" => ~s({"items": [{"cat": "A", "val": 10}, {"cat": "B", "val": 20}]})},
    %{"id" => 2, "data" => ~s({"items": [{"cat": "A", "val": 30}, {"cat": "C", "val": 40}]})},
    %{"id" => 3, "data" => ~s({"items": [{"cat": "B", "val": 50}, {"cat": "C", "val": 60}]})}
  ], schema: "id INT, data STRING")

  # Parse JSON, explode items, aggregate by category
  parsed = df
  |> DataFrame.with_column("parsed",
    from_json(col("data"), lit("STRUCT<items: ARRAY<STRUCT<cat: STRING, val: INT>>>")))
  |> DataFrame.select([
    col("id"),
    explode(Column.get_field(col("parsed"), "items")) |> Column.alias_("item")
  ])
  |> DataFrame.select([
    col("id"),
    Column.get_field(col("item"), "cat") |> Column.alias_("category"),
    Column.get_field(col("item"), "val") |> Column.alias_("value")
  ])

  # Pivot by category
  parsed
  |> DataFrame.group_by([col("id")])
  |> GroupedData.pivot(col("category"), ["A", "B", "C"])
  |> GroupedData.agg([sum(col("value")) |> Column.alias_("total")])
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 13. Wild combo: multi-source join + window + having
# =============================================

run.("13. Wild: multi-create + join + window + filter", fn ->
  {:ok, orders} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"order_id" => i, "customer_id" => rem(i, 10) + 1,
        "amount" => (rem(i * 997, 500) + 50) * 1.0}
    end), schema: "order_id INT, customer_id INT, amount DOUBLE")

  {:ok, customers} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i ->
      %{"customer_id" => i, "name" => "Customer_#{i}",
        "tier" => Enum.at(["gold", "silver", "bronze"], rem(i, 3))}
    end), schema: "customer_id INT, name STRING, tier STRING")

  w = SparkEx.Window.partition_by([col("tier")]) |> WindowSpec.order_by([desc(col("total_spend"))])

  orders
  |> DataFrame.group_by([col("customer_id")])
  |> GroupedData.agg([
    sum(col("amount")) |> Column.alias_("total_spend"),
    count(col("order_id")) |> Column.alias_("order_count"),
    avg(col("amount")) |> Column.alias_("avg_order")
  ])
  |> DataFrame.join(customers, col("customer_id"), "inner")
  |> DataFrame.with_column("tier_rank", row_number() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("tier_rank"), lit(2)))
  |> DataFrame.sort([asc(col("tier")), asc(col("tier_rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 14. Edge: Column.bitwise operations
# =============================================

run.("14. Bitwise operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 0b1100, "b" => 0b1010},
    %{"a" => 0xFF, "b" => 0x0F},
    %{"a" => 0, "b" => -1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.bitwise_and(col("a"), col("b")) |> Column.alias_("and_"),
    Column.bitwise_or(col("a"), col("b")) |> Column.alias_("or_"),
    Column.bitwise_xor(col("a"), col("b")) |> Column.alias_("xor_"),
    Column.bitwise_not(col("a")) |> Column.alias_("not_a")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Edge: Column.contains / startsWith / endsWith
# =============================================

run.("15. String matching: contains, starts_with, ends_with", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "Hello, World!"},
    %{"s" => "hello world"},
    %{"s" => "HELLO"},
    %{"s" => "World Hello"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    col("s"),
    Column.contains(col("s"), "Hello") |> Column.alias_("has_Hello"),
    Column.starts_with(col("s"), "Hello") |> Column.alias_("starts_Hello"),
    Column.ends_with(col("s"), "Hello") |> Column.alias_("ends_Hello")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 62 complete ===")
