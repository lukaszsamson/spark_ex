# Round 63: KLL sketches, count_min_sketch, DataFrame.explain modes,
# DataFrame.schema/schema_json, DataFrame.is_local, DataFrame.to_df,
# DataFrame.with_watermark (non-streaming), percentile_approx,
# try_add/try_subtract/try_multiply/try_divide, width_bucket,
# array_sort with comparator, array_distinct/array_except/array_intersect/array_union,
# map_from_arrays/map_from_entries/map_concat, creative ETL pipelines
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
# 1. DataFrame.explain modes
# =============================================

run.("1a. explain simple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => 100}], schema: "id INT, val INT")
  DataFrame.explain(df)
end)

run.("1b. explain extended", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => 100}], schema: "id INT, val INT")
  DataFrame.explain(df, mode: "extended")
end)

run.("1c. explain formatted", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 100} end),
    schema: "id INT, val INT")
  df
  |> DataFrame.filter(Column.gt(col("val"), lit(200)))
  |> DataFrame.explain(mode: "formatted")
end)

# =============================================
# 2. DataFrame.schema / columns / dtypes
# =============================================

run.("2a. schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "scores" => [90, 85], "meta" => %{"key" => "val"}}
  ], schema: "id INT, name STRING, scores ARRAY<INT>, meta MAP<STRING, STRING>")
  DataFrame.schema(df)
end)

run.("2b. columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "scores" => [90, 85]}
  ], schema: "id INT, name STRING, scores ARRAY<INT>")
  DataFrame.columns(df)
end)

run.("2c. dtypes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "scores" => [90, 85], "active" => true, "ratio" => 0.5}
  ], schema: "id INT, name STRING, scores ARRAY<INT>, active BOOLEAN, ratio DOUBLE")
  DataFrame.dtypes(df)
end)

# =============================================
# 3. try_add / try_subtract / try_multiply / try_divide
# =============================================

run.("3a. try_add (overflow protection)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 2147483647, "b" => 1},  # INT max + 1
    %{"a" => 100, "b" => 200},
    %{"a" => -2147483648, "b" => -1}  # INT min - 1
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    try_add(col("a"), col("b")) |> Column.alias_("try_sum"),
    try_subtract(col("a"), col("b")) |> Column.alias_("try_diff"),
    try_multiply(col("a"), col("b")) |> Column.alias_("try_prod"),
    try_divide(col("a"), col("b")) |> Column.alias_("try_quot")
  ]) |> DataFrame.collect()
end)

run.("3b. try_divide by zero", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 100, "b" => 0}, %{"a" => 0, "b" => 0}, %{"a" => 100, "b" => 3}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    try_divide(col("a"), col("b")) |> Column.alias_("try_div"),
    Column.divide(col("a"), col("b")) |> Column.alias_("normal_div")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. width_bucket
# =============================================

run.("4. width_bucket (histogram binning)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 5.0} end),
    schema: "val DOUBLE")
  df |> DataFrame.select([
    col("val"),
    width_bucket(col("val"), lit(0.0), lit(100.0), lit(10)) |> Column.alias_("bucket")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Array set operations
# =============================================

run.("5a. array_distinct + array_sort", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [3, 1, 4, 1, 5, 9, 2, 6, 5, 3]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    array_distinct(col("arr")) |> Column.alias_("distinct"),
    sort_array(col("arr")) |> Column.alias_("sorted"),
    sort_array(array_distinct(col("arr"))) |> Column.alias_("sorted_distinct"),
    size(array_distinct(col("arr"))) |> Column.alias_("distinct_count")
  ]) |> DataFrame.collect()
end)

run.("5b. array_except / array_intersect / array_union", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3, 4, 5], "b" => [3, 4, 5, 6, 7]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    col("a"), col("b"),
    array_except(col("a"), col("b")) |> Column.alias_("a_minus_b"),
    array_intersect(col("a"), col("b")) |> Column.alias_("a_and_b"),
    array_union(col("a"), col("b")) |> Column.alias_("a_or_b")
  ]) |> DataFrame.collect()
end)

run.("5c. array_sort with comparator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => ["banana", "apple", "cherry", "date"]}
  ], schema: "arr ARRAY<STRING>")
  df |> DataFrame.select([
    col("arr"),
    # Sort by string length (descending)
    array_sort(col("arr"), fn a, b ->
      Column.minus(SparkEx.Functions.length(b), SparkEx.Functions.length(a))
    end) |> Column.alias_("by_length_desc")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Map construction and operations
# =============================================

run.("6a. map_from_arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"keys" => ["a", "b", "c"], "vals" => [1, 2, 3]}
  ], schema: "keys ARRAY<STRING>, vals ARRAY<INT>")
  df |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("map")
  ]) |> DataFrame.collect()
end)

run.("6b. map_from_entries", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"entries" => [%{"key" => "x", "value" => 10}, %{"key" => "y", "value" => 20}]}
  ], schema: "entries ARRAY<STRUCT<key: STRING, value: INT>>")
  df |> DataFrame.select([
    map_from_entries(col("entries")) |> Column.alias_("map")
  ]) |> DataFrame.collect()
end)

run.("6c. map_concat", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m1" => %{"a" => 1, "b" => 2}, "m2" => %{"c" => 3, "d" => 4}}
  ], schema: "m1 MAP<STRING, INT>, m2 MAP<STRING, INT>")
  df |> DataFrame.select([
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged")
  ]) |> DataFrame.collect()
end)

run.("6d. map_keys + map_values + map_entries", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"x" => 1, "y" => 2, "z" => 3}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values"),
    map_entries(col("m")) |> Column.alias_("entries")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. approx_percentile (with accuracy)
# =============================================

run.("7. approx_percentile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10000, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  df |> DataFrame.select([
    approx_percentile(col("val"), lit(0.5), lit(100)) |> Column.alias_("p50"),
    approx_percentile(col("val"), lit(0.95), lit(100)) |> Column.alias_("p95"),
    approx_percentile(col("val"), lit(0.99), lit(100)) |> Column.alias_("p99")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.is_local
# =============================================

run.("8. DataFrame.is_local", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.is_local(df)
end)

# =============================================
# 9. Creative ETL: multi-step with array/map/struct operations
# =============================================

run.("9. Creative ETL: orders → items → category stats → pivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      num_items = rem(i, 4) + 1
      items = Enum.map(1..num_items, fn j ->
        %{"product" => Enum.at(["Widget", "Gadget", "Tool", "Part"], rem(i + j, 4)),
          "qty" => rem(i * j, 10) + 1,
          "price" => (rem(i * j * 7, 50) + 5) * 1.0}
      end)
      %{"order_id" => i,
        "region" => Enum.at(["East", "West", "North", "South"], rem(i, 4)),
        "items" => items}
    end),
    schema: "order_id INT, region STRING, items ARRAY<STRUCT<product: STRING, qty: INT, price: DOUBLE>>")

  w = SparkEx.Window.partition_by([col("region")])

  df
  |> DataFrame.select([
    col("order_id"), col("region"),
    explode(col("items")) |> Column.alias_("item")
  ])
  |> DataFrame.select([
    col("order_id"), col("region"),
    Column.get_field(col("item"), "product") |> Column.alias_("product"),
    Column.get_field(col("item"), "qty") |> Column.alias_("qty"),
    Column.get_field(col("item"), "price") |> Column.alias_("price"),
    Column.multiply(
      Column.cast(Column.get_field(col("item"), "qty"), "DOUBLE"),
      Column.get_field(col("item"), "price")
    ) |> Column.alias_("line_total")
  ])
  |> DataFrame.group_by([col("region"), col("product")])
  |> GroupedData.agg([
    sum(col("line_total")) |> Column.alias_("revenue"),
    sum(col("qty")) |> Column.alias_("units"),
    count(col("order_id")) |> Column.alias_("order_lines")
  ])
  |> DataFrame.with_column("region_total",
    sum(col("revenue")) |> Column.over(w))
  |> DataFrame.with_column("pct_of_region",
    SparkEx.Functions.round(
      Column.multiply(
        Column.divide(col("revenue"), col("region_total")),
        lit(100.0)),
      lit(1)))
  |> DataFrame.sort([asc(col("region")), desc(col("revenue"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. Creative: sliding window + lead/lag chain
# =============================================

run.("10. Sliding window: 3-day moving avg + lead/lag chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"day" => i, "metric" => (rem(i * 997, 100) + 1) * 1.0}
    end), schema: "day INT, metric DOUBLE")

  w = SparkEx.Window.order_by([asc(col("day"))])
  w3 = SparkEx.Window.order_by([asc(col("day"))])
    |> WindowSpec.rows_between(-1, 1)

  df
  |> DataFrame.select([
    col("day"), col("metric"),
    avg(col("metric")) |> Column.over(w3) |> Column.alias_("ma3"),
    lag(col("metric"), offset: 1) |> Column.over(w) |> Column.alias_("prev"),
    lead(col("metric"), offset: 1) |> Column.over(w) |> Column.alias_("next"),
    Column.minus(col("metric"),
      lag(col("metric"), offset: 1) |> Column.over(w)) |> Column.alias_("delta")
  ])
  |> DataFrame.limit(10)
  |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.to_df (rename columns)
# =============================================

run.("11. DataFrame.to_df (column rename)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  df
  |> DataFrame.to_df(["x", "y", "z"])
  |> DataFrame.collect()
end)

# =============================================
# 12. Edge: empty DataFrame operations
# =============================================

run.("12a. empty DataFrame aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, val DOUBLE")
  df |> DataFrame.select([
    count(col("id")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total"),
    avg(col("val")) |> Column.alias_("mean"),
    min(col("val")) |> Column.alias_("min_val"),
    max(col("val")) |> Column.alias_("max_val")
  ]) |> DataFrame.collect()
end)

run.("12b. empty DataFrame filter + union", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT, val STRING")
  {:ok, full} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}
  ], schema: "id INT, val STRING")

  # Filter empty DF
  {:ok, filtered} = empty |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.collect()

  # Union empty with non-empty
  {:ok, unioned} = DataFrame.union(empty, full) |> DataFrame.collect()

  %{filtered: filtered, unioned: unioned}
end)

# =============================================
# 13. Edge: very long column expressions
# =============================================

run.("13. Very long chained expression (20 operations)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 1.0}], schema: "val DOUBLE")
  expr = col("val")
    |> Column.plus(lit(1.0))
    |> Column.multiply(lit(2.0))
    |> Column.minus(lit(0.5))
    |> Column.plus(lit(3.0))
    |> Column.multiply(lit(0.1))
    |> Column.plus(lit(10.0))
    |> Column.minus(lit(2.0))
    |> Column.multiply(lit(3.0))
    |> Column.plus(lit(0.5))
    |> Column.minus(lit(1.0))
    |> Column.multiply(lit(2.0))
    |> Column.plus(lit(5.0))
    |> Column.minus(lit(3.0))
    |> Column.multiply(lit(0.5))
    |> Column.plus(lit(1.0))
    |> Column.minus(lit(0.1))
    |> Column.multiply(lit(10.0))
    |> Column.plus(lit(0.01))
    |> Column.minus(lit(0.001))
    |> Column.multiply(lit(1.0))

  df |> DataFrame.select([
    expr |> Column.alias_("result")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Creative: group_by + collect_list + array ops pipeline
# =============================================

run.("14. group_by → collect_list → array ops pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)),
        "score" => rem(i * 7, 100)}
    end), schema: "dept STRING, score INT")

  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([
    collect_list(col("score")) |> Column.alias_("all_scores"),
    count(col("score")) |> Column.alias_("n")
  ])
  |> DataFrame.with_column("sorted_scores",
    sort_array(col("all_scores")))
  |> DataFrame.with_column("distinct_scores",
    array_distinct(col("all_scores")))
  |> DataFrame.with_column("top_3",
    slice(sort_array(col("all_scores"), lit(false)), lit(1), lit(3)))
  |> DataFrame.select([
    col("dept"), col("n"),
    size(col("distinct_scores")) |> Column.alias_("unique_count"),
    col("top_3"),
    element_at(col("sorted_scores"), lit(-1)) |> Column.alias_("max_score"),
    element_at(col("sorted_scores"), lit(1)) |> Column.alias_("min_score")
  ])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 63 complete ===")
