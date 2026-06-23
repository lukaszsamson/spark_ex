# Round 66: Deep edge cases and creative stress tests
# - create_dataframe with empty arrays/maps/structs
# - create_dataframe with null in nested types
# - Column.try_cast edge cases
# - WriterV2 overwrite + replace
# - DataFrame.crosstab
# - DataFrame.corr / cov
# - DataFrame.freq_items
# - SparkEx.range with step and num_partitions
# - Column.over with range_between / rows_between
# - collect_as_map
# - to_json / from_json roundtrip with options
# - Complex: star schema with 4-way join + aggregation
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
# 1. create_dataframe with empty/null collections
# =============================================

run.("1a. Empty array column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => []},
    %{"id" => 2, "arr" => [1, 2, 3]},
    %{"id" => 3, "arr" => nil}
  ], schema: "id INT, arr ARRAY<INT>")
  df |> DataFrame.select([
    col("id"), col("arr"),
    size(col("arr")) |> Column.alias_("sz"),
    Column.is_null(col("arr")) |> Column.alias_("is_null")
  ]) |> DataFrame.collect()
end)

run.("1b. Empty map column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "m" => %{}},
    %{"id" => 2, "m" => %{"a" => 1}},
    %{"id" => 3, "m" => nil}
  ], schema: "id INT, m MAP<STRING, INT>")
  df |> DataFrame.select([
    col("id"), col("m"),
    size(col("m")) |> Column.alias_("sz"),
    Column.is_null(col("m")) |> Column.alias_("is_null")
  ]) |> DataFrame.collect()
end)

run.("1c. Null within nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"name" => "Alice", "age" => 30}},
    %{"rec" => %{"name" => nil, "age" => nil}},
    %{"rec" => nil}
  ], schema: "rec STRUCT<name: STRING, age: INT>")
  df |> DataFrame.select([
    col("rec"),
    Column.get_field(col("rec"), "name") |> Column.alias_("name"),
    Column.get_field(col("rec"), "age") |> Column.alias_("age"),
    Column.is_null(col("rec")) |> Column.alias_("rec_is_null"),
    Column.is_null(Column.get_field(col("rec"), "name")) |> Column.alias_("name_is_null")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Column.try_cast edge cases
# =============================================

run.("2. try_cast edge cases", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "42"}, %{"s" => "not_a_number"}, %{"s" => "3.14"},
    %{"s" => ""}, %{"s" => nil}, %{"s" => "9999999999999999999"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    col("s"),
    Column.try_cast(col("s"), "INT") |> Column.alias_("try_int"),
    Column.try_cast(col("s"), "DOUBLE") |> Column.alias_("try_dbl"),
    Column.try_cast(col("s"), "BOOLEAN") |> Column.alias_("try_bool"),
    Column.try_cast(col("s"), "DATE") |> Column.alias_("try_date")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. WriterV2 overwrite + replace
# =============================================

run.("3a. WriterV2 overwrite", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r66_ow_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "original"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "overwritten"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df2, table) |> SparkEx.WriterV2.overwrite(lit(true))

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

run.("3b. WriterV2 create_or_replace", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r66_cor_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "first"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create_or_replace()

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "replaced"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df2, table) |> SparkEx.WriterV2.create_or_replace()

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

# =============================================
# 4. DataFrame.crosstab
# =============================================

run.("4. crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "level" => "senior"}, %{"dept" => "eng", "level" => "junior"},
    %{"dept" => "eng", "level" => "senior"}, %{"dept" => "sales", "level" => "senior"},
    %{"dept" => "sales", "level" => "junior"}, %{"dept" => "sales", "level" => "junior"},
    %{"dept" => "ops", "level" => "senior"}, %{"dept" => "ops", "level" => "mid"},
    %{"dept" => "ops", "level" => "mid"}
  ], schema: "dept STRING, level STRING")
  {:ok, result} = DataFrame.crosstab(df, "dept", "level") |> DataFrame.collect()
  result
end)

# =============================================
# 5. DataFrame.corr / cov
# =============================================

run.("5a. corr (Pearson)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"x" => i * 1.0, "y" => i * 2.0 + 5.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.corr(df, "x", "y")
end)

run.("5b. cov", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"x" => i * 1.0, "y" => i * 2.0 + 5.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.cov(df, "x", "y")
end)

# =============================================
# 6. DataFrame.freq_items
# =============================================

run.("6. freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.flat_map(1..50, fn _ ->
      [%{"val" => "common"}, %{"val" => "common"}, %{"val" => "rare"}]
    end), schema: "val STRING")
  {:ok, result} = DataFrame.freq_items(df, ["val"], 0.4) |> DataFrame.collect()
  result
end)

# =============================================
# 7. SparkEx.range
# =============================================

run.("7a. range basic", fn ->
  {:ok, count} = SparkEx.range(s, 0, 100, 1, 4) |> DataFrame.count()
  count
end)

run.("7b. range with step", fn ->
  {:ok, rows} = SparkEx.range(s, 0, 50, 5) |> DataFrame.collect()
  rows
end)

# =============================================
# 8. Window: range_between
# =============================================

run.("8. Window range_between (value-based frame)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"ts" => i, "val" => rem(i * 7, 10) + 1} end),
    schema: "ts INT, val INT")

  w_range = SparkEx.Window.order_by([asc(col("ts"))])
    |> WindowSpec.range_between(-3, 0)

  df |> DataFrame.select([
    col("ts"), col("val"),
    sum(col("val")) |> Column.over(w_range) |> Column.alias_("sum_last_3_units"),
    avg(Column.cast(col("val"), "DOUBLE")) |> Column.over(w_range) |> Column.alias_("avg_last_3_units"),
    count(col("val")) |> Column.over(w_range) |> Column.alias_("count_in_range")
  ])
  |> DataFrame.limit(10)
  |> DataFrame.collect()
end)

# =============================================
# 9. collect_as_map
# =============================================

run.("9. collect_as_map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "val" => 1},
    %{"key" => "b", "val" => 2},
    %{"key" => "c", "val" => 3}
  ], schema: "key STRING, val INT")
  DataFrame.collect_as_map(df)
end)

# =============================================
# 10. to_json + from_json roundtrip
# =============================================

run.("10. to_json → from_json roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5},
    %{"id" => 2, "name" => "Bob", "score" => 88.0}
  ], schema: "id INT, name STRING, score DOUBLE")

  # Convert to JSON string
  json_df = df |> DataFrame.select([
    to_json(SparkEx.Functions.struct([col("id"), col("name"), col("score")])) |> Column.alias_("json_str")
  ])

  # Parse back
  json_df |> DataFrame.select([
    col("json_str"),
    from_json(col("json_str"), "STRUCT<id: INT, name: STRING, score: DOUBLE>")
    |> Column.alias_("parsed")
  ])
  |> DataFrame.select([
    Column.get_field(col("parsed"), "id") |> Column.alias_("id"),
    Column.get_field(col("parsed"), "name") |> Column.alias_("name"),
    Column.get_field(col("parsed"), "score") |> Column.alias_("score")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 11. Star schema 4-way join
# =============================================

run.("11. Star schema: fact + 3 dimensions", fn ->
  # Fact table
  {:ok, facts} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"fact_id" => i,
        "product_id" => rem(i, 5) + 1,
        "store_id" => rem(i, 3) + 1,
        "date_id" => rem(i, 10) + 1,
        "amount" => (rem(i * 997, 500) + 50) * 1.0}
    end), schema: "fact_id INT, product_id INT, store_id INT, date_id INT, amount DOUBLE")

  # Dimension tables
  {:ok, products} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"product_id" => i, "product_name" => "Product_#{i}"} end),
    schema: "product_id INT, product_name STRING")

  {:ok, stores} = SparkEx.create_dataframe(s,
    Enum.map(1..3, fn i -> %{"store_id" => i, "store_name" => Enum.at(["NYC", "LA", "CHI"], i - 1)} end),
    schema: "store_id INT, store_name STRING")

  {:ok, dates} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"date_id" => i, "quarter" => "Q#{div(i - 1, 3) + 1}"} end),
    schema: "date_id INT, quarter STRING")

  facts
  |> DataFrame.join(products, col("product_id"), "inner")
  |> DataFrame.join(stores, col("store_id"), "inner")
  |> DataFrame.join(dates, col("date_id"), "inner")
  |> DataFrame.group_by([col("store_name"), col("quarter")])
  |> GroupedData.agg([
    sum(col("amount")) |> Column.alias_("revenue"),
    count(col("fact_id")) |> Column.alias_("transactions")
  ])
  |> DataFrame.sort([asc(col("store_name")), asc(col("quarter"))])
  |> DataFrame.collect()
end)

# =============================================
# 12. Edge: describe then collect
# =============================================

run.("12. describe → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.describe(df) |> DataFrame.collect()
end)

# =============================================
# 13. Edge: summary → collect
# =============================================

run.("13. summary → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.summary(df, ["count", "mean", "min", "max"]) |> DataFrame.collect()
end)

IO.puts("=== Round 66 complete ===")
