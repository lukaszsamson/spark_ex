# Sections 10-15: Advanced Joins, Set Ops, Windows, NA/Stats, Complex Types, Temp Views
# Usage: mix run checklist/section_10_15.exs

alias SparkEx.{DataFrame, Column, GroupedData, Window, WindowSpec, Catalog, Reader}
import SparkEx.Functions

IO.puts("=== Sections 10-15 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 300)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# === Section 11: Set Operations ===
IO.puts("--- Section 11: Set Operations ---\n")

df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, v)")
df2 = SparkEx.sql(s, "SELECT * FROM VALUES (2, 'b'), (3, 'c'), (4, 'd') AS t(id, v)")

run.("11.1 union", fn ->
  DataFrame.union(df1, df2) |> DataFrame.count()
end)

run.("11.2 union_distinct", fn ->
  DataFrame.union_distinct(df1, df2) |> DataFrame.count()
end)

run.("11.3 union_by_name", fn ->
  DataFrame.union_by_name(df1, df2) |> DataFrame.count()
end)

run.("11.4 intersect", fn ->
  DataFrame.intersect(df1, df2) |> DataFrame.collect()
end)

run.("11.5 intersect_all", fn ->
  DataFrame.intersect_all(df1, df2) |> DataFrame.collect()
end)

run.("11.6 except", fn ->
  DataFrame.except(df1, df2) |> DataFrame.collect()
end)

run.("11.7 except_all", fn ->
  DataFrame.except_all(df1, df2) |> DataFrame.collect()
end)

# === Section 12: Window Functions ===
IO.puts("--- Section 12: Window Functions ---\n")

wdf = SparkEx.sql(s, """
  SELECT * FROM VALUES
    ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('eng', 'Eve', 130),
    ('sales', 'Carol', 90), ('sales', 'Dave', 110)
  AS t(dept, name, salary)
""")
w = Window.partition_by(["dept"]) |> WindowSpec.order_by([col("salary") |> Column.asc()])

run.("12.1 row_number", fn ->
  wdf |> DataFrame.with_column("rn", row_number() |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.2 rank", fn ->
  wdf |> DataFrame.with_column("rnk", rank() |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.3 dense_rank", fn ->
  wdf |> DataFrame.with_column("drnk", dense_rank() |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.4 lag", fn ->
  wdf |> DataFrame.with_column("prev_sal", lag(col("salary"), lit(1)) |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.5 lead", fn ->
  wdf |> DataFrame.with_column("next_sal", lead(col("salary"), lit(1)) |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.6 Running sum", fn ->
  wdf |> DataFrame.with_column("running_sum", sum(col("salary")) |> Column.over(w)) |> DataFrame.collect()
end)

run.("12.7 rows_between", fn ->
  w2 = w |> WindowSpec.rows_between(Window.unbounded_preceding(), Window.current_row())
  wdf |> DataFrame.with_column("cum_sum", sum(col("salary")) |> Column.over(w2)) |> DataFrame.collect()
end)

run.("12.9 first_value / last_value", fn ->
  wdf |> DataFrame.with_column("first_name", first_value(col("name")) |> Column.over(w)) |> DataFrame.collect()
end)

# === Section 13: NA & Statistics ===
IO.puts("--- Section 13: NA & Statistics ---\n")

na_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 10.0, 'a'), (2, NULL, 'b'), (NULL, 30.0, NULL), (4, 40.0, 'a')
  AS t(id, value, category)
""")

run.("13.1 dropna any", fn ->
  DataFrame.dropna(na_df) |> DataFrame.collect()
end)

run.("13.5 fillna scalar", fn ->
  DataFrame.fillna(na_df, 0) |> DataFrame.collect()
end)

run.("13.6 fillna map", fn ->
  DataFrame.fillna(na_df, %{"id" => 0, "value" => 0.0, "category" => "unknown"}) |> DataFrame.collect()
end)

run.("13.9 describe", fn ->
  DataFrame.describe(na_df) |> DataFrame.collect()
end)

run.("13.11 corr", fn ->
  DataFrame.Stat.corr(na_df, "id", "value")
end)

run.("13.12 cov", fn ->
  DataFrame.Stat.cov(na_df, "id", "value")
end)

run.("13.13 crosstab", fn ->
  DataFrame.Stat.crosstab(na_df, "id", "category") |> DataFrame.collect()
end)

run.("13.14 freq_items", fn ->
  DataFrame.Stat.freq_items(na_df, ["category"]) |> DataFrame.collect()
end)

# === Section 14: Complex Types ===
IO.puts("--- Section 14: Complex Types ---\n")

ct_df = SparkEx.sql(s, """
  SELECT
    array(1, 2, 3) AS arr,
    map('a', 1, 'b', 2) AS m,
    named_struct('x', 10, 'y', 'hello') AS st
""")

run.("14.1 Array access", fn ->
  ct_df |> DataFrame.select([col("arr") |> Column.get_item(lit(0)) |> Column.alias_("first")]) |> DataFrame.collect()
end)

run.("14.2 Array size", fn ->
  ct_df |> DataFrame.select([size(col("arr")) |> Column.alias_("arr_size")]) |> DataFrame.collect()
end)

run.("14.3 array_contains", fn ->
  ct_df |> DataFrame.select([array_contains(col("arr"), lit(2)) |> Column.alias_("has_2")]) |> DataFrame.collect()
end)

run.("14.4 explode", fn ->
  ct_df |> DataFrame.select([explode(col("arr")) |> Column.alias_("val")]) |> DataFrame.collect()
end)

run.("14.5 Map access", fn ->
  ct_df |> DataFrame.select([col("m") |> Column.get_item(lit("a")) |> Column.alias_("val_a")]) |> DataFrame.collect()
end)

run.("14.6 map_keys / map_values", fn ->
  ct_df |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values")
  ]) |> DataFrame.collect()
end)

run.("14.7 Struct field", fn ->
  ct_df |> DataFrame.select([col("st") |> Column.get_field("x") |> Column.alias_("x_val")]) |> DataFrame.collect()
end)

run.("14.8 with_field", fn ->
  ct_df |> DataFrame.select([col("st") |> Column.with_field("z", lit(99)) |> Column.alias_("st2")]) |> DataFrame.collect()
end)

run.("14.9 drop_fields", fn ->
  ct_df |> DataFrame.select([col("st") |> Column.drop_fields(["y"]) |> Column.alias_("st3")]) |> DataFrame.collect()
end)

# === Section 15: Temp Views & Table Caching ===
IO.puts("--- Section 15: Temp Views & Table Caching ---\n")

view_df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")

run.("15.1 Create temp view", fn ->
  DataFrame.create_temp_view(view_df, "my_temp_view")
end)

run.("15.2 Create or replace temp view", fn ->
  DataFrame.create_or_replace_temp_view(view_df, "my_temp_view")
end)

run.("15.3 Query temp view", fn ->
  SparkEx.sql(s, "SELECT * FROM my_temp_view") |> DataFrame.collect()
end)

run.("15.4 Create global temp view", fn ->
  DataFrame.create_global_temp_view(view_df, "my_global_view")
end)

run.("15.6 Query global temp view", fn ->
  SparkEx.sql(s, "SELECT * FROM global_temp.my_global_view") |> DataFrame.collect()
end)

run.("15.7 Cache table", fn ->
  r1 = Catalog.cache_table(s, "my_temp_view")
  r2 = Catalog.is_cached?(s, "my_temp_view")
  {r1, r2}
end)

run.("15.8 Refresh table", fn ->
  Catalog.refresh_table(s, "my_temp_view")
end)

run.("15.9 Uncache table", fn ->
  r1 = Catalog.uncache_table(s, "my_temp_view")
  r2 = Catalog.is_cached?(s, "my_temp_view")
  {r1, r2}
end)

run.("15.10 Clear cache", fn ->
  Catalog.clear_cache(s)
end)

run.("15.11 Drop temp view", fn ->
  Catalog.drop_temp_view(s, "my_temp_view")
end)

run.("15.12 Drop global temp view", fn ->
  Catalog.drop_global_temp_view(s, "my_global_view")
end)

IO.puts("=== Sections 10-15 Complete ===")
