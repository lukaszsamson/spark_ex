# Round 22c: Remaining tests after Catalog.create_table crash
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r22c_#{run_id}"

# =============================================
# 7. Catalog.drop_table with purge
# =============================================

run.("7. Catalog.drop_table with purge", fn ->
  tbl = "#{tbl_base}_drop"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  SparkEx.Catalog.drop_table(s, tbl, purge: true)
end)

# =============================================
# 8. Catalog.cache_table
# =============================================

run.("8. Catalog.cache_table default", fn ->
  tbl = "#{tbl_base}_cache"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.Catalog.cache_table(s, tbl)
  SparkEx.Catalog.uncache_table(s, tbl)
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 9. Catalog.alter_table
# =============================================

run.("9. Catalog.alter_table set_properties", fn ->
  tbl = "#{tbl_base}_alter"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.Catalog.alter_table(s, tbl, set_properties: %{"comment" => "test table"})
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 10. Column.name
# =============================================

run.("10. Column.name (alias for alias_)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([Column.name(col("id"), "my_id")]) |> DataFrame.collect()
end)

# =============================================
# 11. Column.transform (higher-order)
# =============================================

run.("11a. Column.transform on array (multiply)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn x -> Column.multiply(x, lit(10)) end) |> Column.alias_("multiplied")
  ]) |> DataFrame.collect()
end)

run.("11b. Column.transform on array (upper)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"names" => ["alice", "bob", "carol"]}
  ], schema: "names ARRAY<STRING>")

  df |> DataFrame.select([
    SparkEx.Functions.transform(col("names"), fn x -> upper(x) end) |> Column.alias_("uppered")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. when_ chaining
# =============================================

run.("12a. when_ without otherwise", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    when_(Column.eq(col("val"), lit(1)), lit("one")) |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

run.("12b. 4 chained when_ conditions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 4}, %{"val" => 5}
  ], schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    when_(Column.eq(col("val"), lit(1)), lit("one"))
    |> Column.when_(Column.eq(col("val"), lit(2)), lit("two"))
    |> Column.when_(Column.eq(col("val"), lit(3)), lit("three"))
    |> Column.when_(Column.eq(col("val"), lit(4)), lit("four"))
    |> Column.otherwise(lit("other"))
    |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. config operations
# =============================================

run.("13a. config_set and config_get", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "50"}])
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
end)

run.("13b. config_get_with_default", fn ->
  SparkEx.config_get_with_default(s, [{"spark.nonexistent.key.#{run_id}", "fallback_value"}])
end)

run.("13c. config_get_option", fn ->
  SparkEx.config_get_option(s, ["spark.sql.shuffle.partitions", "spark.nonexistent.key.#{run_id}"])
end)

run.("13d. config_is_modifiable", fn ->
  SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions", "spark.master"])
end)

run.("13e. config_unset", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "42"}])
  {:ok, before} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  SparkEx.config_unset(s, ["spark.sql.shuffle.partitions"])
  {:ok, after_} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  {before, after_}
end)

# =============================================
# 14. artifact operations
# =============================================

run.("14. artifact_status", fn ->
  SparkEx.artifact_status(s, ["nonexistent.jar"])
end)

# =============================================
# 15. Window frame edge cases
# =============================================

run.("15a. rows_between :unbounded to :current_row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])
  |> SparkEx.WindowSpec.rows_between(:unbounded, :current_row)

  df |> DataFrame.select([
    col("id"), col("val"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("running_sum")
  ]) |> DataFrame.collect()
end)

run.("15b. range_between :unbounded to 0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])
  |> SparkEx.WindowSpec.range_between(:unbounded, 0)

  df |> DataFrame.select([
    col("id"), col("val"),
    sum(col("val")) |> Column.over(w) |> Column.alias_("range_sum")
  ]) |> DataFrame.collect()
end)

run.("15c. rows_between(-1, 1) sliding window", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([])
  |> SparkEx.WindowSpec.order_by([asc(col("id"))])
  |> SparkEx.WindowSpec.rows_between(-1, 1)

  df |> DataFrame.select([
    col("id"), col("val"),
    avg(col("val")) |> Column.over(w) |> Column.alias_("moving_avg")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Catalog.alter_table rename_to
# =============================================

run.("16. Catalog.alter_table rename_to", fn ->
  tbl_old = "#{tbl_base}_rename_old"
  tbl_new = "#{tbl_base}_rename_new"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 42}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl_old)

  rename_result = SparkEx.Catalog.alter_table(s, tbl_old, rename_to: tbl_new)
  # Check if new table exists
  exists_new = SparkEx.Catalog.table_exists?(s, tbl_new)
  exists_old = SparkEx.Catalog.table_exists?(s, tbl_old)

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl_new} PURGE") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl_old} PURGE") |> DataFrame.collect()
  {rename_result, exists_new, exists_old}
end)

# =============================================
# 17. Catalog.get_table
# =============================================

run.("17a. Catalog.get_table on temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  view = "get_table_view_#{run_id}"
  DataFrame.create_or_replace_temp_view(df, view)

  result = SparkEx.Catalog.get_table(s, view)
  DataFrame.drop_temp_view(s, view)
  result
end)

# =============================================
# 18. Reader.table direct (3-arg variant)
# =============================================

run.("18. Reader.table direct session variant", fn ->
  tbl = "#{tbl_base}_rd_direct"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  df |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  result = SparkEx.Reader.table(s, tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 19. DataFrame.to_local_iterator
# =============================================

run.("19. to_local_iterator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, iter} = DataFrame.to_local_iterator(df)
  rows = Enum.to_list(iter)
  {:ok, length(rows)}
end)

# =============================================
# 20. DataFrame.collect_as_map
# =============================================

run.("20. collect_as_map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "val" => 1},
    %{"key" => "b", "val" => 2}
  ], schema: "key STRING, val INT")

  DataFrame.collect_as_map(df)
end)

IO.puts("=== Remaining Round 22 tests complete ===")
