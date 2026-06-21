# Round 73b: Test deferred crashes from lazy validation gaps
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

# =============================================
# 1. unpivot with non-string column names → collect
# =============================================

run.("1a. unpivot non-string variable_column_name → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, [col("id")], [col("a"), col("b")], 42, "val") |> DataFrame.collect()
end)

run.("1b. unpivot non-string value_column_name → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, [col("id")], [col("a"), col("b")], "var", 42) |> DataFrame.collect()
end)

# =============================================
# 2. sql with map args (wrong type) → collect
# =============================================

run.("2. sql with map args → need to find right API", fn ->
  # The correct API for sql with args might use keyword list or different format
  # Let's try different patterns
  df = SparkEx.sql(s, "SELECT 1 AS x")
  df |> DataFrame.collect()
end)

# =============================================
# 3. WriterV2.cluster_by with integer → actual write
# =============================================

run_id = :erlang.system_time(:millisecond) |> to_string()

run.("3. WriterV2.cluster_by([42]) → create", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r73b_cluster_#{run_id}"
  result = DataFrame.write_v2(df, table) |> SparkEx.WriterV2.cluster_by([42]) |> SparkEx.WriterV2.create()
  try do Catalog.drop_table(s, table, if_exists: true, purge: true) rescue _ -> :ok end
  result
end)

# =============================================
# 4. drop_table purge: "yes" (non-boolean) on real table
# =============================================

run.("4. drop_table purge non-boolean on real table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r73b_purge_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_v2(df, table) |> SparkEx.WriterV2.create()
  # Now try drop with non-boolean purge
  Catalog.drop_table(s, table, if_exists: true, purge: "yes")
end)

# =============================================
# 5. Catalog.create_database non-boolean if_not_exists
# =============================================

run.("5. create_database non-boolean if_not_exists", fn ->
  Catalog.create_database(s, "sfx.test_nonexistent_db_r73b", if_not_exists: "yes")
end)

# =============================================
# 6. SparkEx.connect with non-string host
# =============================================

run.("6a. connect with integer port in url", fn ->
  SparkEx.connect(url: "sc://localhost:abc/")
end)

# =============================================
# 7. DataFrame.crosstab with non-string columns
# =============================================

run.("7a. crosstab with non-string col1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => "x", "b" => "y"}], schema: "a STRING, b STRING")
  DataFrame.crosstab(df, 42, "b") |> DataFrame.collect()
end)

run.("7b. crosstab with non-string col2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => "x", "b" => "y"}], schema: "a STRING, b STRING")
  DataFrame.crosstab(df, "a", 42) |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.corr/cov with non-string columns
# =============================================

run.("8a. corr with non-string col1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1.0, "b" => 2.0}], schema: "a DOUBLE, b DOUBLE")
  DataFrame.corr(df, 42, "b")
end)

run.("8b. cov with non-string col2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1.0, "b" => 2.0}], schema: "a DOUBLE, b DOUBLE")
  DataFrame.cov(df, "a", 42)
end)

# =============================================
# 9. DataFrame.sample_by with non-map fractions
# =============================================

run.("9. sample_by with non-map fractions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"grp" => Enum.at(["A", "B"], rem(i, 2)), "val" => i} end),
    schema: "grp STRING, val INT")
  DataFrame.sample_by(df, "grp", "not_a_map")
end)

# =============================================
# 10. DataFrame.freq_items with non-list columns
# =============================================

run.("10. freq_items with non-list columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "a"}], schema: "x STRING")
  DataFrame.freq_items(df, "x", 0.5) |> DataFrame.collect()
end)

IO.puts("=== Round 73b complete ===")
