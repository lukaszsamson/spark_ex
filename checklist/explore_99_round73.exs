# Round 73: Target remaining protobuf validation gaps in write operations
# Focus on command_encoder.ex write paths and other unexplored protobuf fields
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
# 1. Writer v1: sort_by with non-string list elements
# =============================================

run.("1a. Writer.sort_by with integer elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.write(df)
  |> SparkEx.Writer.sort_by([42])
  |> SparkEx.Writer.bucket_by(2, ["x"])
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r73_sortby_#{run_id}")
end)

run.("1b. Writer.partition_by with integer elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.write(df)
  |> SparkEx.Writer.partition_by([42])
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r73_partby_#{run_id}")
end)

run.("1c. Writer.option with non-string value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df)
  |> SparkEx.Writer.option("some_key", 42)
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r73_optv_#{run_id}")
end)

# =============================================
# 2. WriterV2: cluster_by with non-Column elements
# =============================================

run.("2a. WriterV2.cluster_by with strings", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r73_cluster_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.cluster_by(["x"])
  |> SparkEx.WriterV2.create()
end)

run.("2b. WriterV2.cluster_by with integers", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r73_cluster2_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.cluster_by([42])
  |> SparkEx.WriterV2.create()
end)

run.("2c. WriterV2.option with non-string key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r73_v2optk_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.option(42, "value")
  |> SparkEx.WriterV2.create()
end)

run.("2d. WriterV2.option with non-string value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r73_v2optv_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.option("key", 42)
  |> SparkEx.WriterV2.create()
end)

# =============================================
# 3. Reader: option with non-string value
# =============================================

run.("3a. Reader.option with non-string value", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.option("key", 42)
end)

run.("3b. Reader.options with non-string map values", fn ->
  SparkEx.Reader.new(s) |> SparkEx.Reader.options(%{"key" => 42})
end)

# =============================================
# 4. DataFrame.hint with non-string/non-Column parameters
# =============================================

run.("4a. hint with integer parameters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, "coalesce", [42]) |> DataFrame.collect()
end)

run.("4b. hint with mixed parameters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, "repartition", [4, "x"]) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.with_watermark validation
# =============================================

run.("5a. with_watermark with non-string event_time", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_watermark(df, 42, "10 seconds")
end)

run.("5b. with_watermark with non-string delay_threshold", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.with_watermark(df, "x", 42)
end)

# =============================================
# 6. Catalog.drop_table validation
# =============================================

run.("6a. drop_table with non-string name", fn ->
  Catalog.drop_table(s, 42)
end)

run.("6b. drop_table with if_exists non-boolean", fn ->
  Catalog.drop_table(s, "nonexistent_table", if_exists: "yes")
end)

run.("6c. drop_table with purge non-boolean", fn ->
  Catalog.drop_table(s, "nonexistent_table", if_exists: true, purge: "yes")
end)

# =============================================
# 7. Catalog.alter_table validation
# =============================================

run.("7a. alter_table rename with non-string new name", fn ->
  Catalog.alter_table(s, "nonexistent_table", rename: 42)
end)

run.("7b. alter_table set_properties with non-map", fn ->
  Catalog.alter_table(s, "nonexistent_table", set_properties: "not_a_map")
end)

run.("7c. alter_table unset_properties with non-list", fn ->
  Catalog.alter_table(s, "nonexistent_table", unset_properties: 42)
end)

# =============================================
# 8. MergeIntoWriter validation (builder)
# =============================================

run.("8a. MergeIntoWriter with non-string table", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.merge_into(df, 42, col("x") |> Column.eq(lit(1)))
end)

run.("8b. MergeIntoWriter with non-Column condition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.merge_into(df, "some_table", "x = 1")
end)

# =============================================
# 9. StreamWriter option validation
# =============================================

run.("9a. StreamWriter.option with non-string key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.option(42, "val")
end)

run.("9b. StreamWriter.option with non-string value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.option("key", 42)
end)

run.("9c. StreamWriter.partition_by with non-Column elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.partition_by([42])
end)

# =============================================
# 10. SparkEx.sql with args map validation
# =============================================

run.("10a. sql with non-string arg key", fn ->
  SparkEx.sql(s, "SELECT :x", %{42 => "hello"}) |> DataFrame.collect()
end)

run.("10b. sql with non-Column arg value", fn ->
  SparkEx.sql(s, "SELECT :x", %{"x" => 42}) |> DataFrame.collect()
end)

# =============================================
# 11. DataFrame.unpivot validation
# =============================================

run.("11a. unpivot with non-list ids", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, "id", ["a", "b"], "var", "val")
end)

run.("11b. unpivot with non-string variable_column_name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, [col("id")], [col("a"), col("b")], 42, "val")
end)

run.("11c. unpivot with non-string value_column_name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, [col("id")], [col("a"), col("b")], "var", 42)
end)

# =============================================
# 12. Cleanup
# =============================================

run.("12. Cleanup", fn ->
  tables = [
    "sfx.sfx_dev_warehouse.spark_ex_r73_sortby_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_partby_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_optv_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_cluster_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_cluster2_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_v2optk_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r73_v2optv_#{run_id}"
  ]
  Enum.each(tables, fn t ->
    try do Catalog.drop_table(s, t, if_exists: true, purge: true) rescue _ -> :ok end
  end)
  :cleaned
end)

IO.puts("=== Round 73 complete ===")
