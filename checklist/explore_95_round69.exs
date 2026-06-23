# Round 69: create_dataframe encoding deep dive + untested API validation gaps
# Focus areas:
# - create_dataframe with various type combinations that might trigger encoding bugs
# - Nested arrays with struct elements via create_dataframe
# - Boolean/numeric edge values in create_dataframe
# - Catalog API validation gaps (less tested)
# - StreamingQuery/WriterV2 option validation
# - DataFrame.observe / Observation API edge cases
# - from_json / schema_of_json edge cases
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
# 1. create_dataframe: array<struct> with various inner types
# =============================================

run.("1a. create_dataframe array<struct<string,int>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => [%{"name" => "a", "val" => 1}, %{"name" => "b", "val" => 2}]},
    %{"data" => [%{"name" => "c", "val" => nil}]},
    %{"data" => nil}
  ], schema: "data ARRAY<STRUCT<name: STRING, val: INT>>")
  df |> DataFrame.collect()
end)

run.("1b. create_dataframe array<struct<string, array<int>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => [%{"name" => "a", "nums" => [1, 2]}, %{"name" => "b", "nums" => [3]}]},
    %{"data" => [%{"name" => "c", "nums" => nil}]},
    %{"data" => nil}
  ], schema: "data ARRAY<STRUCT<name: STRING, nums: ARRAY<INT>>>")
  df |> DataFrame.collect()
end)

run.("1c. create_dataframe array<struct<string, struct<int>>>", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => [%{"name" => "a", "inner" => %{"x" => 1}}, %{"name" => "b", "inner" => nil}]}
  ], schema: "data ARRAY<STRUCT<name: STRING, inner: STRUCT<x: INT>>>")
  df |> DataFrame.collect()
end)

# =============================================
# 2. create_dataframe: numeric edge values
# =============================================

run.("2a. INT edge values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 2_147_483_647},   # INT_MAX
    %{"val" => -2_147_483_648},  # INT_MIN
    %{"val" => 0},
    %{"val" => nil}
  ], schema: "val INT")
  df |> DataFrame.collect()
end)

run.("2b. BIGINT edge values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 9_223_372_036_854_775_807},   # LONG_MAX
    %{"val" => -9_223_372_036_854_775_808},  # LONG_MIN
    %{"val" => 0}
  ], schema: "val BIGINT")
  df |> DataFrame.collect()
end)

run.("2c. DOUBLE special values", fn ->
  df = SparkEx.sql(s, """
    SELECT
      CAST('NaN' AS DOUBLE) AS nan_val,
      CAST('Infinity' AS DOUBLE) AS inf_val,
      CAST('-Infinity' AS DOUBLE) AS neg_inf_val,
      CAST(0.0 AS DOUBLE) AS zero_val,
      CAST(-0.0 AS DOUBLE) AS neg_zero_val
  """)
  df |> DataFrame.collect()
end)

run.("2d. FLOAT vs DOUBLE precision", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"f" => 3.14159265358979, "d" => 3.14159265358979}
  ], schema: "f FLOAT, d DOUBLE")
  df |> DataFrame.collect()
end)

# =============================================
# 3. create_dataframe: boolean + null combinations
# =============================================

run.("3. Boolean array with nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "flags" => [true, false, nil]},
    %{"id" => 2, "flags" => nil},
    %{"id" => 3, "flags" => []}
  ], schema: "id INT, flags ARRAY<BOOLEAN>")
  df |> DataFrame.collect()
end)

# =============================================
# 4. Catalog validation gaps
# =============================================

run.("4a. Catalog.list_databases with non-string catalog", fn ->
  Catalog.list_databases(s, catalog: 42)
end)

run.("4b. Catalog.list_tables with non-string database", fn ->
  Catalog.list_tables(s, db_name: 42)
end)

run.("4c. Catalog.list_columns with non-string table", fn ->
  Catalog.list_columns(s, 42)
end)

run.("4d. Catalog.set_current_catalog with non-string", fn ->
  Catalog.set_current_catalog(s, 42)
end)

run.("4e. Catalog.set_current_database with non-string", fn ->
  Catalog.set_current_database(s, 42)
end)

run.("4f. Catalog.cache_table with non-string", fn ->
  Catalog.cache_table(s, 42)
end)

run.("4g. Catalog.uncache_table with non-string", fn ->
  Catalog.uncache_table(s, 42)
end)

run.("4h. Catalog.is_cached? with non-string", fn ->
  Catalog.is_cached?(s, 42)
end)

run.("4i. Catalog.refresh_table with non-string", fn ->
  Catalog.refresh_table(s, 42)
end)

# =============================================
# 5. WriterV2 option validation
# =============================================

run.("5a. WriterV2.table_property with non-string key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r69_prop_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.table_property(42, "val")
  |> SparkEx.WriterV2.create()
end)

run.("5b. WriterV2.table_property with non-string value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r69_propv_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.table_property("key", 42)
  |> SparkEx.WriterV2.create()
end)

run.("5c. WriterV2.partitioned_by with non-Column list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r69_part_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.partitioned_by(["x"])
  |> SparkEx.WriterV2.create()
end)

run.("5d. WriterV2.using with non-string format", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  table = "sfx.sfx_dev_warehouse.spark_ex_r69_using_#{run_id}"
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.using(42)
  |> SparkEx.WriterV2.create()
end)

# =============================================
# 6. DataFrame.observe edge cases
# =============================================

run.("6a. observe with empty agg list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.observe(df, "obs1", []) |> DataFrame.collect()
end)

run.("6b. observe with non-string name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.observe(df, 42, [count(col("x"))]) |> DataFrame.collect()
end)

run.("6c. observe with non-Column agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.observe(df, "obs3", ["not_a_column"]) |> DataFrame.collect()
end)

# =============================================
# 7. from_json edge cases
# =============================================

run.("7a. from_json with empty string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ""},
    %{"json" => "{}"},
    %{"json" => nil}
  ], schema: "json STRING")
  df |> DataFrame.select([
    col("json"),
    from_json(col("json"), "STRUCT<x: INT>") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("7b. from_json with malformed JSON", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => "{not valid json}"},
    %{"json" => "{\"x\": 1}"},
    %{"json" => "[1,2,3]"}
  ], schema: "json STRING")
  df |> DataFrame.select([
    col("json"),
    from_json(col("json"), "STRUCT<x: INT>") |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("7c. schema_of_json", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => "{\"name\": \"Alice\", \"age\": 30, \"scores\": [1,2,3]}"}
  ], schema: "json STRING")
  df |> DataFrame.select([
    schema_of_json(col("json")) |> Column.alias_("schema")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Writer v1 validation gaps
# =============================================

run.("8a. Writer.save_as_table with non-string mode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df)
  |> SparkEx.Writer.mode(42)
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r69_mode_#{run_id}")
end)

run.("8b. Writer.partition_by with non-list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df)
  |> SparkEx.Writer.partition_by(42)
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r69_partby_#{run_id}")
end)

run.("8c. Writer.bucket_by with non-integer num_buckets", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df)
  |> SparkEx.Writer.bucket_by("five", ["x"])
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r69_bucket_#{run_id}")
end)

run.("8d. Writer.option with non-string key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df)
  |> SparkEx.Writer.option(42, "value")
  |> SparkEx.Writer.save_as_table("sfx.sfx_dev_warehouse.spark_ex_r69_optk_#{run_id}")
end)

# =============================================
# 9. config API validation
# =============================================

run.("9a. config_set with non-string key", fn ->
  SparkEx.config_set(s, [{42, "value"}])
end)

run.("9b. config_set with non-string value", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", 42}])
end)

run.("9c. config_get with non-string key", fn ->
  SparkEx.config_get(s, [42])
end)

run.("9d. config_unset with non-string key", fn ->
  SparkEx.config_unset(s, 42)
end)

# =============================================
# 10. DataFrame.create_or_replace_temp_view validation
# =============================================

run.("10a. create_or_replace_temp_view with non-string name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_temp_view(df, 42)
end)

run.("10b. create_or_replace_global_temp_view with non-string name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_global_temp_view(df, 42)
end)

# =============================================
# 11. SparkEx.sql with various invalid inputs
# =============================================

run.("11a. sql with non-string query", fn ->
  SparkEx.sql(s, 42)
end)

run.("11b. sql with nil query", fn ->
  SparkEx.sql(s, nil)
end)

run.("11c. sql with args non-map", fn ->
  SparkEx.sql(s, "SELECT 1", "not_a_map") |> DataFrame.collect()
end)

# =============================================
# 12. SparkEx.create_dataframe with invalid inputs
# =============================================

run.("12a. create_dataframe with non-list rows", fn ->
  SparkEx.create_dataframe(s, "not a list", schema: "x INT")
end)

run.("12b. create_dataframe with non-map rows", fn ->
  SparkEx.create_dataframe(s, [1, 2, 3], schema: "x INT")
end)

run.("12c. create_dataframe without schema", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}])
end)

run.("12d. create_dataframe with invalid schema string", fn ->
  SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "NOT VALID SCHEMA !!!")
end)

# =============================================
# 13. create_dataframe type mismatch (string val for int col)
# =============================================

run.("13a. String value for INT column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => "not_a_number"}
  ], schema: "x INT")
  df |> DataFrame.collect()
end)

run.("13b. Integer value for STRING column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 42}
  ], schema: "x STRING")
  df |> DataFrame.collect()
end)

run.("13c. Float value for INT column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 3.14}
  ], schema: "x INT")
  df |> DataFrame.collect()
end)

# =============================================
# 14. Cleanup any tables created
# =============================================

run.("14. Cleanup", fn ->
  tables = [
    "sfx.sfx_dev_warehouse.spark_ex_r69_prop_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_propv_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_part_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_using_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_mode_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_partby_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_bucket_#{run_id}",
    "sfx.sfx_dev_warehouse.spark_ex_r69_optk_#{run_id}"
  ]
  Enum.each(tables, fn t ->
    try do
      Catalog.drop_table(s, t, if_exists: true, purge: true)
    rescue
      _ -> :ok
    end
  end)
  :cleaned
end)

IO.puts("=== Round 69 complete ===")
