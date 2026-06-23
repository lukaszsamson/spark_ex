# Round 22b: WriterV2 (correct API), more Writer edge cases, session operations
alias SparkEx.{DataFrame, Column, Writer, WriterV2, Reader, Catalog}
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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r22b_#{run_id}"

# =============================================
# 1. WriterV2 with correct API (table_name in write_v2)
# =============================================

run.("1a. WriterV2 create", fn ->
  tbl = "#{tbl_base}_v2c"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "test"}], schema: "id INT, val STRING")

  df |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.create()
  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1b. WriterV2 append", fn ->
  tbl = "#{tbl_base}_v2a"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.append()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1c. WriterV2 overwrite (no condition)", fn ->
  tbl = "#{tbl_base}_v2ow"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.overwrite(lit(true))

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1d. WriterV2 overwrite with condition", fn ->
  tbl = "#{tbl_base}_v2owc"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grp" => "a"},
    %{"id" => 2, "grp" => "b"}
  ], schema: "id INT, grp STRING")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 99, "grp" => "a"}
  ], schema: "id INT, grp STRING")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.overwrite(Column.eq(col("grp"), lit("a")))

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1e. WriterV2 replace", fn ->
  tbl = "#{tbl_base}_v2r"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1, "extra" => "new"}], schema: "id INT, extra STRING")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.replace()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1f. WriterV2 create_or_replace", fn ->
  tbl = "#{tbl_base}_v2cor"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.create_or_replace()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1g. WriterV2 overwrite_partitions", fn ->
  tbl = "#{tbl_base}_v2owp"
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grp" => "a"},
    %{"id" => 2, "grp" => "b"}
  ], schema: "id INT, grp STRING")
  df1 |> DataFrame.write() |> Writer.partition_by(["grp"]) |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 99, "grp" => "a"}
  ], schema: "id INT, grp STRING")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.overwrite_partitions()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1h. WriterV2 with table_property", fn ->
  tbl = "#{tbl_base}_v2p"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write_v2(tbl)
  |> WriterV2.using("iceberg")
  |> WriterV2.table_property("write.format.default", "parquet")
  |> WriterV2.create()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1i. WriterV2 with table_properties (map)", fn ->
  tbl = "#{tbl_base}_v2pm"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write_v2(tbl)
  |> WriterV2.using("iceberg")
  |> WriterV2.table_properties(%{"write.format.default" => "parquet", "write.metadata.compression-codec" => "gzip"})
  |> WriterV2.create()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1j. WriterV2 with option/options", fn ->
  tbl = "#{tbl_base}_v2opt"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write_v2(tbl)
  |> WriterV2.using("iceberg")
  |> WriterV2.option("compression", "snappy")
  |> WriterV2.create()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1k. WriterV2 with partitioned_by", fn ->
  tbl = "#{tbl_base}_v2part"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grp" => "a"},
    %{"id" => 2, "grp" => "b"}
  ], schema: "id INT, grp STRING")

  df |> DataFrame.write_v2(tbl)
  |> WriterV2.using("iceberg")
  |> WriterV2.partitioned_by([col("grp")])
  |> WriterV2.create()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 2. Session operations
# =============================================

run.("2a. clone_session", fn ->
  {:ok, s2} = SparkEx.clone_session(s)
  result = SparkEx.sql(s2, "SELECT 1 AS test") |> DataFrame.collect()
  SparkEx.Session.release(s2)
  result
end)

run.("2b. clone_session with custom id", fn ->
  {:ok, s2} = SparkEx.clone_session(s, "custom_session_#{run_id}")
  result = SparkEx.sql(s2, "SELECT 42 AS value") |> DataFrame.collect()
  SparkEx.Session.release(s2)
  result
end)

# =============================================
# 3. Writer.cluster_by
# =============================================

run.("3. Writer with cluster_by", fn ->
  tbl = "#{tbl_base}_cluster"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grp" => "a"},
    %{"id" => 2, "grp" => "b"}
  ], schema: "id INT, grp STRING")

  df |> DataFrame.write() |> Writer.cluster_by(["grp"]) |> Writer.save_as_table(tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 4. Writer format chaining
# =============================================

run.("4. Writer with format('iceberg')", fn ->
  tbl = "#{tbl_base}_fmt"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write() |> Writer.format("iceberg") |> Writer.save_as_table(tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 5. Catalog create/drop database
# =============================================

run.("5a. Catalog.create_database and drop_database", fn ->
  db = "sfx.spark_ex_test_db_#{run_id}"
  create_result = Catalog.create_database(s, db, if_not_exists: true)
  exists = Catalog.database_exists?(s, db)
  Catalog.drop_database(s, db, if_exists: true)
  {create_result, exists}
end)

# =============================================
# 6. Catalog create_table
# =============================================

run.("6. Catalog.create_table with schema", fn ->
  tbl = "#{tbl_base}_cat_tbl"
  result = Catalog.create_table(s, tbl, schema: "id INT, name STRING")
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 7. Catalog drop_table
# =============================================

run.("7. Catalog.drop_table with purge", fn ->
  tbl = "#{tbl_base}_drop"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  Catalog.drop_table(s, tbl, purge: true)
end)

# =============================================
# 8. Catalog cache_table with storage_level
# =============================================

run.("8a. Catalog.cache_table default", fn ->
  tbl = "#{tbl_base}_cache"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = Catalog.cache_table(s, tbl)
  Catalog.uncache_table(s, tbl)
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 9. Catalog.alter_table
# =============================================

run.("9. Catalog.alter_table set_properties", fn ->
  tbl = "#{tbl_base}_alter"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = Catalog.alter_table(s, tbl, set_properties: %{"comment" => "test table"})
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 10. Reader.schema with DDL string
# =============================================

run.("10. Reader with schema DDL string", fn ->
  tbl = "#{tbl_base}_rd_schema"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => 42}], schema: "id INT, val INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s)
  |> Reader.schema("id INT, val INT")
  |> Reader.table(tbl)
  |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 11. Column.name (alias for alias_)
# =============================================

run.("11. Column.name (alias for alias_)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([Column.name(col("id"), "my_id")]) |> DataFrame.collect()
end)

# =============================================
# 12. Column.transform (higher-order)
# =============================================

run.("12a. Column.transform on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3, 4, 5]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    SparkEx.Functions.transform(col("arr"), fn x -> Column.multiply(x, lit(10)) end) |> Column.alias_("multiplied")
  ]) |> DataFrame.collect()
end)

run.("12b. Column.transform on array (string operation)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"names" => ["alice", "bob", "carol"]}
  ], schema: "names ARRAY<STRING>")

  df |> DataFrame.select([
    SparkEx.Functions.transform(col("names"), fn x -> upper(x) end) |> Column.alias_("uppered")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Column.when_/otherwise chaining edge cases
# =============================================

run.("13a. when_ without otherwise (null default)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    when_(Column.eq(col("val"), lit(1)), lit("one")) |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

run.("13b. multiple when_ chains (4 conditions)", fn ->
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
# 14. config operations
# =============================================

run.("14a. config_set and config_get", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "50"}])
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
end)

run.("14b. config_get_with_default", fn ->
  SparkEx.config_get_with_default(s, [{"spark.nonexistent.key.#{run_id}", "fallback_value"}])
end)

run.("14c. config_get_option", fn ->
  SparkEx.config_get_option(s, ["spark.sql.shuffle.partitions", "spark.nonexistent.key.#{run_id}"])
end)

run.("14d. config_is_modifiable", fn ->
  SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions", "spark.master"])
end)

run.("14e. config_unset", fn ->
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "42"}])
  {:ok, before} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  SparkEx.config_unset(s, ["spark.sql.shuffle.partitions"])
  {:ok, after_} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  {before, after_}
end)

# =============================================
# 15. artifact operations
# =============================================

run.("15. artifact_status", fn ->
  SparkEx.artifact_status(s, ["nonexistent.jar"])
end)

# =============================================
# 16. Window frame edge cases
# =============================================

run.("16a. Window.rows_between with :unbounded", fn ->
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

run.("16b. Window.range_between with :unbounded", fn ->
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

run.("16c. Window with rows_between(-1, 1) sliding", fn ->
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

IO.puts("=== Writer V2, Session, Catalog DDL, Config, Window frame tests complete ===")
