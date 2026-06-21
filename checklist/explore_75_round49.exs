# Round 49: MergeIntoWriter, WriterV2 partitioned_by/table_property/overwrite_partitions,
# Catalog create_external_table/set_current_catalog/set_current_database,
# DataFrame observe/input_files/storage_level/is_local/is_streaming/with_columns_renamed,
# protobuf validation gap hunting
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, MergeIntoWriter}
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
# 1. MergeIntoWriter full lifecycle
# =============================================

run.("1a. Create target table for merge", fn ->
  {:ok, target} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 90},
    %{"id" => 2, "name" => "Bob", "score" => 80},
    %{"id" => 3, "name" => "Charlie", "score" => 70}
  ], schema: "id INT, name STRING, score INT")

  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"
  DataFrame.write_v2(target, table) |> SparkEx.WriterV2.create()
end)

run.("1b. Merge: update matched + insert not matched", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob_v2", "score" => 95},    # matched → update
    %{"id" => 4, "name" => "Diana", "score" => 88},      # not matched → insert
    %{"id" => 5, "name" => "Eve", "score" => 92}          # not matched → insert
  ], schema: "id INT, name STRING, score INT")

  DataFrame.merge_into(source, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_matched_update_all(nil)
  |> MergeIntoWriter.when_not_matched_insert_all(nil)
  |> MergeIntoWriter.merge()
end)

run.("1c. Read after merge (should have 5 rows, Bob updated)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"
  SparkEx.Reader.table(s, table)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("1d. Merge: conditional update + delete", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"

  {:ok, source2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice_del", "score" => 0},   # matched, score < 50 → delete
    %{"id" => 3, "name" => "Charlie_v2", "score" => 99},  # matched, score >= 50 → update
    %{"id" => 6, "name" => "Frank", "score" => 77}        # not matched → insert
  ], schema: "id INT, name STRING, score INT")

  DataFrame.merge_into(source2, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_matched_delete(Column.lt(col("source.score"), lit(50)))
  |> MergeIntoWriter.when_matched_update_all(Column.gte(col("source.score"), lit(50)))
  |> MergeIntoWriter.when_not_matched_insert_all(nil)
  |> MergeIntoWriter.merge()
end)

run.("1e. Read after conditional merge", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"
  SparkEx.Reader.table(s, table)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("1f. Merge: when_not_matched_by_source_delete", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"

  # Source only has id=2, so all others should be deleted from target
  {:ok, source3} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob_v3", "score" => 100}
  ], schema: "id INT, name STRING, score INT")

  DataFrame.merge_into(source3, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_matched_update_all(nil)
  |> MergeIntoWriter.when_not_matched_by_source_delete(nil)
  |> MergeIntoWriter.merge()
end)

run.("1g. Read after not_matched_by_source_delete (should have 1 row)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"
  SparkEx.Reader.table(s, table)
  |> DataFrame.collect()
end)

# Clean up merge table
run.("1h. Drop merge table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge_#{run_id}"
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE") |> DataFrame.collect()
end)

# =============================================
# 2. MergeIntoWriter edge cases
# =============================================

run.("2a. merge without on() (should raise ArgumentError)", fn ->
  {:ok, source} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.merge_into(source, "sfx.sfx_dev_warehouse.nonexistent")
  |> MergeIntoWriter.when_matched_update_all(nil)
  |> MergeIntoWriter.merge()
end)

run.("2b. merge with_schema_evolution", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_evo_#{run_id}"

  {:ok, target} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")
  DataFrame.write_v2(target, table) |> SparkEx.WriterV2.create()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "extra" => "new_col"}
  ], schema: "id INT, name STRING, extra STRING")

  result = DataFrame.merge_into(source, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_not_matched_insert_all(nil)
  |> MergeIntoWriter.with_schema_evolution()
  |> MergeIntoWriter.merge()

  # Read back
  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  %{merge: result, rows: rows}
end)

# =============================================
# 3. WriterV2 with partitioned_by
# =============================================

run.("3a. WriterV2 create with partitioned_by", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_part_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "category" => Enum.at(["A", "B", "C", "D"], rem(i, 4)), "val" => i * 10}
    end), schema: "id INT, category STRING, val INT")

  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.partitioned_by([col("category")])
  |> SparkEx.WriterV2.create()
end)

run.("3b. Read partitioned table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_part_#{run_id}"
  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()
  %{count: length(rows), sample: Enum.take(rows, 5)}
end)

run.("3c. WriterV2 overwrite_partitions", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_part_#{run_id}"

  # Overwrite just category A
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 100, "category" => "A", "val" => 999}
  ], schema: "id INT, category STRING, val INT")

  DataFrame.write_v2(df2, table)
  |> SparkEx.WriterV2.overwrite_partitions()
end)

run.("3d. Read after overwrite_partitions (A replaced, BCD intact)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_part_#{run_id}"
  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()
  %{count: length(rows), rows: rows}
end)

# Clean up
run.("3e. Drop partitioned table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_part_#{run_id}"
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE") |> DataFrame.collect()
end)

# =============================================
# 4. WriterV2 with table_property
# =============================================

run.("4. WriterV2 create with table_property", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_prop_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  result = DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.table_property("write.format.default", "parquet")
  |> SparkEx.WriterV2.table_property("comment", "test table from spark_ex")
  |> SparkEx.WriterV2.create()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  result
end)

# =============================================
# 5. WriterV2 with using (provider)
# =============================================

run.("5. WriterV2 create with using (iceberg)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_prov_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "test"}], schema: "id INT, val STRING")

  result = DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.using("iceberg")
  |> SparkEx.WriterV2.create()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  result
end)

# =============================================
# 6. WriterV2 with options
# =============================================

run.("6. WriterV2 create with options map", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_opts_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  result = DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.options(%{"write.metadata.compression-codec" => "gzip"})
  |> SparkEx.WriterV2.create()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  result
end)

# =============================================
# 7. Catalog.set_current_catalog / set_current_database
# =============================================

run.("7a. Catalog.current_catalog + set_current_catalog", fn ->
  orig = Catalog.current_catalog(s)
  set_result = Catalog.set_current_catalog(s, "sfx")
  after_set = Catalog.current_catalog(s)
  %{original: orig, set_result: set_result, after_set: after_set}
end)

run.("7b. Catalog.current_database + set_current_database", fn ->
  orig = Catalog.current_database(s)
  set_result = Catalog.set_current_database(s, "sfx_dev_warehouse")
  after_set = Catalog.current_database(s)
  %{original: orig, set_result: set_result, after_set: after_set}
end)

# =============================================
# 8. DataFrame analyze operations
# =============================================

run.("8a. DataFrame.is_local", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.is_local(df)
end)

run.("8b. DataFrame.is_streaming", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.is_streaming(df)
end)

run.("8c. DataFrame.input_files", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.input_files(df)
end)

run.("8d. DataFrame.storage_level (unpersisted)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.storage_level(df)
end)

run.("8e. DataFrame.storage_level (after persist)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  persisted = DataFrame.persist(df)
  level = DataFrame.storage_level(persisted)
  DataFrame.unpersist(persisted)
  level
end)

# =============================================
# 9. DataFrame.with_columns_renamed
# =============================================

run.("9a. with_columns_renamed map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2}], schema: "a INT, b INT")
  df |> DataFrame.with_columns_renamed(%{"a" => "x", "b" => "y"})
    |> DataFrame.collect()
end)

run.("9b. with_columns_renamed function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2}], schema: "a INT, b INT")
  df |> DataFrame.with_columns_renamed(fn name -> "col_#{name}" end)
    |> DataFrame.collect()
end)

# =============================================
# 10. DataFrame.observe
# =============================================

run.("10. DataFrame.observe with metrics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  observed = df
  |> DataFrame.observe("my_metrics", [
    count(col("val")) |> Column.alias_("cnt"),
    avg(col("val")) |> Column.alias_("avg_val")
  ])

  # Collect triggers the observation
  {:ok, rows} = DataFrame.collect(observed)
  %{row_count: length(rows)}
end)

# =============================================
# 11. Catalog.create_external_table
# =============================================

run.("11. Catalog.create_external_table (investigation)", fn ->
  # This likely needs an actual S3 path; test to see what error we get
  Catalog.create_external_table(s,
    "sfx.sfx_dev_warehouse.spark_ex_r49_ext_#{run_id}",
    "s3://nonexistent-bucket/nonexistent-path",
    source: "parquet",
    schema: "id INT, name STRING")
end)

# =============================================
# 12. Protobuf validation gap hunting
# =============================================

# 12a. WriterV2.partitioned_by with empty list (should raise locally)
run.("12a. partitioned_by with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, "dummy_table")
  |> SparkEx.WriterV2.partitioned_by([])
end)

# 12b. WriterV2.partitioned_by with integer (invalid type)
run.("12b. partitioned_by with integer (invalid type)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, "dummy_table")
  |> SparkEx.WriterV2.partitioned_by([123])
end)

# 12c. WriterV2.cluster_by with empty list
run.("12c. cluster_by with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, "dummy_table")
  |> SparkEx.WriterV2.cluster_by([])
end)

# 12d. WriterV2.table_property with non-string value
run.("12d. table_property with integer value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  writer = DataFrame.write_v2(df, "sfx.sfx_dev_warehouse.dummy_#{run_id}")
  |> SparkEx.WriterV2.table_property("key", 123)
  |> SparkEx.WriterV2.create()
end)

# 12e. WriterV2.option with non-string value
run.("12e. WriterV2.option with integer value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  writer = DataFrame.write_v2(df, "sfx.sfx_dev_warehouse.dummy2_#{run_id}")
  |> SparkEx.WriterV2.option("some_opt", 123)
  |> SparkEx.WriterV2.create()
end)

# 12f. Catalog.create_table with non-string schema (protobuf gap?)
run.("12f. Catalog.create_table with integer schema", fn ->
  Catalog.create_table(s, "sfx.sfx_dev_warehouse.dummy3_#{run_id}",
    schema: 12345)
end)

# 12g. Catalog.set_current_catalog with non-string (should have guard)
run.("12g. set_current_catalog with atom", fn ->
  Catalog.set_current_catalog(s, :sfx)
end)

# 12h. MergeIntoWriter.on with non-Column (protobuf gap?)
run.("12h. merge_into on() with string instead of Column", fn ->
  {:ok, source} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.merge_into(source, "sfx.sfx_dev_warehouse.nonexistent")
  |> MergeIntoWriter.on("id = id")
  |> MergeIntoWriter.when_matched_update_all(nil)
  |> MergeIntoWriter.merge()
end)

# =============================================
# 13. Complex: merge with when_matched_update (specific columns)
# =============================================

run.("13. merge with specific column update map", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge2_#{run_id}"

  {:ok, target} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 90},
    %{"id" => 2, "name" => "Bob", "score" => 80}
  ], schema: "id INT, name STRING, score INT")
  DataFrame.write_v2(target, table) |> SparkEx.WriterV2.create()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice_v2", "score" => 99}
  ], schema: "id INT, name STRING, score INT")

  # Only update score, not name
  result = DataFrame.merge_into(source, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_matched_update(%{"score" => col("source.score")}, nil)
  |> MergeIntoWriter.merge()

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  %{merge: result, rows: rows}
end)

# =============================================
# 14. Complex: merge with when_not_matched_insert (specific columns)
# =============================================

run.("14. merge with specific column insert map", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r49_merge3_#{run_id}"

  {:ok, target} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 90}
  ], schema: "id INT, name STRING, score INT")
  DataFrame.write_v2(target, table) |> SparkEx.WriterV2.create()

  {:ok, source} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob", "score" => 80}
  ], schema: "id INT, name STRING, score INT")

  result = DataFrame.merge_into(source, table)
  |> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
  |> MergeIntoWriter.when_not_matched_insert(
    %{"id" => col("source.id"), "name" => col("source.name"), "score" => lit(0)}, nil)
  |> MergeIntoWriter.merge()

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table} PURGE")
  %{merge: result, rows: rows}
end)

# =============================================
# 15. Catalog.refresh_table / recover_partitions
# =============================================

run.("15a. Catalog.refresh_table on existing table", fn ->
  Catalog.refresh_table(s, "sfx.sfx_dev_warehouse.instruments")
end)

run.("15b. Catalog.recover_partitions on existing table", fn ->
  Catalog.recover_partitions(s, "sfx.sfx_dev_warehouse.instruments")
end)

IO.puts("=== Round 49 complete ===")
