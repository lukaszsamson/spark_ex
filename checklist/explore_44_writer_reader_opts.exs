# Round 22: Writer, Reader, WriterV2, Column, Range, Catalog optional param edge cases
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
tbl_base = "sfx.sfx_dev_warehouse.spark_ex_r22_#{run_id}"

# =============================================
# 1. Writer builder methods
# =============================================

run.("1a. Writer with mode: overwrite", fn ->
  tbl = "#{tbl_base}_mode1"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  df2 |> DataFrame.write() |> Writer.mode("overwrite") |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1b. Writer with mode: append", fn ->
  tbl = "#{tbl_base}_mode2"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")
  df2 |> DataFrame.write() |> Writer.mode("append") |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1c. Writer with partition_by", fn ->
  tbl = "#{tbl_base}_part"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "grp" => "a"},
    %{"id" => 2, "grp" => "b"},
    %{"id" => 3, "grp" => "a"}
  ], schema: "id INT, grp STRING")

  df |> DataFrame.write() |> Writer.partition_by(["grp"]) |> Writer.save_as_table(tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1d. Writer with sort_by", fn ->
  tbl = "#{tbl_base}_sort"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 3}, %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")

  df |> DataFrame.write() |> Writer.sort_by(["id"]) |> Writer.save_as_table(tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1e. Writer with bucket_by", fn ->
  tbl = "#{tbl_base}_bucket"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "grp" => rem(i, 3)} end),
    schema: "id INT, grp INT")

  df |> DataFrame.write() |> Writer.bucket_by(4, ["id"]) |> Writer.save_as_table(tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1f. Writer with option/3", fn ->
  tbl = "#{tbl_base}_opt"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write()
  |> Writer.option("compression", "snappy")
  |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1g. Writer with options/2 (map)", fn ->
  tbl = "#{tbl_base}_opts"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write()
  |> Writer.options(%{"compression" => "snappy"})
  |> Writer.save_as_table(tbl)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("1h. Writer.insert_into with overwrite: true", fn ->
  tbl = "#{tbl_base}_ins_ow"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  df2 |> DataFrame.write() |> Writer.insert_into(tbl, overwrite: true)

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 2. WriterV2 operations
# =============================================

run.("2a. WriterV2 create", fn ->
  tbl = "#{tbl_base}_v2_create"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "test"}], schema: "id INT, val STRING")

  df |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.create()
  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2b. WriterV2 append", fn ->
  tbl = "#{tbl_base}_v2_append"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.append()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2c. WriterV2 overwrite (no condition)", fn ->
  tbl = "#{tbl_base}_v2_ow"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.overwrite(lit(true))

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2d. WriterV2 overwrite with condition", fn ->
  tbl = "#{tbl_base}_v2_owc"
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

run.("2e. WriterV2 replace", fn ->
  tbl = "#{tbl_base}_v2_repl"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df1 |> DataFrame.write() |> Writer.save_as_table(tbl)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1, "extra" => "new"}], schema: "id INT, extra STRING")
  df2 |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.replace()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2f. WriterV2 create_or_replace", fn ->
  tbl = "#{tbl_base}_v2_cor"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write_v2(tbl) |> WriterV2.using("iceberg") |> WriterV2.create_or_replace()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("2g. WriterV2 overwrite_partitions", fn ->
  tbl = "#{tbl_base}_v2_owp"
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

run.("2h. WriterV2 with table_property", fn ->
  tbl = "#{tbl_base}_v2_prop"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df |> DataFrame.write_v2(tbl)
  |> WriterV2.using("iceberg")
  |> WriterV2.table_property("write.format.default", "parquet")
  |> WriterV2.create()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 3. Reader options
# =============================================

run.("3a. Reader.table with session", fn ->
  tbl = "#{tbl_base}_rd_tbl"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 42, "val" => "test"}], schema: "id INT, val STRING")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s) |> Reader.table(tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3b. Reader with format and schema", fn ->
  tbl = "#{tbl_base}_rd_fmt"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s) |> Reader.format("iceberg") |> Reader.table(tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3c. Reader with option/3", fn ->
  tbl = "#{tbl_base}_rd_opt"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s) |> Reader.option("mergeSchema", "true") |> Reader.table(tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

run.("3d. Reader with options/2 (map)", fn ->
  tbl = "#{tbl_base}_rd_opts"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.write() |> Writer.save_as_table(tbl)

  result = SparkEx.read(s) |> Reader.options(%{"mergeSchema" => "true"}) |> Reader.table(tbl) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# =============================================
# 4. SparkEx.range arities
# =============================================

run.("4a. range(session, end) — 2 args", fn ->
  SparkEx.range(s, 5) |> DataFrame.collect()
end)

run.("4b. range(session, start, end) — 3 args", fn ->
  SparkEx.range(s, 10, 15) |> DataFrame.collect()
end)

run.("4c. range(session, start, end, step) — 4 args", fn ->
  SparkEx.range(s, 0, 20, 5) |> DataFrame.collect()
end)

run.("4d. range with num_partitions option", fn ->
  SparkEx.range(s, 0, 100, 1, num_partitions: 4) |> DataFrame.collect()
end)

run.("4e. range/2 with keyword opts (start, step, num_partitions)", fn ->
  SparkEx.range(s, 10, start: 5, step: 2) |> DataFrame.collect()
end)

# =============================================
# 5. SparkEx.sql with args
# =============================================

run.("5a. sql with positional args", fn ->
  SparkEx.sql(s, "SELECT ? + ? AS result", args: [lit(10), lit(20)]) |> DataFrame.collect()
end)

run.("5b. sql with named args", fn ->
  SparkEx.sql(s, "SELECT :a + :b AS result", args: %{"a" => lit(10), "b" => lit(20)}) |> DataFrame.collect()
end)

# =============================================
# 6. Column.alias_ with metadata
# =============================================

run.("6. Column.alias_ with metadata", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df
  |> DataFrame.select([
    Column.alias_(col("id"), "my_id", metadata: %{"comment" => "primary key"})
  ])
  |> DataFrame.collect()
end)

# =============================================
# 7. Column sort ordering variants
# =============================================

run.("7a. asc_nulls_first", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}
  ], schema: "val INT")

  df |> DataFrame.order_by([Column.asc_nulls_first(col("val"))]) |> DataFrame.collect()
end)

run.("7b. asc_nulls_last", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}
  ], schema: "val INT")

  df |> DataFrame.order_by([Column.asc_nulls_last(col("val"))]) |> DataFrame.collect()
end)

run.("7c. desc_nulls_first", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}
  ], schema: "val INT")

  df |> DataFrame.order_by([Column.desc_nulls_first(col("val"))]) |> DataFrame.collect()
end)

run.("7d. desc_nulls_last", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}
  ], schema: "val INT")

  df |> DataFrame.order_by([Column.desc_nulls_last(col("val"))]) |> DataFrame.collect()
end)

# =============================================
# 8. Column struct/array/map access
# =============================================

run.("8a. Column.get_item on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")

  df |> DataFrame.select([
    Column.get_item(col("arr"), 0) |> Column.alias_("first"),
    Column.get_item(col("arr"), 2) |> Column.alias_("third")
  ]) |> DataFrame.collect()
end)

run.("8b. Column.get_item on map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2}}
  ], schema: "m MAP<STRING, INT>")

  df |> DataFrame.select([
    Column.get_item(col("m"), "a") |> Column.alias_("val_a"),
    Column.get_item(col("m"), "b") |> Column.alias_("val_b")
  ]) |> DataFrame.collect()
end)

run.("8c. Column.get_field on struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "score" => 95}}
  ], schema: "info STRUCT<name: STRING, score: INT>")

  df |> DataFrame.select([
    Column.get_field(col("info"), "name") |> Column.alias_("name"),
    Column.get_field(col("info"), "score") |> Column.alias_("score")
  ]) |> DataFrame.collect()
end)

run.("8d. Column.with_field on struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "score" => 95}}
  ], schema: "info STRUCT<name: STRING, score: INT>")

  df |> DataFrame.select([
    Column.with_field(col("info"), "grade", lit("A")) |> Column.alias_("info_updated")
  ]) |> DataFrame.collect()
end)

run.("8e. Column.drop_fields on struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"info" => %{"name" => "Alice", "score" => 95, "age" => 25}}
  ], schema: "info STRUCT<name: STRING, score: INT, age: INT>")

  df |> DataFrame.select([
    Column.drop_fields(col("info"), ["age"]) |> Column.alias_("info_trimmed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. GroupedData with column list variants
# =============================================

run.("9a. GroupedData.min with specific columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "x" => 10, "y" => 100},
    %{"grp" => "a", "x" => 20, "y" => 200},
    %{"grp" => "b", "x" => 30, "y" => 300}
  ], schema: "grp STRING, x INT, y INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.min(["x"])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

run.("9b. GroupedData.max with specific columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "x" => 10, "y" => 100},
    %{"grp" => "a", "x" => 20, "y" => 200},
    %{"grp" => "b", "x" => 30, "y" => 300}
  ], schema: "grp STRING, x INT, y INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.max(["y"])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

run.("9c. GroupedData.sum with specific columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "x" => 10, "y" => 100},
    %{"grp" => "a", "x" => 20, "y" => 200}
  ], schema: "grp STRING, x INT, y INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.sum(["x", "y"])
  |> DataFrame.collect()
end)

run.("9d. GroupedData.avg with specific columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "x" => 10, "y" => 100},
    %{"grp" => "a", "x" => 20, "y" => 200}
  ], schema: "grp STRING, x INT, y INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.avg(["x"])
  |> DataFrame.collect()
end)

run.("9e. GroupedData.mean (alias for avg)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 10},
    %{"grp" => "a", "val" => 20}
  ], schema: "grp STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.mean(["val"])
  |> DataFrame.collect()
end)

run.("9f. GroupedData.agg with map (column name => agg function)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 10},
    %{"grp" => "a", "val" => 20},
    %{"grp" => "b", "val" => 30}
  ], schema: "grp STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> SparkEx.GroupedData.agg(%{"val" => "sum"})
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. Catalog operations with optional params
# =============================================

run.("10a. Catalog.list_databases with pattern", fn ->
  Catalog.list_databases(s, "sfx*")
end)

run.("10b. Catalog.list_tables with db_name", fn ->
  {:ok, tables} = Catalog.list_tables(s, "sfx_dev_warehouse")
  {:ok, tables |> Enum.take(3) |> Enum.map(& &1.name)}
end)

run.("10c. Catalog.list_tables with db_name and pattern", fn ->
  Catalog.list_tables(s, "sfx_dev_warehouse", "spark_ex*")
end)

run.("10d. Catalog.table_exists? with db_name", fn ->
  Catalog.table_exists?(s, "nonexistent_table_#{run_id}", "sfx_dev_warehouse")
end)

run.("10e. Catalog.list_functions with db_name", fn ->
  {:ok, fns} = Catalog.list_functions(s, nil)
  {:ok, fns |> Enum.take(3) |> Enum.map(& &1.name)}
end)

run.("10f. Catalog.list_catalogs with pattern", fn ->
  Catalog.list_catalogs(s, "sfx*")
end)

run.("10g. Catalog.function_exists? with db_name", fn ->
  Catalog.function_exists?(s, "concat", "sfx_dev_warehouse")
end)

# =============================================
# 11. Column.isin with various value types
# =============================================

run.("11a. Column.isin with integer list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 4}, %{"id" => 5}
  ], schema: "id INT")

  df |> DataFrame.filter(Column.isin(col("id"), [1, 3, 5])) |> DataFrame.collect()
end)

run.("11b. Column.isin with string list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Carol"}
  ], schema: "name STRING")

  df |> DataFrame.filter(Column.isin(col("name"), ["Alice", "Carol"])) |> DataFrame.collect()
end)

run.("11c. Column.isin with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")

  df |> DataFrame.filter(Column.isin(col("id"), [])) |> DataFrame.collect()
end)

# =============================================
# 12. Column.between
# =============================================

run.("12a. Column.between with integers", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")

  df |> DataFrame.filter(Column.between(col("val"), 3, 7)) |> DataFrame.collect()
end)

run.("12b. Column.between with Column bounds", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 5, "lo" => 3, "hi" => 7},
    %{"val" => 1, "lo" => 3, "hi" => 7},
    %{"val" => 10, "lo" => 3, "hi" => 7}
  ], schema: "val INT, lo INT, hi INT")

  df |> DataFrame.filter(Column.between(col("val"), col("lo"), col("hi"))) |> DataFrame.collect()
end)

# =============================================
# 13. config_get_all with prefix
# =============================================

run.("13a. config_get_all no prefix", fn ->
  {:ok, all} = SparkEx.config_get_all(s)
  {:ok, all |> Enum.take(5)}
end)

run.("13b. config_get_all with prefix", fn ->
  SparkEx.config_get_all(s, "spark.sql")
end)

# =============================================
# 14. Column.substr variants
# =============================================

run.("14a. Column.substr with integer pos and len", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}
  ], schema: "text STRING")

  df |> DataFrame.select([
    Column.substr(col("text"), 1, 5) |> Column.alias_("sub")
  ]) |> DataFrame.collect()
end)

run.("14b. Column.substr with Column pos", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World", "start" => 7}
  ], schema: "text STRING, start INT")

  df |> DataFrame.select([
    Column.substr(col("text"), col("start"), 5) |> Column.alias_("sub")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Column.cast and try_cast variations
# =============================================

run.("15a. try_cast INT to STRING", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([Column.try_cast(col("val"), "STRING") |> Column.alias_("s")]) |> DataFrame.collect()
end)

run.("15b. try_cast invalid STRING to INT (should return null)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => "not_a_number"}], schema: "val STRING")
  df |> DataFrame.select([Column.try_cast(col("val"), "INT") |> Column.alias_("i")]) |> DataFrame.collect()
end)

run.("15c. Column.astype (alias for cast)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([Column.astype(col("val"), "STRING") |> Column.alias_("s")]) |> DataFrame.collect()
end)

# =============================================
# 16. SparkEx.clone
# =============================================

run.("16. SparkEx session clone", fn ->
  {:ok, s2} = SparkEx.clone_session(s)
  result = SparkEx.sql(s2, "SELECT 1 AS test") |> DataFrame.collect()
  SparkEx.Session.release(s2)
  result
end)

IO.puts("=== Writer, Reader, Column, Range, Catalog optional param tests complete ===")
