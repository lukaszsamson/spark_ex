# Round 56: Writer v1, Session.clone, TableValuedFunction, StreamingQuery introspection,
# multiple observations, UDFRegistration, interrupt, last_execution_metrics
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
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
# 1. Writer v1 API
# =============================================

run.("1a. Writer v1 save_as_table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_v1_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{write: result, rows: rows}
end)

run.("1b. Writer v1 with partition_by", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_v1p_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "grp" => Enum.at(["A", "B"], rem(i, 2)), "val" => i * 10}
    end), schema: "id INT, grp STRING, val INT")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.partition_by(["grp"])
    |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{write: result, row_count: length(rows)}
end)

run.("1c. Writer v1 insert_into", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_ins_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}, %{"id" => 3}], schema: "id INT")
  result = DataFrame.write(df2) |> SparkEx.Writer.insert_into(table)

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{insert: result, rows: rows}
end)

run.("1d. Writer v1 insert_into with overwrite", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_ow_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  result = DataFrame.write(df2) |> SparkEx.Writer.insert_into(table, overwrite: true)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{insert: result, rows: rows}
end)

run.("1e. Writer v1 with options", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_opts_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.option("write.metadata.compression-codec", "gzip")
    |> SparkEx.Writer.save_as_table(table)

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  result
end)

run.("1f. Writer v1 mode as string", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_mstr_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode("overwrite")
    |> SparkEx.Writer.save_as_table(table)

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  result
end)

# =============================================
# 2. Session.clone
# =============================================

run.("2a. Session.clone basic", fn ->
  {:ok, s2} = SparkEx.Session.clone(s)
  {:ok, r1} = SparkEx.sql(s, "SELECT 1 AS val") |> DataFrame.collect()
  {:ok, r2} = SparkEx.sql(s2, "SELECT 2 AS val") |> DataFrame.collect()
  SparkEx.Session.stop(s2)
  %{session1: r1, session2: r2}
end)

run.("2b. Session.clone config isolation", fn ->
  {:ok, s2} = SparkEx.Session.clone(s)
  # Change config in clone, verify original unaffected
  SparkEx.config_set(s2, [{"spark.sql.shuffle.partitions", "7"}])
  {:ok, [{_, clone_val}]} = SparkEx.config_get(s2, ["spark.sql.shuffle.partitions"])
  {:ok, [{_, orig_val}]} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  SparkEx.Session.stop(s2)
  %{clone_partitions: clone_val, original_partitions: orig_val}
end)

# =============================================
# 3. TableValuedFunction
# =============================================

run.("3a. TableValuedFunction.call range", fn ->
  SparkEx.TableValuedFunction.call(s, "range", [lit(10)])
  |> DataFrame.collect()
end)

run.("3b. TableValuedFunction.call range with start/end/step", fn ->
  SparkEx.TableValuedFunction.call(s, "range", [lit(0), lit(100), lit(10)])
  |> DataFrame.collect()
end)

run.("3c. TableValuedFunction.explode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, 2, 3]}
  ], schema: "arr ARRAY<INT>")
  # explode might need a subquery approach
  SparkEx.sql(s, "SELECT explode(array(1,2,3)) AS val") |> DataFrame.collect()
end)

# =============================================
# 4. Multiple observations on same DataFrame
# =============================================

run.("4. Two observations on same DataFrame", fn ->
  obs1 = SparkEx.Observation.new("obs1_#{run_id}")
  obs2 = SparkEx.Observation.new("obs2_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  {:ok, rows} = df
  |> DataFrame.observe(obs1, [count(col("val")) |> Column.alias_("cnt")])
  |> DataFrame.observe(obs2, [sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()

  m1 = SparkEx.Observation.get(obs1)
  m2 = SparkEx.Observation.get(obs2)
  %{obs1: m1, obs2: m2, rows: length(rows)}
end)

# =============================================
# 5. Session.interrupt_all + last_execution_metrics
# =============================================

run.("5a. interrupt_all (no running)", fn ->
  SparkEx.Session.interrupt_all(s)
end)

run.("5b. last_execution_metrics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, _} = DataFrame.collect(df)
  SparkEx.Session.last_execution_metrics(s)
end)

# =============================================
# 6. StreamingQuery introspection
# =============================================

run.("6. StreamingQuery: status, progress, explain", fn ->
  alias SparkEx.{StreamReader, StreamWriter, StreamingQuery}
  stream = StreamReader.rate(s, rows_per_second: 5)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("sq_intro_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)

  active = StreamingQuery.is_active?(query)
  status = StreamingQuery.status(query)
  progress = StreamingQuery.recent_progress(query)
  last = StreamingQuery.last_progress(query)
  name = StreamingQuery.name(query)
  explain = StreamingQuery.explain(query)

  StreamingQuery.stop(query)

  %{active: active, status: status, name: name, explain: explain,
    progress_count: (case progress do {:ok, list} -> length(list); _ -> progress end),
    last_progress: last}
end)

# =============================================
# 7. UDFRegistration (register_java without return_type)
# =============================================

run.("7a. register_java", fn ->
  SparkEx.UDFRegistration.register_java(s, "spark_ex_test_udf_#{run_id}",
    "org.apache.spark.sql.catalyst.expressions.Upper")
end)

run.("7b. use registered UDF via SQL", fn ->
  SparkEx.sql(s, "SELECT spark_ex_test_udf_#{run_id}('hello') AS result")
    |> DataFrame.collect()
end)

# =============================================
# 8. Complex: Writer v1 → read back → Writer v1 (chain)
# =============================================

run.("8. Writer v1 chain: write → read → transform → write again", fn ->
  t1 = "sfx.sfx_dev_warehouse.spark_ex_r56_chain1_#{run_id}"
  t2 = "sfx.sfx_dev_warehouse.spark_ex_r56_chain2_#{run_id}"

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  DataFrame.write(df) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(t1)

  # Read back, transform, write to second table
  SparkEx.Reader.table(s, t1)
  |> DataFrame.filter(Column.gt(col("val"), lit(200)))
  |> DataFrame.with_column("doubled", Column.multiply(col("val"), lit(2)))
  |> DataFrame.write()
  |> SparkEx.Writer.mode(:overwrite)
  |> SparkEx.Writer.save_as_table(t2)

  {:ok, rows} = SparkEx.Reader.table(s, t2) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()

  Catalog.drop_table(s, t1, if_exists: true, purge: true)
  Catalog.drop_table(s, t2, if_exists: true, purge: true)
  %{final_row_count: length(rows), sample: Enum.take(rows, 3)}
end)

# =============================================
# 9. Complex: tags + config + observation in single pipeline
# =============================================

run.("9. Tags + config + observation combined", fn ->
  SparkEx.add_tag(s, "etl_r56_#{run_id}")
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "3"}])

  obs = SparkEx.Observation.new("combo_obs_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)), "amount" => i * 1.0}
    end), schema: "dept STRING, amount DOUBLE")

  {:ok, rows} = df
  |> DataFrame.observe(obs, [
    count(col("amount")) |> Column.alias_("cnt"),
    sum(col("amount")) |> Column.alias_("total")
  ])
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([sum(col("amount")) |> Column.alias_("dept_total")])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()

  metrics = SparkEx.Observation.get(obs)
  tags = SparkEx.get_tags(s)

  SparkEx.clear_tags(s)
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "200"}])

  %{rows: rows, observation: metrics, tags_during: tags}
end)

# =============================================
# 10. Edge: Writer v1 mode atoms
# =============================================

run.("10a. Writer v1 mode :append", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_append_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df1) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(table)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}], schema: "id INT")
  DataFrame.write(df2) |> SparkEx.Writer.mode(:append) |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{rows: rows}
end)

run.("10b. Writer v1 mode :ignore", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_ign_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df1) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(table)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  DataFrame.write(df2) |> SparkEx.Writer.mode(:ignore) |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{rows: rows, note: "should still have id=1, not id=99"}
end)

run.("10c. Writer v1 mode :error_if_exists", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_err_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df1) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(table)

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")
  result = try do
    DataFrame.write(df2) |> SparkEx.Writer.mode(:error_if_exists) |> SparkEx.Writer.save_as_table(table)
  rescue
    e -> {:error, Exception.message(e)}
  end

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{second_write: result, note: "should error since table exists"}
end)

# =============================================
# 11. Edge: Writer v1 sort_by / bucket_by
# =============================================

run.("11. Writer v1 sort_by + bucket_by", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_bucket_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i, "grp" => rem(i, 5)} end),
    schema: "id INT, grp INT")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.sort_by(["id"])
    |> SparkEx.Writer.bucket_by(4, ["grp"])
    |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{result: result, row_count: length(rows)}
end)

# =============================================
# 12. Edge: DataFrame.write without mode (default)
# =============================================

run.("12. Writer v1 default mode (no mode set)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_defm_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  # First write should succeed (table doesn't exist)
  result1 = DataFrame.write(df) |> SparkEx.Writer.save_as_table(table)

  # Second write should error (default mode is error_if_exists)
  result2 = try do
    DataFrame.write(df) |> SparkEx.Writer.save_as_table(table)
  rescue
    e -> {:error, Exception.message(e)}
  end

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{first: result1, second: result2}
end)

# =============================================
# 13. Complex: DataFrame.checkpoint / local_checkpoint
# =============================================

run.("13a. DataFrame.checkpoint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end), schema: "id INT")
  checkpointed = DataFrame.checkpoint(df)
  {:ok, rows} = DataFrame.collect(checkpointed)
  length(rows)
end)

run.("13b. DataFrame.local_checkpoint", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end), schema: "id INT")
  checkpointed = DataFrame.local_checkpoint(df)
  {:ok, rows} = DataFrame.collect(checkpointed)
  length(rows)
end)

# =============================================
# 14. Edge: create_dataframe with empty rows but schema
# =============================================

run.("14. create_dataframe empty + Writer v1", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r56_empty_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, name STRING")
  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.save_as_table(table)
  {:ok, cnt} = SparkEx.Reader.table(s, table) |> DataFrame.count()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{write: result, count: cnt}
end)

IO.puts("=== Round 56 complete ===")
