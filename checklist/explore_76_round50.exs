# Round 50: Catalog alter_database/alter_table/create_function/drop_function,
# DataFrame foreach/foreach_partition/col_regex, Stat functions, NA functions,
# WriterV2 cluster_by, Observation with get, creative edge cases
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
# 1. Catalog.create_database / alter_database / drop_database
# =============================================

run.("1a. create_database", fn ->
  Catalog.create_database(s, "sfx.spark_ex_test_db_#{run_id}",
    if_not_exists: true, comment: "test db from spark_ex round 50")
end)

run.("1b. database_exists? after create", fn ->
  Catalog.database_exists?(s, "sfx.spark_ex_test_db_#{run_id}")
end)

run.("1c. get_database after create", fn ->
  Catalog.get_database(s, "spark_ex_test_db_#{run_id}")
end)

run.("1d. alter_database set_properties", fn ->
  Catalog.alter_database(s, "sfx.spark_ex_test_db_#{run_id}",
    set_properties: %{"spark_ex_test" => "true"})
end)

run.("1e. drop_database", fn ->
  Catalog.drop_database(s, "sfx.spark_ex_test_db_#{run_id}",
    if_exists: true, cascade: true)
end)

run.("1f. database_exists? after drop", fn ->
  Catalog.database_exists?(s, "sfx.spark_ex_test_db_#{run_id}")
end)

# =============================================
# 2. Catalog.drop_table with opts
# =============================================

run.("2. drop_table with if_exists and purge opts", fn ->
  Catalog.drop_table(s, "sfx.sfx_dev_warehouse.nonexistent_table_#{run_id}",
    if_exists: true, purge: true)
end)

# =============================================
# 3. Catalog.alter_table
# =============================================

run.("3a. create table for alter", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r50_alter_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, table) |> SparkEx.WriterV2.create()
end)

run.("3b. alter_table set_properties", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r50_alter_#{run_id}"
  Catalog.alter_table(s, table, set_properties: %{"comment" => "altered by spark_ex"})
end)

run.("3c. alter_table rename_to", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r50_alter_#{run_id}"
  new_name = "sfx.sfx_dev_warehouse.spark_ex_r50_renamed_#{run_id}"
  Catalog.alter_table(s, table, rename_to: new_name)
end)

run.("3d. read renamed table", fn ->
  new_name = "sfx.sfx_dev_warehouse.spark_ex_r50_renamed_#{run_id}"
  SparkEx.Reader.table(s, new_name) |> DataFrame.collect()
end)

run.("3e. drop renamed table", fn ->
  new_name = "sfx.sfx_dev_warehouse.spark_ex_r50_renamed_#{run_id}"
  Catalog.drop_table(s, new_name, if_exists: true, purge: true)
end)

# =============================================
# 4. DataFrame.foreach / foreach_partition
# =============================================

run.("4a. foreach iterates all rows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  counter = :counters.new(1, [:atomics])
  result = DataFrame.foreach_local(df, fn _row -> :counters.add(counter, 1, 1) end)
  %{result: result, iterations: :counters.get(counter, 1)}
end)

run.("4b. foreach_partition receives all rows as list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  ref = make_ref()
  result = DataFrame.foreach_partition_local(df, fn rows ->
    send(self(), {ref, length(rows)})
  end)
  count = receive do {^ref, c} -> c after 5000 -> :timeout end
  %{result: result, row_count: count}
end)

# =============================================
# 5. DataFrame.col_regex
# =============================================

run.("5. col_regex to select columns by pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name_first" => "Alice", "name_last" => "Smith", "age" => 30, "score" => 95}
  ], schema: "name_first STRING, name_last STRING, age INT, score INT")
  # Select all columns matching "name_*"
  df |> DataFrame.select([DataFrame.col_regex(df, "`name_.*`")])
    |> DataFrame.collect()
end)

# =============================================
# 6. Stat functions
# =============================================

run.("6a. corr (Pearson correlation)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => (i * 2 + rem(i, 7)) * 1.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.corr(df, "x", "y")
end)

run.("6b. cov (covariance)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => (i * 2 + rem(i, 7)) * 1.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.cov(df, "x", "y")
end)

run.("6c. crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "level" => "senior"}, %{"dept" => "Eng", "level" => "junior"},
    %{"dept" => "Eng", "level" => "senior"}, %{"dept" => "Sales", "level" => "senior"},
    %{"dept" => "Sales", "level" => "junior"}, %{"dept" => "Ops", "level" => "junior"}
  ], schema: "dept STRING, level STRING")
  DataFrame.crosstab(df, "dept", "level") |> DataFrame.collect()
end)

run.("6d. freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"color" => Enum.at(["red", "red", "red", "blue", "green"], rem(i, 5))}
    end), schema: "color STRING")
  DataFrame.freq_items(df, ["color"], 0.3) |> DataFrame.collect()
end)

run.("6e. sample_by (stratified sampling)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"grp" => Enum.at(["A", "B", "C"], rem(i, 3)), "val" => i}
    end), schema: "grp STRING, val INT")
  sampled = DataFrame.sample_by(df, col("grp"), %{"A" => 0.5, "B" => 0.1, "C" => 0.9})
  {:ok, rows} = DataFrame.collect(sampled)
  # Count by group
  counts = rows |> Enum.group_by(& &1["grp"]) |> Enum.map(fn {k, v} -> {k, length(v)} end) |> Map.new()
  %{total: length(rows), counts: counts}
end)

# =============================================
# 7. NA functions
# =============================================

run.("7a. NA.fill with scalar", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}, %{"id" => 2, "val" => nil}, %{"id" => 3, "val" => nil}
  ], schema: "id INT, val INT")
  DataFrame.NA.fill(df, 0) |> DataFrame.collect()
end)

run.("7b. NA.fill with per-column map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => nil, "score" => nil}, %{"name" => "Bob", "score" => 80}
  ], schema: "name STRING, score INT")
  DataFrame.NA.fill(df, %{"name" => "Unknown", "score" => 0}) |> DataFrame.collect()
end)

run.("7c. NA.drop with how: :any", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 3}, %{"a" => 4, "b" => nil}, %{"a" => 5, "b" => 6}
  ], schema: "a INT, b INT")
  DataFrame.NA.drop(df, how: :any) |> DataFrame.collect()
end)

run.("7d. NA.drop with how: :all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil}, %{"a" => nil, "b" => 3}, %{"a" => 4, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.NA.drop(df, how: :all) |> DataFrame.collect()
end)

run.("7e. NA.drop with thresh", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => nil},
    %{"a" => 1, "b" => 2, "c" => nil},
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  DataFrame.NA.drop(df, thresh: 2) |> DataFrame.collect()
end)

run.("7f. NA.replace", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 1}
  ], schema: "val INT")
  DataFrame.NA.replace(df, %{1 => 100, 2 => 200}) |> DataFrame.collect()
end)

# =============================================
# 8. Observation with get
# =============================================

run.("8. Observation: new → observe → collect → get", fn ->
  obs = SparkEx.Observation.new("r50_obs_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  observed = df
  |> DataFrame.observe(obs, [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total")
  ])

  {:ok, _rows} = DataFrame.collect(observed)

  # Now get the observed metrics
  metrics = SparkEx.Observation.get(obs)
  metrics
end)

# =============================================
# 9. WriterV2.cluster_by
# =============================================

run.("9. WriterV2 create with cluster_by", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r50_cluster_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "region" => Enum.at(["US", "EU", "APAC"], rem(i, 3)), "val" => i * 10}
    end), schema: "id INT, region STRING, val INT")

  result = DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.cluster_by(["region"])
  |> SparkEx.WriterV2.create()

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{create: result, row_count: length(rows)}
end)

# =============================================
# 10. Complex: approx_quantile (Stat)
# =============================================

run.("10. approx_quantile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  SparkEx.DataFrame.Stat.approx_quantile(df, "val", [0.25, 0.5, 0.75, 0.9, 0.99], 0.01)
end)

# =============================================
# 11. Complex: chained NA → Stat → Window pipeline
# =============================================

run.("11. NA fill → corr → window ranking", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"student" => "A", "math" => 90, "science" => nil},
    %{"student" => "B", "math" => nil, "science" => 85},
    %{"student" => "C", "math" => 75, "science" => 80},
    %{"student" => "D", "math" => 88, "science" => 92},
    %{"student" => "E", "math" => 65, "science" => 70},
    %{"student" => "F", "math" => 95, "science" => nil},
    %{"student" => "G", "math" => nil, "science" => 88}
  ], schema: "student STRING, math INT, science INT")

  # Step 1: fill nulls with 50
  filled = DataFrame.NA.fill(df, %{"math" => 50, "science" => 50})

  # Step 2: add total score and window rank
  w = SparkEx.Window.order_by([desc(col("total"))])
  result = filled
    |> DataFrame.with_column("total", Column.plus(col("math"), col("science")))
    |> DataFrame.with_column("rank", dense_rank() |> Column.over(w))
    |> DataFrame.sort([asc(col("rank"))])
    |> DataFrame.collect()

  result
end)

# =============================================
# 12. Complex: Catalog.create_function / drop_function
# =============================================

run.("12a. create_function (temporary, built-in class)", fn ->
  # Create a simple TEMPORARY function wrapping a Java class
  Catalog.create_function(s, "spark_ex_test_fn_#{run_id}",
    "org.apache.spark.sql.catalyst.expressions.Upper",
    temporary: true)
end)

run.("12b. function_exists?", fn ->
  Catalog.function_exists?(s, "spark_ex_test_fn_#{run_id}")
end)

run.("12c. drop_function", fn ->
  Catalog.drop_function(s, "spark_ex_test_fn_#{run_id}",
    temporary: true, if_exists: true)
end)

# =============================================
# 13. Complex: random_split
# =============================================

run.("13. random_split into 3 parts", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")

  {:ok, splits} = DataFrame.random_split(df, [0.5, 0.3, 0.2], seed: 42)

  counts = Enum.map(splits, fn split_df ->
    {:ok, rows} = DataFrame.collect(split_df)
    length(rows)
  end)

  %{split_count: length(splits), row_counts: counts, total: Enum.sum(counts)}
end)

# =============================================
# 14. Complex: multi-level catalog navigation
# =============================================

run.("14. catalog navigation: list_catalogs → set → list_databases → list_tables", fn ->
  {:ok, catalogs} = Catalog.list_catalogs(s)
  {:ok, current_cat} = Catalog.current_catalog(s)
  {:ok, dbs} = Catalog.list_databases(s)
  Catalog.set_current_database(s, "sfx_dev_warehouse")
  {:ok, tables} = Catalog.list_tables(s)

  %{
    catalogs: catalogs,
    current_catalog: current_cat,
    databases: Enum.take(dbs, 5),
    tables_in_warehouse: Enum.take(tables, 5)
  }
end)

# =============================================
# 15. Edge: alter_table with invalid opts
# =============================================

run.("15a. alter_table with no rename_to or set_properties (should raise)", fn ->
  Catalog.alter_table(s, "dummy", [])
end)

run.("15b. alter_table with both rename_to and set_properties (should raise)", fn ->
  Catalog.alter_table(s, "dummy",
    rename_to: "new_name", set_properties: %{"key" => "val"})
end)

run.("15c. alter_database with no opts (should raise)", fn ->
  Catalog.alter_database(s, "dummy", [])
end)

IO.puts("=== Round 50 complete ===")
