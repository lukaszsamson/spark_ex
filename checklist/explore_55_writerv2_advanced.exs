# Round 33: WriterV2 proper API, like_ from Functions, TVF patterns, advanced combos
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, WriterV2}
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
# 1. like_ / ilike_ from Functions module (not Column)
# =============================================

run.("1a. like_ with pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "ALICE"},
    %{"name" => "alice_99"}, %{"name" => "alice%special"}
  ], schema: "name STRING")
  df |> DataFrame.filter(like_(col("name"), lit("Ali%")))
  |> DataFrame.collect()
end)

run.("1b. ilike_ (case-insensitive)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "BOB"}, %{"name" => "ALICE"}
  ], schema: "name STRING")
  df |> DataFrame.filter(ilike_(col("name"), lit("ali%")))
  |> DataFrame.collect()
end)

run.("1c. like_ with escape character", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "50% off"}, %{"name" => "50 items"}, %{"name" => "50%special"}
  ], schema: "name STRING")
  # Match strings containing literal %, using ! as escape
  df |> DataFrame.filter(like_(col("name"), lit("50!%%"), lit("!")))
  |> DataFrame.collect()
end)

# =============================================
# 2. WriterV2 with proper builder pattern
# =============================================

run.("2a. WriterV2.create with builder", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_create_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  result = df
  |> DataFrame.write_v2(table_name)
  |> WriterV2.create()

  read_back = SparkEx.Reader.table(s, table_name) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  {result, read_back}
end)

run.("2b. WriterV2.append", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_append_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2}, %{"id" => 3}], schema: "id INT")

  df1 |> DataFrame.write_v2(table_name) |> WriterV2.create()
  df2 |> DataFrame.write_v2(table_name) |> WriterV2.append()

  result = SparkEx.Reader.table(s, table_name) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  result
end)

run.("2c. WriterV2.overwrite", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_overwrite_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}, %{"id" => 2}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 99}], schema: "id INT")

  df1 |> DataFrame.write_v2(table_name) |> WriterV2.create()
  df2 |> DataFrame.write_v2(table_name) |> WriterV2.overwrite(lit(true))

  result = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  result
end)

run.("2d. WriterV2.replace", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_replace_#{run_id}"
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 77, "extra" => "new_col"}], schema: "id INT, extra STRING")

  df1 |> DataFrame.write_v2(table_name) |> WriterV2.create()
  df2 |> DataFrame.write_v2(table_name) |> WriterV2.replace()

  result = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  result
end)

run.("2e. WriterV2.create_or_replace", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_cor_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "first"}], schema: "id INT, val STRING")

  df |> DataFrame.write_v2(table_name) |> WriterV2.create_or_replace()

  result1 = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()

  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2, "val" => "replaced"}], schema: "id INT, val STRING")
  df2 |> DataFrame.write_v2(table_name) |> WriterV2.create_or_replace()
  result2 = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  {result1, result2}
end)

run.("2f. WriterV2 with table_property and partitioned_by", fn ->
  table_name = "sfx.sfx_dev_warehouse.test_wv2_opts_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"date" => "2024-01-01", "val" => 10},
    %{"date" => "2024-01-02", "val" => 20}
  ], schema: "date STRING, val INT")

  result = df
  |> DataFrame.write_v2(table_name)
  |> WriterV2.table_property("description", "Test table with partitioning")
  |> WriterV2.create()

  read_back = SparkEx.Reader.table(s, table_name) |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{table_name} PURGE")
  {result, read_back}
end)

# =============================================
# 3. Global temp views
# =============================================

run.("3a. create and read global temp view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "global"}], schema: "id INT, val STRING")
  DataFrame.create_or_replace_global_temp_view(df, "gtest_#{run_id}")

  result = SparkEx.Reader.table(s, "global_temp.gtest_#{run_id}") |> DataFrame.collect()
  Catalog.drop_global_temp_view(s, "gtest_#{run_id}")
  result
end)

# =============================================
# 4. DataFrame.repartition + coalesce patterns
# =============================================

run.("4a. repartition by column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"grp" => rem(i, 5), "val" => i} end),
    schema: "grp INT, val INT")
  repartitioned = DataFrame.repartition(df, [col("grp")])
  # Check number of partitions via plan
  {:ok, num} = DataFrame.rdd_num_partitions_approx(repartitioned)
  num
end)

run.("4b. coalesce to 1 partition then count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end), schema: "id INT")
  coalesced = DataFrame.coalesce(df, 1)
  {:ok, num} = DataFrame.rdd_num_partitions_approx(coalesced)
  count = DataFrame.count(coalesced)
  {num, count}
end)

# =============================================
# 5. DataFrame.random_split
# =============================================

run.("5. random_split", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  splits = DataFrame.random_split(df, [0.7, 0.3], 42)
  case splits do
    {:ok, [s1, s2]} ->
      c1 = DataFrame.count(s1)
      c2 = DataFrame.count(s2)
      {c1, c2}
    other -> other
  end
end)

# =============================================
# 6. Complex pipeline: window + conditional + aggregation
# =============================================

run.("6. rolling z-score pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"ts" => i, "val" => :rand.uniform(100) * 1.0}
    end), schema: "ts INT, val DOUBLE")

  # 5-row rolling window
  w_rolling = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("ts"))])
    |> WindowSpec.rows_between(-4, 0)

  # Add rolling stats
  enriched = df
  |> DataFrame.with_column("rolling_avg", avg(col("val")) |> Column.over(w_rolling))
  |> DataFrame.with_column("rolling_std", stddev(col("val")) |> Column.over(w_rolling))
  |> DataFrame.with_column("z_score",
    when_(Column.gt(col("rolling_std"), lit(0.0)),
      Column.divide(Column.minus(col("val"), col("rolling_avg")), col("rolling_std")))
    |> Column.otherwise(lit(0.0)))

  # Find anomalies (|z| > 1.5)
  anomalies = enriched
  |> DataFrame.filter(Column.gt(SparkEx.Functions.abs(col("z_score")), lit(1.5)))
  |> DataFrame.select([col("ts"), col("val"), col("z_score")])
  |> DataFrame.collect()

  {:ok, rows} = anomalies
  IO.puts("  Found #{length(rows)} anomalies")
  Enum.take(rows, 3)
end)

# =============================================
# 7. Self-join with different aliases
# =============================================

run.("7. self-join for pair comparisons", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}, %{"id" => 2, "val" => 20},
    %{"id" => 3, "val" => 30}
  ], schema: "id INT, val INT")

  left = DataFrame.alias_(df, "a")
  right = DataFrame.alias_(df, "b")

  DataFrame.join(left, right,
    Column.lt(col("a.id"), col("b.id")), "inner")
  |> DataFrame.select([
    col("a.id") |> Column.alias_("id1"),
    col("b.id") |> Column.alias_("id2"),
    Column.minus(col("b.val"), col("a.val")) |> Column.alias_("diff")
  ])
  |> DataFrame.sort([asc(col("id1")), asc(col("id2"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.transform (pipeline helper)
# =============================================

run.("8. DataFrame.transform chaining", fn ->
  add_doubled = fn df -> DataFrame.with_column(df, "doubled", Column.multiply(col("val"), lit(2))) end
  add_label = fn df ->
    DataFrame.with_column(df, "label",
      when_(Column.gt(col("val"), lit(5)), lit("high"))
      |> Column.otherwise(lit("low")))
  end

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")

  df
  |> DataFrame.transform(add_doubled)
  |> DataFrame.transform(add_label)
  |> DataFrame.filter(Column.eq(col("label"), lit("high")))
  |> DataFrame.collect()
end)

# =============================================
# 9. Column.over with complex window + lag/lead
# =============================================

run.("9. lag/lead with custom default", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  w = SparkEx.Window.partition_by([]) |> WindowSpec.order_by([asc(col("id"))])
  df |> DataFrame.select([
    col("id"), col("val"),
    lag(col("val"), offset: 1, default: -1) |> Column.over(w) |> Column.alias_("prev_val"),
    lead(col("val"), offset: 1, default: -1) |> Column.over(w) |> Column.alias_("next_val"),
    lag(col("val"), offset: 2, default: 0) |> Column.over(w) |> Column.alias_("prev2_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Complex nested function calls
# =============================================

run.("10a. concat_ws with computed arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"words" => ["hello", "beautiful", "world"]}],
    schema: "words ARRAY<STRING>")
  # Use concat_ws directly
  df |> DataFrame.select([
    concat_ws(", ", [col("words")]) |> Column.alias_("joined")
  ]) |> DataFrame.collect()
end)

run.("10b. overlay function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"txt" => "Hello, World!"}], schema: "txt STRING")
  df |> DataFrame.select([
    overlay(col("txt"), lit("Spark"), 8) |> Column.alias_("overlaid")
  ]) |> DataFrame.collect()
end)

run.("10c. sentences function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "Hello World. How are you? I am fine, thank you."}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    sentences(col("txt")) |> Column.alias_("sents")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Catalog.clear_cache
# =============================================

run.("11. clear_cache", fn ->
  Catalog.clear_cache(s)
end)

# =============================================
# 12. NaiveDateTime / Time literal types
# =============================================

run.("12a. lit with NaiveDateTime", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([
    lit(~N[2024-06-15 14:30:00]) |> Column.alias_("ndt")
  ]) |> DataFrame.collect()
end)

run.("12b. lit with Time", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([
    lit(~T[14:30:45]) |> Column.alias_("time_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Edge: join on expression (not column name)
# =============================================

run.("13. join on complex expression", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "code" => "A-1"}, %{"id" => 2, "code" => "B-2"}
  ], schema: "id INT, code STRING")
  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"ref" => "1", "label" => "first"}, %{"ref" => "2", "label" => "second"}
  ], schema: "ref STRING, label STRING")

  # Join on: substring of code after "-" = ref
  a_alias = DataFrame.alias_(a, "a")
  b_alias = DataFrame.alias_(b, "b")
  DataFrame.join(a_alias, b_alias,
    Column.eq(
      Column.substr(col("a.code"), lit(3), lit(1)),
      col("b.ref")
    ), "inner")
  |> DataFrame.select([col("a.id"), col("a.code"), col("b.label")])
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 33 complete ===")
