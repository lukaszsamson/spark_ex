# Round 52: More wild combinations — real Iceberg tables, time travel SQL,
# schema evolution, DataFrame.hint, Window frame edge cases,
# complex HOF chains, lateral column alias, CUBE/ROLLUP
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
# 1. Iceberg time travel via SQL
# =============================================

run.("1a. Iceberg: create table with 2 versions", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r52_tt_#{run_id}"

  # Version 1
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice_v1"}
  ], schema: "id INT, name STRING")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  # Get snapshot info
  {:ok, snapshots} = SparkEx.sql(s, "SELECT * FROM #{table}.snapshots ORDER BY committed_at")
    |> DataFrame.collect()

  # Version 2: append
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "name" => "Bob_v2"}
  ], schema: "id INT, name STRING")
  DataFrame.write_v2(df2, table) |> SparkEx.WriterV2.append()

  {:ok, snapshots2} = SparkEx.sql(s, "SELECT * FROM #{table}.snapshots ORDER BY committed_at")
    |> DataFrame.collect()

  %{snapshot_count_v1: length(snapshots), snapshot_count_v2: length(snapshots2),
    snapshots: snapshots2}
end)

run.("1b. Iceberg: time travel to version 1 via AS OF", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r52_tt_#{run_id}"

  # Get first snapshot ID
  {:ok, snapshots} = SparkEx.sql(s, "SELECT snapshot_id FROM #{table}.snapshots ORDER BY committed_at LIMIT 1")
    |> DataFrame.collect()
  first_snapshot = hd(snapshots)["snapshot_id"]

  # Read at first snapshot
  {:ok, v1_rows} = SparkEx.sql(s, "SELECT * FROM #{table} VERSION AS OF #{first_snapshot}")
    |> DataFrame.collect()

  # Read current version
  {:ok, current_rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{v1: v1_rows, current: current_rows, first_snapshot: first_snapshot}
end)

# =============================================
# 2. Iceberg metadata tables
# =============================================

run.("2. Iceberg metadata: history, partitions, files", fn ->
  # Use an existing table
  table = "sfx.sfx_dev_warehouse.instruments"

  {:ok, history} = SparkEx.sql(s, "SELECT * FROM #{table}.history LIMIT 3") |> DataFrame.collect()
  {:ok, partitions} = SparkEx.sql(s, "SELECT * FROM #{table}.partitions LIMIT 3") |> DataFrame.collect()

  %{history: history, partitions: partitions}
end)

# =============================================
# 3. CUBE and ROLLUP via SQL
# =============================================

run.("3a. CUBE aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..60, fn i ->
      %{"region" => Enum.at(["North", "South", "East"], rem(i, 3)),
        "product" => Enum.at(["A", "B"], rem(i, 2)),
        "sales" => (rem(i * 1337, 100) + 1) * 1.0}
    end), schema: "region STRING, product STRING, sales DOUBLE")

  DataFrame.create_temp_view(df, "cube_test_#{run_id}")

  result = SparkEx.sql(s, """
    SELECT region, product, SUM(sales) AS total_sales, COUNT(*) AS cnt
    FROM cube_test_#{run_id}
    GROUP BY CUBE (region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "cube_test_#{run_id}")
  result
end)

run.("3b. ROLLUP aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..60, fn i ->
      %{"dept" => Enum.at(["Eng", "Sales", "Ops"], rem(i, 3)),
        "level" => Enum.at(["Jr", "Sr"], rem(i, 2)),
        "salary" => 40000 + rem(i * 997, 40000)}
    end), schema: "dept STRING, level STRING, salary INT")

  DataFrame.create_temp_view(df, "rollup_test_#{run_id}")

  result = SparkEx.sql(s, """
    SELECT dept, level, SUM(salary) AS total, AVG(salary) AS avg_sal, COUNT(*) AS cnt
    FROM rollup_test_#{run_id}
    GROUP BY ROLLUP (dept, level)
    ORDER BY dept NULLS LAST, level NULLS LAST
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "rollup_test_#{run_id}")
  result
end)

# =============================================
# 4. DataFrame.hint
# =============================================

run.("4a. DataFrame.hint broadcast", fn ->
  {:ok, large} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  {:ok, small} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "label" => "A"}, %{"id" => 50, "label" => "B"}
  ], schema: "id INT, label STRING")

  hinted = DataFrame.hint(small, "broadcast")
  DataFrame.join(large, hinted, Column.eq(col("large.id"), col("small.id")), "inner")
  |> DataFrame.collect()
end)

run.("4b. DataFrame.hint with parameters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end), schema: "id INT")
  # COALESCE hint
  DataFrame.hint(df, "coalesce", [2])
  |> DataFrame.rdd_num_partitions_approx()
end)

# =============================================
# 5. Window: rows_between with correct API
# =============================================

run.("5a. Window rows_between(:unbounded, :current_row)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i ->
      %{"dept" => Enum.at(["A", "B"], rem(i, 2)), "val" => i}
    end), schema: "dept STRING, val INT")

  w = SparkEx.Window.partition_by([col("dept")])
    |> WindowSpec.order_by([asc(col("val"))])
    |> WindowSpec.rows_between(:unbounded, :current_row)

  df
  |> DataFrame.with_column("running_sum", sum(col("val")) |> Column.over(w))
  |> DataFrame.sort([asc(col("dept")), asc(col("val"))])
  |> DataFrame.collect()
end)

run.("5b. Window rows_between(-1, 1) — 3-row rolling", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  w = SparkEx.Window.order_by([asc(col("val"))])
    |> WindowSpec.rows_between(-1, 1)

  df
  |> DataFrame.with_column("rolling_avg", avg(col("val")) |> Column.over(w))
  |> DataFrame.collect()
end)

run.("5c. Window range_between(:unbounded, :current_row)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i, "amt" => i * 100} end),
    schema: "val INT, amt INT")

  w = SparkEx.Window.order_by([asc(col("val"))])
    |> WindowSpec.range_between(:unbounded, :current_row)

  df
  |> DataFrame.with_column("cumulative_amt", sum(col("amt")) |> Column.over(w))
  |> DataFrame.collect()
end)

# =============================================
# 6. Complex: multi-table SQL analytics on real warehouse data
# =============================================

run.("6. Analytics on instruments table with windows + having", fn ->
  SparkEx.sql(s, """
    WITH instrument_stats AS (
      SELECT
        name,
        instrument_type,
        country,
        pip_value,
        ROW_NUMBER() OVER (PARTITION BY instrument_type ORDER BY pip_value DESC) as rn,
        COUNT(*) OVER (PARTITION BY instrument_type) as type_count,
        AVG(pip_value) OVER (PARTITION BY instrument_type) as avg_pip
      FROM sfx.sfx_dev_warehouse.instruments
      WHERE pip_value IS NOT NULL AND pip_value > 0
    )
    SELECT instrument_type, type_count, avg_pip,
           name AS top_instrument, pip_value AS top_pip
    FROM instrument_stats
    WHERE rn = 1
    ORDER BY type_count DESC
  """) |> DataFrame.collect()
end)

# =============================================
# 7. Complex: DataFrame operations chained after SQL
# =============================================

run.("7. SQL base → filter → window → aggregate → collect", fn ->
  base = SparkEx.sql(s, """
    SELECT id, id * id AS squared, MOD(id, 5) AS grp, id * 1.5 AS score
    FROM range(1, 101)
  """)

  w = SparkEx.Window.partition_by([col("grp")]) |> WindowSpec.order_by([desc(col("score"))])

  base
  |> DataFrame.filter(Column.neq(col("grp"), lit(0)))
  |> DataFrame.with_column("rank_in_grp", dense_rank() |> Column.over(w))
  |> DataFrame.filter(Column.lte(col("rank_in_grp"), lit(5)))
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    count(col("id")) |> Column.alias_("top5_count"),
    sum(col("score")) |> Column.alias_("top5_score_sum"),
    max(col("squared")) |> Column.alias_("top5_max_squared")
  ])
  |> DataFrame.sort([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. Edge: DataFrame.exceptAll / intersectAll
# =============================================

run.("8a. exceptAll", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")
  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 3}
  ], schema: "id INT")
  DataFrame.except_all(a, b) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
end)

run.("8b. intersectAll", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")
  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 1}, %{"id" => 1}
  ], schema: "id INT")
  DataFrame.intersect_all(a, b) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
end)

# =============================================
# 9. Edge: Column.substr
# =============================================

run.("9. Column.substr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World"}, %{"text" => "SparkEx"}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    Column.substr(col("text"), lit(1), lit(5)) |> Column.alias_("first5"),
    Column.substr(col("text"), lit(7), lit(100)) |> Column.alias_("rest")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Edge: Column.getItem on map and array
# =============================================

run.("10a. getItem on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    Column.get_item(col("arr"), lit(0)) |> Column.alias_("first"),
    Column.get_item(col("arr"), lit(2)) |> Column.alias_("third")
  ]) |> DataFrame.collect()
end)

run.("10b. getItem on map", fn ->
  df = SparkEx.sql(s, "SELECT map('name', 'Alice', 'city', 'NYC') AS m")
  df |> DataFrame.select([
    Column.get_item(col("m"), lit("name")) |> Column.alias_("name"),
    Column.get_item(col("m"), lit("city")) |> Column.alias_("city"),
    Column.get_item(col("m"), lit("missing")) |> Column.alias_("missing")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Edge: Column.getField on struct
# =============================================

run.("11. getField on struct", fn ->
  df = SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  df |> DataFrame.select([
    Column.get_field(col("person"), "name") |> Column.alias_("name"),
    Column.get_field(col("person"), "age") |> Column.alias_("age")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Edge: Column.like_ / rlike / startswith / endswith / contains
# =============================================

run.("12. String pattern matching", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice Smith"}, %{"name" => "Bob Jones"},
    %{"name" => "Alice Brown"}, %{"name" => "Charlie Smith"}
  ], schema: "name STRING")
  df |> DataFrame.select([
    col("name"),
    Column.startswith(col("name"), lit("Alice")) |> Column.alias_("starts_alice"),
    Column.endswith(col("name"), lit("Smith")) |> Column.alias_("ends_smith"),
    Column.contains(col("name"), lit("li")) |> Column.alias_("contains_li"),
    Column.rlike(col("name"), lit("^[AB].*")) |> Column.alias_("starts_AB")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Lateral column alias via SQL
# =============================================

run.("13. Lateral column alias", fn ->
  SparkEx.sql(s, """
    SELECT
      id,
      id * 2 AS doubled,
      doubled + 10 AS doubled_plus_10,
      doubled_plus_10 * 3 AS tripled
    FROM range(1, 6)
  """) |> DataFrame.collect()
end)

# =============================================
# 14. Window: NTILE, PERCENT_RANK, CUME_DIST
# =============================================

run.("14. NTILE, percent_rank, cume_dist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i} end), schema: "val INT")

  w = SparkEx.Window.order_by([asc(col("val"))])

  df |> DataFrame.select([
    col("val"),
    ntile(lit(4)) |> Column.over(w) |> Column.alias_("quartile"),
    percent_rank() |> Column.over(w) |> Column.alias_("pct_rank"),
    cume_dist() |> Column.over(w) |> Column.alias_("cume_dist")
  ])
  |> DataFrame.filter(Column.lte(col("val"), lit(5)))
  |> DataFrame.collect()
end)

# =============================================
# 15. Complex: write partitioned → overwrite one partition → verify isolation
# =============================================

run.("15. Partitioned table: verify partition isolation on overwrite", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r52_iso_#{run_id}"

  # Create with 3 partitions, 5 rows each
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.flat_map(["X", "Y", "Z"], fn part ->
      Enum.map(1..5, fn i -> %{"part" => part, "val" => i} end)
    end), schema: "part STRING, val INT")

  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.partitioned_by([col("part")])
  |> SparkEx.WriterV2.create()

  {:ok, before} = SparkEx.Reader.table(s, table)
    |> DataFrame.group_by([col("part")])
    |> GroupedData.agg([count(col("val")) |> Column.alias_("cnt")])
    |> DataFrame.sort([asc(col("part"))])
    |> DataFrame.collect()

  # Overwrite only partition Y with fewer rows
  {:ok, override} = SparkEx.create_dataframe(s, [
    %{"part" => "Y", "val" => 999}
  ], schema: "part STRING, val INT")

  DataFrame.write_v2(override, table) |> SparkEx.WriterV2.overwrite_partitions()

  {:ok, after_overwrite} = SparkEx.Reader.table(s, table)
    |> DataFrame.group_by([col("part")])
    |> GroupedData.agg([count(col("val")) |> Column.alias_("cnt")])
    |> DataFrame.sort([asc(col("part"))])
    |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{before: before, after: after_overwrite}
end)

IO.puts("=== Round 52 complete ===")
