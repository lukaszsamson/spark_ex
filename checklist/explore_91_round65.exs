# Round 65: Hunting for more bugs with tricky edge cases
# - WriterV2 with complex schemas (nested struct/array/map columns)
# - create_dataframe with Decimal, Date, NaiveDateTime values
# - Column.astype (alias for cast)
# - DataFrame.describe / summary
# - DataFrame.drop_duplicates with subset
# - DataFrame.sample with fraction and seed
# - DataFrame.random_split
# - Multiple GroupedData operations on same grouped object
# - DataFrame.unpivot
# - Complex SQL via SparkEx.sql with CTE + window + join
# - Observation with single metric (regression check)
# - WriterV2 table_property
# - DataFrame.hint with different strategies
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
# 1. create_dataframe with special Elixir types
# =============================================

run.("1a. create_dataframe with Decimal values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "amount" => Decimal.new("123.456")},
    %{"id" => 2, "amount" => Decimal.new("999.999")},
    %{"id" => 3, "amount" => Decimal.new("0.001")}
  ], schema: "id INT, amount DECIMAL(10,3)")
  df |> DataFrame.collect()
end)

run.("1b. create_dataframe with Date values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "dt" => ~D[2024-01-15]},
    %{"id" => 2, "dt" => ~D[2024-06-30]},
    %{"id" => 3, "dt" => ~D[2024-12-31]}
  ], schema: "id INT, dt DATE")
  df |> DataFrame.collect()
end)

run.("1c. create_dataframe with NaiveDateTime values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "ts" => ~N[2024-01-15 10:30:00]},
    %{"id" => 2, "ts" => ~N[2024-06-30 23:59:59]}
  ], schema: "id INT, ts TIMESTAMP")
  df |> DataFrame.collect()
end)

run.("1d. create_dataframe with mixed special types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "amount" => Decimal.new("99.99"), "dt" => ~D[2024-01-01],
      "ts" => ~N[2024-01-01 00:00:00], "active" => true, "tags" => ["a", "b"]},
    %{"id" => 2, "amount" => Decimal.new("0.01"), "dt" => ~D[2024-12-31],
      "ts" => ~N[2024-12-31 23:59:59], "active" => false, "tags" => ["c"]}
  ], schema: "id INT, amount DECIMAL(10,2), dt DATE, ts TIMESTAMP, active BOOLEAN, tags ARRAY<STRING>")
  df |> DataFrame.collect()
end)

# =============================================
# 2. Column.astype (alias for cast?)
# =============================================

run.("2. Column.astype", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    Column.astype(col("val"), "STRING") |> Column.alias_("as_str"),
    Column.astype(col("val"), "DOUBLE") |> Column.alias_("as_dbl")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. DataFrame.describe / summary
# =============================================

run.("3a. describe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"a" => i * 1.0, "b" => rem(i, 10) * 1.0, "c" => "grp_#{rem(i, 5)}"} end),
    schema: "a DOUBLE, b DOUBLE, c STRING")
  DataFrame.describe(df)
end)

run.("3b. summary with custom stats", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  DataFrame.summary(df, ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"])
end)

# =============================================
# 4. DataFrame.drop_duplicates
# =============================================

run.("4a. drop_duplicates (all columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"}, %{"a" => 1, "b" => "x"},
    %{"a" => 2, "b" => "y"}, %{"a" => 2, "b" => "z"}
  ], schema: "a INT, b STRING")
  df |> DataFrame.drop_duplicates() |> DataFrame.sort([asc(col("a")), asc(col("b"))]) |> DataFrame.collect()
end)

run.("4b. drop_duplicates with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x", "c" => 100},
    %{"a" => 1, "b" => "y", "c" => 200},
    %{"a" => 2, "b" => "x", "c" => 300},
    %{"a" => 2, "b" => "x", "c" => 400}
  ], schema: "a INT, b STRING, c INT")
  df |> DataFrame.drop_duplicates(["a"]) |> DataFrame.sort([asc(col("a"))]) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.sample + random_split
# =============================================

run.("5a. sample with fraction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, sampled} = DataFrame.sample(df, 0.1, seed: 42) |> DataFrame.collect()
  length(sampled)
end)

run.("5b. random_split", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, [train, test]} = DataFrame.random_split(df, [0.8, 0.2], seed: 42)
  {:ok, train_count} = DataFrame.count(train)
  {:ok, test_count} = DataFrame.count(test)
  %{train: train_count, test: test_count, total: train_count + test_count}
end)

# =============================================
# 6. Multiple aggs on same GroupedData
# =============================================

run.("6. Same GroupedData, multiple agg calls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i -> %{"grp" => Enum.at(["A", "B", "C"], rem(i, 3)), "val" => i} end),
    schema: "grp STRING, val INT")

  grouped = DataFrame.group_by(df, [col("grp")])

  {:ok, sums} = GroupedData.agg(grouped, [sum(col("val")) |> Column.alias_("total")])
    |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()
  {:ok, counts} = GroupedData.agg(grouped, [count(col("val")) |> Column.alias_("cnt")])
    |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()
  {:ok, maxes} = GroupedData.agg(grouped, [max(col("val")) |> Column.alias_("mx")])
    |> DataFrame.sort([asc(col("grp"))]) |> DataFrame.collect()

  %{sums: sums, counts: counts, maxes: maxes}
end)

# =============================================
# 7. DataFrame.unpivot
# =============================================

run.("7. unpivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "q1" => 100, "q2" => 200, "q3" => 300, "q4" => 400},
    %{"id" => 2, "q1" => 150, "q2" => 250, "q3" => 350, "q4" => 450}
  ], schema: "id INT, q1 INT, q2 INT, q3 INT, q4 INT")
  df |> DataFrame.unpivot(["id"], ["q1", "q2", "q3", "q4"], "quarter", "revenue")
  |> DataFrame.sort([asc(col("id")), asc(col("quarter"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. Complex SQL via SparkEx.sql
# =============================================

run.("8. Complex SQL with CTE + window + aggregation", fn ->
  # First create temp view
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"dept" => Enum.at(["eng", "sales", "ops"], rem(i, 3)),
        "emp" => "emp_#{i}",
        "salary" => (rem(i * 997, 80) + 40) * 1000}
    end), schema: "dept STRING, emp STRING, salary INT")
  DataFrame.create_temp_view(df, "r65_employees")

  SparkEx.sql(s, """
    WITH ranked AS (
      SELECT dept, emp, salary,
        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as dept_rank,
        AVG(salary) OVER (PARTITION BY dept) as dept_avg
      FROM r65_employees
    ),
    top3 AS (
      SELECT * FROM ranked WHERE dept_rank <= 3
    )
    SELECT dept, emp, salary, dept_rank,
      CAST(dept_avg AS INT) as dept_avg,
      salary - CAST(dept_avg AS INT) as above_avg
    FROM top3
    ORDER BY dept, dept_rank
  """) |> DataFrame.collect()
end)

# =============================================
# 9. Observation regression check
# =============================================

run.("9a. Observation with single metric", fn ->
  obs = SparkEx.Observation.new("single_obs_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  {:ok, _rows} = df
  |> DataFrame.observe(obs, [count(col("val")) |> Column.alias_("cnt")])
  |> DataFrame.collect()

  SparkEx.Observation.get(obs)
end)

run.("9b. Observation with 3 metrics", fn ->
  obs = SparkEx.Observation.new("triple_obs_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  {:ok, _rows} = df
  |> DataFrame.observe(obs, [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total"),
    avg(col("val")) |> Column.alias_("mean")
  ])
  |> DataFrame.collect()

  SparkEx.Observation.get(obs)
end)

# =============================================
# 10. WriterV2 table_property + create
# =============================================

run.("10. WriterV2 with table_property", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r65_props_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => "world"}
  ], schema: "id INT, val STRING")

  # Create with properties
  DataFrame.write_v2(df, table)
  |> SparkEx.WriterV2.table_property("write.format.default", "parquet")
  |> SparkEx.WriterV2.create()

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

# =============================================
# 11. DataFrame.hint
# =============================================

run.("11a. hint broadcast", fn ->
  {:ok, small} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "label" => "A"}, %{"id" => 2, "label" => "B"}
  ], schema: "id INT, label STRING")

  {:ok, big} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => rem(i, 2) + 1, "val" => i} end),
    schema: "id INT, val INT")

  hinted = DataFrame.hint(small, "broadcast")
  DataFrame.join(big, hinted, col("id"), "inner")
  |> DataFrame.select([col("val"), col("label")])
  |> DataFrame.count()
end)

run.("11b. hint coalesce", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  hinted = DataFrame.hint(df, "coalesce", [2])
  {:ok, count} = DataFrame.count(hinted)
  count
end)

# =============================================
# 12. Complex: self-join with dot notation + window
# =============================================

run.("12. Self-join via aliases + dot notation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95},
    %{"id" => 2, "name" => "Bob", "score" => 88},
    %{"id" => 3, "name" => "Carol", "score" => 92},
    %{"id" => 4, "name" => "Dave", "score" => 85}
  ], schema: "id INT, name STRING, score INT")

  a = DataFrame.alias_(df, "a")
  b = DataFrame.alias_(df, "b")

  # Use col("a.id") dot notation for qualified references
  DataFrame.join(a, b,
    Column.lt(col("a.id"), col("b.id")),
    "inner")
  |> DataFrame.select([
    col("a.name") |> Column.alias_("person_1"),
    col("b.name") |> Column.alias_("person_2"),
    col("a.score") |> Column.alias_("score_1"),
    col("b.score") |> Column.alias_("score_2"),
    SparkEx.Functions.abs(Column.minus(col("a.score"), col("b.score")))
    |> Column.alias_("score_diff")
  ])
  |> DataFrame.sort([asc(col("score_diff")), asc(col("person_1"))])
  |> DataFrame.collect()
end)

# =============================================
# 13. Edge: WriterV2 append to existing table
# =============================================

run.("13. WriterV2 create + append + verify", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r65_append_#{run_id}"

  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "batch_1"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "val" => "batch_2"},
    %{"id" => 3, "val" => "batch_3"}
  ], schema: "id INT, val STRING")
  DataFrame.write_v2(df2, table) |> SparkEx.WriterV2.append()

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

# =============================================
# 14. Edge: DataFrame.transform (user-defined pipeline)
# =============================================

run.("14. DataFrame.transform with chained transforms", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  add_doubled = fn df -> DataFrame.with_column(df, "doubled", Column.multiply(col("val"), lit(2))) end
  add_flag = fn df -> DataFrame.with_column(df, "big", Column.gt(col("val"), lit(100))) end
  filter_big = fn df -> DataFrame.filter(df, Column.gt(col("val"), lit(50))) end

  df
  |> DataFrame.transform(add_doubled)
  |> DataFrame.transform(add_flag)
  |> DataFrame.transform(filter_big)
  |> DataFrame.collect()
end)

IO.puts("=== Round 65 complete ===")
