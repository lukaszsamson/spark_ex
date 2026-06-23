# Round 59: Streaming with foreach_batch, Reader format-specific, DataFrame.to_pandas_on_spark_df,
# complex nested operations, Column.over with unbounded frames, lateral column alias,
# DataFrame.hint variations, withWatermark, complex expression trees,
# GroupedData.apply_in_pandas equivalent, creative wild combinations
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
# 1. DataFrame.hint variations
# =============================================

run.("1a. hint broadcast join", fn ->
  {:ok, big} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")
  {:ok, small} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "label" => "one"}, %{"id" => 50, "label" => "fifty"}
  ], schema: "id INT, label STRING")

  # Broadcast the small table
  hinted = DataFrame.hint(small, "broadcast")
  DataFrame.join(big, hinted, Column.eq(col("big.id"), col("small.id")), "inner")
  |> DataFrame.sort([asc(col("big.id"))])
  |> DataFrame.collect()
end)

run.("1b. hint coalesce", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  df
  |> DataFrame.repartition(10)
  |> DataFrame.hint("coalesce", [2])
  |> DataFrame.rdd_num_partitions_approx()
end)

# =============================================
# 2. DataFrame.transform with complex pipeline
# =============================================

run.("2. Multi-step transform pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"user_id" => rem(i, 10) + 1,
        "event_type" => Enum.at(["login", "view", "purchase"], rem(i, 3)),
        "revenue" => if(rem(i, 3) == 2, do: (rem(i * 997, 100) + 1) * 1.0, else: 0.0)}
    end), schema: "user_id INT, event_type STRING, revenue DOUBLE")

  df
  |> DataFrame.transform(fn d ->
    # Step 1: Add event flags
    d
    |> DataFrame.with_column("is_purchase",
      when_(Column.eq(col("event_type"), lit("purchase")), lit(1))
      |> Column.otherwise(lit(0)))
  end)
  |> DataFrame.transform(fn d ->
    # Step 2: Aggregate per user
    d
    |> DataFrame.group_by([col("user_id")])
    |> GroupedData.agg([
      count(col("event_type")) |> Column.alias_("total_events"),
      sum(col("is_purchase")) |> Column.alias_("purchases"),
      sum(col("revenue")) |> Column.alias_("total_revenue")
    ])
  end)
  |> DataFrame.transform(fn d ->
    # Step 3: Classify users
    d
    |> DataFrame.with_column("segment",
      when_(Column.gt(col("total_revenue"), lit(50.0)), lit("high_value"))
      |> Column.when_(Column.gt(col("purchases"), lit(0)), lit("buyer"))
      |> Column.otherwise(lit("browser")))
  end)
  |> DataFrame.sort([desc(col("total_revenue"))])
  |> DataFrame.limit(5)
  |> DataFrame.collect()
end)

# =============================================
# 3. Complex: N-way join with different join types
# =============================================

run.("3. 3-way join (inner + left + cross)", fn ->
  {:ok, users} = SparkEx.create_dataframe(s, [
    %{"uid" => 1, "name" => "Alice"}, %{"uid" => 2, "name" => "Bob"},
    %{"uid" => 3, "name" => "Charlie"}
  ], schema: "uid INT, name STRING")

  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"oid" => 101, "uid" => 1, "amount" => 100},
    %{"oid" => 102, "uid" => 1, "amount" => 200},
    %{"oid" => 103, "uid" => 2, "amount" => 150}
  ], schema: "oid INT, uid INT, amount INT")

  {:ok, flags} = SparkEx.create_dataframe(s, [
    %{"flag" => "VIP"}, %{"flag" => "PROMO"}
  ], schema: "flag STRING")

  # Join users → orders (left), then cross join with flags
  users
  |> DataFrame.join(orders,
    Column.eq(col("users.uid"), col("orders.uid")), "left")
  |> DataFrame.cross_join(flags)
  |> DataFrame.select([
    col("users.uid"), col("name"), col("oid"), col("amount"), col("flag")
  ])
  |> DataFrame.sort([asc(col("users.uid")), asc(col("oid")), asc(col("flag"))])
  |> DataFrame.collect()
end)

# =============================================
# 4. Window: running average with rows frame
# =============================================

run.("4. Running average (3-row window)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..12, fn i ->
      %{"month" => i, "sales" => (rem(i * 1337, 100) + 20) * 1.0}
    end), schema: "month INT, sales DOUBLE")

  w = SparkEx.Window.order_by([asc(col("month"))])
    |> WindowSpec.rows_between(-2, :current_row)

  df
  |> DataFrame.with_column("running_avg_3",
    avg(col("sales")) |> Column.over(w))
  |> DataFrame.with_column("running_sum_3",
    sum(col("sales")) |> Column.over(w))
  |> DataFrame.sort([asc(col("month"))])
  |> DataFrame.collect()
end)

# =============================================
# 5. Complex: self-join for gap detection
# =============================================

run.("5. Self-join for gap detection (missing IDs)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    [%{"id" => 1}, %{"id" => 2}, %{"id" => 5}, %{"id" => 6}, %{"id" => 10}],
    schema: "id INT")

  w = SparkEx.Window.order_by([asc(col("id"))])

  df
  |> DataFrame.with_column("next_id", lead(col("id"), offset: 1) |> Column.over(w))
  |> DataFrame.with_column("gap", Column.minus(col("next_id"), col("id")))
  |> DataFrame.filter(Column.gt(col("gap"), lit(1)))
  |> DataFrame.select([
    col("id") |> Column.alias_("gap_start"),
    col("next_id") |> Column.alias_("gap_end"),
    col("gap")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 6. Complex: pivot → unpivot roundtrip
# =============================================

run.("6. Pivot → unpivot roundtrip", fn ->
  {:ok, long} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "quarter" => "Q1", "revenue" => 100},
    %{"dept" => "Eng", "quarter" => "Q2", "revenue" => 120},
    %{"dept" => "Sales", "quarter" => "Q1", "revenue" => 200},
    %{"dept" => "Sales", "quarter" => "Q2", "revenue" => 180}
  ], schema: "dept STRING, quarter STRING, revenue INT")

  # Pivot: long → wide
  {:ok, wide} = long
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.pivot(col("quarter"))
  |> GroupedData.agg([sum(col("revenue")) |> Column.alias_("rev")])
  |> DataFrame.sort([asc(col("dept"))])
  |> DataFrame.collect()

  # Unpivot: wide → long (need original DataFrame, not collected)
  unpivoted = long
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.pivot(col("quarter"))
  |> GroupedData.agg([sum(col("revenue")) |> Column.alias_("rev")])
  |> DataFrame.unpivot(
    [col("dept")],
    [col("Q1"), col("Q2")],
    "quarter", "revenue")
  |> DataFrame.sort([asc(col("dept")), asc(col("quarter"))])
  |> DataFrame.collect()

  %{wide: wide, unpivoted: unpivoted}
end)

# =============================================
# 7. Complex: correlated columns via withColumn chain
# =============================================

run.("7. Correlated columns: each depends on previous", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"base" => 100.0}
  ], schema: "base DOUBLE")

  df
  |> DataFrame.with_column("step1", Column.multiply(col("base"), lit(1.1)))
  |> DataFrame.with_column("step2", Column.multiply(col("step1"), lit(1.1)))
  |> DataFrame.with_column("step3", Column.multiply(col("step2"), lit(1.1)))
  |> DataFrame.with_column("step4", Column.multiply(col("step3"), lit(1.1)))
  |> DataFrame.with_column("step5", Column.multiply(col("step4"), lit(1.1)))
  |> DataFrame.with_column("growth", Column.minus(col("step5"), col("base")))
  |> DataFrame.with_column("pct_growth",
    Column.multiply(
      Column.divide(col("growth"), col("base")),
      lit(100.0)))
  |> DataFrame.collect()
end)

# =============================================
# 8. Edge: DataFrame.randomSplit / random_split
# =============================================

run.("8. random_split with weights", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i} end), schema: "id INT")
  splits = DataFrame.random_split(df, [0.7, 0.3], seed: 42)
  case splits do
    {:ok, [train, test]} ->
      {:ok, train_cnt} = DataFrame.count(train)
      {:ok, test_cnt} = DataFrame.count(test)
      %{train: train_cnt, test: test_cnt, total: train_cnt + test_cnt}
    other -> other
  end
end)

# =============================================
# 9. Edge: SparkEx.sql with DDL (CREATE VIEW, DESCRIBE)
# =============================================

run.("9a. SQL CREATE TEMP VIEW + DESCRIBE", fn ->
  SparkEx.sql(s, """
    CREATE OR REPLACE TEMP VIEW sql_view_#{run_id} AS
    SELECT 1 AS id, 'hello' AS msg, 3.14 AS val
  """)
  SparkEx.sql(s, "DESCRIBE sql_view_#{run_id}") |> DataFrame.collect()
end)

run.("9b. SQL SHOW TABLES", fn ->
  SparkEx.sql(s, "SHOW TABLES IN sfx.sfx_dev_warehouse LIKE 'instruments'")
  |> DataFrame.collect()
end)

run.("9c. SQL SHOW COLUMNS", fn ->
  SparkEx.sql(s, "SHOW COLUMNS IN sfx.sfx_dev_warehouse.instruments")
  |> DataFrame.collect()
end)

# =============================================
# 10. Complex: multi-level aggregation (dept → company)
# =============================================

run.("10. Multi-level aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"company" => Enum.at(["Acme", "Beta"], rem(i, 2)),
        "dept" => Enum.at(["Eng", "Sales", "Ops"], rem(i, 3)),
        "employee" => "emp_#{i}",
        "salary" => (rem(i * 997, 80) + 40) * 1000}
    end), schema: "company STRING, dept STRING, employee STRING, salary INT")

  # Level 1: dept aggregation
  dept_agg = df
  |> DataFrame.group_by([col("company"), col("dept")])
  |> GroupedData.agg([
    count(col("employee")) |> Column.alias_("headcount"),
    avg(col("salary")) |> Column.alias_("avg_salary"),
    sum(col("salary")) |> Column.alias_("dept_total")
  ])

  # Level 2: company totals via window
  w = SparkEx.Window.partition_by([col("company")])

  dept_agg
  |> DataFrame.with_column("company_total",
    sum(col("dept_total")) |> Column.over(w))
  |> DataFrame.with_column("dept_pct_of_company",
    Column.multiply(
      Column.divide(
        Column.cast(col("dept_total"), "DOUBLE"),
        Column.cast(col("company_total"), "DOUBLE")),
      lit(100.0)))
  |> DataFrame.sort([asc(col("company")), desc(col("dept_total"))])
  |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.tail
# =============================================

run.("11. DataFrame.tail", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.tail(3)
end)

# =============================================
# 12. Edge: DataFrame.first / head
# =============================================

run.("12a. DataFrame.first", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 42, "name" => "Alice"}
  ], schema: "id INT, name STRING")
  DataFrame.first(df)
end)

run.("12b. DataFrame.head", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  DataFrame.head(df, 3)
end)

# =============================================
# 13. Edge: DataFrame.take
# =============================================

run.("13. DataFrame.take", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.take(5)
end)

# =============================================
# 14. Complex: Iceberg partition evolution via Writer v1 + WriterV2
# =============================================

run.("14. Write with Writer v1, read back and verify schema", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r59_verify_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5, "active" => true},
    %{"id" => 2, "name" => "Bob", "score" => 87.0, "active" => false}
  ], schema: "id INT, name STRING, score DOUBLE, active BOOLEAN")

  DataFrame.write(df) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(table)

  read_back = SparkEx.Reader.table(s, table)
  {:ok, schema} = DataFrame.schema(read_back)
  {:ok, dtypes} = DataFrame.dtypes(read_back)
  {:ok, cols} = DataFrame.columns(read_back)
  {:ok, rows} = DataFrame.collect(read_back)

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{schema: schema, dtypes: dtypes, columns: cols, rows: rows}
end)

# =============================================
# 15. Edge: DataFrame.to_local_iterator
# =============================================

run.("15. to_local_iterator", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, stream} = df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.to_local_iterator()
  first_5 = stream |> Enum.take(5)
  %{first_5: first_5, type: inspect(stream.__struct__)}
end)

# =============================================
# 16. Complex: mixed SQL + DataFrame pipeline with temp views
# =============================================

run.("16. Complex: SQL → DataFrame → SQL → collect", fn ->
  # Create source data
  {:ok, events} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      %{"uid" => rem(i, 10) + 1,
        "event" => Enum.at(["click", "view", "buy"], rem(i, 3)),
        "ts" => "2024-#{String.pad_leading(to_string(rem(i, 12) + 1), 2, "0")}-15"}
    end), schema: "uid INT, event STRING, ts DATE")

  DataFrame.create_or_replace_temp_view(events, "events_r59_#{run_id}")

  # Step 1: SQL aggregation
  step1 = SparkEx.sql(s, """
    SELECT uid, event, COUNT(*) AS cnt, MIN(ts) AS first_event, MAX(ts) AS last_event
    FROM events_r59_#{run_id}
    GROUP BY uid, event
  """)

  # Step 2: DataFrame transformation
  step2 = step1
  |> DataFrame.filter(Column.gt(col("cnt"), lit(2)))
  |> DataFrame.with_column("event_upper", upper(col("event")))

  DataFrame.create_or_replace_temp_view(step2, "agg_events_r59_#{run_id}")

  # Step 3: Final SQL query
  SparkEx.sql(s, """
    SELECT uid, event_upper, cnt,
           DATEDIFF(last_event, first_event) AS span_days
    FROM agg_events_r59_#{run_id}
    ORDER BY cnt DESC, uid
    LIMIT 5
  """) |> DataFrame.collect()
end)

IO.puts("=== Round 59 complete ===")
