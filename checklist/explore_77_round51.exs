# Round 51: Creative wild combinations — streaming file sources, deeply nested plans,
# edge cases in functions, SQL injection patterns, extreme data types, complex pipelines
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
# 1. Chain 20 withColumn operations
# =============================================

run.("1. 20 chained withColumn operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  result = Enum.reduce(1..20, df, fn i, acc ->
    DataFrame.with_column(acc, "col_#{i}", Column.plus(col("x"), lit(i)))
  end)
  DataFrame.collect(result)
end)

# =============================================
# 2. 10-deep nested subquery via SQL
# =============================================

run.("2. 10-deep nested subquery", fn ->
  # Build: SELECT x+1 FROM (SELECT x+1 FROM (... SELECT 1 AS x ...))
  inner = "SELECT 1 AS x"
  query = Enum.reduce(1..10, inner, fn _i, acc ->
    "SELECT x + 1 AS x FROM (#{acc})"
  end)
  SparkEx.sql(s, query) |> DataFrame.collect()
end)

# =============================================
# 3. Very wide DataFrame (100 columns)
# =============================================

run.("3. 100-column DataFrame", fn ->
  row = Enum.reduce(1..100, %{}, fn i, acc -> Map.put(acc, "c#{i}", i) end)
  schema = Enum.map(1..100, fn i -> "c#{i} INT" end) |> Enum.join(", ")
  {:ok, df} = SparkEx.create_dataframe(s, [row], schema: schema)
  {:ok, rows} = DataFrame.collect(df)
  %{column_count: rows |> hd() |> map_size()}
end)

# =============================================
# 4. Unicode in column names and values
# =============================================

run.("4a. Unicode column names", fn ->
  SparkEx.sql(s, "SELECT 1 AS `日本語`, 2 AS `emoji_🎉`, 3 AS `café`")
  |> DataFrame.collect()
end)

run.("4b. Unicode string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello 世界 🌍"},
    %{"text" => "Ελληνικά"},
    %{"text" => "العربية"},
    %{"text" => "中文测试 🎯✨"}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    length(col("text")) |> Column.alias_("len"),
    upper(col("text")) |> Column.alias_("upper")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Extreme numeric values
# =============================================

run.("5a. Very large LONG values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 9_223_372_036_854_775_807},  # Long.MAX_VALUE
    %{"val" => -9_223_372_036_854_775_808}   # Long.MIN_VALUE
  ], schema: "val LONG")
  DataFrame.collect(df)
end)

run.("5b. Very small/large DOUBLE values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1.7976931348623157e308},   # Double.MAX_VALUE (approx)
    %{"val" => 5.0e-324},                  # Double.MIN_VALUE (approx)
    %{"val" => -1.7976931348623157e308}
  ], schema: "val DOUBLE")
  DataFrame.collect(df)
end)

run.("5c. NaN and Infinity via SQL", fn ->
  SparkEx.sql(s, """
    SELECT
      CAST('NaN' AS DOUBLE) AS nan_val,
      CAST('Infinity' AS DOUBLE) AS pos_inf,
      CAST('-Infinity' AS DOUBLE) AS neg_inf,
      1.0 / 0.0 AS div_zero_inf,
      0.0 / 0.0 AS div_zero_nan
  """) |> DataFrame.collect()
end)

# =============================================
# 6. Empty string vs null
# =============================================

run.("6. Empty string vs null distinction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => ""},
    %{"val" => nil},
    %{"val" => "hello"}
  ], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    Column.is_null(col("val")) |> Column.alias_("is_null"),
    Column.eq(col("val"), lit("")) |> Column.alias_("is_empty"),
    coalesce([col("val"), lit("DEFAULT")]) |> Column.alias_("coalesced"),
    when_(Column.is_null(col("val")), lit("WAS_NULL"))
    |> Column.otherwise(
      when_(Column.eq(col("val"), lit("")), lit("WAS_EMPTY"))
      |> Column.otherwise(col("val")))
    |> Column.alias_("classified")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. Multiple simultaneous temp views with joins
# =============================================

run.("7. Star schema with 4 temp views joined", fn ->
  # Fact table
  {:ok, facts} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"order_id" => i, "product_id" => rem(i, 5) + 1,
        "customer_id" => rem(i, 3) + 1, "region_id" => rem(i, 4) + 1,
        "amount" => (rem(i * 997, 100) + 1) * 1.0}
    end), schema: "order_id INT, product_id INT, customer_id INT, region_id INT, amount DOUBLE")

  {:ok, products} = SparkEx.create_dataframe(s, [
    %{"pid" => 1, "pname" => "Widget"}, %{"pid" => 2, "pname" => "Gadget"},
    %{"pid" => 3, "pname" => "Doohickey"}, %{"pid" => 4, "pname" => "Thingamajig"},
    %{"pid" => 5, "pname" => "Whatchamacallit"}
  ], schema: "pid INT, pname STRING")

  {:ok, customers} = SparkEx.create_dataframe(s, [
    %{"cid" => 1, "cname" => "Alice"}, %{"cid" => 2, "cname" => "Bob"},
    %{"cid" => 3, "cname" => "Charlie"}
  ], schema: "cid INT, cname STRING")

  {:ok, regions} = SparkEx.create_dataframe(s, [
    %{"rid" => 1, "rname" => "North"}, %{"rid" => 2, "rname" => "South"},
    %{"rid" => 3, "rname" => "East"}, %{"rid" => 4, "rname" => "West"}
  ], schema: "rid INT, rname STRING")

  DataFrame.create_temp_view(facts, "tv_facts_#{run_id}")
  DataFrame.create_temp_view(products, "tv_prod_#{run_id}")
  DataFrame.create_temp_view(customers, "tv_cust_#{run_id}")
  DataFrame.create_temp_view(regions, "tv_reg_#{run_id}")

  result = SparkEx.sql(s, """
    SELECT c.cname, p.pname, r.rname,
           COUNT(*) AS orders, SUM(f.amount) AS total
    FROM tv_facts_#{run_id} f
    JOIN tv_prod_#{run_id} p ON f.product_id = p.pid
    JOIN tv_cust_#{run_id} c ON f.customer_id = c.cid
    JOIN tv_reg_#{run_id} r ON f.region_id = r.rid
    GROUP BY c.cname, p.pname, r.rname
    ORDER BY total DESC
    LIMIT 10
  """) |> DataFrame.collect()

  # Clean up
  for view <- ["tv_facts_#{run_id}", "tv_prod_#{run_id}", "tv_cust_#{run_id}", "tv_reg_#{run_id}"] do
    DataFrame.drop_temp_view(s, view)
  end

  result
end)

# =============================================
# 8. DataFrame operations on SQL query results
# =============================================

run.("8. SQL → DataFrame transforms → write → read", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r51_sql_#{run_id}"

  pipeline = SparkEx.sql(s, """
    SELECT id, id * id AS squared, id % 3 AS grp
    FROM range(1, 51)
  """)
  |> DataFrame.filter(Column.gt(col("grp"), lit(0)))
  |> DataFrame.with_column("category",
    when_(Column.eq(col("grp"), lit(1)), lit("odd"))
    |> Column.otherwise(lit("even")))
  |> DataFrame.with_column("normalized",
    Column.divide(
      Column.cast(col("squared"), "DOUBLE"),
      lit(2500.0)))

  DataFrame.write_v2(pipeline, table) |> SparkEx.WriterV2.create()

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.limit(10)
    |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{count: length(rows), sample: rows}
end)

# =============================================
# 9. Complex window: running total + pct of total + rank
# =============================================

run.("9. Complex window: running total + pct of total + rank", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"dept" => Enum.at(["Eng", "Sales", "Ops"], rem(i, 3)),
        "emp" => "E#{i}",
        "salary" => 30000 + rem(i * 1337, 50000)}
    end), schema: "dept STRING, emp STRING, salary INT")

  # Window: partition by dept, order by salary desc
  w_rank = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([desc(col("salary"))])
  w_running = SparkEx.Window.partition_by([col("dept")])
    |> WindowSpec.order_by([desc(col("salary"))])
    |> WindowSpec.rows_between(:unbounded_preceding, :current_row)
  w_total = SparkEx.Window.partition_by([col("dept")])

  df
  |> DataFrame.with_column("rank", dense_rank() |> Column.over(w_rank))
  |> DataFrame.with_column("running_sum",
    sum(col("salary")) |> Column.over(w_running))
  |> DataFrame.with_column("dept_total",
    sum(col("salary")) |> Column.over(w_total))
  |> DataFrame.with_column("pct_of_dept",
    Column.multiply(
      Column.divide(
        Column.cast(col("salary"), "DOUBLE"),
        Column.cast(col("dept_total"), "DOUBLE")),
      lit(100.0)))
  |> DataFrame.filter(Column.lte(col("rank"), lit(3)))
  |> DataFrame.sort([asc(col("dept")), asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. Streaming: rate source → memory sink → SQL query
# =============================================

run.("10. Streaming: rate → memory → query", fn ->
  alias SparkEx.{StreamReader, StreamWriter, StreamingQuery}
  stream = StreamReader.rate(s, rows_per_second: 10)
  {:ok, query} = stream
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("rate_test_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  # Wait for some data
  Process.sleep(3000)

  # Query the in-memory table
  result = SparkEx.sql(s, "SELECT COUNT(*) AS cnt, MIN(value) AS min_val, MAX(value) AS max_val FROM rate_test_#{run_id}")
    |> DataFrame.collect()

  StreamingQuery.stop(query)
  result
end)

# =============================================
# 11. Array/Map/Struct nested operations
# =============================================

run.("11. Deeply nested struct operations", fn ->
  SparkEx.sql(s, """
    SELECT
      named_struct(
        'person', named_struct('name', 'Alice', 'age', 30),
        'address', named_struct(
          'street', '123 Main',
          'city', 'NYC',
          'coords', named_struct('lat', 40.7128, 'lng', -74.0060)
        ),
        'scores', array(95, 88, 92)
      ) AS record
  """)
  |> DataFrame.select([
    col("record.person.name") |> Column.alias_("name"),
    col("record.person.age") |> Column.alias_("age"),
    col("record.address.city") |> Column.alias_("city"),
    col("record.address.coords.lat") |> Column.alias_("lat"),
    col("record.address.coords.lng") |> Column.alias_("lng"),
    element_at(col("record.scores"), lit(1)) |> Column.alias_("first_score"),
    size(col("record.scores")) |> Column.alias_("num_scores"),
    aggregate(col("record.scores"), lit(0), fn acc, x -> Column.plus(acc, x) end)
    |> Column.alias_("total_score")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 12. Extreme: 50 columns in GROUP BY
# =============================================

run.("12. GROUP BY with 10 columns", fn ->
  schema = Enum.map(1..10, fn i -> "c#{i} INT" end) |> Enum.join(", ")
  rows = Enum.map(1..100, fn i ->
    Enum.reduce(1..10, %{}, fn j, acc -> Map.put(acc, "c#{j}", rem(i * j, 3)) end)
  end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: schema <> ", val INT")

  # Hmm we need val column too
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i ->
      row = Enum.reduce(1..10, %{}, fn j, acc -> Map.put(acc, "c#{j}", rem(i * j, 3)) end)
      Map.put(row, "val", i)
    end), schema: schema <> ", val INT")

  group_cols = Enum.map(1..10, fn i -> col("c#{i}") end)
  df
  |> DataFrame.group_by(group_cols)
  |> GroupedData.agg([count(col("val")) |> Column.alias_("cnt")])
  |> DataFrame.sort([desc(col("cnt"))])
  |> DataFrame.limit(5)
  |> DataFrame.collect()
end)

# =============================================
# 13. Chained DataFrame transforms with transform/2
# =============================================

run.("13. 5 chained DataFrame.transform calls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  result = df
  |> DataFrame.transform(fn d -> DataFrame.with_column(d, "step1", Column.multiply(col("val"), lit(2.0))) end)
  |> DataFrame.transform(fn d -> DataFrame.with_column(d, "step2", Column.plus(col("step1"), lit(10.0))) end)
  |> DataFrame.transform(fn d -> DataFrame.with_column(d, "step3", Column.divide(col("step2"), lit(3.0))) end)
  |> DataFrame.transform(fn d -> DataFrame.filter(d, Column.gt(col("step3"), lit(10.0))) end)
  |> DataFrame.transform(fn d -> DataFrame.sort(d, [desc(col("step3"))]) end)
  |> DataFrame.limit(5)
  |> DataFrame.collect()
end)

# =============================================
# 14. Edge: Column.between
# =============================================

run.("14. Column.between", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.between(col("val"), lit(5), lit(15)))
    |> DataFrame.collect()
end)

# =============================================
# 15. Edge: Column.eqNullSafe / Column.eq_null_safe
# =============================================

run.("15. eq_null_safe comparison", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1},
    %{"a" => nil, "b" => nil},
    %{"a" => 1, "b" => nil},
    %{"a" => nil, "b" => 1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.eq(col("a"), col("b")) |> Column.alias_("eq"),
    Column.eq_null_safe(col("a"), col("b")) |> Column.alias_("eq_null_safe")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Edge: Column.bitwiseOR/AND/XOR
# =============================================

run.("16. Bitwise operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 0b1100, "b" => 0b1010}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.bitwise_or(col("a"), col("b")) |> Column.alias_("or"),
    Column.bitwise_and(col("a"), col("b")) |> Column.alias_("and"),
    Column.bitwise_xor(col("a"), col("b")) |> Column.alias_("xor")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 51 complete ===")
