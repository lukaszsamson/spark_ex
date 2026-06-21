# Round 35: fillna/dropna correct API, streaming write_stream pattern,
# DataFrame.replace, DataFrame.offset, complex data pipelines
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, StreamReader, StreamWriter, StreamingQuery}
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
# 1. fillna / dropna (DataFrame methods)
# =============================================

run.("1a. fillna with value map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => "hello"},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c STRING")
  DataFrame.fillna(df, %{"a" => 0, "b" => -1, "c" => "N/A"}) |> DataFrame.collect()
end)

run.("1b. fillna with scalar value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil}, %{"a" => nil, "b" => 2}
  ], schema: "a INT, b INT")
  DataFrame.fillna(df, 99) |> DataFrame.collect()
end)

run.("1c. dropna any", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 2},
    %{"a" => 1, "b" => nil}, %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: "any") |> DataFrame.collect()
end)

run.("1d. dropna all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 2},
    %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: "all") |> DataFrame.collect()
end)

run.("1e. dropna with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c INT")
  DataFrame.dropna(df, how: "any", subset: ["a", "c"]) |> DataFrame.collect()
end)

run.("1f. dropna with thresh", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3},
    %{"a" => 1, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c INT")
  # thresh: min non-null values to keep row
  DataFrame.dropna(df, thresh: 2) |> DataFrame.collect()
end)

# =============================================
# 2. DataFrame.replace
# =============================================

run.("2a. replace values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"color" => "red"}, %{"color" => "blue"}, %{"color" => "green"}
  ], schema: "color STRING")
  DataFrame.replace(df, "red", "crimson") |> DataFrame.collect()
end)

run.("2b. replace with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1}, %{"a" => 2, "b" => 2}, %{"a" => 3, "b" => 3}
  ], schema: "a INT, b INT")
  DataFrame.replace(df, 1, 99, subset: ["a"]) |> DataFrame.collect()
end)

# =============================================
# 3. Streaming with write_stream pattern
# =============================================

run.("3. streaming with write_stream builder", fn ->
  stream = StreamReader.rate(s, 5)
  |> DataFrame.with_column("doubled", Column.multiply(col("value"), lit(2)))

  query = stream
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("ws_test_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(4000)

  active = StreamingQuery.is_active?(query)
  name = StreamingQuery.name(query)
  {:ok, result} = SparkEx.Reader.table(s, "ws_test_#{run_id}") |> DataFrame.collect()

  StreamingQuery.stop(query)
  %{active: active, name: name, count: length(result), sample: Enum.take(result, 3)}
end)

# =============================================
# 4. DataFrame.offset (skip first N rows)
# =============================================

run.("4. offset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")
  df |> DataFrame.sort([asc(col("id"))]) |> DataFrame.offset(7) |> DataFrame.collect()
end)

# =============================================
# 5. DataFrame.limit vs head (difference)
# =============================================

run.("5. limit vs head vs take", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end), schema: "id INT")
  sorted = df |> DataFrame.sort([asc(col("id"))])

  limit_result = sorted |> DataFrame.limit(3) |> DataFrame.collect()
  head_result = DataFrame.head(sorted, 3)
  take_result = DataFrame.take(sorted, 3)
  {limit_result, head_result, take_result}
end)

# =============================================
# 6. Complex: generate, transform, aggregate, window, output
# =============================================

run.("6. full ETL pipeline: generate → transform → agg → window → filter", fn ->
  # Step 1: Generate sales data
  {:ok, sales} = SparkEx.create_dataframe(s,
    Enum.flat_map(1..12, fn month ->
      Enum.map(1..5, fn store ->
        %{
          "store_id" => store,
          "month" => month,
          "revenue" => (store * 1000 + month * 100 + rem(:rand.uniform(500), 500)) * 1.0,
          "region" => if(store <= 3, do: "North", else: "South")
        }
      end)
    end), schema: "store_id INT, month INT, revenue DOUBLE, region STRING")

  # Step 2: Add quarter
  enriched = sales
  |> DataFrame.with_column("quarter",
    when_(Column.lte(col("month"), lit(3)), lit("Q1"))
    |> Column.otherwise(
      when_(Column.lte(col("month"), lit(6)), lit("Q2"))
      |> Column.otherwise(
        when_(Column.lte(col("month"), lit(9)), lit("Q3"))
        |> Column.otherwise(lit("Q4")))))

  # Step 3: Quarterly aggregate by region
  quarterly = enriched
  |> DataFrame.group_by([col("region"), col("quarter")])
  |> GroupedData.agg([
    sum(col("revenue")) |> Column.alias_("total_revenue"),
    avg(col("revenue")) |> Column.alias_("avg_revenue"),
    count(col("revenue")) |> Column.alias_("txn_count")
  ])

  # Step 4: Window: rank quarters by revenue within region
  w = SparkEx.Window.partition_by([col("region")]) |> WindowSpec.order_by([desc(col("total_revenue"))])

  result = quarterly
  |> DataFrame.with_column("rank", row_number() |> Column.over(w))
  |> DataFrame.sort([asc(col("region")), asc(col("rank"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  IO.puts("  #{length(rows)} rows")
  rows
end)

# =============================================
# 7. Complex: multi-level nested struct access
# =============================================

run.("7. deeply nested struct field access", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct(
      'level1', named_struct(
        'level2', named_struct(
          'level3', named_struct(
            'value', 42,
            'name', 'deep'
          )
        )
      )
    ) AS deep
  """)
  df |> DataFrame.select([
    col("deep.level1.level2.level3.value") |> Column.alias_("val"),
    col("deep.level1.level2.level3.name") |> Column.alias_("name")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Multiple temp view operations
# =============================================

run.("8. multiple temp views → complex join SQL", fn ->
  {:ok, products} = SparkEx.create_dataframe(s, [
    %{"pid" => 1, "name" => "Widget", "category" => "A", "price" => 10.0},
    %{"pid" => 2, "name" => "Gadget", "category" => "B", "price" => 25.0},
    %{"pid" => 3, "name" => "Thingamajig", "category" => "A", "price" => 15.0}
  ], schema: "pid INT, name STRING, category STRING, price DOUBLE")

  {:ok, sales} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"sid" => i, "pid" => rem(i, 3) + 1, "qty" => rem(i * 7, 10) + 1}
    end), schema: "sid INT, pid INT, qty INT")

  DataFrame.create_or_replace_temp_view(products, "products_#{run_id}")
  DataFrame.create_or_replace_temp_view(sales, "sales_#{run_id}")

  result = SparkEx.sql(s, """
    WITH totals AS (
      SELECT p.name, p.category, p.price,
             SUM(s.qty) AS total_qty,
             SUM(s.qty * p.price) AS total_revenue
      FROM products_#{run_id} p
      JOIN sales_#{run_id} s ON p.pid = s.pid
      GROUP BY p.name, p.category, p.price
    ),
    ranked AS (
      SELECT *, RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rk
      FROM totals
    )
    SELECT name, category, total_qty, total_revenue, rk
    FROM ranked
    ORDER BY category, rk
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "products_#{run_id}")
  DataFrame.drop_temp_view(s, "sales_#{run_id}")
  result
end)

# =============================================
# 9. Levenshtein with threshold
# =============================================

run.("9. levenshtein / soundex / initcap / ascii / char / base64", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "hello world", "txt2" => "hallo world"}
  ], schema: "txt STRING, txt2 STRING")
  df |> DataFrame.select([
    levenshtein(col("txt"), col("txt2")) |> Column.alias_("edit_dist"),
    soundex(col("txt")) |> Column.alias_("soundex"),
    initcap(col("txt")) |> Column.alias_("initcap"),
    ascii(col("txt")) |> Column.alias_("ascii_first"),
    base64(Column.cast(col("txt"), "BINARY")) |> Column.alias_("b64")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Math edge cases
# =============================================

run.("10. math edge cases: NaN handling, division by zero, overflow", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1.0, "b" => 0.0},
    %{"a" => 0.0, "b" => 0.0},
    %{"a" => -1.0, "b" => 2.0}
  ], schema: "a DOUBLE, b DOUBLE")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.divide(col("a"), col("b")) |> Column.alias_("a_div_b"),
    log(col("a")) |> Column.alias_("log_a"),
    sqrt(col("a")) |> Column.alias_("sqrt_a"),
    isnan(Column.divide(col("a"), col("b"))) |> Column.alias_("is_nan"),
    isnull(col("a")) |> Column.alias_("is_null")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 35 complete ===")
