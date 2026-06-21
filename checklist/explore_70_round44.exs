# Round 44: Creative combinations, complex pipelines, more edge cases
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
# 1. Complex: diamond pipeline (same source, two transforms, join back)
# =============================================

run.("1. diamond pipeline: fork → transform → join", fn ->
  {:ok, base} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  left = base
    |> DataFrame.alias_("l")
    |> DataFrame.filter(Column.lte(col("val"), lit(100)))
    |> DataFrame.with_column("label", lit("low"))

  right = base
    |> DataFrame.alias_("r")
    |> DataFrame.filter(Column.gt(col("val"), lit(100)))
    |> DataFrame.with_column("label", lit("high"))

  DataFrame.union(left, right)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 2. Complex: multi-level window ranking
# =============================================

run.("2. multi-level window: rank within dept, then percentile across all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..60, fn i ->
      %{"dept" => Enum.at(["Eng", "Sales", "Mktg"], rem(i, 3)),
        "salary" => 40000 + rem(i * 1337, 50000)}
    end), schema: "dept STRING, salary INT")

  dept_w = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([desc(col("salary"))])
  all_w = SparkEx.Window.order_by([desc(col("salary"))])

  df
  |> DataFrame.with_column("dept_rank", rank() |> Column.over(dept_w))
  |> DataFrame.with_column("global_pct", percent_rank() |> Column.over(all_w))
  |> DataFrame.filter(Column.lte(col("dept_rank"), lit(3)))
  |> DataFrame.sort([asc(col("dept")), asc(col("dept_rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 3. Complex: pivot with aggregation
# =============================================

run.("3. pivot with sum aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.flat_map(1..4, fn q ->
      Enum.map(["A", "B", "C"], fn p ->
        %{"quarter" => "Q#{q}", "product" => p, "revenue" => q * 1000 + :erlang.phash2(p, 500)}
      end)
    end), schema: "quarter STRING, product STRING, revenue INT")

  df
  |> DataFrame.group_by([col("product")])
  |> GroupedData.pivot(col("quarter"), ["Q1", "Q2", "Q3", "Q4"])
  |> GroupedData.agg([sum(col("revenue")) |> Column.alias_("rev")])
  |> DataFrame.sort([asc(col("product"))])
  |> DataFrame.collect()
end)

# =============================================
# 4. Complex: explode → aggregate → join back
# =============================================

run.("4. explode array → agg → join back to original", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a", "b", "c"]},
    %{"id" => 2, "tags" => ["b", "c", "d"]},
    %{"id" => 3, "tags" => ["a", "d"]}
  ], schema: "id INT, tags ARRAY<STRING>")

  # Explode tags, count per tag, join back
  tag_counts = df
    |> DataFrame.select([col("id"), explode(col("tags")) |> Column.alias_("tag")])
    |> DataFrame.group_by([col("tag")])
    |> GroupedData.agg([
      count(col("id")) |> Column.alias_("tag_count"),
      collect_list(col("id")) |> Column.alias_("ids_with_tag")
    ])
    |> DataFrame.sort([desc(col("tag_count")), asc(col("tag"))])
    |> DataFrame.collect()

  tag_counts
end)

# =============================================
# 5. Complex: cumulative sum with window
# =============================================

run.("5. running/cumulative sum", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..12, fn m -> %{"month" => m, "revenue" => 10000 + rem(m * 1337, 5000)} end),
    schema: "month INT, revenue INT")

  w = SparkEx.Window.order_by([asc(col("month"))])
    |> WindowSpec.rows_between(:unbounded, :current_row)

  df |> DataFrame.select([
    col("month"), col("revenue"),
    sum(col("revenue")) |> Column.over(w) |> Column.alias_("cumulative_revenue"),
    avg(col("revenue")) |> Column.over(w) |> Column.alias_("running_avg")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Complex: self-join for sequential pairs
# =============================================

run.("6. self-join for consecutive pairs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * i} end),
    schema: "id INT, val INT")

  curr = DataFrame.alias_(df, "curr")
  next = DataFrame.alias_(df, "next")

  DataFrame.join(curr, next,
    Column.eq(col("curr.id"), Column.minus(col("next.id"), lit(1))), "inner")
  |> DataFrame.select([
    col("curr.id") |> Column.alias_("id"),
    col("curr.val") |> Column.alias_("curr_val"),
    col("next.val") |> Column.alias_("next_val"),
    Column.minus(col("next.val"), col("curr.val")) |> Column.alias_("delta")
  ])
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

# =============================================
# 7. Complex: map_from_arrays → element access
# =============================================

run.("7. map creation → element access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"keys" => ["name", "age", "city"], "vals" => ["Alice", "30", "NYC"]}
  ], schema: "keys ARRAY<STRING>, vals ARRAY<STRING>")

  df |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("mp"),
    element_at(map_from_arrays(col("keys"), col("vals")), lit("name")) |> Column.alias_("name"),
    element_at(map_from_arrays(col("keys"), col("vals")), lit("age")) |> Column.alias_("age")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Complex: except_all / intersect_all
# =============================================

run.("8. except_all and intersect_all", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 3}, %{"id" => 3}
  ], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 3}, %{"id" => 3}
  ], schema: "id INT")

  except_result = DataFrame.except_all(df1, df2) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  intersect_result = DataFrame.intersect_all(df1, df2) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  %{except_all: except_result, intersect_all: intersect_result}
end)

# =============================================
# 9. Complex: coalesce chain
# =============================================

run.("9. coalesce chain with multiple null columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => 3},
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  df |> DataFrame.select([
    col("a"), col("b"), col("c"),
    coalesce([col("a"), col("b"), col("c")]) |> Column.alias_("first_non_null")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: posexplode via SQL
# =============================================

run.("10. posexplode via SQL", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [10, 20, 30]},
    %{"id" => 2, "arr" => [40, 50]}
  ], schema: "id INT, arr ARRAY<INT>")
  DataFrame.create_temp_view(df, "pex_#{run_id}")
  result = SparkEx.sql(s, """
    SELECT id, pos, val
    FROM pex_#{run_id}
    LATERAL VIEW posexplode(arr) AS pos, val
  """) |> DataFrame.collect()
  DataFrame.drop_temp_view(s, "pex_#{run_id}")
  result
end)

# =============================================
# 11. Complex: multiple streaming queries simultaneously
# =============================================

run.("11. two simultaneous streaming queries", fn ->
  # Start two streaming queries on the same rate source
  stream1 = StreamReader.rate(s, rows_per_second: 5)
  stream2 = StreamReader.rate(s, rows_per_second: 10)

  {:ok, q1} = stream1
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("multi_q1_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  {:ok, q2} = stream2
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.query_name("multi_q2_#{run_id}")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.start()

  Process.sleep(3000)

  {:ok, r1} = SparkEx.Reader.table(s, "multi_q1_#{run_id}") |> DataFrame.collect()
  {:ok, r2} = SparkEx.Reader.table(s, "multi_q2_#{run_id}") |> DataFrame.collect()

  active = SparkEx.StreamingQueryManager.list_active(s)

  StreamingQuery.stop(q1)
  StreamingQuery.stop(q2)

  %{q1_count: length(r1), q2_count: length(r2),
    active_queries: (case active do {:ok, list} -> length(list); _ -> active end)}
end)

# =============================================
# 12. Complex: to_json → from_json roundtrip
# =============================================

run.("12. to_json → from_json roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30, "score" => 95.5},
    %{"name" => "Bob", "age" => 25, "score" => 88.0}
  ], schema: "name STRING, age INT, score DOUBLE")

  # Convert to JSON string
  json_df = df |> DataFrame.select([
    to_json(SparkEx.Functions.struct([col("name"), col("age"), col("score")])) |> Column.alias_("json_str")
  ])

  # Parse back
  json_df |> DataFrame.select([
    col("json_str"),
    from_json(col("json_str"), "name STRING, age INT, score DOUBLE")
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Complex: create_dataframe with 200 rows + full pipeline
# =============================================

run.("13. 200-row ETL pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"customer_id" => rem(i, 50) + 1,
        "product" => Enum.at(["Widget", "Gadget", "Doohickey", "Thingamajig"], rem(i, 4)),
        "amount" => (rem(i * 997, 100) + 1) * 1.0,
        "quarter" => "Q#{rem(i, 4) + 1}"}
    end), schema: "customer_id INT, product STRING, amount DOUBLE, quarter STRING")

  # Full ETL: filter → enrich → group → window → sort → limit
  result = df
  |> DataFrame.filter(Column.gt(col("amount"), lit(20.0)))
  |> DataFrame.with_column("category",
    when_(Column.gte(col("amount"), lit(75.0)), lit("Premium"))
    |> Column.otherwise(
      when_(Column.gte(col("amount"), lit(50.0)), lit("Standard"))
      |> Column.otherwise(lit("Basic"))))
  |> DataFrame.group_by([col("product"), col("category")])
  |> GroupedData.agg([
    count(col("customer_id")) |> Column.alias_("order_count"),
    sum(col("amount")) |> Column.alias_("total_revenue"),
    avg(col("amount")) |> Column.alias_("avg_order")
  ])
  |> DataFrame.transform(fn d ->
    w = SparkEx.Window.partition_by([col("product")]) |> WindowSpec.order_by([desc(col("total_revenue"))])
    d |> DataFrame.with_column("revenue_rank", rank() |> Column.over(w))
  end)
  |> DataFrame.sort([asc(col("product")), asc(col("revenue_rank"))])
  |> DataFrame.collect()

  {:ok, rows} = result
  IO.puts("  #{length(rows)} result rows")
  Enum.take(rows, 6)
end)

# =============================================
# 14. DataFrame.unpivot
# =============================================

run.("14. unpivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "q1" => 100, "q2" => 200, "q3" => 300, "q4" => 400},
    %{"id" => 2, "q1" => 150, "q2" => 250, "q3" => 350, "q4" => 450}
  ], schema: "id INT, q1 INT, q2 INT, q3 INT, q4 INT")
  df |> DataFrame.unpivot([col("id")], [col("q1"), col("q2"), col("q3"), col("q4")],
    "quarter", "revenue")
    |> DataFrame.sort([asc(col("id")), asc(col("quarter"))])
    |> DataFrame.collect()
end)

# =============================================
# 15. Deeply nested expressions
# =============================================

run.("15. deeply nested when/otherwise (8 levels)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(0..100, fn i -> %{"score" => i} end), schema: "score INT")

  grade =
    when_(Column.gte(col("score"), lit(97)), lit("A+"))
    |> Column.otherwise(
      when_(Column.gte(col("score"), lit(93)), lit("A"))
      |> Column.otherwise(
        when_(Column.gte(col("score"), lit(90)), lit("A-"))
        |> Column.otherwise(
          when_(Column.gte(col("score"), lit(87)), lit("B+"))
          |> Column.otherwise(
            when_(Column.gte(col("score"), lit(83)), lit("B"))
            |> Column.otherwise(
              when_(Column.gte(col("score"), lit(80)), lit("B-"))
              |> Column.otherwise(
                when_(Column.gte(col("score"), lit(70)), lit("C"))
                |> Column.otherwise(lit("F"))))))))

  df
  |> DataFrame.with_column("grade", grade)
  |> DataFrame.group_by([col("grade")])
  |> GroupedData.agg([count(col("score")) |> Column.alias_("cnt")])
  |> DataFrame.sort([asc(col("grade"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 44 complete ===")
