# Round 64: Stress tests, edge cases, creative combos
# - Very wide DataFrame (100 columns)
# - Very deep nesting (5-level struct)
# - DataFrame.print_schema / tree_string / html_string
# - DataFrame.count / DataFrame.distinct.count
# - GroupedData.pivot with auto-discover (no values list)
# - Complex when/otherwise with multiple branches
# - Window: ntile, percent_rank, cume_dist, dense_rank
# - Column.between with timestamps
# - from_json + explode + group + collect_set pipeline
# - DataFrame.repartition + coalesce
# - DataFrame.select_expr
# - Catalog.list_functions (search for spark functions)
# - Extremely large create_dataframe (5000 rows)
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
# 1. Very wide DataFrame (100 columns)
# =============================================

run.("1. 100-column DataFrame", fn ->
  cols = Enum.map(1..100, fn i -> {"col_#{String.pad_leading(to_string(i), 3, "0")}", i} end) |> Map.new()
  schema = Enum.map(1..100, fn i -> "col_#{String.pad_leading(to_string(i), 3, "0")} INT" end) |> Enum.join(", ")
  {:ok, df} = SparkEx.create_dataframe(s, [cols], schema: schema)
  {:ok, columns} = DataFrame.columns(df)
  {:ok, rows} = DataFrame.collect(df)
  %{num_columns: length(columns), first_cols: Enum.take(columns, 5), last_cols: Enum.take(columns, -5),
    row_sample: rows |> hd() |> Map.take(["col_001", "col_050", "col_100"])}
end)

# =============================================
# 2. Deep nesting (5-level struct)
# =============================================

run.("2. 5-level nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"l1" => %{"l2" => %{"l3" => %{"l4" => %{"l5" => "deep_value"}}}}}
  ], schema: "l1 STRUCT<l2: STRUCT<l3: STRUCT<l4: STRUCT<l5: STRING>>>>")
  df |> DataFrame.select([
    col("l1"),
    col("l1")
    |> Column.get_field("l2")
    |> Column.get_field("l3")
    |> Column.get_field("l4")
    |> Column.get_field("l5")
    |> Column.alias_("deep_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. DataFrame.print_schema / tree_string / html_string
# =============================================

run.("3a. print_schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"name" => "Alice", "scores" => [90, 85]}}
  ], schema: "id INT, nested STRUCT<name: STRING, scores: ARRAY<INT>>")
  DataFrame.print_schema(df)
end)

run.("3b. tree_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "test"}],
    schema: "id INT, val STRING")
  DataFrame.tree_string(df)
end)

run.("3c. html_string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.html_string(df)
end)

# =============================================
# 4. DataFrame.count / distinct
# =============================================

run.("4a. DataFrame.count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..500, fn i -> %{"val" => rem(i, 50)} end),
    schema: "val INT")
  DataFrame.count(df)
end)

run.("4b. distinct + count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..500, fn i -> %{"val" => rem(i, 50)} end),
    schema: "val INT")
  df |> DataFrame.distinct() |> DataFrame.count()
end)

# =============================================
# 5. GroupedData.pivot without values list (auto-discover)
# =============================================

run.("5. Pivot auto-discover values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"region" => "East", "product" => "A", "sales" => 100},
    %{"region" => "East", "product" => "B", "sales" => 200},
    %{"region" => "West", "product" => "A", "sales" => 150},
    %{"region" => "West", "product" => "C", "sales" => 300}
  ], schema: "region STRING, product STRING, sales INT")
  df
  |> DataFrame.group_by([col("region")])
  |> GroupedData.pivot(col("product"))
  |> GroupedData.agg([sum(col("sales")) |> Column.alias_("total")])
  |> DataFrame.sort([asc(col("region"))])
  |> DataFrame.collect()
end)

# =============================================
# 6. Complex when/otherwise (8 branches)
# =============================================

run.("6. when/otherwise with 8 branches", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(0..100, fn i -> %{"score" => i} end),
    schema: "score INT")
  df |> DataFrame.with_column("grade",
    Column.when_(Column.gte(col("score"), lit(97)), lit("A+"))
    |> Column.when_(Column.gte(col("score"), lit(93)), lit("A"))
    |> Column.when_(Column.gte(col("score"), lit(90)), lit("A-"))
    |> Column.when_(Column.gte(col("score"), lit(87)), lit("B+"))
    |> Column.when_(Column.gte(col("score"), lit(83)), lit("B"))
    |> Column.when_(Column.gte(col("score"), lit(80)), lit("B-"))
    |> Column.when_(Column.gte(col("score"), lit(70)), lit("C"))
    |> Column.when_(Column.gte(col("score"), lit(60)), lit("D"))
    |> Column.otherwise(lit("F"))
  )
  |> DataFrame.group_by([col("grade")])
  |> GroupedData.agg([
    count(col("score")) |> Column.alias_("count"),
    min(col("score")) |> Column.alias_("min_score"),
    max(col("score")) |> Column.alias_("max_score")
  ])
  |> DataFrame.sort([desc(col("min_score"))])
  |> DataFrame.collect()
end)

# =============================================
# 7. Window: ntile, percent_rank, cume_dist, dense_rank
# =============================================

run.("7. Window ranking functions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => rem(i * 7, 15) + 1} end),
    schema: "id INT, val INT")

  w = SparkEx.Window.order_by([desc(col("val"))])

  df |> DataFrame.select([
    col("id"), col("val"),
    row_number() |> Column.over(w) |> Column.alias_("row_num"),
    rank() |> Column.over(w) |> Column.alias_("rank"),
    dense_rank() |> Column.over(w) |> Column.alias_("dense_rank"),
    ntile(lit(4)) |> Column.over(w) |> Column.alias_("quartile"),
    percent_rank() |> Column.over(w) |> Column.alias_("pct_rank"),
    cume_dist() |> Column.over(w) |> Column.alias_("cume_dist")
  ])
  |> DataFrame.limit(10)
  |> DataFrame.collect()
end)

# =============================================
# 8. Column.between with timestamps
# =============================================

run.("8. Column.between with dates", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dt" => "2024-01-15"},
    %{"dt" => "2024-06-30"},
    %{"dt" => "2024-12-25"},
    %{"dt" => "2023-06-01"}
  ], schema: "dt STRING")
  df |> DataFrame.with_column("date", to_date(col("dt")))
  |> DataFrame.select([
    col("date"),
    Column.between(col("date"), lit("2024-01-01"), lit("2024-12-31"))
    |> Column.alias_("in_2024")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. DataFrame.repartition + coalesce
# =============================================

run.("9a. repartition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  before = DataFrame.rdd_num_partitions_approx(df)
  repart = DataFrame.repartition(df, 8)
  after_ = DataFrame.rdd_num_partitions_approx(repart)
  %{before: before, after_repartition: after_}
end)

run.("9b. coalesce", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  repart = DataFrame.repartition(df, 8)
  after_repart = DataFrame.rdd_num_partitions_approx(repart)
  coalesced = DataFrame.coalesce(repart, 2)
  after_coalesce = DataFrame.rdd_num_partitions_approx(coalesced)
  %{after_repartition: after_repart, after_coalesce: after_coalesce}
end)

# =============================================
# 10. DataFrame.select_expr
# =============================================

run.("10. select_expr with SQL expressions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 100, "name" => "Alice"},
    %{"id" => 2, "val" => 200, "name" => "Bob"}
  ], schema: "id INT, val INT, name STRING")
  df |> DataFrame.select_expr(["id", "val * 2 as doubled", "upper(name) as upper_name", "id + val as combined"])
  |> DataFrame.collect()
end)

# =============================================
# 11. Catalog.list_functions
# =============================================

run.("11. Catalog.list_functions (first 10)", fn ->
  {:ok, fns} = Catalog.list_functions(s)
  %{total: length(fns), first_10: Enum.take(fns, 10) |> Enum.map(fn f -> f.name end)}
end)

# =============================================
# 12. Large create_dataframe (5000 rows)
# =============================================

run.("12. 5000-row create_dataframe + aggregate", fn ->
  rows = Enum.map(1..5000, fn i ->
    %{"id" => i,
      "grp" => Enum.at(["A", "B", "C", "D", "E"], rem(i, 5)),
      "val" => rem(i * 997, 1000) * 1.0}
  end)
  {:ok, df} = SparkEx.create_dataframe(s, rows, schema: "id INT, grp STRING, val DOUBLE")
  {:ok, total} = DataFrame.count(df)
  {:ok, agg} = df
    |> DataFrame.group_by([col("grp")])
    |> GroupedData.agg([
      count(col("id")) |> Column.alias_("n"),
      avg(col("val")) |> Column.alias_("avg_val"),
      min(col("val")) |> Column.alias_("min_val"),
      max(col("val")) |> Column.alias_("max_val")
    ])
    |> DataFrame.sort([asc(col("grp"))])
    |> DataFrame.collect()
  %{total_rows: total, groups: agg}
end)

# =============================================
# 13. Creative: from_json + explode + collect_set pipeline
# =============================================

run.("13. from_json → explode → collect_set pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"user" => "Alice", "tags_json" => ~s(["python","java","elixir"])},
    %{"user" => "Bob", "tags_json" => ~s(["java","rust","go"])},
    %{"user" => "Carol", "tags_json" => ~s(["python","elixir","go"])}
  ], schema: "user STRING, tags_json STRING")

  df
  |> DataFrame.with_column("tags", from_json(col("tags_json"), "ARRAY<STRING>"))
  |> DataFrame.select([
    col("user"),
    explode(col("tags")) |> Column.alias_("tag")
  ])
  |> DataFrame.group_by([col("tag")])
  |> GroupedData.agg([
    collect_set(col("user")) |> Column.alias_("users"),
    count(col("user")) |> Column.alias_("user_count")
  ])
  |> DataFrame.sort([desc(col("user_count")), asc(col("tag"))])
  |> DataFrame.collect()
end)

# =============================================
# 14. Creative: self-join for pairs + ranking
# =============================================

run.("14. Self-join for all pairs + similarity ranking", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95},
    %{"id" => 2, "name" => "Bob", "score" => 88},
    %{"id" => 3, "name" => "Carol", "score" => 92},
    %{"id" => 4, "name" => "Dave", "score" => 85}
  ], schema: "id INT, name STRING, score INT")

  a = DataFrame.alias_(df, "a")
  b = DataFrame.alias_(df, "b")

  DataFrame.join(a, b,
    Column.lt(Column.isin("a", "id"), Column.isin("b", "id")),
    "inner")
  |> DataFrame.select([
    Column.isin("a", "name") |> Column.alias_("person_1"),
    Column.isin("b", "name") |> Column.alias_("person_2"),
    Column.isin("a", "score") |> Column.alias_("score_1"),
    Column.isin("b", "score") |> Column.alias_("score_2"),
    SparkEx.Functions.abs(Column.minus(
      Column.isin("a", "score"),
      Column.isin("b", "score"))) |> Column.alias_("score_diff")
  ])
  |> DataFrame.sort([asc(col("score_diff"))])
  |> DataFrame.collect()
end)

# =============================================
# 15. Edge: union chain (5 DataFrames)
# =============================================

run.("15. 5-DataFrame union chain", fn ->
  dfs = Enum.map(1..5, fn batch ->
    {:ok, df} = SparkEx.create_dataframe(s,
      Enum.map(1..10, fn i -> %{"batch" => batch, "id" => (batch - 1) * 10 + i} end),
      schema: "batch INT, id INT")
    df
  end)

  [first | rest] = dfs
  combined = Enum.reduce(rest, first, fn df, acc -> DataFrame.union(acc, df) end)

  {:ok, count} = DataFrame.count(combined)
  {:ok, rows} = combined
    |> DataFrame.group_by([col("batch")])
    |> GroupedData.agg([count(col("id")) |> Column.alias_("n")])
    |> DataFrame.sort([asc(col("batch"))])
    |> DataFrame.collect()
  %{total: count, per_batch: rows}
end)

IO.puts("=== Round 64 complete ===")
