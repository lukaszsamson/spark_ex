# Round 70: More validation gaps + creative edge cases
# Focus: session tags, streaming API validation, GroupedData edge cases,
# DataFrame.to / Column expression edge cases, more protobuf paths
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

# =============================================
# 1. Session tags API validation
# =============================================

run.("1a. add_tag with non-string key", fn ->
  SparkEx.add_tag(s, 42)
end)

run.("1b. add_tag with non-string value", fn ->
  SparkEx.add_tag(s, %{"k" => "v"})
end)

run.("1c. remove_tag with non-string", fn ->
  SparkEx.remove_tag(s, 42)
end)

run.("1d. get_tag with non-string", fn ->
  SparkEx.get_tags(s)
end)

# =============================================
# 2. GroupedData edge cases
# =============================================

run.("2a. group_by with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}, %{"x" => 3}
  ], schema: "x INT")
  DataFrame.group_by(df, [])
  |> GroupedData.agg([count(col("x")) |> Column.alias_("cnt")])
  |> DataFrame.collect()
end)

run.("2b. group_by with non-list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.group_by(df, "x")
end)

run.("2c. GroupedData.pivot with non-string column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"g" => "A", "k" => "x", "v" => 1},
    %{"g" => "A", "k" => "y", "v" => 2}
  ], schema: "g STRING, k STRING, v INT")
  DataFrame.group_by(df, [col("g")])
  |> GroupedData.pivot(42)
  |> GroupedData.agg([sum(col("v"))])
  |> DataFrame.collect()
end)

run.("2d. GroupedData.pivot with non-list values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"g" => "A", "k" => "x", "v" => 1},
    %{"g" => "A", "k" => "y", "v" => 2}
  ], schema: "g STRING, k STRING, v INT")
  DataFrame.group_by(df, [col("g")])
  |> GroupedData.pivot("k", "not_a_list")
  |> GroupedData.agg([sum(col("v"))])
  |> DataFrame.collect()
end)

# =============================================
# 3. Column expression edge cases
# =============================================

run.("3a. Column.between with reversed bounds", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 5}, %{"x" => 10}, %{"x" => 15}
  ], schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    Column.between(col("x"), lit(15), lit(5)) |> Column.alias_("reversed"),
    Column.between(col("x"), lit(5), lit(15)) |> Column.alias_("normal")
  ]) |> DataFrame.collect()
end)

run.("3b. Column.isin with empty list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    Column.isin(col("x"), []) |> Column.alias_("in_empty")
  ]) |> DataFrame.collect()
end)

run.("3c. Column.isin with mixed types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    Column.isin(col("x"), [1, "two", 3.0]) |> Column.alias_("in_mixed")
  ]) |> DataFrame.collect()
end)

run.("3d. Column.substr with negative position", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "hello world"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    Column.substr(col("s"), lit(-5), lit(5)) |> Column.alias_("neg_pos"),
    Column.substr(col("s"), lit(0), lit(5)) |> Column.alias_("zero_pos"),
    Column.substr(col("s"), lit(1), lit(0)) |> Column.alias_("zero_len")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. DataFrame.to validation
# =============================================

run.("4a. DataFrame.to with non-string schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to(df, 42) |> DataFrame.collect()
end)

run.("4b. DataFrame.to with incompatible schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.to(df, "y STRING") |> DataFrame.collect()
end)

# =============================================
# 5. Creative: multiple collects on same DataFrame
# =============================================

run.("5. Double collect same DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  {:ok, r1} = DataFrame.collect(df)
  {:ok, r2} = DataFrame.collect(df)
  {r1, r2, r1 == r2}
end)

# =============================================
# 6. Creative: DataFrame chain with 10 withColumn
# =============================================

run.("6. 10 withColumn chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  result = Enum.reduce(1..10, df, fn i, acc ->
    DataFrame.with_column(acc, "col_#{i}", col("x") |> Column.multiply(lit(i)))
  end)
  result |> DataFrame.collect()
end)

# =============================================
# 7. Creative: nested when/otherwise
# =============================================

run.("7. 5-level nested when/otherwise", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"x" => i} end),
    schema: "x INT")
  df |> DataFrame.select([
    col("x"),
    when_(col("x") |> Column.lt(lit(3)), lit("tiny"))
    |> Column.when_(col("x") |> Column.lt(lit(5)), lit("small"))
    |> Column.when_(col("x") |> Column.lt(lit(7)), lit("medium"))
    |> Column.when_(col("x") |> Column.lt(lit(9)), lit("large"))
    |> Column.otherwise(lit("huge"))
    |> Column.alias_("size")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Creative: self-join with window
# =============================================

run.("8. Self-join + window ranking", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "eng", "name" => "Alice", "salary" => 100},
    %{"dept" => "eng", "name" => "Bob", "salary" => 120},
    %{"dept" => "eng", "name" => "Carol", "salary" => 90},
    %{"dept" => "sales", "name" => "Dave", "salary" => 110},
    %{"dept" => "sales", "name" => "Eve", "salary" => 130}
  ], schema: "dept STRING, name STRING, salary INT")

  w = SparkEx.Window.partition_by([col("dept")])
    |> WindowSpec.order_by([desc(col("salary"))])

  df |> DataFrame.select([
    col("dept"), col("name"), col("salary"),
    rank() |> Column.over(w) |> Column.alias_("rank"),
    sum(col("salary")) |> Column.over(SparkEx.Window.partition_by([col("dept")])) |> Column.alias_("dept_total"),
    (Column.cast(col("salary"), "DOUBLE") |> Column.divide(
      Column.cast(sum(col("salary")) |> Column.over(SparkEx.Window.partition_by([col("dept")])), "DOUBLE")
    )) |> Column.alias_("pct_of_dept")
  ])
  |> DataFrame.sort([asc(col("dept")), asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 9. Creative: complex pipeline with everything
# =============================================

run.("9. Complex pipeline: create → filter → group → window → join → collect", fn ->
  {:ok, orders} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i ->
      %{"order_id" => i,
        "customer_id" => rem(i, 5) + 1,
        "amount" => (rem(i * 997, 200) + 10) * 1.0,
        "status" => if(rem(i, 7) == 0, do: "cancelled", else: "completed")}
    end), schema: "order_id INT, customer_id INT, amount DOUBLE, status STRING")

  {:ok, customers} = SparkEx.create_dataframe(s, [
    %{"customer_id" => 1, "name" => "Alpha"},
    %{"customer_id" => 2, "name" => "Beta"},
    %{"customer_id" => 3, "name" => "Gamma"},
    %{"customer_id" => 4, "name" => "Delta"},
    %{"customer_id" => 5, "name" => "Epsilon"}
  ], schema: "customer_id INT, name STRING")

  # Filter, aggregate, join
  orders
  |> DataFrame.filter(col("status") |> Column.eq(lit("completed")))
  |> DataFrame.group_by([col("customer_id")])
  |> GroupedData.agg([
    sum(col("amount")) |> Column.alias_("total_spend"),
    count(col("order_id")) |> Column.alias_("order_count"),
    avg(col("amount")) |> Column.alias_("avg_order")
  ])
  |> DataFrame.join(customers, col("customer_id"), "inner")
  |> DataFrame.select([
    col("name"), col("total_spend"), col("order_count"), col("avg_order")
  ])
  |> DataFrame.sort([desc(col("total_spend"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. SparkEx.connect validation
# =============================================

run.("10a. connect with empty url", fn ->
  SparkEx.connect(url: "")
end)

run.("10b. connect with no opts", fn ->
  SparkEx.connect([])
end)

# =============================================
# 11. Edge: DataFrame.explain all modes
# =============================================

run.("11. explain modes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  filtered = DataFrame.filter(df, col("x") |> Column.gt(lit(5)))

  simple = DataFrame.explain(filtered, :simple)
  extended = DataFrame.explain(filtered, :extended)
  codegen = DataFrame.explain(filtered, :codegen)
  cost = DataFrame.explain(filtered, :cost)
  formatted = DataFrame.explain(filtered, :formatted)
  {simple, extended, codegen, cost, formatted}
end)

# =============================================
# 12. Edge: DataFrame operations on empty result
# =============================================

run.("12. Operations on empty filtered result", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  empty = DataFrame.filter(df, col("x") |> Column.gt(lit(100)))
  {:ok, count} = DataFrame.count(empty)
  {:ok, rows} = empty |> DataFrame.collect()
  {:ok, desc_df} = DataFrame.describe(empty) |> DataFrame.collect()
  {count, rows, desc_df}
end)

IO.puts("=== Round 70 complete ===")
