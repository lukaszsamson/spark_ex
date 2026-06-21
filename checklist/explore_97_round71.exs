# Round 71: Deeper into less-tested areas
# - Streaming write/read lifecycle with validation gaps
# - MergeIntoWriter validation (even if server rejects)
# - UDFRegistration edge cases
# - Session lifecycle (release, interrupt)
# - DataFrame.foreach / foreach_partition edge cases
# - Observation lifecycle + double observation
# - NA functions with complex types
# - Stat functions with edge data
# - encode/decode with BINARY type
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
# 1. Streaming lifecycle validation
# =============================================

run.("1a. StreamReader with non-string format", fn ->
  SparkEx.StreamReader.new(s) |> SparkEx.StreamReader.format(42)
end)

run.("1b. StreamReader with non-string option key", fn ->
  SparkEx.StreamReader.new(s) |> SparkEx.StreamReader.option(42, "val")
end)

run.("1c. StreamWriter with non-string format", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.format(42)
end)

run.("1d. StreamWriter with non-string output_mode", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.output_mode(42)
end)

run.("1e. StreamWriter with non-string query_name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.query_name(42)
end)

run.("1f. StreamWriter.trigger with non-string type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_stream(df) |> SparkEx.StreamWriter.trigger(42)
end)

# =============================================
# 2. UDFRegistration edge cases
# =============================================

run.("2a. register_java with non-string class_name", fn ->
  SparkEx.UDFRegistration.register_java(s, "my_udf", 42)
end)

run.("2b. register_java with non-string name", fn ->
  SparkEx.UDFRegistration.register_java(s, 42, "com.example.MyUDF")
end)

# =============================================
# 3. Session lifecycle
# =============================================

run.("3a. interrupt_all", fn ->
  SparkEx.interrupt_all(s)
end)

run.("3b. interrupt_all then query", fn ->
  SparkEx.interrupt_all(s)
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.collect()
end)

# =============================================
# 4. foreach / foreach_partition
# =============================================

run.("4a. foreach with function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  DataFrame.foreach_local(df, fn _row -> :ok end)
end)

run.("4b. foreach_partition with function", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  DataFrame.foreach_partition_local(df, fn _partition -> :ok end)
end)

# =============================================
# 5. Observation lifecycle
# =============================================

run.("5a. observe single metric then get", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"x" => i} end), schema: "x INT")
  obs_name = "test_obs_r71"
  {:ok, rows} = DataFrame.observe(df, obs_name, [
    count(col("x")) |> Column.alias_("cnt"),
    sum(col("x")) |> Column.alias_("total")
  ]) |> DataFrame.collect()
  # Try to get observation
  obs = SparkEx.Observation.new(obs_name)
  {Enum.count(rows), obs}
end)

run.("5b. Two observations on same DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"x" => i, "y" => rem(i, 3)} end),
    schema: "x INT, y INT")
  {:ok, rows} = df
  |> DataFrame.observe("obs_a", [count(col("x")) |> Column.alias_("cnt")])
  |> DataFrame.observe("obs_b", [sum(col("x")) |> Column.alias_("total")])
  |> DataFrame.collect()

  obs_a = SparkEx.Observation.new("obs_a")
  obs_b = SparkEx.Observation.new("obs_b")
  {Enum.count(rows), obs_a, obs_b}
end)

# =============================================
# 6. NA functions with edge data
# =============================================

run.("6a. dropna on all-null DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => nil, "y" => nil}
  ], schema: "x INT, y STRING")
  SparkEx.DataFrame.NA.drop(df) |> DataFrame.collect()
end)

run.("6b. fillna on all-null DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => nil, "y" => nil}
  ], schema: "x INT, y STRING")
  SparkEx.DataFrame.NA.fill(df, %{"x" => 0, "y" => "default"}) |> DataFrame.collect()
end)

run.("6c. replace on no matching values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1}, %{"x" => 2}
  ], schema: "x INT")
  SparkEx.DataFrame.NA.replace(df, ["x"], %{999 => 0}) |> DataFrame.collect()
end)

run.("6d. dropna with thresh > column count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1, "y" => nil},
    %{"x" => nil, "y" => nil}
  ], schema: "x INT, y INT")
  SparkEx.DataFrame.NA.drop(df, how: "any", thresh: 100) |> DataFrame.collect()
end)

# =============================================
# 7. Stat functions with edge data
# =============================================

run.("7a. corr on single row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1.0, "y" => 2.0}
  ], schema: "x DOUBLE, y DOUBLE")
  DataFrame.corr(df, "x", "y")
end)

run.("7b. cov on single row", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 1.0, "y" => 2.0}
  ], schema: "x DOUBLE, y DOUBLE")
  DataFrame.cov(df, "x", "y")
end)

run.("7c. corr on all-null data", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => nil, "y" => nil},
    %{"x" => nil, "y" => nil}
  ], schema: "x DOUBLE, y DOUBLE")
  DataFrame.corr(df, "x", "y")
end)

run.("7d. approx_quantile on empty result", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "x DOUBLE")
  DataFrame.approx_quantile(df, ["x"], [0.5], 0.0)
end)

run.("7e. freq_items on single-value column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn _ -> %{"x" => "same"} end), schema: "x STRING")
  {:ok, result} = DataFrame.freq_items(df, ["x"], 0.01) |> DataFrame.collect()
  result
end)

# =============================================
# 8. BINARY type
# =============================================

run.("8a. BINARY via SQL", fn ->
  df = SparkEx.sql(s, "SELECT CAST('hello' AS BINARY) AS bin_val")
  df |> DataFrame.collect()
end)

run.("8b. BINARY comparison", fn ->
  df = SparkEx.sql(s, """
    SELECT
      CAST('hello' AS BINARY) AS b1,
      CAST('world' AS BINARY) AS b2,
      CAST('hello' AS BINARY) = CAST('hello' AS BINARY) AS eq
  """)
  df |> DataFrame.collect()
end)

# =============================================
# 9. Complex: lateral column alias
# =============================================

run.("9. Lateral column alias", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"price" => 100.0, "qty" => 5},
    %{"price" => 200.0, "qty" => 3}
  ], schema: "price DOUBLE, qty INT")
  df |> DataFrame.select([
    col("price"), col("qty"),
    (col("price") |> Column.multiply(Column.cast(col("qty"), "DOUBLE"))) |> Column.alias_("subtotal")
  ]) |> DataFrame.select([
    col("price"), col("qty"), col("subtotal"),
    (col("subtotal") |> Column.multiply(lit(1.1))) |> Column.alias_("with_tax")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Complex: multiple windows in same select
# =============================================

run.("10. Multiple window functions with different specs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "A", "name" => "p1", "val" => 10},
    %{"dept" => "A", "name" => "p2", "val" => 20},
    %{"dept" => "A", "name" => "p3", "val" => 30},
    %{"dept" => "B", "name" => "p4", "val" => 15},
    %{"dept" => "B", "name" => "p5", "val" => 25}
  ], schema: "dept STRING, name STRING, val INT")

  w_dept = SparkEx.Window.partition_by([col("dept")])
  w_rank = w_dept |> WindowSpec.order_by([desc(col("val"))])
  w_global = SparkEx.Window.order_by([desc(col("val"))])

  df |> DataFrame.select([
    col("dept"), col("name"), col("val"),
    rank() |> Column.over(w_rank) |> Column.alias_("dept_rank"),
    rank() |> Column.over(w_global) |> Column.alias_("global_rank"),
    sum(col("val")) |> Column.over(w_dept) |> Column.alias_("dept_sum"),
    avg(Column.cast(col("val"), "DOUBLE")) |> Column.over(w_dept) |> Column.alias_("dept_avg"),
    lag(col("val"), 1) |> Column.over(w_rank) |> Column.alias_("prev_val"),
    lead(col("val"), 1) |> Column.over(w_rank) |> Column.alias_("next_val")
  ])
  |> DataFrame.sort([asc(col("dept")), desc(col("val"))])
  |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.tail
# =============================================

run.("11a. tail(3)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"x" => i} end), schema: "x INT")
  DataFrame.tail(df, 3)
end)

run.("11b. tail(0)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.tail(df, 0)
end)

run.("11c. tail(-1)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.tail(df, -1)
end)

run.("11d. head(0)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.head(df, 0)
end)

run.("11e. first()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 42}, %{"x" => 99}
  ], schema: "x INT")
  DataFrame.first(df)
end)

IO.puts("=== Round 71 complete ===")
