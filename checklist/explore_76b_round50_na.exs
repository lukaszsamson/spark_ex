# Round 50b: NA functions with correct API, cleanup leftover tables
alias SparkEx.{DataFrame, Column, Catalog}
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
# Clean up leftover tables from Round 49 protobuf tests
# =============================================

run.("0a. cleanup dummy_1771809557255", fn ->
  Catalog.drop_table(s, "sfx.sfx_dev_warehouse.dummy_1771809557255", if_exists: true, purge: true)
end)

run.("0b. cleanup dummy2_1771809557255", fn ->
  Catalog.drop_table(s, "sfx.sfx_dev_warehouse.dummy2_1771809557255", if_exists: true, purge: true)
end)

# =============================================
# 1. NA.fill (via DataFrame.fillna)
# =============================================

run.("1a. fillna with scalar", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}, %{"id" => 2, "val" => nil}, %{"id" => 3, "val" => nil}
  ], schema: "id INT, val INT")
  DataFrame.fillna(df, 0) |> DataFrame.collect()
end)

run.("1b. fillna with per-column map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => nil, "score" => nil}, %{"name" => "Bob", "score" => 80}
  ], schema: "name STRING, score INT")
  DataFrame.fillna(df, %{"name" => "Unknown", "score" => 0}) |> DataFrame.collect()
end)

run.("1c. fillna with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil}, %{"a" => 1, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.fillna(df, 99, subset: ["a"]) |> DataFrame.collect()
end)

# =============================================
# 2. NA.drop (via DataFrame.dropna)
# =============================================

run.("2a. dropna with how: :any", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}, %{"a" => nil, "b" => 3}, %{"a" => 4, "b" => nil}, %{"a" => 5, "b" => 6}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: :any) |> DataFrame.collect()
end)

run.("2b. dropna with how: :all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil}, %{"a" => nil, "b" => 3}, %{"a" => 4, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: :all) |> DataFrame.collect()
end)

run.("2c. dropna with thresh", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => nil},
    %{"a" => 1, "b" => 2, "c" => nil},
    %{"a" => 1, "b" => 2, "c" => 3}
  ], schema: "a INT, b INT, c INT")
  DataFrame.dropna(df, thresh: 2) |> DataFrame.collect()
end)

run.("2d. dropna with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => 1}, %{"a" => 2, "b" => nil}, %{"a" => 3, "b" => 3}
  ], schema: "a INT, b INT")
  # Only check column "a" for nulls
  DataFrame.dropna(df, how: :any, subset: ["a"]) |> DataFrame.collect()
end)

# =============================================
# 3. NA.replace (via SparkEx.DataFrame.NA directly)
# =============================================

run.("3a. NA.replace with map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}, %{"val" => 1}
  ], schema: "val INT")
  SparkEx.DataFrame.NA.replace(df, %{1 => 100, 2 => 200}) |> DataFrame.collect()
end)

run.("3b. NA.replace with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1}, %{"a" => 2, "b" => 2}
  ], schema: "a INT, b INT")
  SparkEx.DataFrame.NA.replace(df, %{1 => 100}, subset: ["a"]) |> DataFrame.collect()
end)

run.("3c. NA.replace string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Alice"}
  ], schema: "name STRING")
  SparkEx.DataFrame.NA.replace(df, %{"Alice" => "ALICE", "Bob" => "BOB"}) |> DataFrame.collect()
end)

# =============================================
# 4. Complex: NA pipeline → Stat → Write
# =============================================

run.("4. NA fill → window rank → write → read", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"student" => "A", "math" => 90, "science" => nil},
    %{"student" => "B", "math" => nil, "science" => 85},
    %{"student" => "C", "math" => 75, "science" => 80},
    %{"student" => "D", "math" => 88, "science" => 92},
    %{"student" => "E", "math" => 65, "science" => 70},
    %{"student" => "F", "math" => 95, "science" => nil},
    %{"student" => "G", "math" => nil, "science" => 88}
  ], schema: "student STRING, math INT, science INT")

  w = SparkEx.Window.order_by([desc(col("total"))])

  pipeline = df
    |> DataFrame.fillna(%{"math" => 50, "science" => 50})
    |> DataFrame.with_column("total", Column.plus(col("math"), col("science")))
    |> DataFrame.with_column("rank", dense_rank() |> Column.over(w))
    |> DataFrame.sort([asc(col("rank"))])

  table = "sfx.sfx_dev_warehouse.spark_ex_r50b_pipeline_#{run_id}"
  DataFrame.write_v2(pipeline, table) |> SparkEx.WriterV2.create()

  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("rank"))])
    |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

# =============================================
# 5. Edge: fillna with wrong type (string into INT column)
# =============================================

run.("5. fillna string into INT column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => nil}
  ], schema: "val INT")
  DataFrame.fillna(df, "hello") |> DataFrame.collect()
end)

# =============================================
# 6. Edge: dropna on all-null row DataFrame
# =============================================

run.("6. dropna on all-null DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: :any) |> DataFrame.collect()
end)

# =============================================
# 7. Stat: approx_quantile multi-column
# =============================================

run.("7. approx_quantile multi-column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"a" => i * 1.0, "b" => (101 - i) * 1.0} end),
    schema: "a DOUBLE, b DOUBLE")
  SparkEx.DataFrame.Stat.approx_quantile(df, ["a", "b"], [0.25, 0.5, 0.75], 0.01)
end)

# =============================================
# 8. Complex: crosstab → pivot comparison
# =============================================

run.("8. crosstab vs manual pivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => "A", "y" => "P"}, %{"x" => "A", "y" => "Q"},
    %{"x" => "A", "y" => "P"}, %{"x" => "B", "y" => "P"},
    %{"x" => "B", "y" => "Q"}, %{"x" => "B", "y" => "Q"},
    %{"x" => "C", "y" => "P"}
  ], schema: "x STRING, y STRING")

  # Crosstab
  {:ok, ct_rows} = DataFrame.crosstab(df, "x", "y") |> DataFrame.collect()

  # Manual pivot
  {:ok, pv_rows} = df
    |> DataFrame.group_by([col("x")])
    |> SparkEx.GroupedData.pivot(col("y"))
    |> SparkEx.GroupedData.agg([count(col("y")) |> Column.alias_("cnt")])
    |> DataFrame.sort([asc(col("x"))])
    |> DataFrame.collect()

  %{crosstab: ct_rows, pivot: pv_rows}
end)

IO.puts("=== Round 50b complete ===")
