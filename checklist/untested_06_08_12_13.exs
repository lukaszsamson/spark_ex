# Untested items from sections 6, 8, 12, 13
# Usage: mix run checklist/untested_06_08_12_13.exs

alias SparkEx.{DataFrame, Column, GroupedData, Window, WindowSpec}
import SparkEx.Functions

IO.puts("=== Untested: Sections 6, 8, 12, 13 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 500)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# === 6.10 is_nan ===
IO.puts("--- 6.10 is_nan ---")
run.("is_nan", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1.0), (CAST('NaN' AS DOUBLE)), (3.0) AS t(val)")
  |> DataFrame.select([col("val"), col("val") |> Column.is_nan() |> Column.alias_("is_nan")])
  |> DataFrame.collect()
end)

# === 8.10 grouping_sets ===
IO.puts("--- 8.10 grouping_sets ---")
run.("grouping_sets (via SQL)", fn ->
  SparkEx.sql(s, """
    SELECT dept, name, SUM(salary) AS total
    FROM VALUES ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('sales', 'Carol', 90)
    AS t(dept, name, salary)
    GROUP BY GROUPING SETS ((dept), (name), ())
    ORDER BY dept, name
  """) |> DataFrame.collect()
end)

# === 12.8 range_between ===
IO.puts("--- 12.8 range_between ---")
run.("range_between", fn ->
  wdf = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('eng', 'Eve', 130),
      ('sales', 'Carol', 90), ('sales', 'Dave', 110)
    AS t(dept, name, salary)
  """)
  w = Window.partition_by(["dept"])
    |> WindowSpec.order_by([col("salary") |> Column.asc()])
    |> WindowSpec.range_between(-10, 10)
  wdf
  |> DataFrame.with_column("nearby_sum", sum(col("salary")) |> Column.over(w))
  |> DataFrame.collect()
end)

# === 13.2 dropna all ===
IO.puts("--- 13.2-13.4 dropna variants ---")

null_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 100),
    (NULL, NULL, NULL),
    (3, NULL, 300),
    (4, 'Dave', NULL)
  AS t(id, name, salary)
""")

run.("13.2 dropna all", fn ->
  DataFrame.dropna(null_df, how: :all) |> DataFrame.collect()
end)

run.("13.3 dropna thresh=2", fn ->
  DataFrame.dropna(null_df, thresh: 2) |> DataFrame.collect()
end)

run.("13.4 dropna subset", fn ->
  DataFrame.dropna(null_df, subset: ["name"]) |> DataFrame.collect()
end)

# === 13.7 replace ===
IO.puts("--- 13.7-13.8 replace ---")

run.("13.7 replace (map form)", fn ->
  DataFrame.replace(null_df, %{"Alice" => "ALICE", "Dave" => "DAVE"}) |> DataFrame.collect()
end)

run.("13.8 replace (parallel lists)", fn ->
  DataFrame.replace(null_df, [100, 300], [999, 777]) |> DataFrame.collect()
end)

# === 13.10 summary ===
IO.puts("--- 13.10 summary ---")

run.("summary", fn ->
  sum_df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 100.0), (2, 200.0), (3, 300.0) AS t(id, val)")
  DataFrame.summary(sum_df) |> DataFrame.collect()
end)

IO.puts("=== Sections 6, 8, 12, 13 Complete ===")
