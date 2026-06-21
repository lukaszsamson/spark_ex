# Round 36c: Verify grouping_sets without grouping(), more edge cases
alias SparkEx.{DataFrame, Column, GroupedData, Observation}
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
# 1. grouping_sets WITHOUT grouping() function
# =============================================

run.("1. grouping_sets without grouping()", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "headcount" => 10},
    %{"dept" => "Eng", "team" => "B", "headcount" => 15},
    %{"dept" => "Sales", "team" => "A", "headcount" => 8},
    %{"dept" => "Sales", "team" => "B", "headcount" => 12}
  ], schema: "dept STRING, team STRING, headcount INT")

  df
  |> DataFrame.grouping_sets([
    [col("dept"), col("team")],
    [col("dept")],
    []
  ])
  |> GroupedData.agg([
    sum(col("headcount")) |> Column.alias_("total")
  ])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

# =============================================
# 2. Deeper trim bug investigation
# =============================================

# The issue is: Spark SQL `ltrim(trimChar, string)` has trimChar FIRST
# But SparkEx generates `ltrim(string, trimChar)` — args reversed!
# Let's verify by checking what SparkEx generates

run.("2a. ltrim explain to see arg order", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"txt" => "***hello***"}
  ], schema: "txt STRING")
  df |> DataFrame.select([
    ltrim(col("txt"), "*") |> Column.alias_("ltrimmed")
  ]) |> DataFrame.explain(:simple)
end)

run.("2b. SQL ltrim both orderings", fn ->
  SparkEx.sql(s, """
    SELECT
      ltrim('*', '***hello***') AS correct_order,
      ltrim('***hello***', '*') AS reversed_order
  """) |> DataFrame.collect()
end)

# =============================================
# 3. unix_timestamp correct API (keyword opts)
# =============================================

run.("3. unix_timestamp with format: keyword", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"ts_str" => "2024-06-15 10:30:00"}
  ], schema: "ts_str STRING")
  df |> DataFrame.select([
    unix_timestamp(col("ts_str"), format: "yyyy-MM-dd HH:mm:ss") |> Column.alias_("epoch")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Observation — only single aggregate, check name
# =============================================

run.("4a. Observation with single named agg", fn ->
  obs = Observation.new("single_obs_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 10}], schema: "x INT")
  observed = DataFrame.observe(df, obs, [
    count(col("x")) |> Column.alias_("row_count")
  ])
  {:ok, _} = DataFrame.collect(observed)
  Observation.get(obs)
end)

run.("4b. Observation with 3 named aggs", fn ->
  obs = Observation.new("multi_obs_#{run_id}")
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"x" => i} end), schema: "x INT")
  observed = DataFrame.observe(df, obs, [
    count(col("x")) |> Column.alias_("cnt"),
    sum(col("x")) |> Column.alias_("total"),
    min(col("x")) |> Column.alias_("min_x")
  ])
  {:ok, _} = DataFrame.collect(observed)
  Observation.get(obs)
end)

# =============================================
# 5. to_xml (struct literal, not Kernel.struct)
# =============================================

run.("5. to_xml via SQL struct", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
  """)
  df |> DataFrame.select([
    to_xml(col("person")) |> Column.alias_("xml_str")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. DataFrame.rollup
# =============================================

run.("6. rollup", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "headcount" => 10},
    %{"dept" => "Eng", "team" => "B", "headcount" => 15},
    %{"dept" => "Sales", "team" => "A", "headcount" => 8},
    %{"dept" => "Sales", "team" => "B", "headcount" => 12}
  ], schema: "dept STRING, team STRING, headcount INT")

  df
  |> DataFrame.rollup([col("dept"), col("team")])
  |> GroupedData.agg([
    sum(col("headcount")) |> Column.alias_("total")
  ])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.cube
# =============================================

run.("7. cube", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "team" => "A", "headcount" => 10},
    %{"dept" => "Eng", "team" => "B", "headcount" => 15},
    %{"dept" => "Sales", "team" => "A", "headcount" => 8},
    %{"dept" => "Sales", "team" => "B", "headcount" => 12}
  ], schema: "dept STRING, team STRING, headcount INT")

  df
  |> DataFrame.cube([col("dept"), col("team")])
  |> GroupedData.agg([
    sum(col("headcount")) |> Column.alias_("total")
  ])
  |> DataFrame.sort([asc(col("dept")), asc(col("team"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. DataFrame.hint with repartition
# =============================================

run.("8. hint repartition", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")
  before = DataFrame.rdd_num_partitions_approx(df)
  after_ = df |> DataFrame.hint("repartition", [4]) |> DataFrame.rdd_num_partitions_approx()
  {before, after_}
end)

# =============================================
# 9. parse_url with QUERY key
# =============================================

run.("9. parse_url with QUERY key extraction", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"url" => "https://example.com/search?q=hello&lang=en"}
  ], schema: "url STRING")
  df |> DataFrame.select([
    parse_url(col("url"), lit("QUERY"), lit("q")) |> Column.alias_("q_param"),
    parse_url(col("url"), lit("QUERY"), lit("lang")) |> Column.alias_("lang_param")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Nested struct with to_json_rows
# =============================================

run.("10. to_json_rows with nested struct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nested" => %{"a" => "hello", "b" => 42}}
  ], schema: "id INT, nested STRUCT<a: STRING, b: INT>")
  df |> DataFrame.to_json_rows() |> DataFrame.collect()
end)

# =============================================
# 11. DataFrame.foreach error handling
# =============================================

run.("11. foreach with error in function (should propagate?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.foreach_local(df, fn _row -> raise "boom" end)
end)

# =============================================
# 12. collect_as_map with computed columns
# =============================================

run.("12. collect_as_map with selected/computed columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95},
    %{"id" => 2, "name" => "Bob", "score" => 87}
  ], schema: "id INT, name STRING, score INT")
  df
  |> DataFrame.select([col("name"), col("score")])
  |> DataFrame.collect_as_map()
end)

# =============================================
# 13. assert_true with custom error message
# =============================================

run.("13. assert_true with custom message", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => -1}], schema: "val INT")
  df |> DataFrame.select([
    assert_true(Column.gt(col("val"), lit(0)), lit("Value must be positive!"))
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Multiple observations on same DataFrame
# =============================================

run.("14. Two observations on same DataFrame", fn ->
  obs1 = Observation.new("obs_a_#{run_id}")
  obs2 = Observation.new("obs_b_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")

  observed = df
    |> DataFrame.observe(obs1, [count(col("val")) |> Column.alias_("cnt1")])
    |> DataFrame.observe(obs2, [sum(col("val")) |> Column.alias_("sum1")])

  {:ok, _} = DataFrame.collect(observed)
  {Observation.get(obs1), Observation.get(obs2)}
end)

# =============================================
# 15. Hint join using column names list
# =============================================

run.("15. hint broadcast with USING join (column name list)", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 100}, %{"id" => 2, "score" => 200}
  ], schema: "id INT, score INT")

  hinted = DataFrame.hint(df2, "broadcast")
  DataFrame.join(df1, hinted, ["id"], :inner) |> DataFrame.collect()
end)

IO.puts("=== Round 36c complete ===")
