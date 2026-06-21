# Round 73d: Non-crashing tests from round 73b
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

# 4. drop_table purge non-boolean
run.("4. drop_table purge non-boolean on real table", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r73d_purge_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_v2(df, table) |> SparkEx.WriterV2.create()
  Catalog.drop_table(s, table, if_exists: true, purge: "yes")
end)

# 7. crosstab non-string
run.("7a. crosstab with non-string col1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => "x", "b" => "y"}], schema: "a STRING, b STRING")
  DataFrame.crosstab(df, 42, "b") |> DataFrame.collect()
end)

run.("7b. crosstab with non-string col2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => "x", "b" => "y"}], schema: "a STRING, b STRING")
  DataFrame.crosstab(df, "a", 42) |> DataFrame.collect()
end)

# 8. corr/cov non-string
run.("8a. corr with non-string col1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1.0, "b" => 2.0}], schema: "a DOUBLE, b DOUBLE")
  DataFrame.corr(df, 42, "b")
end)

run.("8b. cov with non-string col2", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1.0, "b" => 2.0}], schema: "a DOUBLE, b DOUBLE")
  DataFrame.cov(df, "a", 42)
end)

# 9. sample_by non-map
run.("9. sample_by with non-map fractions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"grp" => Enum.at(["A", "B"], rem(i, 2)), "val" => i} end),
    schema: "grp STRING, val INT")
  DataFrame.sample_by(df, "grp", "not_a_map")
end)

# 10. freq_items non-list
run.("10. freq_items with non-list columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "a"}], schema: "x STRING")
  DataFrame.freq_items(df, "x", 0.5) |> DataFrame.collect()
end)

# 11. approx_quantile non-list
run.("11. approx_quantile with non-list probabilities", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, Enum.map(1..10, fn i -> %{"x" => i * 1.0} end), schema: "x DOUBLE")
  DataFrame.approx_quantile(df, ["x"], 0.5, 0.0)
end)

IO.puts("=== Round 73d complete ===")
