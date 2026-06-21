# Round 55a: Session config API, DataFrame.show, Session tags
alias SparkEx.{DataFrame, Column, GroupedData, Catalog}
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
# 1. Session configuration API
# =============================================

run.("1a. config_get single key", fn ->
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
end)

run.("1b. config_get multiple keys", fn ->
  SparkEx.config_get(s, ["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled"])
end)

run.("1c. config_set + config_get roundtrip", fn ->
  {:ok, [{_, orig}]} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "42"}])
  {:ok, [{_, new_val}]} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", orig}])
  {:ok, [{_, restored}]} = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
  %{original: orig, set_to: new_val, restored: restored}
end)

run.("1d. config_get_all with prefix", fn ->
  {:ok, configs} = SparkEx.config_get_all(s, "spark.sql.shuffle")
  configs
end)

run.("1e. config_get_option (nil for unset)", fn ->
  SparkEx.config_get_option(s, ["spark.sql.shuffle.partitions", "nonexistent.config.key"])
end)

run.("1f. config_get_with_default", fn ->
  SparkEx.config_get_with_default(s, [
    {"spark.sql.shuffle.partitions", "DEFAULT_VAL"},
    {"nonexistent.key", "MY_DEFAULT"}
  ])
end)

run.("1g. config_is_modifiable", fn ->
  SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions", "spark.master"])
end)

run.("1h. config_unset", fn ->
  SparkEx.config_set(s, [{"spark.sparkex.test.key", "hello"}])
  {:ok, [{_, before}]} = SparkEx.config_get(s, ["spark.sparkex.test.key"])
  SparkEx.config_unset(s, ["spark.sparkex.test.key"])
  after_unset = SparkEx.config_get_option(s, ["spark.sparkex.test.key"])
  %{before: before, after_unset: after_unset}
end)

# =============================================
# 2. DataFrame.show
# =============================================

run.("2a. DataFrame.show basic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

run.("2b. DataFrame.show with truncate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"id" => i, "val" => String.duplicate("x", 50)} end),
    schema: "id INT, val STRING")
  DataFrame.show(df, num_rows: 3, truncate: 10)
end)

run.("2c. DataFrame.show vertical", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5}
  ], schema: "id INT, name STRING, score DOUBLE")
  DataFrame.show(df, vertical: true)
end)

# =============================================
# 3. Session tags
# =============================================

run.("3a. get_tags empty", fn ->
  SparkEx.clear_tags(s)
  SparkEx.get_tags(s)
end)

run.("3b. add_tag + get_tags + remove_tag", fn ->
  SparkEx.add_tag(s, "test_tag_#{run_id}")
  tags = SparkEx.get_tags(s)
  SparkEx.remove_tag(s, "test_tag_#{run_id}")
  after_ = SparkEx.get_tags(s)
  %{with_tag: tags, after_remove: after_}
end)

# =============================================
# 4. Protobuf edge: config_set with non-string values
# =============================================

run.("4a. config_set with atom value", fn ->
  SparkEx.config_set(s, [{"spark.sparkex.test.atom", :hello}])
end)

run.("4b. config_set with integer value", fn ->
  SparkEx.config_set(s, [{"spark.sparkex.test.int", 42}])
end)

run.("4c. config_set with nil value", fn ->
  SparkEx.config_set(s, [{"spark.sparkex.test.nil", nil}])
end)

# =============================================
# 5. Session.interrupt_all and last_execution_metrics
# =============================================

run.("5a. interrupt_all (no running)", fn ->
  SparkEx.Session.interrupt_all(s)
end)

run.("5b. last_execution_metrics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, _} = DataFrame.collect(df)
  SparkEx.Session.last_execution_metrics(s)
end)

# =============================================
# 6. Multiple observations
# =============================================

run.("6. Two observations on same DataFrame", fn ->
  obs1 = SparkEx.Observation.new("obs1_#{run_id}")
  obs2 = SparkEx.Observation.new("obs2_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")

  {:ok, rows} = df
  |> DataFrame.observe(obs1, [count(col("val")) |> Column.alias_("cnt")])
  |> DataFrame.observe(obs2, [sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()

  m1 = SparkEx.Observation.get(obs1)
  m2 = SparkEx.Observation.get(obs2)
  %{obs1: m1, obs2: m2, rows: length(rows)}
end)

IO.puts("=== Round 55a complete ===")
