# Round 19c: Remaining DataFrame optional param tests (after 14a crash)
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

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
# 14. persist/unpersist (without storage_level to avoid crash)
# =============================================

run.("14c. persist default (no options)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  persisted = DataFrame.persist(df)
  {:ok, result} = DataFrame.collect(persisted)
  DataFrame.unpersist(persisted)
  {:ok, length(result)}
end)

run.("14d. unpersist with blocking: true", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  persisted = DataFrame.persist(df)
  {:ok, _} = DataFrame.collect(persisted)
  DataFrame.unpersist(persisted, blocking: true)
end)

# =============================================
# 15. drop_duplicates variants
# =============================================

run.("15a. drop_duplicates (all columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"},
    %{"a" => 1, "b" => "x"},
    %{"a" => 2, "b" => "y"},
    %{"a" => 2, "b" => "z"}
  ], schema: "a INT, b STRING")

  df |> DataFrame.drop_duplicates() |> DataFrame.order_by([asc(col("a")), asc(col("b"))]) |> DataFrame.collect()
end)

run.("15b. drop_duplicates with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"},
    %{"a" => 1, "b" => "y"},
    %{"a" => 2, "b" => "z"}
  ], schema: "a INT, b STRING")

  df |> DataFrame.drop_duplicates(["a"]) |> DataFrame.order_by([asc(col("a"))]) |> DataFrame.collect()
end)

# =============================================
# 16. hint with parameters
# =============================================

run.("16a. hint broadcast", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")

  df2_hinted = DataFrame.hint(df2, "broadcast")
  DataFrame.join(df1, df2_hinted, ["id"]) |> DataFrame.collect()
end)

run.("16b. hint coalesce with num parameter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end),
    schema: "id INT")

  hinted = DataFrame.hint(df, "coalesce", [2])
  {:ok, plan} = DataFrame.explain(hinted)
  {:ok, String.contains?(plan, "Coalesce") || String.contains?(plan, "coalesce")}
end)

# =============================================
# 17. explain modes
# =============================================

run.("17a. explain :simple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, plan} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.explain(:simple)
  {:ok, String.length(plan)}
end)

run.("17b. explain :extended", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, plan} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.explain(:extended)
  {:ok, String.length(plan)}
end)

run.("17c. explain :codegen", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, plan} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.explain(:codegen)
  {:ok, String.length(plan)}
end)

run.("17d. explain :cost", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, plan} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.explain(:cost)
  {:ok, String.length(plan)}
end)

run.("17e. explain :formatted", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, plan} = df |> DataFrame.filter(Column.gt(col("id"), lit(0))) |> DataFrame.explain(:formatted)
  {:ok, String.length(plan)}
end)

# =============================================
# 18. head/take
# =============================================

run.("18a. head default (20 rows)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..25, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, rows} = DataFrame.head(df)
  {:ok, length(rows)}
end)

run.("18b. take with n=3", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  DataFrame.take(df, 3)
end)

# =============================================
# 19. order_by with ascending option
# =============================================

run.("19a. order_by descending via opts", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 3}, %{"id" => 2}
  ], schema: "id INT")

  df |> DataFrame.order_by([col("id")], ascending: [false]) |> DataFrame.collect()
end)

run.("19b. order_by multi-column with mixed ascending", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 3},
    %{"a" => 1, "b" => 1},
    %{"a" => 2, "b" => 2}
  ], schema: "a INT, b INT")

  df |> DataFrame.order_by([col("a"), col("b")], ascending: [true, false]) |> DataFrame.collect()
end)

# =============================================
# 20. sort_within_partitions with ascending
# =============================================

run.("20. sort_within_partitions with ascending option", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "grp" => rem(i, 3)} end),
    schema: "id INT, grp INT")

  df
  |> DataFrame.repartition(2)
  |> DataFrame.sort_within_partitions([col("id")], ascending: [false])
  |> DataFrame.collect()
end)

# =============================================
# 21. collect with timeout
# =============================================

run.("21. collect with timeout option", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.collect(df, timeout: 30_000)
end)

# =============================================
# 22. DataFrame.to
# =============================================

run.("22a. DataFrame.to type widening", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id INT, val INT")

  df |> DataFrame.to("id BIGINT, val DOUBLE") |> DataFrame.collect()
end)

run.("22b. DataFrame.to string conversion", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id INT, val INT")

  df |> DataFrame.to("id STRING, val STRING") |> DataFrame.collect()
end)

# =============================================
# 23. transpose with index_column
# =============================================

run.("23. transpose with index_column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "math" => 90, "english" => 80},
    %{"name" => "Bob", "math" => 85, "english" => 95}
  ], schema: "name STRING, math INT, english INT")

  DataFrame.transpose(df, index_column: "name")
  |> DataFrame.collect()
end)

# =============================================
# 24. corr with spearman
# =============================================

run.("24. corr with spearman method", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"a" => i, "b" => i * i} end),
    schema: "a INT, b INT")

  DataFrame.corr(df, "a", "b", "spearman")
end)

# =============================================
# 25. register_temp_table
# =============================================

run.("25. register_temp_table", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 42}], schema: "id INT")
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "reg_temp_#{run_id}"

  DataFrame.register_temp_table(df, tbl)
  result = SparkEx.sql(s, "SELECT * FROM #{tbl}") |> DataFrame.collect()
  DataFrame.drop_temp_view(s, tbl)
  result
end)

# =============================================
# 26. DataFrame.parse
# =============================================

run.("26a. DataFrame.parse csv", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => "1,Alice,90"},
    %{"data" => "2,Bob,85"}
  ], schema: "data STRING")

  DataFrame.parse(df, :csv, "id INT, name STRING, score INT")
  |> DataFrame.collect()
end)

run.("26b. DataFrame.parse json", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => ~s({"id":1,"name":"Alice"})},
    %{"data" => ~s({"id":2,"name":"Bob"})}
  ], schema: "data STRING")

  DataFrame.parse(df, :json, "id INT, name STRING")
  |> DataFrame.collect()
end)

# =============================================
# 27. table_function
# =============================================

run.("27. table_function: range", fn ->
  DataFrame.table_function(s, "range", [lit(5)])
  |> DataFrame.collect()
end)

# =============================================
# 28. pivot with raw values (not lit-wrapped)
# =============================================

run.("28. pivot with raw string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "cat" => "x", "val" => 10},
    %{"grp" => "a", "cat" => "y", "val" => 20},
    %{"grp" => "a", "cat" => "z", "val" => 99},
    %{"grp" => "b", "cat" => "x", "val" => 30},
    %{"grp" => "b", "cat" => "y", "val" => 40}
  ], schema: "grp STRING, cat STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> DataFrame.pivot(col("cat"), ["x", "y"])
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 29. repartition (number only, no rdd_num_partitions)
# =============================================

run.("29a. repartition num only → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, rows} = df |> DataFrame.repartition(3) |> DataFrame.collect()
  {:ok, length(rows)}
end)

run.("29b. coalesce → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, rows} = df |> DataFrame.coalesce(1) |> DataFrame.collect()
  {:ok, length(rows)}
end)

# =============================================
# 30. as_of_join
# =============================================

run.("30. as_of_join basic", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "ts" => "2025-01-01 10:00:00"},
    %{"id" => 1, "ts" => "2025-01-01 11:00:00"},
    %{"id" => 2, "ts" => "2025-01-01 10:30:00"}
  ], schema: "id INT, ts STRING")

  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "ts" => "2025-01-01 09:00:00", "val" => "before"},
    %{"id" => 1, "ts" => "2025-01-01 10:30:00", "val" => "middle"},
    %{"id" => 2, "ts" => "2025-01-01 10:00:00", "val" => "exact"}
  ], schema: "id INT, ts STRING, val STRING")

  DataFrame.as_of_join(left, right, col("ts"), col("ts"), on: ["id"])
  |> DataFrame.collect()
end)

IO.puts("=== Optional parameter edge case tests (part C) complete ===")
