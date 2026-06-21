# Round 13: Re-verify EX-48..55 and push deeper on complex data structures
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
# RE-VERIFY: EX-48..55
# =============================================

run.("1. [EX-48] lit() map with array values", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("data", lit(%{"scores" => [1, 2, 3], "tags" => ["a", "b"]}))
  |> DataFrame.collect()
end)

run.("2. [EX-49] lit() nested map", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("profile", lit(%{"name" => "Alice", "addr" => %{"city" => "NYC"}}))
  |> DataFrame.collect()
end)

run.("3. [EX-50] collect_as_map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"key" => "a", "value" => 10},
    %{"key" => "b", "value" => 20}
  ], schema: "key STRING, value INT")
  DataFrame.collect_as_map(df)
end)

run.("4. [EX-51] create_dataframe dtypes for complex types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "tags" => ["a"], "meta" => %{"k" => "v"}, "info" => %{"name" => "test"}}
  ], schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>, info STRUCT<name: STRING>")
  DataFrame.dtypes(df)
end)

run.("5. [EX-52/54] create_dataframe binary", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "data" => <<0xDE, 0xAD, 0xBE, 0xEF>>},
    %{"id" => 2, "data" => <<65, 66, 67>>}
  ], schema: "id INT, data BINARY")
  DataFrame.collect(df)
end)

run.("6. [EX-53] Join two create_dataframes", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")

  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 95.5}, %{"id" => 2, "score" => 87.3}
  ], schema: "id INT, score DOUBLE")

  DataFrame.join(left, right, ["id"])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("7. [EX-55] to_explorer with complex columns", fn ->
  df = SparkEx.sql(s, "SELECT array(1,2,3) AS arr, map('a',1) AS m, named_struct('n','x') AS st")
  DataFrame.to_explorer(df)
end)

# =============================================
# NEW: Push harder on combinations
# =============================================

# Three create_dataframes joined
run.("8. Join three create_dataframes", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")

  {:ok, b} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "dept" => "eng"}, %{"id" => 2, "dept" => "sales"}
  ], schema: "id INT, dept STRING")

  {:ok, c} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "salary" => 100000}, %{"id" => 2, "salary" => 90000}
  ], schema: "id INT, salary INT")

  a
  |> DataFrame.join(b, ["id"])
  |> DataFrame.join(c, ["id"])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# create_dataframe → union with another create_dataframe
run.("9. Union two create_dataframes", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"}, %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 3, "val" => "c"}, %{"id" => 4, "val" => "d"}
  ], schema: "id INT, val STRING")

  DataFrame.union(df1, df2)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# create_dataframe with 500 rows of complex data
run.("10. create_dataframe 500 rows with struct + array", fn ->
  rows = for i <- 1..500 do
    %{"id" => i, "info" => %{"label" => "item_#{i}"}, "tags" => ["t#{rem(i, 5)}"]}
  end
  {:ok, df} = SparkEx.create_dataframe(s, rows,
    schema: "id INT, info STRUCT<label: STRING>, tags ARRAY<STRING>")

  df
  |> DataFrame.select([count(lit(1)) |> Column.alias_("cnt")])
  |> DataFrame.collect()
end)

# lit() with list of Decimals
run.("11. lit() with list of Decimals", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("prices", lit([Decimal.new("10.50"), Decimal.new("20.75")]))
  |> DataFrame.collect()
end)

# lit() with list of booleans
run.("12. lit() with list of booleans", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("flags", lit([true, false, true]))
  |> DataFrame.collect()
end)

# lit() with empty list
run.("13. lit() with empty list", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("empty", lit([]))
  |> DataFrame.collect()
end)

# lit() with empty map
run.("14. lit() with empty map", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("empty_map", lit(%{}))
  |> DataFrame.collect()
end)

# lit() with single-element list
run.("15. lit() with single-element list", fn ->
  SparkEx.sql(s, "SELECT 1 AS id")
  |> DataFrame.with_column("single", lit([42]))
  |> DataFrame.collect()
end)

# create_dataframe → cross join with itself
run.("16. create_dataframe → self cross join", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"}
  ], schema: "id INT, val STRING")

  a = DataFrame.alias_(df, "a")
  b = DataFrame.alias_(df, "b")
  DataFrame.cross_join(a, b)
  |> DataFrame.collect()
end)

# create_dataframe → except / intersect
run.("17. create_dataframe except / intersect", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2}, %{"id" => 3}, %{"id" => 4}
  ], schema: "id INT")

  {:ok, exc} = DataFrame.except(df1, df2) |> DataFrame.collect()
  {:ok, inter} = DataFrame.intersect(df1, df2) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
  {exc, inter}
end)

# create_dataframe with struct → get_field → filter → agg
run.("18. create_dataframe struct → get_field pipeline", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "rec" => %{"category" => "cat_#{rem(i, 3)}", "amount" => Decimal.new("#{i * 100}.50")}}
    end),
    schema: "id INT, rec STRUCT<category: STRING, amount: DECIMAL(10,2)>")

  df
  |> DataFrame.with_column("cat", col("rec") |> Column.get_field("category"))
  |> DataFrame.with_column("amt", col("rec") |> Column.get_field("amount"))
  |> DataFrame.filter(Column.gt(col("amt"), lit(Decimal.new("1000"))))
  |> DataFrame.group_by([col("cat")])
  |> DataFrame.agg([
    count(lit(1)) |> Column.alias_("cnt"),
    sum(col("amt")) |> Column.alias_("total")
  ])
  |> DataFrame.order_by([asc(col("cat"))])
  |> DataFrame.collect()
end)

# create_dataframe with MAP → explode → agg
run.("19. create_dataframe MAP → explode → agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "scores" => %{"math" => 90, "eng" => 80}},
    %{"id" => 2, "scores" => %{"math" => 70, "eng" => 95, "sci" => 85}}
  ], schema: "id INT, scores MAP<STRING, INT>")

  df
  |> DataFrame.select([col("id"), explode(col("scores"))])
  |> DataFrame.group_by([col("key")])
  |> DataFrame.agg([avg(col("value")) |> Column.alias_("avg_score")])
  |> DataFrame.order_by([asc(col("key"))])
  |> DataFrame.collect()
end)

# create_dataframe → Iceberg → read → join with another create_dataframe
run.("20. create_dataframe → Iceberg → read → join create_dataframe", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_rj_#{run_id}"

  {:ok, saved} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")

  saved |> DataFrame.write() |> SparkEx.Writer.save_as_table(tbl)

  read_back = SparkEx.sql(s, "SELECT * FROM #{tbl}")

  {:ok, scores} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "score" => 95}, %{"id" => 2, "score" => 87}
  ], schema: "id INT, score INT")

  result = DataFrame.join(read_back, scores, ["id"])
    |> DataFrame.order_by([asc(col("id"))])
    |> DataFrame.collect()

  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# create_dataframe → sample
run.("21. create_dataframe → sample", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, sampled} = df
    |> DataFrame.sample(0.1, seed: 42)
    |> DataFrame.collect()

  IO.puts("  Sampled #{length(sampled)} rows from 100")
  {:ok, length(sampled)}
end)

# create_dataframe → random_split
run.("22. create_dataframe → random_split", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  {:ok, [train, test]} = DataFrame.random_split(df, [0.8, 0.2], seed: 42)
  {:ok, train_rows} = DataFrame.collect(train)
  {:ok, test_rows} = DataFrame.collect(test)
  {length(train_rows), length(test_rows), length(train_rows) + length(test_rows)}
end)

# create_dataframe → dropna
run.("23. create_dataframe → dropna", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => nil},
    %{"id" => nil, "val" => "c"},
    %{"id" => 4, "val" => "d"}
  ], schema: "id INT, val STRING")

  df |> DataFrame.dropna() |> DataFrame.collect()
end)

# create_dataframe → fillna
run.("24. create_dataframe → fillna", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10.0},
    %{"id" => 2, "val" => nil},
    %{"id" => 3, "val" => nil}
  ], schema: "id INT, val DOUBLE")

  df |> DataFrame.fillna(0.0) |> DataFrame.collect()
end)

# create_dataframe → replace
run.("25. create_dataframe → replace", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "status" => "active"},
    %{"id" => 2, "status" => "inactive"},
    %{"id" => 3, "status" => "active"}
  ], schema: "id INT, status STRING")

  df |> DataFrame.replace("active", "enabled") |> DataFrame.collect()
end)

# create_dataframe with very long string values
run.("26. create_dataframe: very long string (10000 chars)", fn ->
  long = String.duplicate("abcdefghij", 1000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "text" => long}
  ], schema: "id INT, text STRING")

  df
  |> DataFrame.select([col("id"), SparkEx.Functions.length(col("text")) |> Column.alias_("len")])
  |> DataFrame.collect()
end)

# create_dataframe with unicode strings
run.("27. create_dataframe: unicode and emoji strings", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "text" => "日本語テスト"},
    %{"id" => 2, "text" => "emoji: 🎉🚀💯"},
    %{"id" => 3, "text" => "mixed: café résumé naïve"}
  ], schema: "id INT, text STRING")
  DataFrame.collect(df)
end)

# create_dataframe with special chars that need JSON escaping
run.("28. create_dataframe: JSON-special characters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "text" => "line1\nline2\ttab"},
    %{"id" => 2, "text" => "quote: \"hello\" and backslash: \\"},
    %{"id" => 3, "text" => "null byte: \0 end"}
  ], schema: "id INT, text STRING")
  DataFrame.collect(df)
end)

IO.puts("=== Re-verify and advanced combination tests complete ===")
