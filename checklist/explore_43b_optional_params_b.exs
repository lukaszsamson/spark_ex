# Round 19b: DataFrame optional parameter edge cases
# Focus: join types, pivot values, repartition, random_split, persist options,
# fillna/dropna options, replace, freq_items, approx_quantile, union_by_name,
# html_string, print_schema levels, tree_string levels, checkpoint, head/take opts,
# drop_duplicates, lateral_join, grouping_sets, as_of_join, show vertical
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
# 1. Join type variants
# =============================================

run.("1a. join :left_outer", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "a" => "x"}, %{"id" => 2, "a" => "y"}, %{"id" => 3, "a" => "z"}
  ], schema: "id INT, a STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "b" => "p"}, %{"id" => 3, "b" => "q"}
  ], schema: "id INT, b STRING")

  DataFrame.join(df1, df2, ["id"], :left_outer) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

run.("1b. join :right_outer", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "a" => "x"}
  ], schema: "id INT, a STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "b" => "p"}, %{"id" => 2, "b" => "q"}
  ], schema: "id INT, b STRING")

  DataFrame.join(df1, df2, ["id"], :right_outer) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

run.("1c. join :full_outer", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "a" => "x"}, %{"id" => 2, "a" => "y"}
  ], schema: "id INT, a STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2, "b" => "p"}, %{"id" => 3, "b" => "q"}
  ], schema: "id INT, b STRING")

  DataFrame.join(df1, df2, ["id"], :full_outer) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

run.("1d. join :left_semi", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "a" => "x"}, %{"id" => 2, "a" => "y"}, %{"id" => 3, "a" => "z"}
  ], schema: "id INT, a STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "b" => "p"}, %{"id" => 3, "b" => "q"}
  ], schema: "id INT, b STRING")

  DataFrame.join(df1, df2, ["id"], :left_semi) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

run.("1e. join :left_anti", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "a" => "x"}, %{"id" => 2, "a" => "y"}, %{"id" => 3, "a" => "z"}
  ], schema: "id INT, a STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "b" => "p"}, %{"id" => 3, "b" => "q"}
  ], schema: "id INT, b STRING")

  DataFrame.join(df1, df2, ["id"], :left_anti) |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

run.("1f. join :cross", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"a" => 1}, %{"a" => 2}
  ], schema: "a INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"b" => "x"}, %{"b" => "y"}
  ], schema: "b STRING")

  DataFrame.join(df1, df2, [], :cross) |> DataFrame.order_by([asc(col("a")), asc(col("b"))]) |> DataFrame.collect()
end)

# =============================================
# 2. Pivot with explicit values
# =============================================

run.("2a. pivot without values (auto-detect)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "cat" => "x", "val" => 10},
    %{"grp" => "a", "cat" => "y", "val" => 20},
    %{"grp" => "b", "cat" => "x", "val" => 30},
    %{"grp" => "b", "cat" => "y", "val" => 40}
  ], schema: "grp STRING, cat STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> DataFrame.pivot(col("cat"))
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

run.("2b. pivot with explicit values list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "cat" => "x", "val" => 10},
    %{"grp" => "a", "cat" => "y", "val" => 20},
    %{"grp" => "a", "cat" => "z", "val" => 99},
    %{"grp" => "b", "cat" => "x", "val" => 30},
    %{"grp" => "b", "cat" => "y", "val" => 40}
  ], schema: "grp STRING, cat STRING, val INT")

  DataFrame.group_by(df, [col("grp")])
  |> DataFrame.pivot(col("cat"), [lit("x"), lit("y")])
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 3. Repartition variants
# =============================================

run.("3a. repartition by column only", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "grp" => rem(i, 3)} end),
    schema: "id INT, grp INT")

  partitions = df
  |> DataFrame.repartition([col("grp")])
  |> DataFrame.rdd_num_partitions_approx()

  partitions
end)

run.("3b. repartition by num + columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "grp" => rem(i, 3)} end),
    schema: "id INT, grp INT")

  partitions = df
  |> DataFrame.repartition(5, [col("grp")])
  |> DataFrame.rdd_num_partitions_approx()

  partitions
end)

# =============================================
# 4. random_split
# =============================================

run.("4a. random_split without seed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  dfs = DataFrame.random_split(df, [0.7, 0.3])
  case dfs do
    {:ok, splits} ->
      counts = Enum.map(splits, fn split_df ->
        {:ok, cnt} = DataFrame.count(split_df)
        cnt
      end)
      {:ok, counts}
    other -> other
  end
end)

run.("4b. random_split with seed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  dfs = DataFrame.random_split(df, [0.5, 0.3, 0.2], 42)
  case dfs do
    {:ok, splits} ->
      counts = Enum.map(splits, fn split_df ->
        {:ok, cnt} = DataFrame.count(split_df)
        cnt
      end)
      {:ok, counts}
    other -> other
  end
end)

# =============================================
# 5. fillna with subset
# =============================================

run.("5a. fillna with single value (all columns)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => nil},
    %{"a" => nil, "b" => 2, "c" => nil}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.fillna(0) |> DataFrame.collect()
end)

run.("5b. fillna with map (different values per column)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => nil, "age" => nil, "score" => nil}
  ], schema: "name STRING, age INT, score DOUBLE")

  df |> DataFrame.fillna(%{"name" => "unknown", "age" => 0, "score" => 0.0}) |> DataFrame.collect()
end)

# =============================================
# 6. dropna variants
# =============================================

run.("6a. dropna with how: :all", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil},
    %{"a" => nil, "b" => nil},
    %{"a" => 3, "b" => 4}
  ], schema: "a INT, b INT")

  df |> DataFrame.dropna(how: :all) |> DataFrame.collect()
end)

run.("6b. dropna with subset", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => 10},
    %{"a" => nil, "b" => 2, "c" => 20},
    %{"a" => 3, "b" => 3, "c" => nil}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.dropna(subset: ["a", "b"]) |> DataFrame.collect()
end)

run.("6c. dropna with thresh (integer)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => nil},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => 3, "b" => 4, "c" => 5}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.dropna(thresh: 2) |> DataFrame.collect()
end)

# =============================================
# 7. DataFrame.replace variants
# =============================================

run.("7a. DataFrame.replace single value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  df |> DataFrame.replace(1, 100) |> DataFrame.collect()
end)

run.("7b. DataFrame.replace map of values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")

  df |> DataFrame.replace(%{1 => 100, 2 => 200}) |> DataFrame.collect()
end)

# =============================================
# 8. freq_items
# =============================================

run.("8a. freq_items with default support", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.flat_map(1..10, fn i ->
      Enum.map(1..i, fn _ -> %{"val" => rem(i, 5)} end)
    end),
    schema: "val INT")

  df |> DataFrame.freq_items(["val"]) |> DataFrame.collect()
end)

run.("8b. freq_items with custom support threshold", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => rem(i, 5)} end),
    schema: "val INT")

  df |> DataFrame.freq_items(["val"], 0.5) |> DataFrame.collect()
end)

# =============================================
# 9. approx_quantile
# =============================================

run.("9a. approx_quantile single probability", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  DataFrame.approx_quantile(df, "val", [0.5], 0.01)
end)

run.("9b. approx_quantile multiple probabilities", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  DataFrame.approx_quantile(df, "val", [0.25, 0.5, 0.75], 0.01)
end)

run.("9c. approx_quantile with relative_error", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => i} end),
    schema: "val INT")

  DataFrame.approx_quantile(df, "val", [0.5], 0.01)
end)

# =============================================
# 10. union_by_name variants
# =============================================

run.("10a. union_by_name matching schemas", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"}
  ], schema: "a INT, b STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"b" => "y", "a" => 2}
  ], schema: "b STRING, a INT")

  DataFrame.union_by_name(df1, df2) |> DataFrame.collect()
end)

run.("10b. union_by_name with allow_missing: true", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "x"}
  ], schema: "a INT, b STRING")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"a" => 2, "c" => 99}
  ], schema: "a INT, c INT")

  DataFrame.union_by_name(df1, df2, allow_missing: true) |> DataFrame.collect()
end)

# =============================================
# 11. show with vertical
# =============================================

run.("11a. show with vertical: true", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5},
    %{"id" => 2, "name" => "Bob", "score" => 87.3}
  ], schema: "id INT, name STRING, score DOUBLE")

  DataFrame.show(df, num_rows: 1, vertical: true)
end)

run.("11b. show with truncate: 5", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello World This Is A Long String"}
  ], schema: "text STRING")

  DataFrame.show(df, truncate: 5)
end)

# =============================================
# 12. html_string
# =============================================

run.("12a. html_string default", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")

  DataFrame.html_string(df)
end)

run.("12b. html_string with options", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"},
    %{"id" => 3, "name" => "Carol"}
  ], schema: "id INT, name STRING")

  DataFrame.html_string(df, num_rows: 2, truncate: 3)
end)

# =============================================
# 13. print_schema and tree_string with level
# =============================================

run.("13a. print_schema with nested struct (default level)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "test", "nested" => %{"deep" => 42}}}
  ], schema: "id INT, info STRUCT<name: STRING, nested: STRUCT<deep: INT>>")

  DataFrame.print_schema(df)
end)

run.("13b. print_schema with level: 1 (shallow)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "test", "nested" => %{"deep" => 42}}}
  ], schema: "id INT, info STRUCT<name: STRING, nested: STRUCT<deep: INT>>")

  DataFrame.print_schema(df, level: 1)
end)

run.("13c. tree_string with level: 1", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"name" => "test"}}
  ], schema: "id INT, info STRUCT<name: STRING>")

  DataFrame.tree_string(df, level: 1)
end)

# =============================================
# 14. persist/unpersist with options
# =============================================

run.("14a. persist with storage_level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  persisted = DataFrame.persist(df, storage_level: :memory_only)
  {:ok, result} = DataFrame.collect(persisted)
  DataFrame.unpersist(persisted)
  {:ok, result}
end)

run.("14b. persist with disk_only", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  persisted = DataFrame.persist(df, storage_level: :disk_only)
  {:ok, cnt} = DataFrame.count(persisted)
  DataFrame.unpersist(persisted, blocking: true)
  {:ok, cnt}
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

run.("16a. hint broadcast (no params)", fn ->
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
  {:ok, String.contains?(plan, "Coalesce")}
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
# 18. head/take with opts
# =============================================

run.("18a. head with default (20 rows)", fn ->
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
# 22. DataFrame.to with schema string
# =============================================

run.("22a. DataFrame.to for type widening", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.to("id BIGINT, val DOUBLE")
  |> DataFrame.collect()
end)

run.("22b. DataFrame.to for string conversion", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 42}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.to("id STRING, val STRING")
  |> DataFrame.collect()
end)

# =============================================
# 23. transpose with index_column
# =============================================

run.("23. transpose with index_column option", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "math" => 90, "english" => 80},
    %{"name" => "Bob", "math" => 85, "english" => 95}
  ], schema: "name STRING, math INT, english INT")

  DataFrame.transpose(df, index_column: "name")
  |> DataFrame.collect()
end)

# =============================================
# 24. corr with different method
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
# 26. DataFrame.parse (csv and json)
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

IO.puts("=== Optional parameter edge case tests (part B) complete ===")
