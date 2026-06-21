# Round 15: Edge cases in various operations — less-tested APIs, empty DFs,
# subqueries, grouping_sets, transpose, try_cast, drop_fields, cache lifecycle
alias SparkEx.{DataFrame, Column, Catalog}
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

# === 1. try_cast (returns null on failure instead of error) ===
run.("1. try_cast: string to INT (valid and invalid)", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES ('42'), ('abc'), (NULL) AS t(val)")
  |> DataFrame.select([
    col("val"),
    Column.try_cast(col("val"), "INT") |> Column.alias_("as_int")
  ])
  |> DataFrame.collect()
end)

# === 2. Column.ilike (case-insensitive LIKE) ===
run.("2. Column.ilike: case-insensitive pattern match", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES ('Hello'), ('HELLO'), ('hello'), ('world') AS t(word)")
  |> DataFrame.filter(Column.ilike(col("word"), "hello"))
  |> DataFrame.collect()
end)

# === 3. Column.drop_fields on struct ===
run.("3. Column.drop_fields: remove fields from struct", fn ->
  SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS st")
  |> DataFrame.select([
    Column.drop_fields(col("st"), ["b"]) |> Column.alias_("trimmed")
  ])
  |> DataFrame.collect()
end)

# === 4. Column.with_field: add/replace field in struct ===
run.("4. Column.with_field: add field to struct", fn ->
  SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  |> DataFrame.select([
    Column.with_field(col("person"), "title", lit("Dr.")) |> Column.alias_("updated")
  ])
  |> DataFrame.collect()
end)

# === 5. DataFrame.transpose ===
run.("5. DataFrame.transpose", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"metric" => "revenue", "q1" => 100, "q2" => 200, "q3" => 300},
    %{"metric" => "cost", "q1" => 50, "q2" => 80, "q3" => 120}
  ], schema: "metric STRING, q1 INT, q2 INT, q3 INT")
  DataFrame.transpose(df, "metric") |> DataFrame.collect()
end)

# === 6. DataFrame.grouping_sets ===
run.("6. DataFrame.grouping_sets", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"dept" => "d#{rem(i, 3)}", "region" => "r#{rem(i, 2)}", "val" => i * 10}
    end),
    schema: "dept STRING, region STRING, val INT")

  DataFrame.grouping_sets(df, [[col("dept")], [col("region")], []])
  |> SparkEx.GroupedData.agg([
    sum(col("val")) |> Column.alias_("total"),
    count(lit(1)) |> Column.alias_("cnt")
  ])
  |> DataFrame.order_by([asc(col("dept")), asc(col("region"))])
  |> DataFrame.collect()
end)

# === 7. DataFrame.scalar subquery ===
run.("7. DataFrame.scalar subquery in filter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  avg_df = df |> DataFrame.select([avg(col("val")) |> Column.alias_("avg_val")])
  scalar = DataFrame.scalar(avg_df)

  df
  |> DataFrame.filter(Column.gt(col("val"), scalar))
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 8. DataFrame.exists subquery ===
run.("8. DataFrame.exists subquery", fn ->
  {:ok, main} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"},
    %{"id" => 3, "name" => "Carol"}
  ], schema: "id INT, name STRING")

  {:ok, lookup} = SparkEx.create_dataframe(s, [
    %{"ref_id" => 1}, %{"ref_id" => 3}
  ], schema: "ref_id INT")

  exists_sub = lookup
    |> DataFrame.filter(Column.eq(col("ref_id"), DataFrame.col(main, "id")))
    |> DataFrame.exists()

  main
  |> DataFrame.filter(exists_sub)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 9. DataFrame.in_subquery ===
run.("9. DataFrame.in_subquery", fn ->
  {:ok, main} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"},
    %{"id" => 2, "name" => "Bob"},
    %{"id" => 3, "name" => "Carol"}
  ], schema: "id INT, name STRING")

  {:ok, allowed} = SparkEx.create_dataframe(s, [
    %{"aid" => 1}, %{"aid" => 3}
  ], schema: "aid INT")

  allowed_ids = allowed |> DataFrame.select([col("aid")])

  main
  |> DataFrame.filter(DataFrame.in_subquery(col("id"), allowed_ids))
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 10. Empty DataFrame through various operations ===
run.("10a. Empty DF → filter (no matches)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}
  ], schema: "id INT")
  df |> DataFrame.filter(Column.gt(col("id"), lit(100))) |> DataFrame.collect()
end)

run.("10b. Empty DF → group_by → agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}
  ], schema: "id INT, val INT")
  df
  |> DataFrame.filter(Column.gt(col("id"), lit(100)))
  |> DataFrame.group_by([col("id")])
  |> SparkEx.GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

run.("10c. Empty DF → union with non-empty", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [
    %{"id" => 1}
  ], schema: "id INT")
  empty_df = DataFrame.filter(empty, Column.gt(col("id"), lit(100)))

  {:ok, full} = SparkEx.create_dataframe(s, [
    %{"id" => 10}, %{"id" => 20}
  ], schema: "id INT")

  DataFrame.union(empty_df, full) |> DataFrame.collect()
end)

run.("10d. Empty DF → is_empty?", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}
  ], schema: "id INT")
  empty_df = DataFrame.filter(df, Column.gt(col("id"), lit(100)))
  DataFrame.is_empty?(empty_df)
end)

# === 11. DataFrame.distinct ===
run.("11. DataFrame.distinct", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "a"},
    %{"id" => 1, "val" => "a"},
    %{"id" => 2, "val" => "b"},
    %{"id" => 2, "val" => "b"},
    %{"id" => 3, "val" => "c"}
  ], schema: "id INT, val STRING")

  df |> DataFrame.distinct() |> DataFrame.order_by([asc(col("id"))]) |> DataFrame.collect()
end)

# === 12. DataFrame.subtract (alias for except) ===
run.("12. DataFrame.subtract", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 2}, %{"id" => 3}
  ], schema: "id INT")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"id" => 2}
  ], schema: "id INT")

  DataFrame.subtract(df1, df2)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 13. Cache/Persist lifecycle ===
run.("13a. cache → is_cached? → collect → unpersist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i} end),
    schema: "id INT")

  cached = DataFrame.cache(df)
  is_cached_before = DataFrame.is_cached?(cached)
  {:ok, rows} = DataFrame.collect(cached)
  DataFrame.unpersist(cached)
  is_cached_after = DataFrame.is_cached?(cached)
  {is_cached_before, length(rows), is_cached_after}
end)

run.("13b. persist → storage_level → unpersist", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  persisted = DataFrame.persist(df)
  level = DataFrame.storage_level(persisted)
  DataFrame.unpersist(persisted)
  level
end)

# === 14. DataFrame.repartition variants ===
run.("14a. repartition by number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  df
  |> DataFrame.repartition(4)
  |> DataFrame.select([spark_partition_id() |> Column.alias_("part")])
  |> DataFrame.distinct()
  |> DataFrame.collect()
end)

run.("14b. repartition by column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "group" => "g#{rem(i, 3)}"}
    end),
    schema: "id INT, group STRING")

  df
  |> DataFrame.repartition(3, [col("group")])
  |> DataFrame.select([
    col("group"),
    spark_partition_id() |> Column.alias_("part")
  ])
  |> DataFrame.distinct()
  |> DataFrame.order_by([asc(col("group")), asc(col("part"))])
  |> DataFrame.collect()
end)

run.("14c. repartition_by_range", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  df
  |> DataFrame.repartition_by_range(4, [col("id")])
  |> DataFrame.select([
    min(col("id")) |> Column.alias_("min_id"),
    max(col("id")) |> Column.alias_("max_id"),
    spark_partition_id() |> Column.alias_("part")
  ])
  |> DataFrame.collect()
end)

# === 15. DataFrame.coalesce ===
run.("15. coalesce partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  orig_parts = df
    |> DataFrame.select([spark_partition_id() |> Column.alias_("part")])
    |> DataFrame.distinct()
    |> DataFrame.count()

  coalesced_parts = df
    |> DataFrame.coalesce(2)
    |> DataFrame.select([spark_partition_id() |> Column.alias_("part")])
    |> DataFrame.distinct()
    |> DataFrame.count()

  {orig_parts, coalesced_parts}
end)

# === 16. DataFrame.sort_within_partitions ===
run.("16. sort_within_partitions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "grp" => rem(i, 3)} end),
    schema: "id INT, grp INT")

  df
  |> DataFrame.repartition(3, [col("grp")])
  |> DataFrame.sort_within_partitions([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 17. DataFrame.explain ===
run.("17. DataFrame.explain modes", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10}
  ], schema: "id INT, val INT")

  filtered = df |> DataFrame.filter(Column.gt(col("val"), lit(5)))
  {:ok, plan} = DataFrame.explain(filtered, "simple")
  IO.puts("  Plan length: #{String.length(plan)} chars")
  {:ok, String.slice(plan, 0..200)}
end)

# === 18. DataFrame.hint (broadcast, repartition) ===
run.("18. DataFrame.hint broadcast join", fn ->
  {:ok, big} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  {:ok, small} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "label" => "one"},
    %{"id" => 2, "label" => "two"}
  ], schema: "id INT, label STRING")

  small_broadcast = DataFrame.hint(small, "broadcast")
  DataFrame.join(big, small_broadcast, ["id"])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 19. DataFrame.observe ===
run.("19. DataFrame.observe metrics", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"id" => i, "val" => i * 10} end),
    schema: "id INT, val INT")

  df
  |> DataFrame.observe("my_metrics", [
    count(lit(1)) |> Column.alias_("row_count"),
    sum(col("val")) |> Column.alias_("total_val")
  ])
  |> DataFrame.collect()
end)

# === 20. DataFrame.with_column_renamed ===
run.("20. with_column_renamed", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"old_name" => 1, "keep" => "a"}
  ], schema: "old_name INT, keep STRING")

  df
  |> DataFrame.with_column_renamed("old_name", "new_name")
  |> DataFrame.columns()
end)

# === 21. DataFrame.drop multiple columns ===
run.("21. drop multiple columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2, "c" => 3, "d" => 4}
  ], schema: "a INT, b INT, c INT, d INT")

  df
  |> DataFrame.drop(["b", "d"])
  |> DataFrame.columns()
end)

# === 22. Column.between ===
run.("22. Column.between edge cases", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 5}, %{"val" => 10},
    %{"val" => nil}
  ], schema: "val INT")

  df
  |> DataFrame.select([
    col("val"),
    Column.between(col("val"), lit(3), lit(8)) |> Column.alias_("in_range")
  ])
  |> DataFrame.collect()
end)

# === 23. DataFrame.take ===
run.("23. DataFrame.take", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end),
    schema: "id INT")

  DataFrame.take(df, 3)
end)

# === 24. SparkEx.range ===
run.("24. SparkEx.range", fn ->
  df = SparkEx.range(s, 10)
  DataFrame.collect(df)
end)

run.("24b. SparkEx.range with start/end/step", fn ->
  df = SparkEx.range(s, 0, 100, 10)
  DataFrame.collect(df)
end)

# === 25. DataFrame.semantic_hash ===
run.("25. same_semantics and semantic_hash", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df3 = DataFrame.filter(df1, Column.gt(col("id"), lit(0)))

  same = DataFrame.same_semantics(df1, df2)
  hash1 = DataFrame.semantic_hash(df1)
  hash3 = DataFrame.semantic_hash(df3)
  {same, hash1, hash3}
end)

IO.puts("=== Operation edge case tests complete ===")
