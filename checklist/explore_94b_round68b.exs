# Round 68b: Continue serialization tests (skip MAP-in-STRUCT crashers)
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

# =============================================
# 5b. Wide struct WITHOUT map field (isolate crash trigger)
# =============================================

run.("5b. Wide struct: 5 fields WITHOUT map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{
      "f_int" => 1,
      "f_str" => "hello",
      "f_arr" => [1, 2],
      "f_struct" => %{"x" => 10},
      "f_bool" => true
    }},
    %{"rec" => %{
      "f_int" => nil,
      "f_str" => nil,
      "f_arr" => nil,
      "f_struct" => nil,
      "f_bool" => nil
    }},
    %{"rec" => nil}
  ], schema: "rec STRUCT<f_int: INT, f_str: STRING, f_arr: ARRAY<INT>, f_struct: STRUCT<x: INT>, f_bool: BOOLEAN>")
  df |> DataFrame.collect()
end)

# =============================================
# 6. Single-row edge cases
# =============================================

run.("6a. Single row with nested types (no map)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => %{"items" => [%{"name" => "x", "val" => 1}], "meta" => %{"ver" => 1}}}
  ], schema: "data STRUCT<items: ARRAY<STRUCT<name: STRING, val: INT>>, meta: STRUCT<ver: INT>>")
  df |> DataFrame.collect()
end)

run.("6b. Single row with all-null complex fields (no map)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "c" => nil}
  ], schema: "a ARRAY<INT>, c STRUCT<x: INT>")
  df |> DataFrame.collect()
end)

# =============================================
# 7. Transforms on complex types then collect
# =============================================

run.("7a. Filter + select on nested struct then collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i ->
      %{"id" => i, "rec" => %{"val" => rem(i, 5), "name" => "item_#{i}"}}
    end), schema: "id INT, rec STRUCT<val: INT, name: STRING>")
  df
  |> DataFrame.filter(Column.get_field(col("rec"), "val") |> Column.gt(lit(2)))
  |> DataFrame.select([
    col("id"),
    Column.get_field(col("rec"), "val") |> Column.alias_("val"),
    Column.get_field(col("rec"), "name") |> Column.alias_("name")
  ])
  |> DataFrame.collect()
end)

run.("7b. Group by struct field + agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"id" => i, "info" => %{"cat" => Enum.at(["A", "B", "C"], rem(i, 3)), "val" => i * 10}}
    end), schema: "id INT, info STRUCT<cat: STRING, val: INT>")
  df
  |> DataFrame.group_by([Column.get_field(col("info"), "cat") |> Column.alias_("category")])
  |> GroupedData.agg([
    sum(Column.get_field(col("info"), "val")) |> Column.alias_("total"),
    count(col("id")) |> Column.alias_("cnt")
  ])
  |> DataFrame.sort([asc(col("category"))])
  |> DataFrame.collect()
end)

# =============================================
# 8. create_dataframe encoding edge cases
# =============================================

run.("8a. Empty DataFrame (zero rows) with complex schema", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [],
    schema: "id INT, arr ARRAY<INT>, s STRUCT<x: INT>")
  {:ok, count} = DataFrame.count(df)
  {:ok, schema} = DataFrame.schema(df)
  {count, schema}
end)

run.("8b. Very long string values", fn ->
  long_str = String.duplicate("a", 10_000)
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => long_str},
    %{"id" => 2, "val" => "short"}
  ], schema: "id INT, val STRING")
  df |> DataFrame.select([
    col("id"),
    length(col("val")) |> Column.alias_("len")
  ]) |> DataFrame.collect()
end)

run.("8c. Special characters in strings", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello\nworld"},
    %{"id" => 2, "val" => "tab\there"},
    %{"id" => 3, "val" => "quote\"inside"},
    %{"id" => 4, "val" => "backslash\\here"},
    %{"id" => 5, "val" => "emoji 🎉🔥"},
    %{"id" => 6, "val" => ""}
  ], schema: "id INT, val STRING")
  df |> DataFrame.collect()
end)

# =============================================
# 9. Collect after write+read roundtrip (no maps in struct)
# =============================================

run.("9. Write complex types → read back (no map in struct)", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r68b_complex_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [1, 2, 3], "nested" => %{"x" => 1, "y" => "hello"}},
    %{"id" => 2, "arr" => nil, "nested" => nil},
    %{"id" => 3, "arr" => [], "nested" => %{"x" => nil, "y" => nil}}
  ], schema: "id INT, arr ARRAY<INT>, nested STRUCT<x: INT, y: STRING>")

  DataFrame.write_v2(df, table) |> SparkEx.WriterV2.create()

  {:ok, read_back} = SparkEx.Reader.table(s, table) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  read_back
end)

# =============================================
# 10. SQL-generated complex types
# =============================================

run.("10a. SQL named_struct with nulls", fn ->
  df = SparkEx.sql(s, """
    SELECT
      named_struct('a', 1, 'b', 'hello', 'c', array(1,2,3)) AS s1,
      named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS ARRAY<INT>)) AS s2,
      CAST(NULL AS STRUCT<a: INT, b: STRING, c: ARRAY<INT>>) AS s3
  """)
  df |> DataFrame.collect()
end)

run.("10b. SQL deeply nested struct<array<struct>>", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct(
      'items', array(
        named_struct('name', 'a', 'val', 1),
        named_struct('name', 'b', 'val', 2)
      )
    ) AS deep
  """)
  df |> DataFrame.collect()
end)

run.("10c. SQL array(named_struct(...)) with mixed nulls", fn ->
  df = SparkEx.sql(s, """
    SELECT array(
      named_struct('id', 1, 'val', 'a'),
      named_struct('id', 2, 'val', CAST(NULL AS STRING)),
      CAST(NULL AS STRUCT<id: INT, val: STRING>)
    ) AS arr_struct
  """)
  df |> DataFrame.collect()
end)

# =============================================
# 11. Array operations chain
# =============================================

run.("11. Array transform chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "nums" => [5, 3, 8, 1, 9, 2]},
    %{"id" => 2, "nums" => [10, 20]},
    %{"id" => 3, "nums" => nil}
  ], schema: "id INT, nums ARRAY<INT>")
  df |> DataFrame.select([
    col("id"),
    col("nums"),
    array_sort(col("nums")) |> Column.alias_("sorted"),
    size(col("nums")) |> Column.alias_("sz"),
    array_distinct(col("nums")) |> Column.alias_("distinct"),
    slice(col("nums"), lit(1), lit(3)) |> Column.alias_("first_3")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. Map operations (top-level only, not nested)
# =============================================

run.("12. Map operations chain (top-level maps)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "m1" => %{"a" => 1, "b" => 2}, "m2" => %{"b" => 3, "c" => 4}},
    %{"id" => 2, "m1" => %{"x" => 10}, "m2" => %{"y" => 20}},
    %{"id" => 3, "m1" => nil, "m2" => %{"z" => 30}}
  ], schema: "id INT, m1 MAP<STRING, INT>, m2 MAP<STRING, INT>")
  df |> DataFrame.select([
    col("id"),
    map_concat([col("m1"), col("m2")]) |> Column.alias_("merged"),
    map_keys(col("m1")) |> Column.alias_("m1_keys"),
    map_values(col("m2")) |> Column.alias_("m2_vals"),
    size(col("m1")) |> Column.alias_("m1_sz"),
    element_at(col("m1"), lit("a")) |> Column.alias_("m1_a")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. Struct field access after join
# =============================================

run.("13. Join on struct field then access nested", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "info" => %{"cat" => "A", "val" => 100}},
    %{"id" => 2, "info" => %{"cat" => "B", "val" => 200}}
  ], schema: "id INT, info STRUCT<cat: STRING, val: INT>")

  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"cat" => "A", "multiplier" => 1.5},
    %{"cat" => "B", "multiplier" => 2.0}
  ], schema: "cat STRING, multiplier DOUBLE")

  left = DataFrame.alias_(df1, "l")
  right = DataFrame.alias_(df2, "r")

  DataFrame.join(left, right,
    Column.get_field(col("l.info"), "cat") |> Column.eq(col("r.cat")),
    "inner")
  |> DataFrame.select([
    col("l.id"),
    Column.get_field(col("l.info"), "val") |> Column.alias_("val"),
    col("r.multiplier"),
    (Column.get_field(col("l.info"), "val") |> Column.cast("DOUBLE") |> Column.multiply(col("r.multiplier")))
    |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 14. 3-level nested struct with nulls
# =============================================

run.("14. 3-level nested struct with nulls at each level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"l1" => %{"l2" => %{"l3" => %{"val" => 1}}}},
    %{"l1" => %{"l2" => %{"l3" => nil}}},
    %{"l1" => %{"l2" => nil}},
    %{"l1" => nil}
  ], schema: "l1 STRUCT<l2: STRUCT<l3: STRUCT<val: INT>>>")
  df |> DataFrame.select([
    col("l1"),
    Column.get_field(col("l1"), "l2") |> Column.alias_("l2"),
    Column.get_field(Column.get_field(col("l1"), "l2"), "l3") |> Column.alias_("l3"),
    Column.get_field(Column.get_field(Column.get_field(col("l1"), "l2"), "l3"), "val") |> Column.alias_("val"),
    Column.is_null(col("l1")) |> Column.alias_("l1_null"),
    Column.is_null(Column.get_field(col("l1"), "l2")) |> Column.alias_("l2_null"),
    Column.is_null(Column.get_field(Column.get_field(col("l1"), "l2"), "l3")) |> Column.alias_("l3_null")
  ]) |> DataFrame.collect()
end)

# =============================================
# 15. Window over struct fields
# =============================================

run.("15. Window over struct fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..15, fn i ->
      %{"id" => i,
        "info" => %{"group" => Enum.at(["X", "Y", "Z"], rem(i, 3)), "score" => rem(i * 13, 100)}}
    end), schema: "id INT, info STRUCT<group: STRING, score: INT>")

  w = SparkEx.Window.partition_by([Column.get_field(col("info"), "group")])
    |> WindowSpec.order_by([desc(Column.get_field(col("info"), "score"))])

  df |> DataFrame.select([
    col("id"),
    col("info"),
    rank() |> Column.over(w) |> Column.alias_("rank"),
    sum(Column.get_field(col("info"), "score")) |> Column.over(
      SparkEx.Window.partition_by([Column.get_field(col("info"), "group")])
    ) |> Column.alias_("group_total")
  ])
  |> DataFrame.sort([asc(Column.get_field(col("info"), "group")), asc(col("rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 16. collect_list of structs
# =============================================

run.("16. collect_list of structs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "name" => "Alice", "val" => 10},
    %{"grp" => "A", "name" => "Bob", "val" => 20},
    %{"grp" => "B", "name" => "Carol", "val" => 30},
    %{"grp" => "B", "name" => "Dave", "val" => 40},
    %{"grp" => "B", "name" => "Eve", "val" => 50}
  ], schema: "grp STRING, name STRING, val INT")
  df
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    collect_list(SparkEx.Functions.struct([col("name"), col("val")])) |> Column.alias_("members"),
    count(col("name")) |> Column.alias_("cnt")
  ])
  |> DataFrame.sort([asc(col("grp"))])
  |> DataFrame.collect()
end)

# =============================================
# 17. create_map then collect
# =============================================

run.("17. create_map from columns", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"k1" => "a", "v1" => 1, "k2" => "b", "v2" => 2},
    %{"k1" => "c", "v1" => 3, "k2" => "d", "v2" => 4}
  ], schema: "k1 STRING, v1 INT, k2 STRING, v2 INT")
  df |> DataFrame.select([
    create_map([col("k1"), col("v1"), col("k2"), col("v2")]) |> Column.alias_("merged_map")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. to_json of nested struct with nulls (no map)
# =============================================

run.("18. to_json of struct with null inner fields (no map)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"a" => 1, "inner" => %{"x" => 10}, "arr" => [1, 2]}},
    %{"rec" => %{"a" => nil, "inner" => nil, "arr" => nil}},
    %{"rec" => nil}
  ], schema: "rec STRUCT<a: INT, inner: STRUCT<x: INT>, arr: ARRAY<INT>>")
  df |> DataFrame.select([
    to_json(col("rec")) |> Column.alias_("json")
  ]) |> DataFrame.collect()
end)

# =============================================
# 19. Isolate MAP-in-STRUCT: minimal repro
# =============================================

run.("19a. STRUCT<m: MAP<STRING,INT>> only (minimal)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"m" => %{"a" => 1}}}
  ], schema: "rec STRUCT<m: MAP<STRING, INT>>")
  df |> DataFrame.collect()
end)

run.("19b. STRUCT<x: INT, m: MAP<STRING,INT>> (struct+map)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"x" => 1, "m" => %{"a" => 1}}}
  ], schema: "rec STRUCT<x: INT, m: MAP<STRING, INT>>")
  df |> DataFrame.collect()
end)

run.("19c. Top-level MAP<STRING,INT> (no struct wrapper)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => 1, "b" => 2}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.collect()
end)

# =============================================
# 20. More validation gap hunting
# =============================================

run.("20a. DataFrame.limit with non-integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.limit(df, "five") |> DataFrame.collect()
end)

run.("20b. DataFrame.offset with non-integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.offset(df, "three") |> DataFrame.collect()
end)

run.("20c. DataFrame.repartition with non-integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.repartition(df, "four") |> DataFrame.collect()
end)

run.("20d. DataFrame.coalesce with non-integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.coalesce(df, "two") |> DataFrame.collect()
end)

run.("20e. WriterV2.create with non-string table name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_v2(df, 12345) |> SparkEx.WriterV2.create()
end)

run.("20f. DataFrame.hint with non-string hint name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.hint(df, 42) |> DataFrame.collect()
end)

run.("20g. Column.alias_ with non-string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([Column.alias_(col("x"), 123)]) |> DataFrame.collect()
end)

run.("20h. Column.cast with non-string type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  df |> DataFrame.select([Column.cast(col("x"), 42)]) |> DataFrame.collect()
end)

IO.puts("=== Round 68b complete ===")
