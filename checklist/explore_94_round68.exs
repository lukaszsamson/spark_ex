# Round 68: Serialization/encoding deep dive
# Focus on the #2 bug category (25 historical bugs) — specifically:
# - Null inner struct decode (extending EX-129)
# - Nested array<struct> with nulls
# - Map with struct values containing nulls
# - Mixed null/non-null complex fields
# - Double-nested arrays/maps
# - Large struct with many nullable complex fields
# - create_dataframe with deeply nested nulls
# - Arrow IPC decode edge cases (empty batches, single row)
# - Collect after multiple transforms on complex types
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

# =============================================
# 1. Extend EX-129: null inner struct patterns
# =============================================

run.("1a. Struct with null inner struct (simple)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"outer" => %{"inner" => %{"x" => 1, "y" => "a"}, "z" => 10}},
    %{"outer" => %{"inner" => nil, "z" => 20}},
    %{"outer" => nil}
  ], schema: "outer STRUCT<inner: STRUCT<x: INT, y: STRING>, z: INT>")
  df |> DataFrame.collect()
end)

run.("1b. Struct with null inner struct + array field", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"inner" => %{"a" => 1}, "arr" => [1, 2, 3]}},
    %{"rec" => %{"inner" => nil, "arr" => [4, 5]}},
    %{"rec" => %{"inner" => %{"a" => 2}, "arr" => nil}},
    %{"rec" => nil}
  ], schema: "rec STRUCT<inner: STRUCT<a: INT>, arr: ARRAY<INT>>")
  df |> DataFrame.collect()
end)

run.("1c. Struct with null inner struct + map field", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"inner" => %{"a" => 1}, "m" => %{"k" => 1}}},
    %{"rec" => %{"inner" => nil, "m" => %{"k" => 2}}},
    %{"rec" => %{"inner" => %{"a" => 2}, "m" => nil}},
    %{"rec" => nil}
  ], schema: "rec STRUCT<inner: STRUCT<a: INT>, m: MAP<STRING, INT>>")
  df |> DataFrame.collect()
end)

run.("1d. Two inner structs, one null one not", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s1" => %{"a" => 1}, "s2" => %{"b" => "x"}},
    %{"s1" => nil, "s2" => %{"b" => "y"}},
    %{"s1" => %{"a" => 2}, "s2" => nil},
    %{"s1" => nil, "s2" => nil}
  ], schema: "s1 STRUCT<a: INT>, s2 STRUCT<b: STRING>")
  df |> DataFrame.collect()
end)

# =============================================
# 2. Array<struct> with null elements
# =============================================

run.("2a. Array of structs with null elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [%{"x" => 1}, %{"x" => 2}]},
    %{"arr" => [%{"x" => 3}, nil]},
    %{"arr" => [nil, nil]},
    %{"arr" => nil}
  ], schema: "arr ARRAY<STRUCT<x: INT>>")
  df |> DataFrame.collect()
end)

run.("2b. Array of structs - explode then collect back", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [%{"x" => 10, "y" => "a"}, %{"x" => 20, "y" => "b"}]},
    %{"id" => 2, "arr" => [%{"x" => 30, "y" => "c"}]},
    %{"id" => 3, "arr" => nil}
  ], schema: "id INT, arr ARRAY<STRUCT<x: INT, y: STRING>>")
  df
  |> DataFrame.select([col("id"), explode_outer(col("arr")) |> Column.alias_("elem")])
  |> DataFrame.select([
    col("id"),
    Column.get_field(col("elem"), "x") |> Column.alias_("x"),
    Column.get_field(col("elem"), "y") |> Column.alias_("y")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 3. Map with struct values
# =============================================

run.("3a. Map<string, struct> with nulls", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"k1" => %{"v" => 1}}},
    %{"m" => %{"k2" => nil}},
    %{"m" => nil}
  ], schema: "m MAP<STRING, STRUCT<v: INT>>")
  df |> DataFrame.collect()
end)

run.("3b. Map<string, array<int>> with empty/null values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => [1, 2, 3], "b" => [4, 5]}},
    %{"m" => %{"c" => [], "d" => nil}},
    %{"m" => nil}
  ], schema: "m MAP<STRING, ARRAY<INT>>")
  df |> DataFrame.collect()
end)

# =============================================
# 4. Double-nested arrays and maps
# =============================================

run.("4a. Array<array<int>> with nulls at each level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"aa" => [[1, 2], [3, 4]]},
    %{"aa" => [[5], nil]},
    %{"aa" => [nil, [6]]},
    %{"aa" => nil}
  ], schema: "aa ARRAY<ARRAY<INT>>")
  df |> DataFrame.collect()
end)

run.("4b. Array<map<string, int>>", fn ->
  # This is the EX-1 pattern (array of maps) - known Polars bigidx issue
  # Testing via SQL to see if it still crashes
  {:ok, df} = SparkEx.sql(s, "SELECT array(map('a', 1, 'b', 2)) AS arr_map")
  df |> DataFrame.collect()
end)

run.("4c. Map<string, map<string, int>> (nested maps)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"mm" => %{"outer1" => %{"inner1" => 1, "inner2" => 2}}},
    %{"mm" => %{"outer2" => nil}},
    %{"mm" => nil}
  ], schema: "mm MAP<STRING, MAP<STRING, INT>>")
  df |> DataFrame.collect()
end)

# =============================================
# 5. Large struct with many nullable complex fields
# =============================================

run.("5. Wide struct: 6 fields of different complex types", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{
      "f_int" => 1,
      "f_str" => "hello",
      "f_arr" => [1, 2],
      "f_map" => %{"k" => 1},
      "f_struct" => %{"x" => 10},
      "f_bool" => true
    }},
    %{"rec" => %{
      "f_int" => nil,
      "f_str" => nil,
      "f_arr" => nil,
      "f_map" => nil,
      "f_struct" => nil,
      "f_bool" => nil
    }},
    %{"rec" => nil}
  ], schema: "rec STRUCT<f_int: INT, f_str: STRING, f_arr: ARRAY<INT>, f_map: MAP<STRING, INT>, f_struct: STRUCT<x: INT>, f_bool: BOOLEAN>")
  df |> DataFrame.collect()
end)

# =============================================
# 6. Single-row edge cases
# =============================================

run.("6a. Single row with complex nested type", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => %{"items" => [%{"name" => "x", "val" => 1}], "meta" => %{"ver" => 1}}}
  ], schema: "data STRUCT<items: ARRAY<STRUCT<name: STRING, val: INT>>, meta: STRUCT<ver: INT>>")
  df |> DataFrame.collect()
end)

run.("6b. Single row with all-null complex fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a ARRAY<INT>, b MAP<STRING, INT>, c STRUCT<x: INT>")
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
    schema: "id INT, arr ARRAY<INT>, m MAP<STRING, INT>, s STRUCT<x: INT>")
  {:ok, count} = DataFrame.count(df)
  {:ok, schema} = DataFrame.schema(df)
  {count, schema}
end)

run.("8b. create_dataframe with very long string values", fn ->
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

run.("8c. create_dataframe with special characters in strings", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello\nworld"},
    %{"id" => 2, "val" => "tab\there"},
    %{"id" => 3, "val" => "quote\"inside"},
    %{"id" => 4, "val" => "backslash\\here"},
    %{"id" => 5, "val" => "null\0byte"},
    %{"id" => 6, "val" => "emoji 🎉🔥"},
    %{"id" => 7, "val" => ""}
  ], schema: "id INT, val STRING")
  df |> DataFrame.collect()
end)

# =============================================
# 9. Collect after write+read roundtrip of complex types
# =============================================

run_id = :erlang.system_time(:millisecond) |> to_string()

run.("9. Write complex types → read back → verify", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r68_complex_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [1, 2, 3], "m" => %{"a" => 10}, "nested" => %{"x" => 1, "y" => "hello"}},
    %{"id" => 2, "arr" => nil, "m" => nil, "nested" => nil},
    %{"id" => 3, "arr" => [], "m" => %{}, "nested" => %{"x" => nil, "y" => nil}}
  ], schema: "id INT, arr ARRAY<INT>, m MAP<STRING, INT>, nested STRUCT<x: INT, y: STRING>")

  DataFrame.write_v2(df, table) |> SparkEx.WriterV2.create()

  {:ok, read_back} = SparkEx.Reader.table(s, table) |> DataFrame.sort([asc(col("id"))]) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  read_back
end)

# =============================================
# 10. SQL-generated complex types then collect
# =============================================

run.("10a. SQL named_struct with nulls", fn ->
  {:ok, df} = SparkEx.sql(s, """
    SELECT
      named_struct('a', 1, 'b', 'hello', 'c', array(1,2,3)) AS s1,
      named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS ARRAY<INT>)) AS s2,
      CAST(NULL AS STRUCT<a: INT, b: STRING, c: ARRAY<INT>>) AS s3
  """)
  df |> DataFrame.collect()
end)

run.("10b. SQL deeply nested: struct<array<struct<map<string,int>>>>", fn ->
  {:ok, df} = SparkEx.sql(s, """
    SELECT named_struct(
      'items', array(
        named_struct('props', map('x', 1, 'y', 2)),
        named_struct('props', map('z', 3))
      )
    ) AS deep
  """)
  df |> DataFrame.collect()
end)

run.("10c. SQL array(named_struct(...)) with mixed nulls", fn ->
  {:ok, df} = SparkEx.sql(s, """
    SELECT array(
      named_struct('id', 1, 'val', 'a'),
      named_struct('id', 2, 'val', CAST(NULL AS STRING)),
      CAST(NULL AS STRUCT<id: INT, val: STRING>)
    ) AS arr_struct
  """)
  df |> DataFrame.collect()
end)

# =============================================
# 11. Chained operations on arrays
# =============================================

run.("11. Array transform chain: filter → sort → size → collect", fn ->
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
# 12. Map operations chain
# =============================================

run.("12. Map operations chain", fn ->
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

run.("13. Join on struct field then access nested fields", fn ->
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
# 14. Deeply nested: 3-level struct
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
# 15. Struct + window function decode
# =============================================

run.("15. Window over struct fields then collect", fn ->
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
# 16. Edge: collect_list of structs
# =============================================

run.("16. collect_list of structs (group → array<struct>)", fn ->
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
# 17. Edge: create_map then collect
# =============================================

run.("17. create_map from columns then collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"k1" => "a", "v1" => 1, "k2" => "b", "v2" => 2},
    %{"k1" => "c", "v1" => 3, "k2" => "d", "v2" => 4}
  ], schema: "k1 STRING, v1 INT, k2 STRING, v2 INT")
  df |> DataFrame.select([
    create_map([col("k1"), col("v1"), col("k2"), col("v2")]) |> Column.alias_("merged_map")
  ]) |> DataFrame.collect()
end)

# =============================================
# 18. Edge: to_json of nested struct with nulls
# =============================================

run.("18. to_json of struct with null inner fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"a" => 1, "inner" => %{"x" => 10}, "arr" => [1, 2]}},
    %{"rec" => %{"a" => nil, "inner" => nil, "arr" => nil}},
    %{"rec" => nil}
  ], schema: "rec STRUCT<a: INT, inner: STRUCT<x: INT>, arr: ARRAY<INT>>")
  df |> DataFrame.select([
    to_json(col("rec")) |> Column.alias_("json")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 68 complete ===")
