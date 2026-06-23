# Round 67: Targeted bug hunting — protobuf validation gaps, option type mismatches,
# complex nested encoding edge cases, argument handling gaps
# Focus areas: session crash patterns, serialization edge cases, option validation
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
# 1. as_of_join :tolerance with invalid types
# =============================================

run.("1a. as_of_join tolerance: string (should be Column/lit)", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"ts" => 1, "val" => "a"}, %{"ts" => 5, "val" => "b"}
  ], schema: "ts INT, val STRING")
  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"ts" => 2, "val2" => "x"}, %{"ts" => 6, "val2" => "y"}
  ], schema: "ts INT, val2 STRING")
  DataFrame.as_of_join(left, right, col("ts"), col("ts"),
    tolerance: "invalid_string")
  |> DataFrame.collect()
end)

run.("1b. as_of_join tolerance: integer (should be Column/lit)", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"ts" => 1, "val" => "a"}, %{"ts" => 5, "val" => "b"}
  ], schema: "ts INT, val STRING")
  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"ts" => 2, "val2" => "x"}, %{"ts" => 6, "val2" => "y"}
  ], schema: "ts INT, val2 STRING")
  DataFrame.as_of_join(left, right, col("ts"), col("ts"),
    tolerance: 42)
  |> DataFrame.collect()
end)

run.("1c. as_of_join tolerance: atom", fn ->
  {:ok, left} = SparkEx.create_dataframe(s, [
    %{"ts" => 1, "val" => "a"}
  ], schema: "ts INT, val STRING")
  {:ok, right} = SparkEx.create_dataframe(s, [
    %{"ts" => 2, "val2" => "x"}
  ], schema: "ts INT, val2 STRING")
  DataFrame.as_of_join(left, right, col("ts"), col("ts"),
    tolerance: :infinity)
  |> DataFrame.collect()
end)

# =============================================
# 2. StreamWriter trigger with invalid types
# =============================================

run.("2a. StreamWriter trigger: bare atom", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  SparkEx.DataFrame.write_stream(df)
  |> SparkEx.StreamWriter.format("memory")
  |> SparkEx.StreamWriter.query_name("test_trigger_atom_#{run_id}")
  |> SparkEx.StreamWriter.trigger(:invalid_atom)
  |> SparkEx.StreamWriter.start()
end)

run.("2b. StreamWriter trigger: integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  SparkEx.DataFrame.write_stream(df)
  |> SparkEx.StreamWriter.format("memory")
  |> SparkEx.StreamWriter.query_name("test_trigger_int_#{run_id}")
  |> SparkEx.StreamWriter.trigger(42)
  |> SparkEx.StreamWriter.start()
end)

run.("2c. StreamWriter trigger: string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  SparkEx.DataFrame.write_stream(df)
  |> SparkEx.StreamWriter.format("memory")
  |> SparkEx.StreamWriter.query_name("test_trigger_str_#{run_id}")
  |> SparkEx.StreamWriter.trigger("once")
  |> SparkEx.StreamWriter.start()
end)

# =============================================
# 3. Catalog.create_database with invalid option types
# =============================================

run.("3a. create_database comment: integer", fn ->
  Catalog.create_database(s, "spark_ex_test_db_#{run_id}", comment: 42)
end)

run.("3b. create_database set_properties: string (should be map)", fn ->
  Catalog.create_database(s, "spark_ex_test_db2_#{run_id}", set_properties: "not_a_map")
end)

run.("3c. create_database set_properties: list", fn ->
  Catalog.create_database(s, "spark_ex_test_db3_#{run_id}", set_properties: [{"a", "b"}])
end)

run.("3d. create_database location: integer", fn ->
  Catalog.create_database(s, "spark_ex_test_db4_#{run_id}", location: 42)
end)

# =============================================
# 4. Writer mode with invalid types
# =============================================

run.("4a. Writer mode: integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df) |> SparkEx.Writer.mode(42) |> SparkEx.Writer.save_as_table("dummy")
end)

run.("4b. Writer mode: nil", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df) |> SparkEx.Writer.mode(nil) |> SparkEx.Writer.save_as_table("dummy")
end)

run.("4c. Writer mode: list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write(df) |> SparkEx.Writer.mode([:overwrite]) |> SparkEx.Writer.save_as_table("dummy")
end)

# =============================================
# 5. from_protobuf / to_protobuf with invalid options
# =============================================

run.("5a. from_json with options: string instead of map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ~s({"a":1})}
  ], schema: "json STRING")
  df |> DataFrame.select([
    from_json(col("json"), "STRUCT<a: INT>", "not_a_map")
    |> Column.alias_("parsed")
  ]) |> DataFrame.collect()
end)

run.("5b. to_json with options: string instead of map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => "hello"}
  ], schema: "a INT, b STRING")
  df |> DataFrame.select([
    to_json(SparkEx.Functions.struct([col("a"), col("b")]), "not_a_map")
    |> Column.alias_("json_str")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Complex nested encoding edge cases
# =============================================

run.("6a. Array of arrays of arrays (3-deep)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"nested" => [[[1, 2], [3, 4]], [[5, 6]]]}
  ], schema: "nested ARRAY<ARRAY<ARRAY<INT>>>")
  df |> DataFrame.collect()
end)

run.("6b. Map with array values containing structs", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => %{"team_a" => [%{"name" => "Alice", "score" => 95}],
                   "team_b" => [%{"name" => "Bob", "score" => 88}, %{"name" => "Carol", "score" => 92}]}}
  ], schema: "data MAP<STRING, ARRAY<STRUCT<name: STRING, score: INT>>>")
  df |> DataFrame.collect()
end)

run.("6c. Struct containing map of arrays", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"id" => 1, "tags" => %{"colors" => ["red", "blue"], "sizes" => ["S", "M", "L"]}}}
  ], schema: "rec STRUCT<id: INT, tags: MAP<STRING, ARRAY<STRING>>>")
  df |> DataFrame.select([
    col("rec"),
    Column.get_field(col("rec"), "id") |> Column.alias_("id"),
    Column.get_field(col("rec"), "tags") |> Column.alias_("tags")
  ]) |> DataFrame.collect()
end)

run.("6d. Array of maps with integer keys", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"data" => [%{1 => "one", 2 => "two"}, %{3 => "three"}]}
  ], schema: "data ARRAY<MAP<INT, STRING>>")
  df |> DataFrame.collect()
end)

run.("6e. Deeply nested: struct → array → struct → map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"root" => %{"items" => [
      %{"props" => %{"color" => "red", "weight" => "5kg"}},
      %{"props" => %{"color" => "blue", "weight" => "3kg"}}
    ]}}
  ], schema: "root STRUCT<items: ARRAY<STRUCT<props: MAP<STRING, STRING>>>>")
  df |> DataFrame.select([
    col("root"),
    Column.get_field(col("root"), "items") |> Column.alias_("items")
  ]) |> DataFrame.collect()
end)

# =============================================
# 7. lit() with edge case types
# =============================================

run.("7a. lit(tuple)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([lit({1, 2, 3}) |> Column.alias_("tuple_lit")]) |> DataFrame.collect()
end)

run.("7b. lit(pid)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([lit(self()) |> Column.alias_("pid_lit")]) |> DataFrame.collect()
end)

run.("7c. lit(reference)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([lit(make_ref()) |> Column.alias_("ref_lit")]) |> DataFrame.collect()
end)

run.("7d. lit(atom)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([lit(:hello) |> Column.alias_("atom_lit")]) |> DataFrame.collect()
end)

run.("7e. lit(very large integer, > INT64)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(99999999999999999999999) |> Column.alias_("huge_int")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. create_dataframe with type mismatches
# =============================================

run.("8a. create_dataframe INT column with float values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1.5}, %{"val" => 2.7}
  ], schema: "val INT")
  df |> DataFrame.collect()
end)

run.("8b. create_dataframe STRING column with integer values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 42}, %{"val" => 100}
  ], schema: "val STRING")
  df |> DataFrame.collect()
end)

run.("8c. create_dataframe BOOLEAN column with string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "true"}, %{"val" => "false"}, %{"val" => "yes"}
  ], schema: "val BOOLEAN")
  df |> DataFrame.collect()
end)

run.("8d. create_dataframe ARRAY<INT> with mixed types in array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [1, "two", 3.0, nil]}
  ], schema: "arr ARRAY<STRING>")
  df |> DataFrame.collect()
end)

# =============================================
# 9. DataFrame.join with invalid join types
# =============================================

run.("9a. join type: invalid string", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, b} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.join(a, b, col("id"), "invalid_join_type") |> DataFrame.collect()
end)

run.("9b. join type: integer", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, b} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.join(a, b, col("id"), 42) |> DataFrame.collect()
end)

run.("9c. join type: nil", fn ->
  {:ok, a} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  {:ok, b} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.join(a, b, col("id"), nil) |> DataFrame.collect()
end)

# =============================================
# 10. GroupedData.pivot with invalid column types
# =============================================

run.("10a. pivot with string column name (not Column)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "cat" => "x", "val" => 1}
  ], schema: "grp STRING, cat STRING, val INT")
  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.pivot("cat", ["x"])
  |> GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

run.("10b. pivot with integer value list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "cat" => 1, "val" => 100},
    %{"grp" => "A", "cat" => 2, "val" => 200}
  ], schema: "grp STRING, cat INT, val INT")
  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.pivot(col("cat"), [1, 2])
  |> GroupedData.agg([sum(col("val")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# =============================================
# 11. WriterV2 with invalid table names
# =============================================

run.("11a. WriterV2 with empty string table name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, "") |> SparkEx.WriterV2.create()
end)

run.("11b. WriterV2 with nil table name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.write_v2(df, nil) |> SparkEx.WriterV2.create()
end)

# =============================================
# 12. DataFrame.with_column invalid column name types
# =============================================

run.("12a. with_column name: integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.with_column(df, 42, lit("hello")) |> DataFrame.collect()
end)

run.("12b. with_column name: atom", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.with_column(df, :new_col, lit("hello")) |> DataFrame.collect()
end)

run.("12c. with_column name: nil", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.with_column(df, nil, lit("hello")) |> DataFrame.collect()
end)

# =============================================
# 13. DataFrame.fillna / dropna with edge case types
# =============================================

run.("13a. fillna with atom value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => 1}
  ], schema: "a INT, b INT")
  DataFrame.fillna(df, :zero) |> DataFrame.collect()
end)

run.("13b. fillna with list value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => nil, "b" => 1}
  ], schema: "a INT, b INT")
  DataFrame.fillna(df, [0, 1]) |> DataFrame.collect()
end)

run.("13c. dropna thresh: negative", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil}, %{"a" => nil, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, thresh: -1) |> DataFrame.collect()
end)

run.("13d. dropna how: integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, how: 42) |> DataFrame.collect()
end)

run.("13e. dropna subset: integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil}
  ], schema: "a INT, b INT")
  DataFrame.dropna(df, subset: 42) |> DataFrame.collect()
end)

# =============================================
# 14. Catalog with invalid types
# =============================================

run.("14a. Catalog.drop_table with integer table name", fn ->
  Catalog.drop_table(s, 42)
end)

run.("14b. Catalog.table_exists? with integer", fn ->
  Catalog.table_exists?(s, 42)
end)

run.("14c. Catalog.list_tables with integer db_name", fn ->
  Catalog.list_tables(s, 42)
end)

# =============================================
# 15. Complex encoding: create_dataframe with heterogeneous nested nulls
# =============================================

run.("15a. Array with all null elements", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [nil, nil, nil]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.collect()
end)

run.("15b. Map with null values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"a" => nil, "b" => nil}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.collect()
end)

run.("15c. Struct with all null fields", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => %{"a" => nil, "b" => nil, "c" => nil}}
  ], schema: "s STRUCT<a: INT, b: STRING, c: DOUBLE>")
  df |> DataFrame.collect()
end)

run.("15d. Mixed null at every level", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"outer" => %{"arr" => [nil], "m" => %{"k" => nil}, "inner" => nil}}
  ], schema: "outer STRUCT<arr: ARRAY<INT>, m: MAP<STRING, INT>, inner: STRUCT<x: INT>>")
  df |> DataFrame.collect()
end)

IO.puts("=== Round 67 complete ===")
