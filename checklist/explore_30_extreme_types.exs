# Round 10: Extreme data type edge cases — binary, timestamps, deeply nested,
# type coercions, schema evolution, complex collect patterns
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

# === 1. SQL-sourced deeply nested: struct containing array of structs ===
run.("1. SQL: struct<array<struct>>", fn ->
  SparkEx.sql(s, """
    SELECT
      1 AS id,
      named_struct(
        'team', 'alpha',
        'members', array(
          named_struct('name', 'Alice', 'role', 'lead'),
          named_struct('name', 'Bob', 'role', 'dev')
        )
      ) AS dept
  """)
  |> DataFrame.collect()
end)

# === 2. SQL: map<string, array<int>> ===
run.("2. SQL: map<string, array<int>>", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id, map('scores', array(90, 85, 92), 'weights', array(1, 2, 3)) AS data
  """)
  |> DataFrame.collect()
end)

# === 3. SQL: array<map<string, string>> ===
run.("3. SQL: array<map<string, string>>", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id, array(map('a', '1', 'b', '2'), map('c', '3')) AS items
  """)
  |> DataFrame.collect()
end)

# === 4. SQL: map<string, struct<...>> ===
run.("4. SQL: map<string, struct>", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id, map('alice', named_struct('age', 30, 'active', true)) AS people
  """)
  |> DataFrame.collect()
end)

# === 5. SQL: 4-level deep nesting ===
run.("5. SQL: 4-level deep nesting", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id,
      named_struct(
        'l1', named_struct(
          'l2', named_struct(
            'l3', named_struct(
              'value', 42
            )
          )
        )
      ) AS deep
  """)
  |> DataFrame.collect()
end)

# === 6. Binary data roundtrip ===
run.("6. SQL: binary data", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id,
           CAST(X'DEADBEEF' AS BINARY) AS bin_data,
           CAST('hello' AS BINARY) AS bin_str
  """)
  |> DataFrame.collect()
end)

# === 7. Timestamp with timezone ===
run.("7. SQL: timestamp_ntz vs timestamp", fn ->
  SparkEx.sql(s, """
    SELECT
      TIMESTAMP '2025-06-15 10:30:00' AS ts,
      CAST('2025-06-15 10:30:00' AS TIMESTAMP_NTZ) AS ts_ntz,
      CURRENT_TIMESTAMP() AS now
  """)
  |> DataFrame.collect()
end)

# === 8. All numeric types in one row ===
run.("8. SQL: all numeric types", fn ->
  SparkEx.sql(s, """
    SELECT
      CAST(1 AS TINYINT) AS tiny,
      CAST(2 AS SMALLINT) AS small,
      CAST(3 AS INT) AS int_val,
      CAST(4 AS BIGINT) AS big,
      CAST(5.5 AS FLOAT) AS flt,
      CAST(6.6 AS DOUBLE) AS dbl,
      CAST(7.77 AS DECIMAL(10,2)) AS dec
  """)
  |> DataFrame.collect()
end)

# === 9. create_map from columns ===
run.("9. create_map from columns", fn ->
  SparkEx.sql(s, "SELECT 'k1' AS key1, 10 AS val1, 'k2' AS key2, 20 AS val2")
  |> DataFrame.select([
    create_map([col("key1"), col("val1"), col("key2"), col("val2")])
    |> Column.alias_("result_map")
  ])
  |> DataFrame.collect()
end)

# === 10. create_array from columns ===
run.("10. create_array from columns", fn ->
  SparkEx.sql(s, "SELECT 10 AS a, 20 AS b, 30 AS c")
  |> DataFrame.select([
    array([col("a"), col("b"), col("c")]) |> Column.alias_("arr")
  ])
  |> DataFrame.collect()
end)

# === 11. Explode + re-collect ===
run.("11. Explode array then re-aggregate", fn ->
  SparkEx.sql(s, """
    SELECT 1 AS id, array(10, 20, 30) AS nums
    UNION ALL
    SELECT 2, array(40, 50)
  """)
  |> DataFrame.select([col("id"), explode(col("nums")) |> Column.alias_("num")])
  |> DataFrame.group_by([col("id")])
  |> DataFrame.agg([
    collect_list(col("num")) |> Column.alias_("collected"),
    sum(col("num")) |> Column.alias_("total")
  ])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

# === 12. Explode map (posexplode) ===
run.("12. posexplode on map", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([posexplode(col("m"))])
  |> DataFrame.collect()
end)

# === 13. collect_set vs collect_list ===
run.("13. collect_set vs collect_list", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('a', 1), ('a', 2), ('a', 1), ('b', 3), ('b', 3) AS t(grp, val)
  """)
  |> DataFrame.group_by([col("grp")])
  |> DataFrame.agg([
    collect_list(col("val")) |> Column.alias_("list_vals"),
    collect_set(col("val")) |> Column.alias_("set_vals")
  ])
  |> DataFrame.order_by([asc(col("grp"))])
  |> DataFrame.collect()
end)

# === 14. array_contains, array_distinct, array_sort ===
run.("14. Array manipulation functions", fn ->
  SparkEx.sql(s, "SELECT array(3, 1, 2, 1, 3) AS arr")
  |> DataFrame.select([
    col("arr"),
    array_contains(col("arr"), lit(2)) |> Column.alias_("has_2"),
    array_distinct(col("arr")) |> Column.alias_("distinct"),
    array_sort(col("arr")) |> Column.alias_("sorted"),
    array_size(col("arr")) |> Column.alias_("sz")
  ])
  |> DataFrame.collect()
end)

# === 15. map_keys, map_values, map_from_arrays ===
run.("15. Map manipulation functions", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([
    col("m"),
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("vals")
  ])
  |> DataFrame.collect()
end)

# === 16. map_from_arrays ===
run.("16. map_from_arrays", fn ->
  SparkEx.sql(s, "SELECT array('x', 'y', 'z') AS keys, array(10, 20, 30) AS vals")
  |> DataFrame.select([
    map_from_arrays(col("keys"), col("vals")) |> Column.alias_("built_map")
  ])
  |> DataFrame.collect()
end)

# === 17. struct() function ===
run.("17. struct() function to build struct column", fn ->
  SparkEx.sql(s, "SELECT 'Alice' AS name, 30 AS age, true AS active")
  |> DataFrame.select([
    SparkEx.Functions.struct([col("name"), col("age"), col("active")]) |> Column.alias_("person")
  ])
  |> DataFrame.collect()
end)

# === 18. get_json_object on complex JSON ===
run.("18. get_json_object on nested JSON", fn ->
  SparkEx.sql(s, """
    SELECT '{"user":{"name":"Alice","scores":[90,85,92],"addr":{"city":"NYC"}}}' AS json_str
  """)
  |> DataFrame.select([
    get_json_object(col("json_str"), "$.user.name") |> Column.alias_("name"),
    get_json_object(col("json_str"), "$.user.scores[0]") |> Column.alias_("first_score"),
    get_json_object(col("json_str"), "$.user.addr.city") |> Column.alias_("city")
  ])
  |> DataFrame.collect()
end)

# === 19. from_json with complex schema ===
run.("19. from_json with struct schema", fn ->
  SparkEx.sql(s, "SELECT '{\"name\":\"Alice\",\"age\":30}' AS json_str")
  |> DataFrame.select([
    from_json(col("json_str"), "name STRING, age INT") |> Column.alias_("parsed")
  ])
  |> DataFrame.collect()
end)

# === 20. to_json on struct ===
run.("20. to_json on struct column", fn ->
  SparkEx.sql(s, "SELECT named_struct('name', 'Alice', 'age', 30) AS person")
  |> DataFrame.select([
    to_json(col("person")) |> Column.alias_("json_out")
  ])
  |> DataFrame.collect()
end)

# === 21. schema_of_json ===
run.("21. schema_of_json", fn ->
  SparkEx.sql(s, """
    SELECT schema_of_json('[{"name":"Alice","scores":[1,2]}]') AS inferred_schema
  """)
  |> DataFrame.collect()
end)

# === 22. arrays_zip ===
run.("22. arrays_zip", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS ids, array('a', 'b', 'c') AS labels")
  |> DataFrame.select([
    arrays_zip([col("ids"), col("labels")]) |> Column.alias_("zipped")
  ])
  |> DataFrame.collect()
end)

# === 23. flatten nested arrays ===
run.("23. flatten nested arrays", fn ->
  SparkEx.sql(s, "SELECT array(array(1, 2), array(3, 4), array(5)) AS nested")
  |> DataFrame.select([
    flatten(col("nested")) |> Column.alias_("flat")
  ])
  |> DataFrame.collect()
end)

# === 24. sequence function ===
run.("24. sequence function", fn ->
  SparkEx.sql(s, "SELECT 1 AS start_val, 10 AS end_val, 2 AS step_val")
  |> DataFrame.select([
    sequence(col("start_val"), col("end_val"), col("step_val")) |> Column.alias_("seq")
  ])
  |> DataFrame.collect()
end)

# === 25. transform higher-order function ===
run.("25. transform higher-order (double each element)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS nums")
  |> DataFrame.select([
    transform(col("nums"), fn x -> Column.multiply(x, lit(2)) end) |> Column.alias_("doubled")
  ])
  |> DataFrame.collect()
end)

# === 26. filter higher-order function ===
run.("26. filter higher-order (keep evens)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5, 6) AS nums")
  |> DataFrame.select([
    SparkEx.Functions.filter(col("nums"), fn x -> Column.eq(Column.mod(x, lit(2)), lit(0)) end)
    |> Column.alias_("evens")
  ])
  |> DataFrame.collect()
end)

# === 27. aggregate higher-order function ===
run.("27. aggregate higher-order (sum array)", fn ->
  SparkEx.sql(s, "SELECT array(10, 20, 30, 40) AS nums")
  |> DataFrame.select([
    aggregate(col("nums"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("total")
  ])
  |> DataFrame.collect()
end)

# === 28. transform_keys and transform_values ===
run.("28. transform_keys / transform_values", fn ->
  SparkEx.sql(s, "SELECT map('hello', 1, 'world', 2) AS m")
  |> DataFrame.select([
    transform_keys(col("m"), fn k, _v -> upper(k) end) |> Column.alias_("upper_keys"),
    transform_values(col("m"), fn _k, v -> Column.multiply(v, lit(10)) end)
    |> Column.alias_("scaled_vals")
  ])
  |> DataFrame.collect()
end)

# === 29. exists and forall ===
run.("29. exists / forall on arrays", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS nums")
  |> DataFrame.select([
    exists(col("nums"), fn x -> Column.gt(x, lit(4)) end) |> Column.alias_("has_gt_4"),
    forall(col("nums"), fn x -> Column.gt(x, lit(0)) end) |> Column.alias_("all_positive")
  ])
  |> DataFrame.collect()
end)

# === 30. zip_with ===
run.("30. zip_with (element-wise multiply)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS a, array(10, 20, 30) AS b")
  |> DataFrame.select([
    zip_with(col("a"), col("b"), fn x, y -> Column.multiply(x, y) end)
    |> Column.alias_("products")
  ])
  |> DataFrame.collect()
end)

# === 31. Write array+map columns to Iceberg table and read back ===
run.("31. Complex types → Iceberg → read back", fn ->
  run_id = :erlang.system_time(:millisecond) |> to_string()
  tbl = "sfx.sfx_dev_warehouse.spark_ex_cplx_#{run_id}"

  SparkEx.sql(s, """
    CREATE TABLE #{tbl} (
      id INT,
      tags ARRAY<STRING>,
      scores MAP<STRING, INT>,
      info STRUCT<name: STRING, active: BOOLEAN>
    ) USING iceberg
  """)
  |> DataFrame.collect()

  SparkEx.sql(s, """
    INSERT INTO #{tbl} VALUES
      (1, array('a', 'b'), map('math', 90, 'eng', 85), named_struct('name', 'Alice', 'active', true)),
      (2, array('c'), map('math', 70), named_struct('name', 'Bob', 'active', false))
  """)
  |> DataFrame.collect()

  result = SparkEx.sql(s, "SELECT * FROM #{tbl} ORDER BY id") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE") |> DataFrame.collect()
  result
end)

# === 32. size() on MAP (EX-39 re-verify) ===
run.("32. [EX-39] size() on MAP column", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2, 'c', 3) AS m")
  |> DataFrame.select([size(col("m")) |> Column.alias_("map_size")])
  |> DataFrame.collect()
end)

# === 33. element_at on map and array ===
run.("33. element_at on map and array", fn ->
  SparkEx.sql(s, """
    SELECT map('a', 10, 'b', 20) AS m, array(100, 200, 300) AS arr
  """)
  |> DataFrame.select([
    element_at(col("m"), lit("a")) |> Column.alias_("map_val"),
    element_at(col("arr"), lit(2)) |> Column.alias_("arr_val")
  ])
  |> DataFrame.collect()
end)

# === 34. slice on array ===
run.("34. slice on array", fn ->
  SparkEx.sql(s, "SELECT array(10, 20, 30, 40, 50) AS arr")
  |> DataFrame.select([
    slice(col("arr"), lit(2), lit(3)) |> Column.alias_("sliced")
  ])
  |> DataFrame.collect()
end)

# === 35. array_union, array_intersect, array_except ===
run.("35. Array set operations", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4) AS a, array(3, 4, 5, 6) AS b")
  |> DataFrame.select([
    array_union(col("a"), col("b")) |> Column.alias_("union_"),
    array_intersect(col("a"), col("b")) |> Column.alias_("intersect_"),
    array_except(col("a"), col("b")) |> Column.alias_("except_")
  ])
  |> DataFrame.collect()
end)

IO.puts("=== Extreme type tests complete ===")
