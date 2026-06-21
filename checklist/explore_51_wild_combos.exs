# Round 29: Wild API combinations — creative edge cases
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1]

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
# 1. Empty DataFrame edge cases
# =============================================

run.("1a. empty DataFrame (no rows)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, name STRING")
  df |> DataFrame.collect()
end)

run.("1b. empty DataFrame count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT")
  DataFrame.count(df)
end)

run.("1c. empty DataFrame group_by + agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "dept STRING, salary INT")
  DataFrame.group_by(df, [col("dept")])
  |> GroupedData.agg([sum(col("salary")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

run.("1d. empty DataFrame join non-empty", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT")
  {:ok, full} = SparkEx.create_dataframe(s, [%{"id" => 1, "v" => "x"}], schema: "id INT, v STRING")
  DataFrame.join(empty, full, ["id"]) |> DataFrame.collect()
end)

run.("1e. empty DataFrame union non-empty", fn ->
  {:ok, empty} = SparkEx.create_dataframe(s, [], schema: "id INT")
  {:ok, full} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.union(empty, full) |> DataFrame.collect()
end)

# =============================================
# 2. Single-column DataFrame edge cases
# =============================================

run.("2a. single-column select star", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 42}], schema: "x INT")
  df |> DataFrame.select([col("*")]) |> DataFrame.collect()
end)

run.("2b. single-column unpivot (should fail?)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 42}], schema: "x INT")
  DataFrame.unpivot(df, [], [col("x")], "var", "val") |> DataFrame.collect()
end)

# =============================================
# 3. HOF + Window combinations
# =============================================

run.("3a. transform inside window aggregation", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "scores" => [90, 85, 95]},
    %{"id" => 2, "scores" => [70, 80, 60]}
  ], schema: "id INT, scores ARRAY<INT>")

  df |> DataFrame.select([
    col("id"),
    transform(col("scores"), fn x -> Column.multiply(x, lit(2)) end) |> Column.alias_("doubled"),
    aggregate(col("scores"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("total")
  ]) |> DataFrame.collect()
end)

run.("3b. filter HOF then size", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
  ], schema: "id INT, arr ARRAY<INT>")

  df |> DataFrame.select([
    col("id"),
    size(filter(col("arr"), fn x -> Column.gt(x, lit(5)) end)) |> Column.alias_("count_gt5")
  ]) |> DataFrame.collect()
end)

run.("3c. nested HOFs: transform inside filter", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"matrix" => [[1, 2], [3, 4], [5, 6]]}
  ], schema: "matrix ARRAY<ARRAY<INT>>")

  # Filter sub-arrays where first element > 2
  df |> DataFrame.select([
    filter(col("matrix"), fn row ->
      Column.gt(element_at(row, lit(1)), lit(2))
    end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. when_/otherwise with complex expressions
# =============================================

run.("4a. nested when_ with arithmetic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    when_(Column.lt(col("val"), lit(3)), Column.multiply(col("val"), lit(100)))
    |> Column.when_(Column.lt(col("val"), lit(6)), Column.multiply(col("val"), lit(10)))
    |> Column.when_(Column.lt(col("val"), lit(9)), col("val"))
    |> Column.otherwise(lit(0))
    |> Column.alias_("bucketed")
  ]) |> DataFrame.collect()
end)

run.("4b. when_ with null check", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => nil}, %{"val" => 3}
  ], schema: "val INT")

  df |> DataFrame.select([
    col("val"),
    when_(Column.is_null(col("val")), lit("NULL"))
    |> Column.otherwise(Column.cast(col("val"), "STRING"))
    |> Column.alias_("safe_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. to_json + from_json roundtrip (via SQL source)
# =============================================

run.("5a. struct → to_json → from_json roundtrip", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct('name', 'Alice', 'age', 30, 'active', true) AS person
  """)

  df |> DataFrame.select([
    col("person"),
    to_json(col("person")) |> Column.alias_("json"),
    from_json(to_json(col("person")), "name STRING, age INT, active BOOLEAN")
    |> Column.alias_("roundtripped")
  ]) |> DataFrame.collect()
end)

run.("5b. nested struct → to_json → from_json", fn ->
  df = SparkEx.sql(s, """
    SELECT named_struct(
      'user', named_struct('name', 'Bob', 'age', 25),
      'scores', array(90, 85, 95)
    ) AS data
  """)

  df |> DataFrame.select([
    to_json(col("data")) |> Column.alias_("json")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Self-referential operations
# =============================================

run.("6a. self-join on same DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "parent_id" => nil},
    %{"id" => 2, "parent_id" => 1},
    %{"id" => 3, "parent_id" => 1},
    %{"id" => 4, "parent_id" => 2}
  ], schema: "id INT, parent_id INT")

  parent = DataFrame.alias_(df, "p")
  child = DataFrame.alias_(df, "c")

  DataFrame.join(child, parent,
    Column.eq(col("c.parent_id"), col("p.id")),
    "left"
  )
  |> DataFrame.select([
    col("c.id") |> Column.alias_("child_id"),
    col("p.id") |> Column.alias_("parent_id"),
  ])
  |> DataFrame.collect()
end)

run.("6b. same DataFrame used in union with itself", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.union(df, df) |> DataFrame.collect()
end)

run.("6c. DataFrame referenced after transformation (lazy evaluation)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 10},
    %{"id" => 2, "val" => 20}
  ], schema: "id INT, val INT")

  filtered = DataFrame.filter(df, Column.gt(col("val"), lit(5)))
  doubled = DataFrame.with_column(df, "val2", Column.multiply(col("val"), lit(2)))

  # Both should work independently
  {DataFrame.collect(filtered), DataFrame.collect(doubled)}
end)

# =============================================
# 7. Wide schema stress test
# =============================================

run.("7a. 50-column DataFrame", fn ->
  data = Map.new(1..50, fn i -> {"col_#{String.pad_leading(to_string(i), 2, "0")}", i} end)
  schema = Enum.map_join(1..50, ", ", fn i -> "col_#{String.pad_leading(to_string(i), 2, "0")} INT" end)

  {:ok, df} = SparkEx.create_dataframe(s, [data], schema: schema)
  {:ok, cols} = DataFrame.columns(df)
  {:ok, length(cols), List.first(cols), List.last(cols)}
end)

run.("7b. 50-column select all + computed", fn ->
  data = Map.new(1..50, fn i -> {"c#{i}", i} end)
  schema = Enum.map_join(1..50, ", ", fn i -> "c#{i} INT" end)

  {:ok, df} = SparkEx.create_dataframe(s, [data], schema: schema)
  sum_expr = Enum.reduce(1..50, lit(0), fn i, acc ->
    Column.plus(acc, col("c#{i}"))
  end)

  df |> DataFrame.select([sum_expr |> Column.alias_("total")]) |> DataFrame.collect()
end)

# =============================================
# 8. Type coercion edge cases
# =============================================

run.("8a. lit(true) and lit(false)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(true) |> Column.alias_("t"),
    lit(false) |> Column.alias_("f"),
    lit(nil) |> Column.alias_("n")
  ]) |> DataFrame.collect()
end)

run.("8b. lit with Decimal", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(Decimal.new("3.14159")) |> Column.alias_("pi")
  ]) |> DataFrame.collect()
end)

run.("8c. lit with Date and DateTime", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(~D[2025-06-15]) |> Column.alias_("dt"),
    lit(~N[2025-06-15 10:30:00]) |> Column.alias_("ts")
  ]) |> DataFrame.collect()
end)

run.("8d. lit with list (array literal)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit([1, 2, 3]) |> Column.alias_("arr")
  ]) |> DataFrame.collect()
end)

run.("8e. lit with map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(%{"key" => "val"}) |> Column.alias_("m")
  ]) |> DataFrame.collect()
end)

run.("8f. lit with very large integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit(9_999_999_999_999_999_999) |> Column.alias_("big")
  ]) |> DataFrame.collect()
end)

run.("8g. lit with empty string", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  df |> DataFrame.select([
    lit("") |> Column.alias_("empty")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Nested operations on computed columns
# =============================================

run.("9a. withColumn referencing previous withColumn", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")

  df
  |> DataFrame.with_column("y", Column.multiply(col("x"), lit(2)))
  |> DataFrame.with_column("z", Column.plus(col("x"), col("y")))
  |> DataFrame.with_column("w", Column.multiply(col("z"), col("y")))
  |> DataFrame.collect()
end)

run.("9b. filter on computed column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end),
    schema: "val INT")

  df
  |> DataFrame.with_column("squared", Column.multiply(col("val"), col("val")))
  |> DataFrame.filter(Column.gt(col("squared"), lit(25)))
  |> DataFrame.select([col("val"), col("squared")])
  |> DataFrame.collect()
end)

# =============================================
# 10. Multiple aggregations chained
# =============================================

run.("10a. agg result as input to further operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "A", "val" => 10},
    %{"dept" => "A", "val" => 20},
    %{"dept" => "B", "val" => 30},
    %{"dept" => "B", "val" => 40}
  ], schema: "dept STRING, val INT")

  agg_result = DataFrame.group_by(df, [col("dept")])
  |> GroupedData.agg([
    sum(col("val")) |> Column.alias_("total"),
    avg(col("val")) |> Column.alias_("avg_val")
  ])

  # Now filter and transform the aggregation result
  agg_result
  |> DataFrame.filter(Column.gt(col("total"), lit(25)))
  |> DataFrame.with_column("normalized", Column.divide(col("total"), col("avg_val")))
  |> DataFrame.collect()
end)

# =============================================
# 11. Complex SQL mixed with DataFrame API
# =============================================

run.("11a. SQL subquery → DataFrame transform → collect", fn ->
  sql_df = SparkEx.sql(s, """
    SELECT id, id * 10 AS val
    FROM (SELECT explode(sequence(1, 10)) AS id)
  """)

  sql_df
  |> DataFrame.filter(Column.gt(col("val"), lit(30)))
  |> DataFrame.with_column("category",
    when_(Column.gt(col("val"), lit(70)), lit("high"))
    |> Column.otherwise(lit("medium"))
  )
  |> DataFrame.collect()
end)

run.("11b. SQL UDF + DataFrame pipeline", fn ->
  # Use SQL to register a temp view, then use DataFrame API
  source = SparkEx.sql(s, """
    SELECT
      posexplode(array('alpha', 'beta', 'gamma', 'delta')) AS (idx, word)
  """)
  DataFrame.create_or_replace_temp_view(source, "words_#{run_id}")

  SparkEx.Reader.table(s, "words_#{run_id}")
  |> DataFrame.with_column("upper_word", upper(col("word")))
  |> DataFrame.with_column("word_len", SparkEx.Functions.length(col("word")))
  |> DataFrame.sort([desc(col("word_len"))])
  |> DataFrame.collect()
end)

# =============================================
# 12. Column expression nesting depth
# =============================================

run.("12a. deeply nested arithmetic (10 levels)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 2}], schema: "x INT")

  # ((((((((((x+1)+1)+1)+1)+1)+1)+1)+1)+1)+1) = x + 10
  expr = Enum.reduce(1..10, col("x"), fn _, acc ->
    Column.plus(acc, lit(1))
  end)

  df |> DataFrame.select([expr |> Column.alias_("result")]) |> DataFrame.collect()
end)

run.("12b. deeply nested when_/otherwise (10 levels)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(0..10, fn i -> %{"val" => i} end),
    schema: "val INT")

  expr = Enum.reduce(0..9, nil, fn i, acc ->
    cond_expr = Column.eq(col("val"), lit(i))
    label = lit("label_#{i}")
    case acc do
      nil -> when_(cond_expr, label)
      chain -> Column.when_(chain, cond_expr, label)
    end
  end)
  |> Column.otherwise(lit("other"))

  df |> DataFrame.select([col("val"), expr |> Column.alias_("label")])
  |> DataFrame.sort([asc(col("val"))])
  |> DataFrame.collect()
end)

run.("12c. 20 chained filters", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")

  # Chain 20 filter conditions: id > 1, id > 2, ..., id > 20
  result = Enum.reduce(1..20, df, fn i, acc ->
    DataFrame.filter(acc, Column.gt(col("id"), lit(i)))
  end)
  |> DataFrame.count()

  result
end)

# =============================================
# 13. Mixed column types in same operation
# =============================================

run.("13a. concat mixing types with cast", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice", "age" => 30, "score" => 95.5}
  ], schema: "name STRING, age INT, score DOUBLE")

  df |> DataFrame.select([
    concat([col("name"), lit(": age="), Column.cast(col("age"), "STRING"),
            lit(", score="), Column.cast(col("score"), "STRING")])
    |> Column.alias_("summary")
  ]) |> DataFrame.collect()
end)

run.("13b. array of mixed-origin expressions", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 5}], schema: "x INT")

  df |> DataFrame.select([
    array([
      col("x"),
      Column.plus(col("x"), lit(1)),
      Column.multiply(col("x"), lit(2)),
      lit(100)
    ]) |> Column.alias_("computed_array")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Pivot with complex aggregation
# =============================================

run.("14a. pivot with multiple aggregation values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"year" => 2024, "quarter" => "Q1", "region" => "US", "revenue" => 100},
    %{"year" => 2024, "quarter" => "Q2", "region" => "US", "revenue" => 150},
    %{"year" => 2024, "quarter" => "Q1", "region" => "EU", "revenue" => 80},
    %{"year" => 2024, "quarter" => "Q2", "region" => "EU", "revenue" => 120},
    %{"year" => 2025, "quarter" => "Q1", "region" => "US", "revenue" => 200}
  ], schema: "year INT, quarter STRING, region STRING, revenue INT")

  DataFrame.group_by(df, [col("region"), col("year")])
  |> GroupedData.pivot("quarter", ["Q1", "Q2"])
  |> GroupedData.agg([sum(col("revenue"))])
  |> DataFrame.sort([asc(col("region")), asc(col("year"))])
  |> DataFrame.collect()
end)

# =============================================
# 15. DataFrame.observe (metrics collection)
# =============================================

run.("15. DataFrame.observe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i} end),
    schema: "val INT")

  df
  |> DataFrame.observe("my_metrics", [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 16. Special characters in column names
# =============================================

run.("16a. column names with spaces", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS `my column`, 2 AS `another col`")
  df |> DataFrame.collect()
end)

run.("16b. column names with special chars", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS `col.with.dots`, 2 AS `col-with-dashes`")
  df |> DataFrame.collect()
end)

run.("16c. backtick column reference", fn ->
  df = SparkEx.sql(s, "SELECT 1 AS `my column`")
  df |> DataFrame.select([col("`my column`") |> Column.alias_("renamed")]) |> DataFrame.collect()
end)

# =============================================
# 17. Lateral column alias (Spark 3.4+)
# =============================================

run.("17. lateral column alias via SQL", fn ->
  SparkEx.sql(s, """
    SELECT id, id * 10 AS val, val + 1 AS val_plus
    FROM (SELECT 5 AS id)
  """) |> DataFrame.collect()
end)

# =============================================
# 18. Very long string handling
# =============================================

run.("18a. 10K character string", fn ->
  long_str = String.duplicate("x", 10_000)
  {:ok, df} = SparkEx.create_dataframe(s, [%{"text" => long_str}], schema: "text STRING")
  {:ok, result} = df |> DataFrame.select([
    SparkEx.Functions.length(col("text")) |> Column.alias_("len")
  ]) |> DataFrame.collect()
  result
end)

run.("18b. 100K character string", fn ->
  long_str = String.duplicate("abcde", 20_000)
  {:ok, df} = SparkEx.create_dataframe(s, [%{"text" => long_str}], schema: "text STRING")
  {:ok, result} = df |> DataFrame.select([
    SparkEx.Functions.length(col("text")) |> Column.alias_("len")
  ]) |> DataFrame.collect()
  result
end)

# =============================================
# 19. Multiple temp views + cross-reference
# =============================================

run.("19. multiple temp views joined", fn ->
  {:ok, users} = SparkEx.create_dataframe(s, [
    %{"uid" => 1, "name" => "Alice"},
    %{"uid" => 2, "name" => "Bob"}
  ], schema: "uid INT, name STRING")

  {:ok, orders} = SparkEx.create_dataframe(s, [
    %{"oid" => 100, "uid" => 1, "amount" => 50},
    %{"oid" => 101, "uid" => 1, "amount" => 30},
    %{"oid" => 102, "uid" => 2, "amount" => 70}
  ], schema: "oid INT, uid INT, amount INT")

  DataFrame.create_or_replace_temp_view(users, "users_#{run_id}")
  DataFrame.create_or_replace_temp_view(orders, "orders_#{run_id}")

  # Join via SQL referencing both views
  result = SparkEx.sql(s, """
    SELECT u.name, SUM(o.amount) AS total
    FROM users_#{run_id} u
    JOIN orders_#{run_id} o ON u.uid = o.uid
    GROUP BY u.name
    ORDER BY total DESC
  """) |> DataFrame.collect()

  DataFrame.drop_temp_view(s, "users_#{run_id}")
  DataFrame.drop_temp_view(s, "orders_#{run_id}")
  result
end)

# =============================================
# 20. Chained renames + transformations
# =============================================

run.("20. chained with_columns_renamed + transforms", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"firstName" => "Alice", "lastName" => "Smith", "yearOfBirth" => 1990}
  ], schema: "firstName STRING, lastName STRING, yearOfBirth INT")

  df
  |> DataFrame.with_columns_renamed(fn name -> Macro.underscore(name) end)
  |> DataFrame.with_column("full_name",
    concat([col("first_name"), lit(" "), col("last_name")]))
  |> DataFrame.with_column("age", Column.minus(lit(2026), col("year_of_birth")))
  |> DataFrame.select([col("full_name"), col("age")])
  |> DataFrame.collect()
end)

# =============================================
# 21. create_dataframe with 1000 rows
# =============================================

run.("21. 1000-row create_dataframe", fn ->
  data = Enum.map(1..1000, fn i -> %{"id" => i, "val" => rem(i * 7, 100)} end)
  {:ok, df} = SparkEx.create_dataframe(s, data, schema: "id INT, val INT")

  df
  |> DataFrame.group_by([col("val")])
  |> GroupedData.agg([count(col("id")) |> Column.alias_("cnt")])
  |> DataFrame.sort([desc(col("cnt"))])
  |> DataFrame.limit(5)
  |> DataFrame.collect()
end)

# =============================================
# 22. Nested maps/arrays in SQL + DataFrame ops
# =============================================

run.("22a. map_from_entries", fn ->
  df = SparkEx.sql(s, """
    SELECT map_from_entries(array(struct('a', 1), struct('b', 2), struct('c', 3))) AS m
  """)
  df |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("vals"),
    Column.get_item(col("m"), "b") |> Column.alias_("b_val")
  ]) |> DataFrame.collect()
end)

run.("22b. array of structs → explode → aggregate", fn ->
  df = SparkEx.sql(s, """
    SELECT array(
      named_struct('product', 'Widget', 'qty', 5, 'price', 10.0),
      named_struct('product', 'Gadget', 'qty', 3, 'price', 25.0),
      named_struct('product', 'Widget', 'qty', 2, 'price', 10.0)
    ) AS items
  """)

  df
  |> DataFrame.select([explode(col("items")) |> Column.alias_("item")])
  |> DataFrame.select([
    col("item.product") |> Column.alias_("product"),
    Column.multiply(col("item.qty"), col("item.price")) |> Column.alias_("line_total")
  ])
  |> DataFrame.group_by([col("product")])
  |> GroupedData.agg([
    sum(col("line_total")) |> Column.alias_("total"),
    sum(col("line_total")) |> Column.alias_("total_qty")
  ])
  |> DataFrame.collect()
end)

# =============================================
# 23. Column.is_null / is_not_null chains
# =============================================

run.("23. complex null logic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => 3},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c INT")

  df |> DataFrame.select([
    col("a"), col("b"), col("c"),
    # Count non-null columns
    Column.plus(
      Column.plus(
        when_(Column.is_not_null(col("a")), lit(1)) |> Column.otherwise(lit(0)),
        when_(Column.is_not_null(col("b")), lit(1)) |> Column.otherwise(lit(0))
      ),
      when_(Column.is_not_null(col("c")), lit(1)) |> Column.otherwise(lit(0))
    ) |> Column.alias_("non_null_count")
  ]) |> DataFrame.collect()
end)

# =============================================
# 24. Window function with different frame specs
# =============================================

run.("24. multiple window specs on same query", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "grp" => rem(i, 3), "val" => i * 10} end),
    schema: "id INT, grp INT, val INT")

  w_global = SparkEx.Window.partition_by([])
  |> WindowSpec.order_by([asc(col("id"))])

  w_group = SparkEx.Window.partition_by([col("grp")])
  |> WindowSpec.order_by([asc(col("id"))])

  w_sliding = SparkEx.Window.partition_by([])
  |> WindowSpec.order_by([asc(col("id"))])
  |> WindowSpec.rows_between(-2, 0)

  df |> DataFrame.select([
    col("id"), col("grp"), col("val"),
    row_number() |> Column.over(w_global) |> Column.alias_("global_rn"),
    row_number() |> Column.over(w_group) |> Column.alias_("group_rn"),
    sum(col("val")) |> Column.over(w_global) |> Column.alias_("running_total"),
    avg(col("val")) |> Column.over(w_sliding) |> Column.alias_("moving_avg_3")
  ]) |> DataFrame.collect()
end)

# =============================================
# 25. DataFrame.sample edge cases
# =============================================

run.("25a. sample with fraction 0.0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")
  {:ok, result} = DataFrame.sample(df, 0.0) |> DataFrame.collect()
  length(result)
end)

run.("25b. sample with fraction 1.0", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end),
    schema: "id INT")
  {:ok, result} = DataFrame.sample(df, 1.0) |> DataFrame.collect()
  length(result)
end)

IO.puts("=== Round 29 wild combinations complete ===")
