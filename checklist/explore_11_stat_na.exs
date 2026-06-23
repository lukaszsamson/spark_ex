# Exploratory: DataFrame.stat methods, DataFrame.na methods, complex null handling
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

# === DataFrame.na operations ===
na_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 50000.0, 'Engineering'),
    (2, NULL, 60000.0, 'Sales'),
    (3, 'Carol', NULL, NULL),
    (4, 'Dave', 70000.0, 'HR'),
    (5, NULL, NULL, NULL)
  AS t(id, name, salary, dept)
""")

run.("1. DataFrame.dropna (any)", fn ->
  DataFrame.dropna(na_df) |> DataFrame.collect()
end)

run.("2. DataFrame.dropna with how: :all", fn ->
  DataFrame.dropna(na_df, how: :all) |> DataFrame.collect()
end)

run.("3. DataFrame.dropna with subset", fn ->
  DataFrame.dropna(na_df, subset: ["name"]) |> DataFrame.collect()
end)

run.("4. DataFrame.dropna with thresh", fn ->
  DataFrame.dropna(na_df, thresh: 3) |> DataFrame.collect()
end)

run.("5. DataFrame.fillna with value", fn ->
  DataFrame.fillna(na_df, "UNKNOWN") |> DataFrame.collect()
end)

run.("6. DataFrame.fillna with map (different values per column)", fn ->
  DataFrame.fillna(na_df, %{"name" => "UNKNOWN", "salary" => 0.0, "dept" => "Unassigned"})
  |> DataFrame.collect()
end)

run.("7. DataFrame.replace with multiple replacements", fn ->
  DataFrame.replace(na_df, %{"Engineering" => "ENG", "Sales" => "SALES", "HR" => "HUMAN_RES"})
  |> DataFrame.collect()
end)

# === DataFrame.stat operations ===
stat_df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 10.0, 100.0), (2, 20.0, 200.0), (3, 30.0, 150.0),
    (4, 40.0, 250.0), (5, 50.0, 300.0), (6, 60.0, 350.0)
  AS t(id, x, y)
""")

run.("8. DataFrame.corr", fn ->
  DataFrame.corr(stat_df, "x", "y")
end)

run.("9. DataFrame.cov", fn ->
  DataFrame.cov(stat_df, "x", "y")
end)

run.("10. DataFrame.approx_quantile (single column)", fn ->
  DataFrame.approx_quantile(stat_df, ["x"], [0.25, 0.5, 0.75], 0.01)
end)

run.("11. DataFrame.approx_quantile (multiple columns)", fn ->
  DataFrame.approx_quantile(stat_df, ["x", "y"], [0.25, 0.5, 0.75], 0.01)
end)

run.("12. DataFrame.freq_items", fn ->
  freq_df = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('a', 1), ('a', 2), ('a', 1), ('b', 2), ('a', 1), ('c', 3), ('a', 1)
    AS t(letter, num)
  """)
  DataFrame.freq_items(freq_df, ["letter", "num"])
end)

run.("13. DataFrame.sample_by (stratified sampling)", fn ->
  sample_df = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('a', 1), ('a', 2), ('a', 3), ('a', 4), ('a', 5),
      ('b', 6), ('b', 7), ('b', 8), ('b', 9), ('b', 10)
    AS t(grp, val)
  """)
  DataFrame.sample_by(sample_df, "grp", %{"a" => 0.4, "b" => 0.6}, 42)
end)

run.("14. DataFrame.crosstab", fn ->
  ct_df = SparkEx.sql(s, """
    SELECT * FROM VALUES
      ('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'x'), ('a', 'x')
    AS t(col1, col2)
  """)
  DataFrame.crosstab(ct_df, "col1", "col2") |> DataFrame.collect()
end)

# === Complex null patterns ===
run.("15. Null-safe equal (eqNullSafe / <=>)", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, NULL), (2, 'a'), (3, NULL), (4, 'b') AS t(id, val)
  """)
  |> DataFrame.filter(Column.eq_null_safe(col("val"), lit(nil)))
  |> DataFrame.collect()
end)

run.("16. isNull + isNotNull + isNaN", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1, 1.0), (2, CAST('NaN' AS DOUBLE)), (3, NULL) AS t(id, val)
  """)
  |> DataFrame.select([
    col("id"),
    Column.is_null(col("val")) |> Column.alias_("is_null"),
    Column.is_not_null(col("val")) |> Column.alias_("is_not_null"),
    isnan(col("val")) |> Column.alias_("is_nan")
  ])
  |> DataFrame.collect()
end)

run.("17. coalesce chain with multiple nulls", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES
      (NULL, NULL, NULL, 'last_resort'),
      (NULL, 'second', 'third', 'fourth'),
      ('first', NULL, NULL, NULL)
    AS t(a, b, c, d)
  """)
  |> DataFrame.select([
    coalesce([col("a"), col("b"), col("c"), col("d")]) |> Column.alias_("result")
  ])
  |> DataFrame.collect()
end)

# === exceptAll and intersectAll ===
run.("18. exceptAll (preserves duplicates)", fn ->
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1),(1),(2),(3),(3),(3) AS t(id)")
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (1),(3) AS t(id)")
  DataFrame.except_all(df1, df2) |> DataFrame.collect()
end)

run.("19. intersectAll (preserves duplicates)", fn ->
  df1 = SparkEx.sql(s, "SELECT * FROM VALUES (1),(1),(2),(3),(3),(3) AS t(id)")
  df2 = SparkEx.sql(s, "SELECT * FROM VALUES (1),(1),(3),(3) AS t(id)")
  DataFrame.intersect_all(df1, df2) |> DataFrame.collect()
end)

# === Sort within partitions ===
run.("20. sortWithinPartitions", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'b',10), (2,'a',20), (3,'c',30), (4,'a',40), (5,'b',50)
    AS t(id, grp, val)
  """)
  |> DataFrame.sort_within_partitions([asc(col("grp")), desc(col("val"))])
  |> DataFrame.collect()
end)

# === Repartition with expression ===
run.("21. repartition by column expression", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'a'), (4,'b'), (5,'a') AS t(id, grp)
  """)
  |> DataFrame.repartition(2, [col("grp")])
  |> DataFrame.collect()
end)

# === DataFrame.hint ===
run.("22. DataFrame.hint (broadcast)", fn ->
  small = SparkEx.sql(s, "SELECT 1 AS id, 'one' AS name")
  big = SparkEx.sql(s, "SELECT * FROM VALUES (1, 100), (2, 200) AS t(id, val)")
  small_hint = DataFrame.hint(small, "broadcast")
  DataFrame.join(big, small_hint, ["id"], :inner) |> DataFrame.collect()
end)

# === Column.between ===
run.("23. Column.between", fn ->
  SparkEx.sql(s, "SELECT * FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(id)")
  |> DataFrame.filter(Column.between(col("id"), lit(3), lit(7)))
  |> DataFrame.collect()
end)

# === Column.like / rlike ===
run.("24. Column.like pattern matching", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('hello world'), ('hello spark'), ('goodbye world'), ('hi there')
    AS t(text)
  """)
  |> DataFrame.filter(Column.like(col("text"), "%world%"))
  |> DataFrame.collect()
end)

run.("25. Column.rlike regex matching", fn ->
  SparkEx.sql(s, """
    SELECT * FROM VALUES ('abc123'), ('def456'), ('ghi'), ('jkl789xyz')
    AS t(text)
  """)
  |> DataFrame.filter(Column.rlike(col("text"), "\\d{3}"))
  |> DataFrame.collect()
end)

# === Bitwise operations ===
run.("26. Bitwise AND, OR, XOR", fn ->
  SparkEx.sql(s, "SELECT 12 AS a, 10 AS b")
  |> DataFrame.select([
    Column.bitwise_and(col("a"), col("b")) |> Column.alias_("and"),
    Column.bitwise_or(col("a"), col("b")) |> Column.alias_("or"),
    Column.bitwise_xor(col("a"), col("b")) |> Column.alias_("xor")
  ])
  |> DataFrame.collect()
end)

# === create_map / create_array / struct from columns ===
run.("27. create_map from columns", fn ->
  SparkEx.sql(s, "SELECT 'key1' AS k1, 10 AS v1, 'key2' AS k2, 20 AS v2")
  |> DataFrame.select([
    create_map([col("k1"), col("v1"), col("k2"), col("v2")]) |> Column.alias_("my_map")
  ])
  |> DataFrame.collect()
end)

run.("28. array from columns", fn ->
  SparkEx.sql(s, "SELECT 1 AS a, 2 AS b, 3 AS c")
  |> DataFrame.select([
    array([col("a"), col("b"), col("c")]) |> Column.alias_("my_arr")
  ])
  |> DataFrame.collect()
end)

run.("29. struct from columns", fn ->
  SparkEx.sql(s, "SELECT 'Alice' AS name, 30 AS age, 'NYC' AS city")
  |> DataFrame.select([
    SparkEx.Functions.struct([col("name"), col("age"), col("city")]) |> Column.alias_("person")
  ])
  |> DataFrame.collect()
end)

IO.puts("=== Stat/NA exploration complete ===")
