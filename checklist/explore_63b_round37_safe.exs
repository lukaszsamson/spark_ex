# Round 37 (safe): Skip session-crashing bare string tests, test everything else
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog, StreamReader, StreamWriter, StreamingQuery}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

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

# Note: Tests 1a-1c from round 37 CONFIRMED that config_get, config_set, config_unset
# all crash with bare strings (EX-107 siblings). Skipping those here.

# =============================================
# 2. DataFrame.Stat methods
# =============================================

run.("2a. Stat.corr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => (i * 2 + 1) * 1.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.Stat.corr(df, "x", "y")
end)

run.("2b. Stat.cov", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..50, fn i -> %{"x" => i * 1.0, "y" => (i * 2) * 1.0} end),
    schema: "x DOUBLE, y DOUBLE")
  DataFrame.Stat.cov(df, "x", "y")
end)

run.("2c. Stat.crosstab", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"gender" => "M", "dept" => "Eng"},
    %{"gender" => "F", "dept" => "Eng"},
    %{"gender" => "M", "dept" => "Sales"},
    %{"gender" => "M", "dept" => "Sales"},
    %{"gender" => "F", "dept" => "HR"}
  ], schema: "gender STRING, dept STRING")
  DataFrame.Stat.crosstab(df, "gender", "dept") |> DataFrame.collect()
end)

run.("2d. Stat.freq_items", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => rem(i, 5)} end), schema: "val INT")
  DataFrame.Stat.freq_items(df, ["val"]) |> DataFrame.collect()
end)

run.("2e. Stat.approx_quantile", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end), schema: "val DOUBLE")
  DataFrame.Stat.approx_quantile(df, "val", [0.25, 0.5, 0.75], 0.01)
end)

run.("2f. Stat.sample_by", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "a", "val" => 1}, %{"grp" => "a", "val" => 2},
    %{"grp" => "a", "val" => 3}, %{"grp" => "a", "val" => 4},
    %{"grp" => "b", "val" => 5}, %{"grp" => "b", "val" => 6},
    %{"grp" => "b", "val" => 7}, %{"grp" => "b", "val" => 8}
  ], schema: "grp STRING, val INT")
  DataFrame.Stat.sample_by(df, "grp", %{"a" => 0.5, "b" => 1.0}, 42) |> DataFrame.collect()
end)

# =============================================
# 3. Streaming query exception
# =============================================

run.("3a. StreamingQuery.exception on healthy query", fn ->
  stream = StreamReader.rate(s, rows_per_second: 5)
  query = stream
  |> StreamWriter.format("memory")
  |> StreamWriter.query_name("except_test_#{run_id}")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.start()

  Process.sleep(2000)
  exc = StreamingQuery.exception(query)
  StreamingQuery.stop(query)
  exc
end)

# =============================================
# 4. with_columns_renamed
# =============================================

run.("4a. with_columns_renamed (happy path)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"old_name" => 1, "another_old" => "hello"}
  ], schema: "old_name INT, another_old STRING")
  df
  |> DataFrame.with_columns_renamed(%{"old_name" => "new_name", "another_old" => "another_new"})
  |> DataFrame.columns()
end)

run.("4b. with_columns_renamed (non-existent column)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}
  ], schema: "a INT, b INT")
  df
  |> DataFrame.with_columns_renamed(%{"nonexistent" => "c"})
  |> DataFrame.columns()
end)

run.("4c. with_columns_renamed (rename to conflicting name)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 2}
  ], schema: "a INT, b INT")
  df
  |> DataFrame.with_columns_renamed(%{"a" => "b"})
  |> DataFrame.collect()
end)

# =============================================
# 5. input_files
# =============================================

run.("5. input_files on in-memory DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.input_files(df)
end)

# =============================================
# 6. Catalog
# =============================================

run.("6a. list_functions (first 5)", fn ->
  {:ok, fns} = Catalog.list_functions(s)
  Enum.take(fns, 5) |> Enum.map(& &1.name)
end)

run.("6b. function_exists? (abs)", fn ->
  Catalog.function_exists?(s, "abs")
end)

run.("6c. function_exists? (nonexistent)", fn ->
  Catalog.function_exists?(s, "definitely_not_a_function_xyz123")
end)

# =============================================
# 7. DataFrame.describe
# =============================================

run.("7. describe with column selection", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i * 1.0, "name" => "item_#{i}"} end),
    schema: "val DOUBLE, name STRING")
  DataFrame.describe(df, ["val"]) |> DataFrame.collect()
end)

# =============================================
# 8. select_expr
# =============================================

run.("8. select_expr", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 10, "b" => 3}
  ], schema: "a INT, b INT")
  df |> DataFrame.select_expr(["a + b AS sum", "a * b AS product", "a % b AS modulo"])
  |> DataFrame.collect()
end)

# =============================================
# 9. DataFrameNaFunctions.replace
# =============================================

run.("9. na.replace", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"status" => "active"}, %{"status" => "inactive"}, %{"status" => "pending"}
  ], schema: "status STRING")
  DataFrame.na(df)
  |> SparkEx.DataFrame.NA.replace(%{"active" => "ACT", "inactive" => "INA"})
  |> DataFrame.collect()
end)

# =============================================
# 10. Edge: very long column name
# =============================================

run.("10. very long column name (200 chars)", fn ->
  long_name = String.duplicate("a", 200)
  {:ok, df} = SparkEx.create_dataframe(s, [%{long_name => 42}], schema: "#{long_name} INT")
  {:ok, rows} = DataFrame.collect(df)
  String.length(Map.keys(hd(rows)) |> hd())
end)

# =============================================
# 11. Edge: empty DataFrame
# =============================================

run.("11a. empty DataFrame operations", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, name STRING")
  count = DataFrame.count(df)
  columns = DataFrame.columns(df)
  dtypes = DataFrame.dtypes(df)
  {count, columns, dtypes}
end)

run.("11b. collect_as_map on empty DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "key STRING, value INT")
  DataFrame.collect_as_map(df)
end)

# =============================================
# 12. sample
# =============================================

run.("12. sample with different params", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"id" => i} end), schema: "id INT")

  sample1 = df |> DataFrame.sample(0.1, seed: 42) |> DataFrame.count()
  sample2 = df |> DataFrame.sample(0.5, seed: 42) |> DataFrame.count()
  {sample1, sample2}
end)

# =============================================
# 13. Multi-line string values
# =============================================

run.("13. multi-line string values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "line1\nline2\nline3"}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    split(col("text"), "\n") |> Column.alias_("lines")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Column.between
# =============================================

run.("14. Column.between", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.between(col("val"), lit(3), lit(7)))
  |> DataFrame.collect()
end)

# =============================================
# 15. Column.isin
# =============================================

run.("15. Column.isin", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"val" => i} end), schema: "val INT")
  df |> DataFrame.filter(Column.isin(col("val"), [lit(2), lit(4), lit(6), lit(8)]))
  |> DataFrame.collect()
end)

# =============================================
# 16. pivot
# =============================================

run.("16. pivot with explicit values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"dept" => "Eng", "quarter" => "Q1", "revenue" => 100},
    %{"dept" => "Eng", "quarter" => "Q2", "revenue" => 150},
    %{"dept" => "Sales", "quarter" => "Q1", "revenue" => 200},
    %{"dept" => "Sales", "quarter" => "Q2", "revenue" => 250}
  ], schema: "dept STRING, quarter STRING, revenue INT")

  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.pivot(col("quarter"), ["Q1", "Q2"])
  |> GroupedData.agg([sum(col("revenue")) |> Column.alias_("total")])
  |> DataFrame.collect()
end)

# =============================================
# 17. unpivot
# =============================================

run.("17. unpivot", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "q1" => 100, "q2" => 200, "q3" => 300},
    %{"id" => 2, "q1" => 150, "q2" => 250, "q3" => 350}
  ], schema: "id INT, q1 INT, q2 INT, q3 INT")
  DataFrame.unpivot(df, [col("id")], [{col("q1"), "q1"}, {col("q2"), "q2"}, {col("q3"), "q3"}],
    "quarter", "revenue")
  |> DataFrame.sort([asc(col("id")), asc(col("quarter"))])
  |> DataFrame.collect()
end)

# =============================================
# 18. except_all / intersect_all
# =============================================

run.("18a. except_all", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}, %{"val" => 3}
  ], schema: "val INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 3}
  ], schema: "val INT")
  DataFrame.except_all(df1, df2) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

run.("18b. intersect_all", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 2}
  ], schema: "val INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [
    %{"val" => 1}, %{"val" => 1}, %{"val" => 1}
  ], schema: "val INT")
  DataFrame.intersect_all(df1, df2) |> DataFrame.sort([asc(col("val"))]) |> DataFrame.collect()
end)

# =============================================
# 19. Catalog
# =============================================

run.("19a. current_database", fn ->
  Catalog.current_database(s)
end)

run.("19b. current_catalog", fn ->
  Catalog.current_catalog(s)
end)

run.("19c. list_databases (first 5)", fn ->
  {:ok, dbs} = Catalog.list_databases(s)
  Enum.map(dbs, & &1.name) |> Enum.take(5)
end)

run.("19d. list_catalogs", fn ->
  Catalog.list_catalogs(s)
end)

# =============================================
# 20. Window frames
# =============================================

run.("20a. range_between with unbounded", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end), schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([lit(true)])
    |> WindowSpec.order_by([asc(col("id"))])
    |> WindowSpec.range_between(:unbounded_preceding, :current_row)

  df |> DataFrame.with_column("running_sum",
    sum(col("val")) |> Column.over(w))
  |> DataFrame.collect()
end)

run.("20b. rows_between specific offsets", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 10} end), schema: "id INT, val INT")

  w = SparkEx.Window.partition_by([lit(true)])
    |> WindowSpec.order_by([asc(col("id"))])
    |> WindowSpec.rows_between(-1, 1)

  df |> DataFrame.with_column("moving_avg",
    avg(col("val")) |> Column.over(w))
  |> DataFrame.collect()
end)

# =============================================
# 21. Catalog.table_exists? / database_exists?
# =============================================

run.("21a. table_exists? (nonexistent)", fn ->
  Catalog.table_exists?(s, "definitely_not_a_table_xyz")
end)

run.("21b. database_exists?", fn ->
  Catalog.database_exists?(s, "default")
end)

# =============================================
# 22. DataFrame.storage_level
# =============================================

run.("22. storage_level on uncached DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.storage_level(df)
end)

# =============================================
# 23. DataFrame.with_metadata
# =============================================

run.("23. with_metadata", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}
  ], schema: "id INT, name STRING")
  df2 = DataFrame.with_metadata(df, "id", %{"comment" => "primary key"})
  DataFrame.collect(df2)
end)

# =============================================
# 24. DataFrameNaFunctions chain
# =============================================

run.("24. na.fill then na.drop chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => nil},
    %{"a" => nil, "b" => 2, "c" => "x"},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c STRING")
  # Fill just b, then drop rows where a is still null
  filled = DataFrame.na(df) |> SparkEx.DataFrame.NA.fill(%{"b" => -1})
  filled |> DataFrame.dropna(how: :any, subset: ["a"]) |> DataFrame.collect()
end)

IO.puts("=== Round 37 (safe) complete ===")
