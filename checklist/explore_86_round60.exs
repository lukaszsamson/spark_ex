# Round 60: StreamWriter.foreach_batch, Reader format options, HOF edge cases,
# UDFRegistration, DataFrame.with_watermark, complex type operations,
# error recovery patterns, DataFrame.observe with multiple metrics
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
# 1. HOF: transform, filter, aggregate on arrays
# =============================================

run.("1a. HOF transform on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"prices" => [10.0, 20.0, 30.0, 40.0]}
  ], schema: "prices ARRAY<DOUBLE>")
  df |> DataFrame.select([
    col("prices"),
    SparkEx.Functions.transform(col("prices"), fn x -> Column.multiply(x, lit(1.1)) end)
    |> Column.alias_("with_tax")
  ]) |> DataFrame.collect()
end)

run.("1b. HOF filter on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"nums" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
  ], schema: "nums ARRAY<INT>")
  df |> DataFrame.select([
    col("nums"),
    SparkEx.Functions.filter(col("nums"), fn x -> Column.gt(x, lit(5)) end)
    |> Column.alias_("gt_5")
  ]) |> DataFrame.collect()
end)

run.("1c. HOF aggregate with initial value", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"vals" => [1, 2, 3, 4, 5]}
  ], schema: "vals ARRAY<INT>")
  df |> DataFrame.select([
    col("vals"),
    aggregate(col("vals"), lit(0), fn acc, x -> Column.plus(acc, x) end)
    |> Column.alias_("sum"),
    aggregate(col("vals"), lit(1), fn acc, x -> Column.multiply(acc, x) end)
    |> Column.alias_("product")
  ]) |> DataFrame.collect()
end)

run.("1d. HOF exists / forall on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"nums" => [2, 4, 6, 8]},
    %{"nums" => [1, 2, 3, 4]},
    %{"nums" => [10, 20, 30]}
  ], schema: "nums ARRAY<INT>")
  df |> DataFrame.select([
    col("nums"),
    exists(col("nums"), fn x -> Column.gt(x, lit(5)) end)
    |> Column.alias_("has_gt_5"),
    forall(col("nums"), fn x -> Column.gt(x, lit(0)) end)
    |> Column.alias_("all_positive")
  ]) |> DataFrame.collect()
end)

run.("1e. HOF zip_with", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => [1, 2, 3], "b" => [10, 20, 30]}
  ], schema: "a ARRAY<INT>, b ARRAY<INT>")
  df |> DataFrame.select([
    col("a"), col("b"),
    zip_with(col("a"), col("b"), fn x, y -> Column.plus(x, y) end)
    |> Column.alias_("summed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. Complex type operations: nested struct access, map element
# =============================================

run.("2a. Nested struct field access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"person" => %{"name" => "Alice", "address" => %{"city" => "NYC", "zip" => "10001"}}},
    %{"person" => %{"name" => "Bob", "address" => %{"city" => "LA", "zip" => "90001"}}}
  ], schema: "person STRUCT<name: STRING, address: STRUCT<city: STRING, zip: STRING>>")
  df |> DataFrame.select([
    Column.get_field(col("person"), "name") |> Column.alias_("name"),
    Column.get_field(Column.get_field(col("person"), "address"), "city") |> Column.alias_("city"),
    Column.get_field(Column.get_field(col("person"), "address"), "zip") |> Column.alias_("zip")
  ]) |> DataFrame.collect()
end)

run.("2b. Map element access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"props" => %{"color" => "red", "size" => "large", "weight" => "5kg"}}
  ], schema: "props MAP<STRING, STRING>")
  df |> DataFrame.select([
    col("props"),
    element_at(col("props"), lit("color")) |> Column.alias_("color"),
    element_at(col("props"), lit("size")) |> Column.alias_("size"),
    element_at(col("props"), lit("missing")) |> Column.alias_("missing")
  ]) |> DataFrame.collect()
end)

run.("2c. Array element access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    element_at(col("arr"), lit(1)) |> Column.alias_("first"),
    element_at(col("arr"), lit(-1)) |> Column.alias_("last"),
    slice(col("arr"), lit(2), lit(3)) |> Column.alias_("middle_3")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Observation with multiple metrics
# =============================================

run.("3. Observation with 4 metrics", fn ->
  obs = SparkEx.Observation.new("multi_obs_#{run_id}")

  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0, "grp" => Enum.at(["A", "B"], rem(i, 2))} end),
    schema: "val DOUBLE, grp STRING")

  {:ok, rows} = df
  |> DataFrame.observe(obs, [
    count(col("val")) |> Column.alias_("cnt"),
    sum(col("val")) |> Column.alias_("total"),
    avg(col("val")) |> Column.alias_("mean"),
    min(col("val")) |> Column.alias_("min_val")
  ])
  |> DataFrame.collect()

  metrics = SparkEx.Observation.get(obs)
  %{rows_count: length(rows), metrics: metrics}
end)

# =============================================
# 4. UDFRegistration
# =============================================

run.("4a. UDFRegistration.register_java (without return_type)", fn ->
  SparkEx.UDFRegistration.register_java(s, "spark_ex_udf_#{run_id}",
    "org.apache.spark.sql.functions.upper")
end)

run.("4b. UDFRegistration.register_java (with return_type string)", fn ->
  SparkEx.UDFRegistration.register_java(s, "spark_ex_udf2_#{run_id}",
    "org.apache.spark.sql.functions.upper",
    return_type: "STRING")
end)

# =============================================
# 5. Writer v1: format-specific options
# =============================================

run.("5a. Writer v1 with format option", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r60_fmt_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i, "val" => i * 100} end),
    schema: "id INT, val INT")

  result = DataFrame.write(df)
    |> SparkEx.Writer.mode(:overwrite)
    |> SparkEx.Writer.option("write.format.default", "parquet")
    |> SparkEx.Writer.save_as_table(table)

  {:ok, rows} = SparkEx.Reader.table(s, table) |> DataFrame.collect()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{write: result, row_count: length(rows)}
end)

# =============================================
# 6. Reader with schema option
# =============================================

run.("6. Reader.table with schema enforcement", fn ->
  # Read existing table, project specific columns
  SparkEx.Reader.table(s, "sfx.sfx_dev_warehouse.instruments")
  |> DataFrame.select([col("symbol"), col("type")])
  |> DataFrame.limit(3)
  |> DataFrame.collect()
end)

# =============================================
# 7. Complex: DataFrame.na operations chained
# =============================================

run.("7. NA operations chained", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => nil, "c" => "x"},
    %{"a" => nil, "b" => 2, "c" => nil},
    %{"a" => 3, "b" => 3, "c" => "z"},
    %{"a" => nil, "b" => nil, "c" => nil}
  ], schema: "a INT, b INT, c STRING")

  # Chain: dropna → fillna
  dropped = DataFrame.dropna(df, how: "any", subset: ["a", "b"])
  {:ok, after_drop} = DataFrame.collect(dropped)

  filled = DataFrame.fillna(df, %{"a" => 0, "b" => -1, "c" => "MISSING"})
  {:ok, after_fill} = DataFrame.collect(filled)

  %{after_drop: after_drop, after_fill: after_fill}
end)

# =============================================
# 8. Complex: DataFrame.withColumn overwriting existing column
# =============================================

run.("8. with_column overwriting existing column", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => 100}, %{"id" => 2, "val" => 200}
  ], schema: "id INT, val INT")

  df
  |> DataFrame.with_column("val", Column.multiply(col("val"), lit(2)))
  |> DataFrame.with_column("val", Column.plus(col("val"), lit(1)))
  |> DataFrame.collect()
end)

# =============================================
# 9. Complex: Multiple window functions in single select
# =============================================

run.("9. Multiple window functions in one select", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..30, fn i ->
      %{"dept" => Enum.at(["A", "B", "C"], rem(i, 3)),
        "name" => "emp_#{i}",
        "salary" => (rem(i * 997, 80) + 40) * 1000}
    end), schema: "dept STRING, name STRING, salary INT")

  w_dept = SparkEx.Window.partition_by([col("dept")]) |> WindowSpec.order_by([desc(col("salary"))])
  w_all = SparkEx.Window.order_by([desc(col("salary"))])

  df
  |> DataFrame.select([
    col("dept"), col("name"), col("salary"),
    row_number() |> Column.over(w_dept) |> Column.alias_("dept_rank"),
    row_number() |> Column.over(w_all) |> Column.alias_("global_rank"),
    avg(col("salary")) |> Column.over(SparkEx.Window.partition_by([col("dept")])) |> Column.alias_("dept_avg"),
    lag(col("salary"), offset: 1) |> Column.over(w_dept) |> Column.alias_("prev_salary_in_dept")
  ])
  |> DataFrame.filter(Column.lte(col("dept_rank"), lit(3)))
  |> DataFrame.sort([asc(col("dept")), asc(col("dept_rank"))])
  |> DataFrame.collect()
end)

# =============================================
# 10. Edge: DataFrame.create_dataframe with boolean column
# =============================================

run.("10. create_dataframe with boolean values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "active" => true, "verified" => false},
    %{"id" => 2, "active" => false, "verified" => true},
    %{"id" => 3, "active" => true, "verified" => true}
  ], schema: "id INT, active BOOLEAN, verified BOOLEAN")
  df
  |> DataFrame.filter(Column.and_(col("active"), col("verified")))
  |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.select with star expression
# =============================================

run.("11. Select * via col('*')", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"a" => 1, "b" => 2, "c" => 3}],
    schema: "a INT, b INT, c INT")
  df |> DataFrame.select([col("*")]) |> DataFrame.collect()
end)

# =============================================
# 12. Complex: WriterV2 create + Writer v1 insert_into
# =============================================

run.("12. WriterV2 create then Writer v1 insert_into", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r60_v2v1_#{run_id}"

  # Create table with WriterV2
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"id" => 1, "val" => "initial"}],
    schema: "id INT, val STRING")
  DataFrame.write_v2(df1, table) |> SparkEx.WriterV2.create()

  # Insert with Writer v1
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"id" => 2, "val" => "added"}],
    schema: "id INT, val STRING")
  DataFrame.write(df2) |> SparkEx.Writer.insert_into(table)

  # Verify
  {:ok, rows} = SparkEx.Reader.table(s, table)
    |> DataFrame.sort([asc(col("id"))])
    |> DataFrame.collect()

  Catalog.drop_table(s, table, if_exists: true, purge: true)
  rows
end)

# =============================================
# 13. Edge: Column.eq_null_safe
# =============================================

run.("13. Column.eq_null_safe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"a" => 1, "b" => 1},
    %{"a" => nil, "b" => nil},
    %{"a" => 1, "b" => nil},
    %{"a" => nil, "b" => 1}
  ], schema: "a INT, b INT")
  df |> DataFrame.select([
    col("a"), col("b"),
    Column.eq(col("a"), col("b")) |> Column.alias_("regular_eq"),
    Column.eq_null_safe(col("a"), col("b")) |> Column.alias_("null_safe_eq")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Edge: Column.cast chain
# =============================================

run.("14. Column.cast chain: INT → STRING → DOUBLE", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"val" => 42}], schema: "val INT")
  df |> DataFrame.select([
    col("val"),
    col("val") |> Column.cast("STRING") |> Column.alias_("as_string"),
    col("val") |> Column.cast("STRING") |> Column.cast("DOUBLE") |> Column.alias_("as_double"),
    col("val") |> Column.cast("DOUBLE") |> Column.cast("STRING") |> Column.cast("INT") |> Column.alias_("roundtrip")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 60 complete ===")
