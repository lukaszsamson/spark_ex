# Round 57: TableValuedFunction (correct API), Types module, SqlFormatter,
# Artifacts, Reader with programmatic schema, Writer v1 format-specific,
# creative edge cases
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]
import SparkEx.Types

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
# 1. TableValuedFunction (correct API)
# =============================================

run.("1a. TVF.call range", fn ->
  tvf = SparkEx.TableValuedFunction.new(s)
  SparkEx.TableValuedFunction.call(tvf, "range", [lit(10)])
  |> DataFrame.collect()
end)

run.("1b. TVF.call range with start/end/step", fn ->
  tvf = SparkEx.TableValuedFunction.new(s)
  SparkEx.TableValuedFunction.call(tvf, "range", [lit(0), lit(100), lit(10)])
  |> DataFrame.collect()
end)

run.("1c. TVF.explode via SQL expr", fn ->
  # explode needs an array expression - use SparkEx.Functions
  tvf = SparkEx.TableValuedFunction.new(s)
  # Try using col reference approach
  SparkEx.sql(s, "SELECT explode(array(1,2,3)) AS val") |> DataFrame.collect()
end)

run.("1d. TVF.stack", fn ->
  tvf = SparkEx.TableValuedFunction.new(s)
  SparkEx.TableValuedFunction.stack(tvf, 3,
    [lit("key1"), lit(10), lit("key2"), lit(20), lit("key3"), lit(30)])
  |> DataFrame.collect()
end)

# =============================================
# 2. Types module
# =============================================

run.("2a. Types.struct_type + to_ddl", fn ->
  schema = struct_type([
    struct_field("id", :long),
    struct_field("name", :string),
    struct_field("score", :double, nullable: false)
  ])
  SparkEx.Types.to_ddl(schema)
end)

run.("2b. Types.to_json", fn ->
  schema = struct_type([
    struct_field("id", :long),
    struct_field("tags", {:array, :string}),
    struct_field("props", {:map, :string, :integer})
  ])
  SparkEx.Types.to_json(schema)
end)

run.("2c. Types.to_proto + roundtrip", fn ->
  schema = struct_type([
    struct_field("id", :long),
    struct_field("name", :string),
    struct_field("nested", {:struct, [
      struct_field("x", :integer),
      struct_field("y", :double)
    ]})
  ])
  proto = SparkEx.Types.to_proto(schema)
  json = SparkEx.Types.data_type_to_json(proto)
  %{ddl: SparkEx.Types.to_ddl(schema), json: json}
end)

run.("2d. Types with decimal, date, timestamp", fn ->
  schema = struct_type([
    struct_field("price", {:decimal, 10, 2}),
    struct_field("event_date", :date),
    struct_field("created_at", :timestamp),
    struct_field("updated_at", :timestamp_ntz)
  ])
  %{ddl: SparkEx.Types.to_ddl(schema), json: SparkEx.Types.to_json(schema)}
end)

run.("2e. Types with array of structs", fn ->
  schema = struct_type([
    struct_field("id", :long),
    struct_field("addresses", {:array, {:struct, [
      struct_field("street", :string),
      struct_field("city", :string),
      struct_field("zip", :string)
    ]}})
  ])
  %{ddl: SparkEx.Types.to_ddl(schema), json: SparkEx.Types.to_json(schema)}
end)

run.("2f. Types with char/varchar", fn ->
  schema = struct_type([
    struct_field("code", {:char, 3}),
    struct_field("description", {:varchar, 255})
  ])
  %{ddl: SparkEx.Types.to_ddl(schema), json: SparkEx.Types.to_json(schema)}
end)

# =============================================
# 3. Types used with Reader.schema
# =============================================

run.("3. create_dataframe with programmatic schema via Types", fn ->
  schema = struct_type([
    struct_field("id", :long),
    struct_field("name", :string),
    struct_field("score", :double)
  ])

  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice", "score" => 95.5},
    %{"id" => 2, "name" => "Bob", "score" => 87.0}
  ], schema: schema)

  {:ok, rows} = DataFrame.collect(df)
  {:ok, dtypes} = DataFrame.dtypes(df)
  %{rows: rows, dtypes: dtypes}
end)

# =============================================
# 4. SqlFormatter
# =============================================

run.("4a. SqlFormatter positional args", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT ? AS a, ? AS b", [42, "hello"])
  sql
end)

run.("4b. SqlFormatter named args", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT :id AS id, :name AS name", %{id: 1, name: "Alice"})
  sql
end)

run.("4c. SqlFormatter with nil/bool", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT ? AS a, ? AS b, ? AS c", [nil, true, false])
  sql
end)

run.("4d. SqlFormatter with date/datetime", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT ? AS d, ? AS ts", [
    ~D[2024-01-15], ~N[2024-01-15 10:30:00]
  ])
  sql
end)

run.("4e. SqlFormatter SQL injection prevention", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT ? AS val", ["Robert'; DROP TABLE students;--"])
  sql
end)

run.("4f. SqlFormatter placeholder in string literal (should skip)", fn ->
  sql = SparkEx.SqlFormatter.format("SELECT '?' AS literal, ? AS actual", [42])
  sql
end)

run.("4g. SqlFormatter too many args", fn ->
  SparkEx.SqlFormatter.format("SELECT ? AS a", [1, 2])
end)

run.("4h. SqlFormatter too few args", fn ->
  SparkEx.SqlFormatter.format("SELECT ? AS a, ? AS b", [1])
end)

# =============================================
# 5. Artifacts (local file upload)
# =============================================

run.("5a. Artifacts.prepare (JAR prefix)", fn ->
  # Create a dummy file to test prepare
  File.write!("/tmp/spark_ex_test_dummy.jar", "fake-jar-content")
  SparkEx.Artifacts.prepare("/tmp/spark_ex_test_dummy.jar", "jars/")
end)

run.("5b. Artifacts.add_jars (upload dummy JAR)", fn ->
  File.write!("/tmp/spark_ex_test_artifact.jar", "fake-jar-content-for-upload")
  SparkEx.Artifacts.add_jars(s, "/tmp/spark_ex_test_artifact.jar")
end)

run.("5c. Artifacts.add_files", fn ->
  File.write!("/tmp/spark_ex_test_file.txt", "test file content")
  SparkEx.Artifacts.add_files(s, "/tmp/spark_ex_test_file.txt")
end)

run.("5d. Artifacts.prepare with nonexistent file", fn ->
  SparkEx.Artifacts.prepare("/tmp/nonexistent_file_xyz.jar", "jars/")
end)

run.("5e. Artifacts.prepare with multiple files", fn ->
  File.write!("/tmp/spark_ex_test_a.txt", "aaa")
  File.write!("/tmp/spark_ex_test_b.txt", "bbb")
  SparkEx.Artifacts.prepare(["/tmp/spark_ex_test_a.txt", "/tmp/spark_ex_test_b.txt"], "files/")
end)

run.("5f. Artifacts.prepare with duplicate basenames (should raise)", fn ->
  File.write!("/tmp/spark_ex_test_dup.txt", "xxx")
  SparkEx.Artifacts.prepare(["/tmp/spark_ex_test_dup.txt", "/tmp/spark_ex_test_dup.txt"], "files/")
end)

# =============================================
# 6. config_get_all (no prefix = all configs)
# =============================================

run.("6. config_get_all without prefix", fn ->
  {:ok, configs} = SparkEx.config_get_all(s)
  %{total: length(configs), sample: Enum.take(configs, 3)}
end)

# =============================================
# 7. DataFrame.show with edge cases
# =============================================

run.("7a. show empty DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [], schema: "id INT, name STRING")
  DataFrame.show(df)
end)

run.("7b. show with 0 rows", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1}], schema: "id INT")
  DataFrame.show(df, num_rows: 0)
end)

run.("7c. show with null values", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "val" => "hello"}, %{"id" => 2, "val" => nil}
  ], schema: "id INT, val STRING")
  DataFrame.show(df)
end)

run.("7d. show with wide DataFrame (many columns)", fn ->
  {:ok, df} = SparkEx.sql(s, """
    SELECT 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5,
           6 AS c6, 7 AS c7, 8 AS c8, 9 AS c9, 10 AS c10,
           'x' AS c11, 'y' AS c12, 'z' AS c13
  """) |> DataFrame.collect()
  # Actually need the DataFrame not collected
  SparkEx.sql(s, """
    SELECT 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5,
           6 AS c6, 7 AS c7, 8 AS c8, 9 AS c9, 10 AS c10,
           'x' AS c11, 'y' AS c12, 'z' AS c13
  """) |> DataFrame.show()
end)

# =============================================
# 8. Complex: Writer v1 → append → count verification
# =============================================

run.("8. Writer v1 append idempotency", fn ->
  table = "sfx.sfx_dev_warehouse.spark_ex_r57_append_#{run_id}"
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..10, fn i -> %{"id" => i} end), schema: "id INT")

  # Initial write
  DataFrame.write(df) |> SparkEx.Writer.mode(:overwrite) |> SparkEx.Writer.save_as_table(table)

  # Append 3 times
  Enum.each(1..3, fn _ ->
    {:ok, df2} = SparkEx.create_dataframe(s,
      Enum.map(1..5, fn i -> %{"id" => i + 100} end), schema: "id INT")
    DataFrame.write(df2) |> SparkEx.Writer.mode(:append) |> SparkEx.Writer.save_as_table(table)
  end)

  {:ok, cnt} = SparkEx.Reader.table(s, table) |> DataFrame.count()
  Catalog.drop_table(s, table, if_exists: true, purge: true)
  %{expected: 25, actual: cnt}
end)

# =============================================
# 9. Complex: DataFrame.create_or_replace_temp_view + SQL
# =============================================

run.("9a. create_or_replace_temp_view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}
  ], schema: "id INT, name STRING")
  DataFrame.create_or_replace_temp_view(df, "test_view_#{run_id}")

  SparkEx.sql(s, "SELECT * FROM test_view_#{run_id} WHERE id = 1")
  |> DataFrame.collect()
end)

run.("9b. create_temp_view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"x" => 100}
  ], schema: "x INT")
  DataFrame.create_temp_view(df, "temp_view_#{run_id}")
  SparkEx.sql(s, "SELECT x * 2 AS doubled FROM temp_view_#{run_id}")
  |> DataFrame.collect()
end)

run.("9c. create_or_replace_global_temp_view", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"g" => 42}], schema: "g INT")
  DataFrame.create_or_replace_global_temp_view(df, "global_view_#{run_id}")
  SparkEx.sql(s, "SELECT * FROM global_temp.global_view_#{run_id}")
  |> DataFrame.collect()
end)

# =============================================
# 10. Complex: SqlFormatter + SparkEx.sql integration
# =============================================

run.("10. SqlFormatter used with SparkEx.sql", fn ->
  sql = SparkEx.SqlFormatter.format(
    "SELECT ? AS id, ? AS name WHERE ? > 0",
    [42, "Alice", 42])
  SparkEx.sql(s, sql) |> DataFrame.collect()
end)

# =============================================
# 11. Edge: DataFrame.with_columns (plural, multiple at once)
# =============================================

run.("11. with_columns multiple", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..5, fn i -> %{"a" => i, "b" => i * 10} end),
    schema: "a INT, b INT")
  df
  |> DataFrame.with_columns(%{
    "sum" => Column.plus(col("a"), col("b")),
    "product" => Column.multiply(col("a"), col("b")),
    "diff" => Column.minus(col("b"), col("a"))
  })
  |> DataFrame.collect()
end)

# =============================================
# 12. Edge: DataFrame.describe
# =============================================

run.("12. DataFrame.describe", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0, "cat" => Enum.at(["A", "B"], rem(i, 2))} end),
    schema: "val DOUBLE, cat STRING")
  DataFrame.describe(df) |> DataFrame.collect()
end)

# =============================================
# 13. Edge: DataFrame.sample with seed
# =============================================

run.("13. DataFrame.sample with seed (reproducible)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"id" => i} end), schema: "id INT")
  {:ok, r1} = DataFrame.sample(df, fraction: 0.1, seed: 42) |> DataFrame.count()
  {:ok, r2} = DataFrame.sample(df, fraction: 0.1, seed: 42) |> DataFrame.count()
  %{sample1_count: r1, sample2_count: r2, reproducible: r1 == r2}
end)

# =============================================
# 14. Edge: DataFrame.cross_join
# =============================================

run.("14. cross_join", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s, [%{"a" => 1}, %{"a" => 2}], schema: "a INT")
  {:ok, df2} = SparkEx.create_dataframe(s, [%{"b" => "x"}, %{"b" => "y"}], schema: "b STRING")
  DataFrame.cross_join(df1, df2)
  |> DataFrame.sort([asc(col("a")), asc(col("b"))])
  |> DataFrame.collect()
end)

# =============================================
# 15. Edge: DataFrame.except / intersect
# =============================================

run.("15a. DataFrame.except_all", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s,
    [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 2}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s,
    [%{"id" => 2}, %{"id" => 3}], schema: "id INT")
  DataFrame.except_all(df1, df2)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("15b. DataFrame.intersect_all", fn ->
  {:ok, df1} = SparkEx.create_dataframe(s,
    [%{"id" => 1}, %{"id" => 2}, %{"id" => 2}, %{"id" => 3}], schema: "id INT")
  {:ok, df2} = SparkEx.create_dataframe(s,
    [%{"id" => 2}, %{"id" => 2}, %{"id" => 4}], schema: "id INT")
  DataFrame.intersect_all(df1, df2)
  |> DataFrame.sort([asc(col("id"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 57 complete ===")
