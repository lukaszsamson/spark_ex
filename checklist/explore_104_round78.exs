# Round 78: Final sweep — Catalog.create_function, register_data_source,
# TableValuedFunction, Artifacts, and other less-tested modules
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
# 1. Catalog.create_function with non-string args
# =============================================

run.("1a. create_function with integer name", fn ->
  Catalog.create_function(s, 42, "com.example.MyUDF", "com.example.MyUDF")
end)

run.("1b. create_function with integer class_name", fn ->
  Catalog.create_function(s, "my_udf", 42, "com.example.MyUDF")
end)

# =============================================
# 2. Catalog.drop_function with non-string
# =============================================

run.("2a. drop_function with integer name", fn ->
  Catalog.drop_function(s, 42)
end)

run.("2b. drop_function with if_exists non-boolean", fn ->
  Catalog.drop_function(s, "nonexistent_fn", if_exists: "yes")
end)

# =============================================
# 3. Catalog.alter_table with non-string table name
# =============================================

run.("3a. alter_table with integer table name", fn ->
  Catalog.alter_table(s, 42, rename: "new_name")
end)

# =============================================
# 4. SparkEx.Artifacts edge cases
# =============================================

run.("4a. Artifacts.add_jars with integer in list", fn ->
  SparkEx.Artifacts.add_jars(s, [42])
end)

run.("4b. Artifacts.add_files with integer in list", fn ->
  SparkEx.Artifacts.add_files(s, [42])
end)

run.("4c. Artifacts.add_jars with nil in list", fn ->
  SparkEx.Artifacts.add_jars(s, [nil])
end)

# =============================================
# 5. UDFRegistration edge cases
# =============================================

run.("5a. register_java with non-string name", fn ->
  SparkEx.UDFRegistration.register_java(s, 42, "com.Example")
end)

run.("5b. register_java with non-string class_name", fn ->
  SparkEx.UDFRegistration.register_java(s, "my_udf", 42)
end)

# =============================================
# 6. Session.add_tag/remove_tag with non-string
# =============================================

run.("6a. add_tag with integer", fn ->
  SparkEx.Session.add_tag(s, 42)
end)

run.("6b. remove_tag with integer", fn ->
  SparkEx.Session.remove_tag(s, 42)
end)

# =============================================
# 7. DataFrame.create_or_replace_temp_view and sql roundtrip
# with special characters in view name
# =============================================

run.("7a. temp view with backtick-escaped name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_temp_view(df, "my`view")
  SparkEx.sql(s, "SELECT * FROM `my``view`") |> DataFrame.collect()
end)

run.("7b. temp view with dots in name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_temp_view(df, "my.view.test")
  SparkEx.sql(s, "SELECT * FROM `my.view.test`") |> DataFrame.collect()
end)

# =============================================
# 8. SparkEx.sql with args - correct format
# =============================================

run.("8a. sql with named args (map with string keys + Column values)", fn ->
  args = %{"x" => lit(42)}
  SparkEx.sql(s, "SELECT :x AS val", args) |> DataFrame.collect()
end)

run.("8b. sql with named args (keyword list)", fn ->
  SparkEx.sql(s, "SELECT :x AS val", x: lit(42)) |> DataFrame.collect()
end)

# =============================================
# 9. DataFrame.col_regex
# =============================================

run.("9a. col_regex basic", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"abc" => 1, "abd" => 2, "xyz" => 3}], schema: "abc INT, abd INT, xyz INT")
  DataFrame.col_regex(df, "`ab.*`") |> DataFrame.collect()
end)

run.("9b. col_regex with non-string pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.col_regex(df, 42) |> DataFrame.collect()
end)

# =============================================
# 10. DataFrame.same_semantics / semantic_hash
# =============================================

run.("10a. same_semantics with non-DataFrame", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.same_semantics(df, 42)
end)

run.("10b. semantic_hash", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.semantic_hash(df)
end)

IO.puts("=== Round 78 complete ===")
