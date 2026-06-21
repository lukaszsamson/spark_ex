# Round 74c: WindowSpec with invalid types, Column string fns, view names, etc.
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
# 3. WindowSpec.partition_by/order_by with invalid types
# =============================================

run.("3a. WindowSpec.partition_by with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  w = WindowSpec.partition_by(%WindowSpec{}, [42])
  df |> DataFrame.select([
    col("x"),
    sum(col("y")) |> Column.over(w) |> Column.alias_("sum_y")
  ]) |> DataFrame.collect()
end)

run.("3b. WindowSpec.order_by with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  w = WindowSpec.order_by(%WindowSpec{}, [42])
  df |> DataFrame.select([
    col("x"),
    sum(col("y")) |> Column.over(w) |> Column.alias_("sum_y")
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. Column.like/rlike/ilike with non-string patterns
# =============================================

run.("4a. Column.like with integer pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.like(col("x"), 42)) |> DataFrame.collect()
end)

run.("4b. Column.rlike with integer pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.rlike(col("x"), 42)) |> DataFrame.collect()
end)

run.("4c. Column.ilike with nil pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.ilike(col("x"), nil)) |> DataFrame.collect()
end)

run.("4d. Column.startswith with integer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.startswith(col("x"), 42)) |> DataFrame.collect()
end)

run.("4e. Column.endswith with list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.endswith(col("x"), [1, 2])) |> DataFrame.collect()
end)

run.("4f. Column.contains with map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => "hello"}], schema: "x STRING")
  df |> DataFrame.filter(Column.contains(col("x"), %{"a" => 1})) |> DataFrame.collect()
end)

IO.puts("=== Round 74c part 1 done ===")

# =============================================
# 5. Column.get_item with unusual types
# =============================================

run.("5a. get_item on map with string key (valid)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"m" => %{"a" => 1, "b" => 2}}], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    Column.get_item(col("m"), "a") |> Column.alias_("val")
  ]) |> DataFrame.collect()
end)

run.("5b. get_item with nil key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"m" => %{"a" => 1}}], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    Column.get_item(col("m"), nil) |> Column.alias_("val")
  ]) |> DataFrame.collect()
end)

run.("5c. get_item with list key", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"m" => %{"a" => 1}}], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    Column.get_item(col("m"), [1, 2]) |> Column.alias_("val")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Round 74c part 2 done ===")

# =============================================
# 6. View names with non-string types
# =============================================

run.("6a. create_or_replace_temp_view with integer name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_or_replace_temp_view(df, 42)
end)

run.("6b. create_temp_view with atom name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_temp_view(df, :my_view)
end)

run.("6c. create_global_temp_view with integer name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.create_global_temp_view(df, 42)
end)

IO.puts("=== Round 74c part 3 done ===")

# =============================================
# 7. write_v2 / save_as_table with non-string table names
# =============================================

run.("7a. write_v2 with integer table name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write_v2(df, 42)
end)

run.("7b. Writer.save_as_table with integer table name", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.write(df) |> SparkEx.Writer.save_as_table(42)
end)

# =============================================
# 8. read_table with non-string table name
# =============================================

run.("8a. read_table with integer", fn ->
  SparkEx.Reader.table(s, 42)
end)

run.("8b. read_table with atom", fn ->
  SparkEx.Reader.table(s, :my_table)
end)

# =============================================
# 9. describe/summary with non-string elements
# =============================================

run.("9a. describe with integer in cols list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1, "y" => 2}], schema: "x INT, y INT")
  DataFrame.describe(df, [42]) |> DataFrame.collect()
end)

run.("9b. summary with integer in stats list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.summary(df, [42]) |> DataFrame.collect()
end)

# =============================================
# 10. select_expr with non-string expressions
# =============================================

run.("10a. select_expr with integer in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select_expr(df, [42]) |> DataFrame.collect()
end)

run.("10b. select_expr with atom in list", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"x" => 1}], schema: "x INT")
  DataFrame.select_expr(df, [:x]) |> DataFrame.collect()
end)

IO.puts("=== Round 74c complete ===")
