# Section 5: Basic Transforms & Actions
# Usage: mix run checklist/section_05_transforms.exs

alias SparkEx.{DataFrame, Column, Types}
import SparkEx.Functions

IO.puts("=== Section 5: Basic Transforms & Actions ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Connected!\n")

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5), (3, 'Carol', 150.0) AS t(id, name, salary)")

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# 5.1 select columns
run.("5.1 select columns", fn ->
  DataFrame.select(df, [col("id"), col("name")]) |> DataFrame.collect()
end)

# 5.2 select_expr
run.("5.2 select_expr", fn ->
  DataFrame.select_expr(df, ["id * 2 AS double_id", "upper(name) AS upper_name"]) |> DataFrame.collect()
end)

# 5.3 col (bound)
run.("5.3 col (bound)", fn ->
  DataFrame.col(df, "name")
end)

# 5.4 col_regex
run.("5.4 col_regex", fn ->
  DataFrame.col_regex(df, "`(id|name)`") |> DataFrame.collect()
end)

# 5.5 filter
run.("5.5 filter", fn ->
  DataFrame.filter(df, col("id") |> Column.gt(lit(1))) |> DataFrame.collect()
end)

# 5.6 with_column
run.("5.6 with_column", fn ->
  DataFrame.with_column(df, "new_col", col("id") |> Column.plus(lit(100))) |> DataFrame.collect()
end)

# 5.7 with_columns
run.("5.7 with_columns", fn ->
  DataFrame.with_columns(df, [{"a", lit(1)}, {"b", lit(2)}]) |> DataFrame.collect()
end)

# 5.8 drop
run.("5.8 drop", fn ->
  DataFrame.drop(df, ["salary"]) |> DataFrame.collect()
end)

# 5.9 with_column_renamed
run.("5.9 with_column_renamed", fn ->
  DataFrame.with_column_renamed(df, "name", "full_name") |> DataFrame.collect()
end)

# 5.10 with_columns_renamed
run.("5.10 with_columns_renamed", fn ->
  DataFrame.with_columns_renamed(df, %{"id" => "identifier", "name" => "full_name"}) |> DataFrame.collect()
end)

# 5.11 to_df
run.("5.11 to_df", fn ->
  DataFrame.to_df(df, ["x", "y", "z"]) |> DataFrame.collect()
end)

# 5.12 alias
run.("5.12 alias", fn ->
  aliased = DataFrame.alias_(df, "t1")
  DataFrame.select(aliased, [col("t1.id")]) |> DataFrame.collect()
end)

# 5.13 order_by asc
run.("5.13 order_by asc", fn ->
  DataFrame.order_by(df, [asc("id")]) |> DataFrame.collect()
end)

# 5.14 order_by desc
run.("5.14 order_by desc", fn ->
  DataFrame.order_by(df, [desc("id")]) |> DataFrame.collect()
end)

# 5.15 order_by nulls first/last
run.("5.15 order_by nulls first/last", fn ->
  DataFrame.order_by(df, [col("salary") |> Column.asc_nulls_first()]) |> DataFrame.collect()
end)

# 5.16 limit
run.("5.16 limit", fn ->
  DataFrame.limit(df, 1) |> DataFrame.collect()
end)

# 5.17 offset
run.("5.17 offset", fn ->
  DataFrame.offset(df, 1) |> DataFrame.collect()
end)

# 5.18 tail
run.("5.18 tail", fn ->
  DataFrame.tail(df, 2)
end)

# 5.19 distinct
run.("5.19 distinct", fn ->
  DataFrame.distinct(df) |> DataFrame.count()
end)

# 5.20 drop_duplicates
run.("5.20 drop_duplicates", fn ->
  DataFrame.drop_duplicates(df, ["name"]) |> DataFrame.count()
end)

# 5.21 collect
run.("5.21 collect", fn ->
  DataFrame.collect(df)
end)

# 5.22 take
run.("5.22 take", fn ->
  DataFrame.take(df, 2)
end)

# 5.23 head
run.("5.23 head", fn ->
  DataFrame.head(df)
end)

# 5.24 first
run.("5.24 first", fn ->
  DataFrame.first(df)
end)

# 5.25 count
run.("5.25 count", fn ->
  DataFrame.count(df)
end)

# 5.26 is_empty
run.("5.26 is_empty", fn ->
  DataFrame.is_empty(df)
end)

# 5.27 sample
run.("5.27 sample", fn ->
  DataFrame.sample(df, 0.5) |> DataFrame.count()
end)

# 5.28 random_split
run.("5.28 random_split", fn ->
  parts = DataFrame.random_split(df, [0.7, 0.3])
  Enum.map(parts, fn p -> DataFrame.count(p) end)
end)

# 5.29 transform
run.("5.29 transform", fn ->
  DataFrame.transform(df, fn d -> DataFrame.limit(d, 1) end) |> DataFrame.collect()
end)

# 5.30 to (cast schema)
run.("5.30 to (cast schema)", fn ->
  DataFrame.to(df, Types.struct_type([
    Types.struct_field("id", :long),
    Types.struct_field("name", :string),
    Types.struct_field("salary", :string)
  ])) |> DataFrame.collect()
end)

IO.puts("=== Section 5 Complete ===")
