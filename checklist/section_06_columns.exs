# Section 6: Column Expressions & Operators
# Usage: mix run checklist/section_06_columns.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Section 6: Column Expressions & Operators ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 100.0, true, NULL),
    (2, 'Bob', 200.5, false, 'x'),
    (3, 'Charlie', NULL, true, 'y')
  AS t(id, name, salary, active, tag)
""")

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  result", limit: 20, printable_limit: 200)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason)}")
  end
  IO.puts("")
end

# 6.1 Arithmetic
run.("6.1 Arithmetic", fn ->
  df |> DataFrame.select([col("salary") |> Column.plus(lit(50.0)) |> Column.alias_("sal_plus")]) |> DataFrame.collect()
end)

# 6.2 Multiply/divide
run.("6.2 Multiply/divide", fn ->
  df |> DataFrame.select([
    col("salary") |> Column.multiply(lit(2.0)) |> Column.alias_("mul"),
    col("salary") |> Column.divide(lit(2.0)) |> Column.alias_("div")
  ]) |> DataFrame.collect()
end)

# 6.3 Modulo
run.("6.3 Modulo", fn ->
  df |> DataFrame.select([col("id") |> Column.mod(lit(2)) |> Column.alias_("mod_result")]) |> DataFrame.collect()
end)

# 6.4 Comparison eq
run.("6.4 Comparison eq", fn ->
  df |> DataFrame.filter(col("name") |> Column.eq(lit("Alice"))) |> DataFrame.collect()
end)

# 6.5 Comparison neq/gt/gte/lt/lte
run.("6.5 Comparison neq/gt/gte/lt/lte", fn ->
  results = %{
    neq: df |> DataFrame.filter(col("id") |> Column.neq(lit(1))) |> DataFrame.count(),
    gt: df |> DataFrame.filter(col("id") |> Column.gt(lit(1))) |> DataFrame.count(),
    gte: df |> DataFrame.filter(col("id") |> Column.gte(lit(2))) |> DataFrame.count(),
    lt: df |> DataFrame.filter(col("id") |> Column.lt(lit(3))) |> DataFrame.count(),
    lte: df |> DataFrame.filter(col("id") |> Column.lte(lit(2))) |> DataFrame.count()
  }
  results
end)

# 6.6 eq_null_safe
run.("6.6 eq_null_safe", fn ->
  df |> DataFrame.select([Column.eq_null_safe(col("tag"), lit(nil)) |> Column.alias_("is_null_safe")]) |> DataFrame.collect()
end)

# 6.7 Logical and/or
run.("6.7 Logical and/or", fn ->
  df |> DataFrame.filter(
    Column.and_(col("active") |> Column.eq(lit(true)), col("id") |> Column.gt(lit(1)))
  ) |> DataFrame.collect()
end)

# 6.8 not
run.("6.8 not", fn ->
  df |> DataFrame.select([Column.not_(col("active")) |> Column.alias_("not_active")]) |> DataFrame.collect()
end)

# 6.9 is_null / is_not_null
run.("6.9 is_null / is_not_null", fn ->
  null_rows = df |> DataFrame.filter(Column.is_null(col("tag"))) |> DataFrame.count()
  not_null_rows = df |> DataFrame.filter(Column.is_not_null(col("tag"))) |> DataFrame.count()
  {null_rows, not_null_rows}
end)

# 6.11 between
run.("6.11 between", fn ->
  df |> DataFrame.filter(Column.between(col("id"), lit(1), lit(2))) |> DataFrame.collect()
end)

# 6.12 isin
run.("6.12 isin", fn ->
  df |> DataFrame.filter(Column.isin(col("name"), [lit("Alice"), lit("Bob")])) |> DataFrame.collect()
end)

# 6.13 like
run.("6.13 like", fn ->
  df |> DataFrame.filter(Column.like(col("name"), "%li%")) |> DataFrame.collect()
end)

# 6.14 rlike
run.("6.14 rlike", fn ->
  df |> DataFrame.filter(Column.rlike(col("name"), "^A.*")) |> DataFrame.collect()
end)

# 6.15 ilike
run.("6.15 ilike", fn ->
  df |> DataFrame.filter(Column.ilike(col("name"), "%alice%")) |> DataFrame.collect()
end)

# 6.16 contains / starts_with / ends_with
run.("6.16 contains/starts_with/ends_with", fn ->
  contains = df |> DataFrame.filter(Column.contains(col("name"), lit("li"))) |> DataFrame.count()
  starts = df |> DataFrame.filter(Column.starts_with(col("name"), lit("A"))) |> DataFrame.count()
  ends = df |> DataFrame.filter(Column.ends_with(col("name"), lit("e"))) |> DataFrame.count()
  %{contains: contains, starts_with: starts, ends_with: ends}
end)

# 6.17 cast
run.("6.17 cast", fn ->
  df |> DataFrame.select([Column.cast(col("id"), "string") |> Column.alias_("id_str")]) |> DataFrame.collect()
end)

# 6.18 try_cast
run.("6.18 try_cast", fn ->
  df |> DataFrame.select([Column.try_cast(col("name"), "int") |> Column.alias_("name_int")]) |> DataFrame.collect()
end)

# 6.19 alias
run.("6.19 alias", fn ->
  df |> DataFrame.select([Column.alias_(col("id"), "identifier")]) |> DataFrame.collect()
end)

# 6.20 when/otherwise
run.("6.20 when/otherwise", fn ->
  df |> DataFrame.select([
    Functions.when_(col("id") |> Column.eq(lit(1)), lit("one")) |> Column.otherwise(lit("other")) |> Column.alias_("label")
  ]) |> DataFrame.collect()
end)

# 6.21 negate
run.("6.21 negate", fn ->
  df |> DataFrame.select([Column.negate(col("salary")) |> Column.alias_("neg_sal")]) |> DataFrame.collect()
end)

# 6.22 Bitwise operators
run.("6.22 Bitwise operators", fn ->
  df |> DataFrame.select([
    Column.bitwise_and(col("id"), lit(1)) |> Column.alias_("band"),
    Column.bitwise_or(col("id"), lit(2)) |> Column.alias_("bor"),
    Column.bitwise_xor(col("id"), lit(3)) |> Column.alias_("bxor")
  ]) |> DataFrame.collect()
end)

# 6.23 substr
run.("6.23 substr", fn ->
  df |> DataFrame.select([Column.substr(col("name"), 1, 3) |> Column.alias_("sub")]) |> DataFrame.collect()
end)

IO.puts("=== Section 6 Complete ===")
