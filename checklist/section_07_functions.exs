# Section 7: Built-in Functions
# Usage: mix run checklist/section_07_functions.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Section 7: Built-in Functions ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

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

df = SparkEx.sql(s, """
  SELECT * FROM VALUES
    (1, 'Alice', 100.0, true, NULL),
    (2, 'Bob', 200.5, false, 'x'),
    (3, 'Charlie', NULL, true, 'y')
  AS t(id, name, salary, active, tag)
""")

# 7.1 Basic string
run.("7.1 Basic string functions", fn ->
  df |> DataFrame.select([
    upper(col("name")) |> Column.alias_("upper"),
    lower(col("name")) |> Column.alias_("lower"),
    Functions.length(col("name")) |> Column.alias_("len"),
    trim(col("name")) |> Column.alias_("trimmed"),
    substring(col("name"), lit(1), lit(3)) |> Column.alias_("sub")
  ]) |> DataFrame.collect()
end)

# 7.2 concat (n_col — takes a list)
run.("7.2 concat", fn ->
  df |> DataFrame.select([
    concat([col("name"), lit(" - "), col("tag")]) |> Column.alias_("combined")
  ]) |> DataFrame.collect()
end)

# 7.3 split
run.("7.3 split", fn ->
  df |> DataFrame.select([
    split(col("name"), lit("l")) |> Column.alias_("parts")
  ]) |> DataFrame.collect()
end)

# 7.4 regexp_extract
run.("7.4 regexp_extract", fn ->
  df |> DataFrame.select([
    regexp_extract(col("name"), lit("(A.*)"), lit(1)) |> Column.alias_("extracted")
  ]) |> DataFrame.collect()
end)

# 7.5 Basic math
run.("7.5 Basic math", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    Functions.abs(lit(-5)) |> Column.alias_("abs_val"),
    Functions.ceil(lit(1.2)) |> Column.alias_("ceil_val"),
    Functions.floor(lit(1.8)) |> Column.alias_("floor_val"),
    Functions.round(lit(3.14159), lit(2)) |> Column.alias_("round_val"),
    sqrt(lit(16.0)) |> Column.alias_("sqrt_val")
  ]) |> DataFrame.collect()
end)

# 7.6 greatest / least (n_col — takes a list)
run.("7.6 greatest / least", fn ->
  df |> DataFrame.select([
    greatest([col("id"), lit(2)]) |> Column.alias_("greatest_val"),
    least([col("id"), lit(2)]) |> Column.alias_("least_val")
  ]) |> DataFrame.collect()
end)

# 7.7 Current date/time
run.("7.7 Current date/time", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    current_date() |> Column.alias_("today"),
    current_timestamp() |> Column.alias_("now")
  ]) |> DataFrame.collect()
end)

# 7.9 date_format
run.("7.9 date_format", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    date_format(current_timestamp(), lit("yyyy-MM-dd")) |> Column.alias_("formatted")
  ]) |> DataFrame.collect()
end)

# 7.10 to_date / to_timestamp
run.("7.10 to_date / to_timestamp", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    to_date(lit("2024-01-15"), lit("yyyy-MM-dd")) |> Column.alias_("parsed_date")
  ]) |> DataFrame.collect()
end)

# 7.11 coalesce (n_col — takes a list)
run.("7.11 coalesce", fn ->
  df |> DataFrame.select([
    coalesce([col("tag"), col("name"), lit("default")]) |> Column.alias_("coalesced")
  ]) |> DataFrame.collect()
end)

# 7.12 nullif
run.("7.12 nullif", fn ->
  df |> DataFrame.select([
    nullif(col("id"), lit(1)) |> Column.alias_("nullif_result")
  ]) |> DataFrame.collect()
end)

# 7.13 when/otherwise
run.("7.13 when/otherwise", fn ->
  df |> DataFrame.select([
    when_(col("salary") |> Column.gt(lit(150)), lit("high")) |> Column.otherwise(lit("low")) |> Column.alias_("level")
  ]) |> DataFrame.collect()
end)

# 7.14 typeof
run.("7.14 typeof", fn ->
  df |> DataFrame.select([
    typeof(col("salary")) |> Column.alias_("type_name")
  ]) |> DataFrame.collect()
end)

# 7.15 array construction (n_col — takes a list)
run.("7.15 array construction", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    array([lit(1), lit(2), lit(3)]) |> Column.alias_("arr")
  ]) |> DataFrame.collect()
end)

# 7.16 array_contains
run.("7.16 array_contains", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    array_contains(col("arr"), lit(2)) |> Column.alias_("has_2")
  ]) |> DataFrame.collect()
end)

# 7.17 explode
run.("7.17 explode", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    explode(col("arr")) |> Column.alias_("val")
  ]) |> DataFrame.collect()
end)

# 7.23 map construction (n_col)
run.("7.23 map construction", fn ->
  SparkEx.sql(s, "SELECT 1") |> DataFrame.select([
    create_map([lit("k1"), lit("v1"), lit("k2"), lit("v2")]) |> Column.alias_("m")
  ]) |> DataFrame.collect()
end)

# 7.24 map_keys / map_values
run.("7.24 map_keys / map_values", fn ->
  SparkEx.sql(s, "SELECT map('a', 1, 'b', 2) AS m") |> DataFrame.select([
    map_keys(col("m")) |> Column.alias_("keys"),
    map_values(col("m")) |> Column.alias_("values")
  ]) |> DataFrame.collect()
end)

# 7.25 transform (higher-order)
run.("7.25 transform (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    Functions.transform(col("arr"), fn x -> Column.plus(x, lit(10)) end) |> Column.alias_("transformed")
  ]) |> DataFrame.collect()
end)

# 7.26 filter (higher-order)
run.("7.26 filter (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3, 4, 5) AS arr") |> DataFrame.select([
    Functions.filter(col("arr"), fn x -> Column.gt(x, lit(2)) end) |> Column.alias_("filtered")
  ]) |> DataFrame.collect()
end)

# 7.27 aggregate (higher-order)
run.("7.27 aggregate (higher-order)", fn ->
  SparkEx.sql(s, "SELECT array(1, 2, 3) AS arr") |> DataFrame.select([
    Functions.aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("sum")
  ]) |> DataFrame.collect()
end)

# 7.29 count_distinct
run.("7.29 count_distinct", fn ->
  df |> DataFrame.select([
    count_distinct(col("name")) |> Column.alias_("distinct_names")
  ]) |> DataFrame.collect()
end)

# 7.30 collect_list / collect_set
run.("7.30 collect_list / collect_set", fn ->
  df |> DataFrame.select([
    collect_list(col("name")) |> Column.alias_("name_list"),
    collect_set(col("name")) |> Column.alias_("name_set")
  ]) |> DataFrame.collect()
end)

# 7.31 Hash functions
run.("7.31 Hash functions", fn ->
  df |> DataFrame.select([
    md5(col("name")) |> Column.alias_("md5_hash"),
    sha1(col("name")) |> Column.alias_("sha1_hash"),
    xxhash64([col("name")]) |> Column.alias_("xx_hash")
  ]) |> DataFrame.collect()
end)

# 7.32 to_json
run.("7.32 to_json", fn ->
  SparkEx.sql(s, "SELECT named_struct('a', 1, 'b', 'hello') AS st") |> DataFrame.select([
    to_json(col("st")) |> Column.alias_("json_str")
  ]) |> DataFrame.collect()
end)

# 7.34 monotonically_increasing_id
run.("7.34 monotonically_increasing_id", fn ->
  df |> DataFrame.select([
    monotonically_increasing_id() |> Column.alias_("mono_id")
  ]) |> DataFrame.collect()
end)

# 7.35 expr
run.("7.35 expr", fn ->
  df |> DataFrame.select([
    Functions.expr("id + 100") |> Column.alias_("id_plus")
  ]) |> DataFrame.collect()
end)

IO.puts("=== Section 7 Complete ===")
