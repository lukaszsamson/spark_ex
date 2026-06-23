import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

df =
  SparkEx.sql(
    s,
    """
    SELECT
      '2024-01-01 12:34:56' AS t1,
      '2024-01-01 10:00:00' AS t2,
      'https://ex.test/path?x=1' AS url,
      '{\"a\":1}' AS js
    """
  )

checks = [
  {"to_time", fn -> DataFrame.select(df, [to_time(col("t1")) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"try_to_time", fn -> DataFrame.select(df, [try_to_time(col("t1")) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"time_diff", fn -> DataFrame.select(df, [time_diff("HOUR", to_timestamp(col("t1")), to_timestamp(col("t2"))) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"time_trunc", fn -> DataFrame.select(df, [time_trunc("HOUR", to_timestamp(col("t1"))) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"parse_json", fn -> DataFrame.select(df, [parse_json(col("js")) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"try_parse_json", fn -> DataFrame.select(df, [try_parse_json(col("js")) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"variant_get", fn -> DataFrame.select(df, [variant_get(parse_json(col("js")), "$.a", "int") |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"schema_of_variant", fn -> DataFrame.select(df, [schema_of_variant(parse_json(col("js"))) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"schema_of_variant_agg", fn -> DataFrame.select(df, [schema_of_variant_agg(parse_json(col("js"))) |> Column.alias_("v")]) |> DataFrame.collect() end},
  {"try_parse_url", fn -> DataFrame.select(df, [try_parse_url(col("url"), lit("HOST")) |> Column.alias_("v")]) |> DataFrame.collect() end}
]

Enum.each(checks, fn {name, fun} ->
  IO.puts("\n=== #{name} ===")
  IO.inspect(fun.(), label: "result", limit: 10)
end)

IO.inspect(
  DataFrame.select(df, [to_timestamp(col("t1")) |> Column.alias_("v")]) |> DataFrame.collect(),
  label: "control to_timestamp"
)

IO.inspect(
  DataFrame.select(df, [get_json_object(col("js"), "$.a") |> Column.alias_("v")]) |> DataFrame.collect(),
  label: "control get_json_object"
)

IO.puts("session_alive?: #{Process.alive?(s)}")
