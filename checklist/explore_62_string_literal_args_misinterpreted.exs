import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
url_df = SparkEx.sql(s, "SELECT 'https://ex.test/path?x=1' AS url")
str_df = SparkEx.sql(s, "SELECT 'Abc' AS s")

parse_part_raw = DataFrame.select(url_df, [parse_url(col("url"), "HOST") |> Column.alias_("v")]) |> DataFrame.collect()
parse_part_lit = DataFrame.select(url_df, [parse_url(col("url"), lit("HOST")) |> Column.alias_("v")]) |> DataFrame.collect()
parse_key_raw = DataFrame.select(url_df, [parse_url(col("url"), lit("QUERY"), "x") |> Column.alias_("v")]) |> DataFrame.collect()
parse_key_lit = DataFrame.select(url_df, [parse_url(col("url"), lit("QUERY"), lit("x")) |> Column.alias_("v")]) |> DataFrame.collect()

like_raw = DataFrame.select(str_df, [like_(col("s"), "%b%") |> Column.alias_("v")]) |> DataFrame.collect()
like_lit = DataFrame.select(str_df, [like_(col("s"), lit("%b%")) |> Column.alias_("v")]) |> DataFrame.collect()
ilike_raw = DataFrame.select(str_df, [ilike_(col("s"), "%b%") |> Column.alias_("v")]) |> DataFrame.collect()
ilike_lit = DataFrame.select(str_df, [ilike_(col("s"), lit("%b%")) |> Column.alias_("v")]) |> DataFrame.collect()

IO.inspect(parse_part_raw, label: "parse_url raw part")
IO.inspect(parse_part_lit, label: "parse_url lit part control")
IO.inspect(parse_key_raw, label: "parse_url raw key")
IO.inspect(parse_key_lit, label: "parse_url lit key control")
IO.inspect(like_raw, label: "like_ raw pattern")
IO.inspect(like_lit, label: "like_ lit control")
IO.inspect(ilike_raw, label: "ilike_ raw pattern")
IO.inspect(ilike_lit, label: "ilike_ lit control")
IO.puts("session_alive?: #{Process.alive?(s)}")
