import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

IO.puts("== base dataframe + nested transforms ==")
{:ok, base} =
  SparkEx.create_dataframe(
    s,
    [
      %{"id" => 1, "grp" => "a", "vals" => [1, 2], "meta" => %{"k" => "v1"}, "ts" => "2024-01-01 10:00:00"},
      %{"id" => 2, "grp" => "a", "vals" => [2, 3], "meta" => %{"k" => "v2"}, "ts" => "2024-01-01 10:05:00"},
      %{"id" => 3, "grp" => "b", "vals" => [4], "meta" => %{"k" => "v3"}, "ts" => "2024-01-01 10:10:00"}
    ],
    schema: "id INT, grp STRING, vals ARRAY<INT>, meta MAP<STRING, STRING>, ts STRING"
  )

nested =
  base
  |> DataFrame.with_column("vals_sz", size(col("vals")))
  |> DataFrame.with_column("meta_k", element_at(col("meta"), lit("k")))
  |> DataFrame.with_column("t", to_timestamp(col("ts")))
  |> DataFrame.with_column("t_time", to_time(col("ts")))
  |> DataFrame.with_column("host", parse_url(lit("https://emr-scratchpad.simplefx.it:15002/path?x=1"), lit("HOST")))
  |> DataFrame.select([
    col("id"),
    col("grp"),
    col("vals_sz"),
    col("meta_k"),
    col("t_time"),
    col("host")
  ])

IO.inspect(DataFrame.collect(nested), label: "nested pipeline")

IO.puts("== explode + regroup + aggregation ==")
agg =
  base
  |> DataFrame.select([col("grp"), explode(col("vals")) |> Column.alias_("v")])
  |> DataFrame.group_by(["grp"])
  |> SparkEx.GroupedData.agg([sum(col("v")) |> Column.alias_("sum_v"), count(col("v")) |> Column.alias_("cnt")])
  |> DataFrame.order_by(["grp"])

IO.inspect(DataFrame.collect(agg), label: "explode regroup")

IO.puts("== union_by_name with missing columns ==")
{:ok, left} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => "x"}], schema: "id INT, a STRING")
{:ok, right} = SparkEx.create_dataframe(s, [%{"id" => 2, "b" => "y"}], schema: "id INT, b STRING")

unioned =
  DataFrame.union_by_name(left, right, allow_missing: true)
  |> DataFrame.order_by(["id"])

IO.inspect(DataFrame.collect(unioned), label: "union_by_name missing")

IO.puts("== window + lag/lead + conditional ==")
w = SparkEx.Window.partition_by(["grp"]) |> SparkEx.Window.order_by([col("id")])

wdf =
  base
  |> DataFrame.with_column("lag_id", lag(col("id"), offset: 1) |> Column.over(w))
  |> DataFrame.with_column("lead_id", lead(col("id"), offset: 1) |> Column.over(w))
  |> DataFrame.with_column("bucket", when_(col("id") |> Column.gt(1), lit("gt1")) |> otherwise(lit("le1")))
  |> DataFrame.order_by(["id"])

IO.inspect(DataFrame.collect(wdf), label: "window pipeline")

IO.puts("== as_of_join smoke test ==")
left_asof = SparkEx.sql(s, "SELECT 1 AS id, TIMESTAMP('2024-01-01 10:05:00') AS ts")
right_asof = SparkEx.sql(s, "SELECT * FROM VALUES (1, TIMESTAMP('2024-01-01 10:00:00')), (1, TIMESTAMP('2024-01-01 10:10:00')) AS t(id, ts2)")

asof =
  left_asof
  |> DataFrame.as_of_join(right_asof, col("ts"), col("ts2"), on: ["id"], direction: "backward")
  |> DataFrame.collect()

IO.inspect(asof, label: "as_of_join smoke")
IO.puts("session_alive?: #{Process.alive?(s)}")
