# Round 81: targeted edge combinations (read-only, no table mutations)
import SparkEx.Functions
alias SparkEx.{DataFrame, Column, GroupedData}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, fun ->
  IO.puts("\n=== #{label} ===")

  try do
    res = fun.()
    IO.inspect(res, label: "OK", limit: 80, printable_limit: 4000)
  rescue
    e ->
      IO.puts("ERROR: #{Exception.message(e)}")
      IO.puts(Exception.format_stacktrace(__STACKTRACE__))
  catch
    kind, reason ->
      IO.puts("CATCH (#{kind}): #{inspect(reason, limit: 30, printable_limit: 2000)}")
  end
end

base =
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      (1, 'x', array(1,2,3), map('k1', 10, 'k2', 20)),
      (2, 'x', array(2,3,4), map('k1', 30, 'k3', 40)),
      (3, 'y', array(10), map('k4', 99)),
      (4, 'y', array(), map()),
      (5, 'z', NULL, NULL)
    AS t(id, grp, nums, attrs)
    """
  )

run.("1) explode_outer + map_keys + regroup", fn ->
  exploded =
    base
    |> DataFrame.with_column("n", explode_outer(col("nums")))
    |> DataFrame.with_column("attr_keys", map_keys(col("attrs")))

  exploded
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    count(col("n")) |> Column.alias_("n_non_null"),
    collect_set(col("n")) |> Column.alias_("distinct_n"),
    max(size(col("attr_keys"))) |> Column.alias_("max_attr_keys")
  ])
  |> DataFrame.order_by([col("grp")])
  |> DataFrame.collect()
end)

run.("2) two-level aggregate with join back to detail", fn ->
  detail = base |> DataFrame.with_column("n_sum", expr("aggregate(nums, 0, (acc, x) -> acc + x)"))

  per_grp =
    detail
    |> DataFrame.group_by([col("grp")])
    |> GroupedData.agg([
      sum(col("n_sum")) |> Column.alias_("sum_nums"),
      count(lit(1)) |> Column.alias_("rows")
    ])

  DataFrame.join(detail, per_grp, ["grp"], :left)
  |> DataFrame.with_column("ratio", Column.divide(col("n_sum"), col("sum_nums")))
  |> DataFrame.order_by([col("grp"), col("id")])
  |> DataFrame.select([col("id"), col("grp"), col("n_sum"), col("sum_nums"), col("ratio")])
  |> DataFrame.collect()
end)

run.("3) SQL: nested CTE + lateral explode + regroup + window", fn ->
  SparkEx.sql(
    s,
    """
    WITH src AS (
      SELECT * FROM VALUES
        (1, 'x', array(1,2,3)),
        (2, 'x', array(2,3,4)),
        (3, 'y', array(10)),
        (4, 'y', array()),
        (5, 'z', CAST(NULL AS ARRAY<INT>))
      AS t(id, grp, nums)
    ),
    flat AS (
      SELECT id, grp, n
      FROM src
      LATERAL VIEW OUTER explode(nums) e AS n
    ),
    agg AS (
      SELECT grp, n, COUNT(*) AS c
      FROM flat
      GROUP BY grp, n
    )
    SELECT
      grp,
      n,
      c,
      DENSE_RANK() OVER (PARTITION BY grp ORDER BY c DESC, n ASC) AS rnk
    FROM agg
    ORDER BY grp, rnk, n
    """
  )
  |> DataFrame.collect()
end)

run.("4) collect_list(struct(...)) + to_json", fn ->
  packed =
    base
    |> DataFrame.with_column(
      "pair",
      SparkEx.Functions.struct([
        col("id") |> Column.alias_("id"),
        col("grp") |> Column.alias_("grp")
      ])
    )

  packed
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([collect_list(col("pair")) |> Column.alias_("pairs")])
  |> DataFrame.with_column("pairs_json", to_json(col("pairs")))
  |> DataFrame.order_by([col("grp")])
  |> DataFrame.collect()
end)

run.("5) group_by + pivot + fillna + unpivot", fn ->
  sales =
    SparkEx.sql(
      s,
      """
      SELECT * FROM VALUES
        ('x', 'jan', 10), ('x', 'feb', 20), ('y', 'jan', 5), ('z', 'mar', 7)
      AS t(grp, mon, v)
      """
    )

  wide =
    sales
    |> DataFrame.group_by([col("grp")])
    |> GroupedData.pivot("mon", ["jan", "feb", "mar"])
    |> GroupedData.sum("v")

  wide
  |> DataFrame.na()
  |> SparkEx.DataFrame.NA.fill(0)
  |> DataFrame.unpivot(["grp"], ["jan", "feb", "mar"], "mon", "v")
  |> DataFrame.order_by([col("grp"), col("mon")])
  |> DataFrame.collect()
end)

IO.puts("\nsession_alive?: #{Process.alive?(s)}")
