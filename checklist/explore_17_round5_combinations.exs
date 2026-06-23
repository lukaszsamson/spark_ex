# Exploratory Round 5: joins, complex types, transforms, aggregations
# Usage: mix run checklist/explore_17_round5_combinations.exs

alias SparkEx.{Column, DataFrame}
import SparkEx.Functions

IO.puts("=== Exploratory Round 5: combinations ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, fun ->
  IO.puts(label)

  try do
    result = fun.()
    IO.inspect(result, label: "  result", limit: 30, printable_limit: 2000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason, limit: 10)}")
  end

  IO.puts("")
end

# ---- Joins ----

run.("J1. 4-way mixed joins", fn ->
  a = SparkEx.sql(s, "SELECT * FROM VALUES (1,'A'), (2,'B'), (3,'C') AS t(id, a_val)")
  b = SparkEx.sql(s, "SELECT * FROM VALUES (1,'B1'), (2,'B2') AS t(id, b_val)")
  c = SparkEx.sql(s, "SELECT * FROM VALUES (2,'C2'), (3,'C3') AS t(id, c_val)")
  d = SparkEx.sql(s, "SELECT * FROM VALUES (1,'D1'), (3,'D3') AS t(id, d_val)")

  DataFrame.join(a, b, ["id"], :left)
  |> DataFrame.join(c, ["id"], :left)
  |> DataFrame.join(d, ["id"], :inner)
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("J2. Null-safe join expression", fn ->
  l = SparkEx.sql(s, "SELECT * FROM VALUES (1,'x'), (NULL,'n1'), (2,'y') AS t(id, v1)")
  r = SparkEx.sql(s, "SELECT * FROM VALUES (1,'x2'), (NULL,'n2'), (3,'z2') AS t(id, v2)")

  la = DataFrame.alias_(l, "l")
  ra = DataFrame.alias_(r, "r")

  DataFrame.join(
    la,
    ra,
    Column.eq_null_safe(DataFrame.col(la, "id"), DataFrame.col(ra, "id")),
    :inner
  )
  |> DataFrame.select([
    DataFrame.col(la, "id") |> Column.alias_("l_id"),
    DataFrame.col(ra, "id") |> Column.alias_("r_id"),
    DataFrame.col(la, "v1"),
    DataFrame.col(ra, "v2")
  ])
  |> DataFrame.collect()
end)

run.("J3. Join with duplicate non-key names", fn ->
  left_df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'Alice'), (2,'Bob') AS t(id, name)")
  right_df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'Sales'), (3,'Ops') AS t(id, name)")

  DataFrame.join(left_df, right_df, ["id"], :left)
  |> DataFrame.collect()
end)

# ---- Complex data types ----

run.("T1. Map<int, struct> decode", fn ->
  SparkEx.sql(
    s,
    """
    SELECT map(
      1, named_struct('name', 'alice', 'scores', array(1,2,3)),
      2, named_struct('name', 'bob', 'scores', array(4,5))
    ) AS m
    """
  )
  |> DataFrame.collect()
end)

run.("T2. Array<map<int, array<int>>> decode", fn ->
  SparkEx.sql(
    s,
    """
    SELECT array(
      map(1, array(10, 20), 2, array(30, 40)),
      map(3, array(50), 4, array(60, 70))
    ) AS nested
    """
  )
  |> DataFrame.collect()
end)

run.("T3. Deep nested map/array/struct decode", fn ->
  SparkEx.sql(
    s,
    """
    SELECT named_struct(
      'outer',
      map(
        'k1', array(named_struct('x', 1, 'y', map('a', 10))),
        'k2', array(named_struct('x', 2, 'y', map('b', 20)))
      )
    ) AS payload
    """
  )
  |> DataFrame.collect()
end)

# ---- Transforms ----

run.("X1. explode_outer + regroup + collect_list", fn ->
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      (1, array('a', 'b')),
      (2, array('b', 'c')),
      (3, NULL)
    AS t(id, tags)
    """
  )
  |> DataFrame.select([col("id"), explode_outer(col("tags")) |> Column.alias_("tag")])
  |> DataFrame.group_by([col("tag")])
  |> SparkEx.GroupedData.agg([collect_list(col("id")) |> Column.alias_("ids")])
  |> DataFrame.order_by([asc(col("tag"))])
  |> DataFrame.collect()
end)

run.("X2. transform + aggregate higher-order functions", fn ->
  SparkEx.sql(s, "SELECT array(1,2,3,4) AS nums")
  |> DataFrame.select([
    transform(col("nums"), fn x -> Column.multiply(x, lit(2)) end) |> Column.alias_("doubled"),
    aggregate(col("nums"), lit(0), fn acc, x -> Column.plus(acc, x) end) |> Column.alias_("sum_nums")
  ])
  |> DataFrame.collect()
end)

# ---- Aggregations ----

run.("A1. grouping_id with cube", fn ->
  SparkEx.sql(
    s,
    """
    SELECT dept, role, SUM(salary) AS total, grouping_id(dept, role) AS gid
    FROM VALUES
      ('eng', 'dev', 100),
      ('eng', 'qa', 120),
      ('sales', 'ae', 90)
    AS t(dept, role, salary)
    GROUP BY CUBE(dept, role)
    ORDER BY gid, dept, role
    """
  )
  |> DataFrame.collect()
end)

run.("A2. Percentile aggregation on decimals", fn ->
  SparkEx.sql(
    s,
    """
    SELECT percentile_approx(val, 0.5) AS p50
    FROM VALUES
      (CAST(1.11 AS DECIMAL(10,2))),
      (CAST(2.22 AS DECIMAL(10,2))),
      (CAST(3.33 AS DECIMAL(10,2))),
      (CAST(4.44 AS DECIMAL(10,2)))
    AS t(val)
    """
  )
  |> DataFrame.collect()
end)

SparkEx.Session.release(s)

IO.puts("=== Round 5 complete ===")
