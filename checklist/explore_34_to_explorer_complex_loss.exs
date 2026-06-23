# Exploratory repro: to_explorer stringifies complex columns (array/map/struct)
# Usage: mix run checklist/explore_34_to_explorer_complex_loss.exs

alias SparkEx.{Column, DataFrame}
import SparkEx.Functions

IO.puts("=== Repro: to_explorer complex type loss ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()
tbl = "sfx.sfx_dev_warehouse.spark_ex_r10_explorer_#{run_id}"

complex_df =
  SparkEx.sql(
    s,
    """
    SELECT
      array(1,2,3) AS arr,
      map('a', 1, 'b', 2) AS m,
      named_struct('name', 'alice', 'age', 30) AS st
    """
  )

IO.inspect(DataFrame.collect(complex_df), label: "collect complex_df")
IO.inspect(DataFrame.to_explorer(complex_df), label: "to_explorer complex_df")

agg_df =
  SparkEx.sql(s, "SELECT * FROM VALUES ('g',1),('g',2),('g',3) AS t(grp, x)")
  |> DataFrame.group_by([col("grp")])
  |> SparkEx.GroupedData.agg([collect_list(col("x")) |> Column.alias_("xs")])

IO.inspect(DataFrame.collect(agg_df), label: "collect agg_df")
IO.inspect(DataFrame.to_explorer(agg_df), label: "to_explorer agg_df")

SparkEx.sql(
  s,
  """
  CREATE TABLE #{tbl} (
    id INT,
    arr ARRAY<INT>
  ) USING iceberg
  """
)
|> DataFrame.collect()

SparkEx.sql(s, "INSERT INTO #{tbl} VALUES (1, array(10, 20, 30))")
|> DataFrame.collect()

rt_df = SparkEx.sql(s, "SELECT * FROM #{tbl}")
IO.inspect(DataFrame.collect(rt_df), label: "collect roundtrip_df")
IO.inspect(DataFrame.to_explorer(rt_df), label: "to_explorer roundtrip_df")

SparkEx.sql(s, "DROP TABLE IF EXISTS #{tbl} PURGE")
|> DataFrame.collect()

IO.puts("\n=== Done ===")
