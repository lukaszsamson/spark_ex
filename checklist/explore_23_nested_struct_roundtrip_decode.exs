# Exploratory repro: nested struct/map/array decode after table roundtrip
# Usage: mix run checklist/explore_23_nested_struct_roundtrip_decode.exs

alias SparkEx.{DataFrame, Reader}

IO.puts("=== Repro: nested struct roundtrip decode ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()
fqn = "sfx.sfx_dev_warehouse.spark_ex_r6_nested_repro_#{run_id}"

SparkEx.sql(
  s,
  """
  CREATE TABLE #{fqn} (
    id INT,
    payload STRUCT<numbers: ARRAY<INT>, attrs: MAP<STRING, STRING>>
  ) USING iceberg
  """
)
|> DataFrame.collect()

SparkEx.sql(
  s,
  """
  INSERT INTO #{fqn} VALUES
    (1, named_struct('numbers', array(1,2,3), 'attrs', map('x','10','y','20')))
  """
)
|> DataFrame.collect()

result =
  Reader.table(s, fqn)
  |> DataFrame.collect()

IO.inspect(result, label: "roundtrip read result")

SparkEx.sql(s, "DROP TABLE IF EXISTS #{fqn} PURGE")
|> DataFrame.collect()

IO.puts("\n=== Done ===")
