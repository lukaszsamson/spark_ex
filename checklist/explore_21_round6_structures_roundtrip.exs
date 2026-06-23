# Exploratory Round 6: advanced data structures, column order, roundtrips
# Usage: mix run checklist/explore_21_round6_structures_roundtrip.exs

alias SparkEx.{DataFrame, Reader, Writer}
import SparkEx.Functions

IO.puts("=== Exploratory Round 6: structures + column order + roundtrips ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
run_id = :erlang.system_time(:millisecond) |> to_string()

roundtrip_fqn = "sfx.sfx_dev_warehouse.spark_ex_r6_roundtrip_#{run_id}"
nested_fqn = "sfx.sfx_dev_warehouse.spark_ex_r6_nested_#{run_id}"

run = fn label, fun ->
  IO.puts(label)

  try do
    result = fun.()
    IO.inspect(result, label: "  result", limit: 30, printable_limit: 2000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  ERROR (#{kind}): #{inspect(reason, limit: 8)}")
  end

  IO.puts("")
end

# --- Advanced data structures ---
run.("S1. Duplicate aliases in projection", fn ->
  SparkEx.sql(s, "SELECT 1 AS dup, 2 AS dup")
  |> DataFrame.collect()
end)

run.("S2. Struct->array->map->struct decode", fn ->
  SparkEx.sql(
    s,
    """
    SELECT named_struct(
      'items',
      array(
        map('k1', named_struct('vals', array(1,2), 'flag', true)),
        map('k2', named_struct('vals', array(3,4), 'flag', false))
      )
    ) AS payload
    """
  )
  |> DataFrame.collect()
end)

run.("S3. Array<struct<map<string,array<int>>>> decode", fn ->
  SparkEx.sql(
    s,
    """
    SELECT array(
      named_struct('m', map('a', array(1,2), 'b', array(3,4))),
      named_struct('m', map('c', array(5), 'd', array(6,7)))
    ) AS deep_arr
    """
  )
  |> DataFrame.collect()
end)

# --- Column-order behavior ---
run.("C1. Select reorders columns (c3,c1,c2)", fn ->
  df =
    SparkEx.sql(s, "SELECT 1 AS c1, 2 AS c2, 3 AS c3")
    |> DataFrame.select([col("c3"), col("c1"), col("c2")])

  {DataFrame.columns(df), DataFrame.collect(df)}
end)

# --- Roundtrip behavior ---
run.("R1. Create roundtrip table", fn ->
  SparkEx.sql(
    s,
    """
    CREATE TABLE #{roundtrip_fqn} (
      id INT,
      age INT,
      score INT
    ) USING iceberg
    """
  )
  |> DataFrame.collect()
end)

run.("R2. Insert baseline row by SQL", fn ->
  SparkEx.sql(s, "INSERT INTO #{roundtrip_fqn} VALUES (1, 20, 100)")
  |> DataFrame.collect()
end)

run.("R3. Append reordered columns via Writer.save_as_table", fn ->
  reordered = SparkEx.sql(s, "SELECT 900 AS score, 2 AS id, 30 AS age")

  DataFrame.write(reordered)
  |> Writer.mode("append")
  |> Writer.save_as_table(roundtrip_fqn)
end)

run.("R4. Verify roundtrip rows", fn ->
  SparkEx.sql(s, "SELECT id, age, score FROM #{roundtrip_fqn} ORDER BY id")
  |> DataFrame.collect()
end)

run.("R5. Create nested roundtrip table", fn ->
  SparkEx.sql(
    s,
    """
    CREATE TABLE #{nested_fqn} (
      id INT,
      payload STRUCT<numbers: ARRAY<INT>, attrs: MAP<STRING, STRING>>
    ) USING iceberg
    """
  )
  |> DataFrame.collect()
end)

run.("R6. Insert nested payload and read via Reader.table", fn ->
  SparkEx.sql(
    s,
    """
    INSERT INTO #{nested_fqn} VALUES
      (1, named_struct('numbers', array(1,2,3), 'attrs', map('x','10','y','20')))
    """
  )
  |> DataFrame.collect()

  Reader.table(s, nested_fqn)
  |> DataFrame.collect()
end)

run.("Cleanup roundtrip tables", fn ->
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{nested_fqn} PURGE") |> DataFrame.collect()
  SparkEx.sql(s, "DROP TABLE IF EXISTS #{roundtrip_fqn} PURGE") |> DataFrame.collect()
end)

IO.puts("=== Round 6 complete ===")
