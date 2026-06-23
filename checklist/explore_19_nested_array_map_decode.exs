# Exploratory repro: nested array/map decoding panic class
# Usage: mix run checklist/explore_19_nested_array_map_decode.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: nested array/map decode ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

result =
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

IO.inspect(result, label: "nested decode result")

SparkEx.Session.release(s)

IO.puts("\n=== Done ===")
