# Exploratory repro: join output with duplicate column names
# Usage: mix run checklist/explore_18_join_duplicate_columns.exs

alias SparkEx.DataFrame

IO.puts("=== Repro: join duplicate column names ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

left_df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'Alice'), (2,'Bob') AS t(id, name)")
right_df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'Sales'), (3,'Ops') AS t(id, name)")

result =
  DataFrame.join(left_df, right_df, ["id"], :left)
  |> DataFrame.collect()

IO.inspect(result, label: "join result")

SparkEx.Session.release(s)

IO.puts("\n=== Done ===")
