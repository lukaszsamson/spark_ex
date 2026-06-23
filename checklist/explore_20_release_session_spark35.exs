# Exploratory repro: Session.release on Spark 3.5 endpoint
# Usage: mix run checklist/explore_20_release_session_spark35.exs

IO.puts("=== Repro: Session.release on Spark 3.5 ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.inspect(SparkEx.spark_version(s), label: "spark_version")
IO.inspect(SparkEx.Session.release(s), label: "release_result")

IO.puts("\n=== Done ===")
