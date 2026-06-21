# Exploratory: StreamingQueryManager timeout behavior behind ALB/TLS
# Usage: mix run checklist/explore_15_streaming_manager_timeout.exs

alias SparkEx.StreamingQueryManager

IO.puts("=== Exploratory: StreamingQueryManager.await_any_termination timeout ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

result = StreamingQueryManager.await_any_termination(s, timeout: 1)
IO.inspect(result, label: "await_any_termination(timeout: 1)")

IO.inspect(SparkEx.spark_version(s), label: "spark_version after call")

SparkEx.Session.release(s)

IO.puts("\n=== Done ===")
