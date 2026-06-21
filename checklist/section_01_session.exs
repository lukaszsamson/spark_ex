# Section 1: Session & Configuration
# Usage: mix run checklist/section_01_session.exs

IO.puts("=== Section 1: Session & Configuration ===\n")

# Connect with TLS
IO.puts("Connecting to cluster...")
{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Connected!\n")

# 1.1 Spark version
IO.puts("1.1 Spark version")
result = SparkEx.spark_version(s)
IO.inspect(result, label: "  result")
IO.puts("")

# 1.2 Set config
IO.puts("1.2 Set config")
result = SparkEx.config_set(s, [{"spark.sql.shuffle.partitions", "4"}])
IO.inspect(result, label: "  result")
IO.puts("")

# 1.3 Get config
IO.puts("1.3 Get config")
result = SparkEx.config_get(s, ["spark.sql.shuffle.partitions"])
IO.inspect(result, label: "  result")
IO.puts("")

# 1.4 Get config with default
IO.puts("1.4 Get config with default")
result = SparkEx.config_get_with_default(s, [{"spark.nonexistent.key", "fallback"}])
IO.inspect(result, label: "  result")
IO.puts("")

# 1.5 Get optional config
IO.puts("1.5 Get optional config")
result = SparkEx.config_get_option(s, ["spark.sql.shuffle.partitions"])
IO.inspect(result, label: "  result (set key)")
result2 = SparkEx.config_get_option(s, ["spark.nonexistent.key.xyz"])
IO.inspect(result2, label: "  result (unset key)")
IO.puts("")

# 1.6 Get all config with prefix
IO.puts("1.6 Get all config with prefix")
result = SparkEx.config_get_all(s, "spark.sql")
case result do
  {:ok, list} when is_list(list) -> IO.puts("  OK — #{length(list)} keys with prefix spark.sql")
  other -> IO.inspect(other, label: "  result")
end
IO.puts("")

# 1.7 Get all config (no prefix)
IO.puts("1.7 Get all config (no prefix)")
result = SparkEx.config_get_all(s)
case result do
  {:ok, list} when is_list(list) -> IO.puts("  OK — #{length(list)} total config keys")
  other -> IO.inspect(other, label: "  result")
end
IO.puts("")

# 1.8 Config is modifiable
IO.puts("1.8 Config is modifiable")
result = SparkEx.config_is_modifiable(s, ["spark.sql.shuffle.partitions"])
IO.inspect(result, label: "  result")
IO.puts("")

# 1.9 Unset config
IO.puts("1.9 Unset config")
result = SparkEx.config_unset(s, ["spark.sql.shuffle.partitions"])
IO.inspect(result, label: "  result")
# Verify it's unset
result2 = SparkEx.config_get_option(s, ["spark.sql.shuffle.partitions"])
IO.inspect(result2, label: "  verify after unset")
IO.puts("")

# 1.10 Tags
IO.puts("1.10 Tags")
r1 = SparkEx.add_tag(s, "test")
IO.inspect(r1, label: "  add_tag")
r2 = SparkEx.get_tags(s)
IO.inspect(r2, label: "  get_tags")
r3 = SparkEx.remove_tag(s, "test")
IO.inspect(r3, label: "  remove_tag")
r4 = SparkEx.get_tags(s)
IO.inspect(r4, label: "  get_tags after remove")
r5 = SparkEx.clear_tags(s)
IO.inspect(r5, label: "  clear_tags")
IO.puts("")

# 1.11 Clone session
IO.puts("1.11 Clone session")
result = SparkEx.clone_session(s)
case result do
  {:ok, s2} ->
    IO.puts("  OK — cloned session: #{inspect(s2)}")
    ver = SparkEx.spark_version(s2)
    IO.inspect(ver, label: "  cloned session version")
    SparkEx.Session.release(s2)
  other ->
    IO.inspect(other, label: "  result")
end
IO.puts("")

# 1.12 is_stopped
IO.puts("1.12 is_stopped")
result = SparkEx.is_stopped(s)
IO.inspect(result, label: "  result")
IO.puts("")

IO.puts("=== Section 1 Complete ===")

# Clean up
SparkEx.Session.release(s)
