# Section 4: DataFrame Schema Inspection
# Usage: mix run checklist/section_04_schema.exs

alias SparkEx.{DataFrame, Column}
import SparkEx.Functions

IO.puts("=== Section 4: DataFrame Schema Inspection ===\n")

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Connected!\n")

df = SparkEx.sql(s, "SELECT * FROM VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.5) AS t(id, name, salary)")

# 4.1 schema
IO.puts("4.1 schema")
result = DataFrame.schema(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.2 columns
IO.puts("4.2 columns")
result = DataFrame.columns(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.3 dtypes
IO.puts("4.3 dtypes")
result = DataFrame.dtypes(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.4 explain simple
IO.puts("4.4 explain simple")
result = DataFrame.explain(df, :simple)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.5 explain extended
IO.puts("4.5 explain extended")
result = DataFrame.explain(df, :extended)
case result do
  {:ok, plan} -> IO.puts("  OK — plan length: #{String.length(plan)}")
  other -> IO.inspect(other, label: "  result")
end
IO.puts("")

# 4.6 show
IO.puts("4.6 show")
result = DataFrame.show(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.7 print_schema
IO.puts("4.7 print_schema")
result = DataFrame.print_schema(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.8 tree_string
IO.puts("4.8 tree_string")
result = DataFrame.tree_string(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.9 html_string
IO.puts("4.9 html_string")
result = DataFrame.html_string(df)
case result do
  {:ok, html} -> IO.puts("  OK — html length: #{String.length(html)}")
  other -> IO.inspect(other, label: "  result")
end
IO.puts("")

# 4.10 is_local
IO.puts("4.10 is_local")
result = DataFrame.is_local(df)
IO.inspect(result, label: "  result")
IO.puts("")

# 4.11 is_streaming
IO.puts("4.11 is_streaming")
result = DataFrame.is_streaming(df)
IO.inspect(result, label: "  result")
IO.puts("")

IO.puts("=== Section 4 Complete ===")
