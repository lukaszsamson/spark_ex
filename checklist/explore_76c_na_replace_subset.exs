# Quick verification: NA.replace subset option
alias SparkEx.{DataFrame, Column}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

# Test: replace 1→100 only in column "a", column "b" should remain 1
{:ok, df} = SparkEx.create_dataframe(s, [
  %{"a" => 1, "b" => 1}, %{"a" => 2, "b" => 2}
], schema: "a INT, b INT")

IO.puts("Input data:")
{:ok, input} = DataFrame.collect(df)
IO.inspect(input)

IO.puts("\nNA.replace with subset: [\"a\"] (only replace in column a):")
result = SparkEx.DataFrame.NA.replace(df, %{1 => 100}, subset: ["a"])
{:ok, rows} = DataFrame.collect(result)
IO.inspect(rows)
IO.puts("Expected: a=100,b=1 and a=2,b=2")
IO.puts("Got b=#{hd(rows)["b"]} (should be 1, not 100)")

IO.puts("\nNA.replace WITHOUT subset (replace in all columns):")
result2 = SparkEx.DataFrame.NA.replace(df, %{1 => 100})
{:ok, rows2} = DataFrame.collect(result2)
IO.inspect(rows2)
IO.puts("Expected: a=100,b=100 and a=2,b=2")
