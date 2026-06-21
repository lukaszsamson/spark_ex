# Round 73c: Remaining tests after unpivot crash
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 80, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

run_id = :erlang.system_time(:millisecond) |> to_string()

# Skip 1a (crashes), test 1b separately
run.("1b. unpivot non-string value_column_name → collect", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [%{"id" => 1, "a" => 10, "b" => 20}], schema: "id INT, a INT, b INT")
  DataFrame.unpivot(df, [col("id")], [col("a"), col("b")], "var", 42) |> DataFrame.collect()
end)

# This may crash too, so continue with fresh session

IO.puts("=== Round 73c part 1 ===")
