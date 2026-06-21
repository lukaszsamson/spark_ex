# Round 69d: Remaining config API + other tests after 69c crash
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

# 17b-f: Config API validation (17a already crashed)

run.("17b. config_unset with non-string", fn ->
  SparkEx.config_unset(s, 42)
end)

# Test 17b may crash session - if so, remaining tests won't run
# Let's test one at a time with fresh sessions

IO.puts("=== Round 69d complete ===")
