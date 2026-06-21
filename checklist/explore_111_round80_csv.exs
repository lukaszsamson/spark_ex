alias SparkEx.{DataFrame, Column}

import SparkEx.Functions,
  except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1, min: 1, max: 1, sum: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)

  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 30, printable_limit: 1000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end

  IO.puts("")
end

run.("1. from_csv native", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"csv" => "1,Alice"}
      ], schema: "csv STRING")

  df
  |> DataFrame.with_column(
    "parsed",
    SparkEx.Functions.from_csv(col("csv"), "id INT, name STRING", %{"header" => "false"})
  )
  |> DataFrame.collect()
end)

run.("2. schema_of_csv native", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1}
      ], schema: "id INT")

  df
  |> DataFrame.with_column("sch", SparkEx.Functions.schema_of_csv(lit("1,Alice")))
  |> DataFrame.collect()
end)

run.("3. to_csv", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"nested" => %{"id" => 1, "name" => "Alice"}}
      ], schema: "nested STRUCT<id: INT, name: STRING>")

  df
  |> DataFrame.with_column(
    "csv_str",
    SparkEx.Functions.to_csv(col("nested"), %{"header" => "false"})
  )
  |> DataFrame.collect()
end)
