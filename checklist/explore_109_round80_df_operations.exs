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

run.("1. summary with custom percentiles", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"val" => 10},
        %{"val" => 20},
        %{"val" => 30},
        %{"val" => 40},
        %{"val" => 50}
      ], schema: "val INT")

  df
  |> DataFrame.summary(["count", "25%", "50%", "75%", "max"])
  |> DataFrame.collect()
end)

run.("2. random_split and limit", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"val" => 10},
        %{"val" => 20},
        %{"val" => 30},
        %{"val" => 40},
        %{"val" => 50}
      ], schema: "val INT")

  [d1, d2] = DataFrame.random_split(df, [0.5, 0.5], 42)

  IO.inspect(DataFrame.count(d1))
  IO.inspect(DataFrame.count(d2))

  d1 |> DataFrame.limit(1) |> DataFrame.collect()
end)

run.("3. describe specific columns", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"val1" => 10, "val2" => "A"},
        %{"val1" => 20, "val2" => "B"}
      ], schema: "val1 INT, val2 STRING")

  df
  |> DataFrame.describe(["val2"])
  |> DataFrame.collect()
end)

run.("4. offset", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1},
        %{"id" => 2},
        %{"id" => 3}
      ], schema: "id INT")

  df
  |> DataFrame.offset(1)
  |> DataFrame.collect()
end)
