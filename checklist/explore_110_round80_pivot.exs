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

run.("1. pivot with unpivoted columns (melt -> pivot)", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "q1" => 10, "q2" => 15, "q3" => 12},
        %{"id" => 2, "q1" => 20, "q2" => 25, "q3" => 22}
      ], schema: "id INT, q1 INT, q2 INT, q3 INT")

  # melt q1, q2, q3 into quarter, revenue
  melted =
    df
    |> DataFrame.unpivot(["id"], [{"q1", "q1"}, {"q2", "q2"}, {"q3", "q3"}], "quarter", "revenue")

  # now pivot it back but sum(revenue) and max(revenue) 
  melted
  |> DataFrame.group_by([col("id")])
  |> DataFrame.pivot(col("quarter"), ["q1", "q2", "q3"])
  |> SparkEx.GroupedData.agg([
    SparkEx.Functions.sum(col("revenue")) |> Column.alias_("tot"),
    SparkEx.Functions.max(col("revenue")) |> Column.alias_("mx")
  ])
  |> DataFrame.order_by([asc(col("id"))])
  |> DataFrame.collect()
end)

run.("2. unpivot with mismatched types?", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "int_col" => 10, "str_col" => "15"}
      ], schema: "id INT, int_col INT, str_col STRING")

  # unpivot columns of different types (Spark usually casts them to string or errors)
  df
  |> DataFrame.unpivot(["id"], ["int_col", "str_col"], "key", "val")
  |> DataFrame.collect()
end)

run.("3. melt missing columns in values?", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "a" => 10}
      ], schema: "id INT, a INT")

  # what if we refer to non-existent column?
  df
  |> DataFrame.unpivot(["id"], ["a", "b"], "key", "val")
  |> DataFrame.collect()
end)

run.("4. GroupedData.pivot with missing values list", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "q" => "Q1", "val" => 10},
        %{"id" => 1, "q" => "Q2", "val" => 20}
      ], schema: "id INT, q STRING, val INT")

  # what if we omit the values list? Spark allows computing it from data, but Connect needs an explicit list or a special API?
  # Let's check PySpark. It's pivot("col"). 
  df
  |> DataFrame.group_by([col("id")])
  # Try passing nil?
  |> DataFrame.pivot(col("q"), nil)
  |> SparkEx.GroupedData.agg([SparkEx.Functions.sum(col("val"))])
  |> DataFrame.collect()
end)
