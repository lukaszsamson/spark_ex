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

run.("1. filter with index", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => ["a", "b", "c", "d"]}
      ],
      schema: "arr ARRAY<STRING>"
    )

  # keep odd elements (index % 2 == 1). In Spark, index passed to filter/transform is 0-based.
  df
  |> DataFrame.with_column(
    "odds",
    SparkEx.Functions.filter(col("arr"), fn _x, i ->
      Column.eq(Column.mod(i, lit(2)), lit(1))
    end)
  )
  |> DataFrame.collect()
end)

run.("2. transform with index", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => [10, 20, 30]}
      ],
      schema: "arr ARRAY<INT>"
    )

  # add index to element
  df
  |> DataFrame.with_column(
    "idx_added",
    SparkEx.Functions.transform(col("arr"), fn x, i ->
      Column.plus(x, i)
    end)
  )
  |> DataFrame.collect()
end)

run.("3. array_sort with comparator", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => [3, 1, 4, 2]}
      ],
      schema: "arr ARRAY<INT>"
    )

  # Wait, does array_sort support a lambda in spark_ex? 
  # Let's try it. PySpark array_sort(col, comparator). If it fails, we catch it.
  df
  |> DataFrame.with_column(
    "sorted_desc",
    SparkEx.Functions.array_sort(col("arr"), fn left, right ->
      # comparator: if left < right return 1, if left == right return 0, else -1
      SparkEx.Functions.when_(Column.lt(left, right), lit(1))
      |> Column.when_(Column.eq(left, right), lit(0))
      |> Column.otherwise(lit(-1))
    end)
  )
  |> DataFrame.collect()
end)
