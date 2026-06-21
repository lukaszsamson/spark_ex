alias SparkEx.{DataFrame, Column, WindowSpec, Window}

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

run.("1. lead and lag with default values", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "val" => "A"},
        %{"id" => 2, "val" => "B"},
        %{"id" => 3, "val" => "C"}
      ],
      schema: "id INT, val STRING"
    )

  w = Window.order_by([asc(col("id"))])

  df
  |> DataFrame.with_column(
    "nxt",
    SparkEx.Functions.lead(col("val"), offset: 1, default: lit("NONE")) |> Column.over(w)
  )
  |> DataFrame.with_column(
    "prv",
    SparkEx.Functions.lag(col("val"), offset: 1, default: lit("NONE")) |> Column.over(w)
  )
  |> DataFrame.collect()
end)

run.("2. nth_value", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "val" => "A"},
        %{"id" => 2, "val" => "B"},
        %{"id" => 3, "val" => "C"}
      ],
      schema: "id INT, val STRING"
    )

  w = Window.order_by([asc(col("id"))])

  df
  |> DataFrame.with_column("first", SparkEx.Functions.first(col("val")) |> Column.over(w))
  |> DataFrame.with_column("last", SparkEx.Functions.last(col("val")) |> Column.over(w))
  |> DataFrame.with_column("nth2", SparkEx.Functions.nth_value(col("val"), 2) |> Column.over(w))
  |> DataFrame.collect()
end)

run.("3. cume_dist and ntile and percent_rank", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "val" => 10},
        %{"id" => 2, "val" => 10},
        %{"id" => 3, "val" => 30},
        %{"id" => 4, "val" => 40}
      ],
      schema: "id INT, val INT"
    )

  w = Window.order_by([asc(col("val"))])

  df
  |> DataFrame.with_column("cume", SparkEx.Functions.cume_dist() |> Column.over(w))
  |> DataFrame.with_column("nt", SparkEx.Functions.ntile(2) |> Column.over(w))
  |> DataFrame.with_column("pct_rk", SparkEx.Functions.percent_rank() |> Column.over(w))
  |> DataFrame.collect()
end)
