# Exploratory testing of complex aggregations, grouping sets, rollup, cube, and windowing
alias SparkEx.{DataFrame, Column, GroupedData, Window, WindowSpec}

import SparkEx.Functions,
  except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1, min: 1, max: 1, sum: 1]

# Some functions might be missing, we'll alias them directly if needed.

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

run.("1. Cube with multiple group levels, complex aggregations", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"region" => "NA", "country" => "US", "product" => "A", "sales" => 100},
        %{"region" => "NA", "country" => "US", "product" => "B", "sales" => 200},
        %{"region" => "NA", "country" => "CA", "product" => "A", "sales" => 150},
        %{"region" => "EU", "country" => "UK", "product" => "A", "sales" => 300},
        %{"region" => "EU", "country" => "FR", "product" => "B", "sales" => 250}
      ],
      schema: "region STRING, country STRING, product STRING, sales INT"
    )

  # Cube by region, country, product
  DataFrame.cube(df, [col("region"), col("country"), col("product")])
  |> SparkEx.GroupedData.agg([
    SparkEx.Functions.sum(col("sales")) |> Column.alias_("total_sales"),
    SparkEx.Functions.approx_count_distinct(col("product")) |> Column.alias_("unique_products"),
    SparkEx.Functions.collect_list(col("sales")) |> Column.alias_("all_sales"),
    SparkEx.Functions.grouping_id([col("region"), col("country"), col("product")])
    |> Column.alias_("g_id")
  ])
  |> DataFrame.order_by([desc(col("g_id")), asc(col("region"))])
  |> DataFrame.collect()
end)

run.("2. Rollup with complex aggregations and grouping filters", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"year" => 2023, "month" => 1, "revenue" => 1000},
        %{"year" => 2023, "month" => 2, "revenue" => 1500},
        %{"year" => 2024, "month" => 1, "revenue" => 2000}
      ],
      schema: "year INT, month INT, revenue INT"
    )

  # Rollup
  DataFrame.rollup(df, [col("year"), col("month")])
  |> SparkEx.GroupedData.agg([
    SparkEx.Functions.sum(col("revenue")) |> Column.alias_("total"),
    SparkEx.Functions.grouping(col("year")) |> Column.alias_("is_year_total"),
    SparkEx.Functions.grouping(col("month")) |> Column.alias_("is_month_total")
  ])
  |> DataFrame.filter(Column.eq(col("is_month_total"), lit(1)))
  |> DataFrame.collect()
end)

run.("3. Window with range_between using temporal logic (simulate integers)", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "day" => 1, "val" => 10},
        %{"id" => 1, "day" => 2, "val" => 20},
        %{"id" => 1, "day" => 4, "val" => 40},
        %{"id" => 1, "day" => 5, "val" => 50}
      ],
      schema: "id INT, day INT, val INT"
    )

  # sum values over a window of day-1 to day+1
  w =
    Window.partition_by([col("id")])
    |> WindowSpec.order_by([asc(col("day"))])
    |> WindowSpec.range_between(-1, 1)

  df
  |> DataFrame.with_column("rolling_sum", SparkEx.Functions.sum(col("val")) |> Column.over(w))
  |> DataFrame.collect()
end)

run.("4. Window with range_between on dates (if supported)", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"dt" => ~D[2024-01-01], "val" => 10},
        %{"dt" => ~D[2024-01-02], "val" => 20},
        %{"dt" => ~D[2024-01-04], "val" => 40}
      ],
      schema: "dt DATE, val INT"
    )

  # usually range_between on date requires days in PySpark: e.g. -1, 1 as integers
  w =
    WindowSpec.order_by(%WindowSpec{}, [asc(col("dt"))])
    |> WindowSpec.range_between(-1, 1)

  df
  |> DataFrame.with_column("rolling_sum", SparkEx.Functions.sum(col("val")) |> Column.over(w))
  |> DataFrame.collect()
end)

run.("5. aggregate (reduce) function with complex structures", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => [1, 2, 3, 4, 5]}
      ],
      schema: "arr ARRAY<INT>"
    )

  # fold/reduce over array
  # syntax: aggregate(col, initialValue, merge_func, finish_func)
  df
  |> DataFrame.with_column(
    "sum",
    SparkEx.Functions.aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end)
  )
  |> DataFrame.collect()
end)

run.("6. array_aggregate with finish func", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => [1, 2, 3]}
      ],
      schema: "arr ARRAY<INT>"
    )

  # calculate average manually via aggregate
  # state is a struct {sum, count} ... but we can't easily express struct state in Elixir lambda without array
  # let's use an array as state [sum, count]
  df
  |> DataFrame.with_column(
    "avg",
    SparkEx.Functions.aggregate(
      col("arr"),
      SparkEx.Functions.array([lit(0), lit(0)]),
      fn acc, x ->
        SparkEx.Functions.array([
          Column.plus(SparkEx.Functions.element_at(acc, 1), x),
          Column.plus(SparkEx.Functions.element_at(acc, 2), lit(1))
        ])
      end,
      fn acc ->
        Column.divide(
          SparkEx.Functions.element_at(acc, 1),
          SparkEx.Functions.element_at(acc, 2)
        )
      end
    )
  )
  |> DataFrame.collect()
end)

run.("7. zip_with of mismatched lengths", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"a1" => [1, 2, 3], "a2" => [10, 20]}
      ],
      schema: "a1 ARRAY<INT>, a2 ARRAY<INT>"
    )

  # what happens when we zip_with unequal lengths? (Usually null-pads)
  df
  |> DataFrame.with_column(
    "zipped",
    SparkEx.Functions.zip_with(col("a1"), col("a2"), fn x, y ->
      SparkEx.Functions.coalesce([Column.plus(x, y), x, y])
    end)
  )
  |> DataFrame.collect()
end)

run.("8. transform_keys and transform_values with nested data", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"m" => %{"a" => %{"x" => 1}, "b" => %{"y" => 2}}}
      ],
      schema: "m MAP<STRING, MAP<STRING, INT>>"
    )

  # transform keys to upper
  # transform values to size of the nested map
  df
  |> DataFrame.with_column(
    "k_upper",
    SparkEx.Functions.transform_keys(col("m"), fn k, _v -> SparkEx.Functions.upper(k) end)
  )
  |> DataFrame.with_column(
    "v_sizes",
    SparkEx.Functions.transform_values(col("m"), fn _k, v -> SparkEx.Functions.size(v) end)
  )
  |> DataFrame.collect()
end)
