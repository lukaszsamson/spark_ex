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

run.("1. Column alias metadata", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"val" => 10}
      ],
      schema: "val INT"
    )

  # Fetch schema to verify metadata
  schema =
    DataFrame.schema(
      df
      |> DataFrame.select([col("val") |> Column.alias_("v2", metadata: %{"my_key" => "my_val"})])
    )

  IO.inspect(schema)
end)

run.("2. to_timestamp and to_date with format", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"dt_str" => "2024-05-15 14:30:00"}
      ],
      schema: "dt_str STRING"
    )

  df
  |> DataFrame.with_column(
    "dt",
    SparkEx.Functions.to_date(col("dt_str"), format: "yyyy-MM-dd HH:mm:ss")
  )
  |> DataFrame.with_column(
    "ts",
    SparkEx.Functions.to_timestamp(col("dt_str"), format: "yyyy-MM-dd HH:mm:ss")
  )
  |> DataFrame.collect()
end)

run.("3. explode with posexplode", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"id" => 1, "arr" => ["A", "B", "C"]}
      ],
      schema: "id INT, arr ARRAY<STRING>"
    )

  # In spark_ex, Generator functions like posexplode must be used carefully. In PySpark it's select(posexplode(...))
  # In DataFrame API, select(col("id"), posexplode(col("arr"))) returns 3 columns: id, pos, col.
  df
  |> DataFrame.select([col("id"), SparkEx.Functions.posexplode(col("arr"))])
  |> DataFrame.collect()
end)

run.("4. arrays_overlap", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"a1" => [1, 2, 3], "a2" => [3, 4, 5]},
        %{"a1" => [1, 2], "a2" => [3, 4]}
      ],
      schema: "a1 ARRAY<INT>, a2 ARRAY<INT>"
    )

  df
  |> DataFrame.with_column("overlap", SparkEx.Functions.arrays_overlap(col("a1"), col("a2")))
  |> DataFrame.collect()
end)

run.("5. aggregate arity-3 (without finish)", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"arr" => [1, 2, 3, 4]}
      ],
      schema: "arr ARRAY<INT>"
    )

  # fold over array without finish func
  df
  |> DataFrame.with_column(
    "sum",
    SparkEx.Functions.aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end)
  )
  |> DataFrame.collect()
end)

run.("6. array_repeat vs repeat", fn ->
  {:ok, df} =
    SparkEx.create_dataframe(
      s,
      [
        %{"str" => "hi", "arr" => [1, 2]}
      ],
      schema: "str STRING, arr ARRAY<INT>"
    )

  df
  |> DataFrame.with_column("repeated_str", SparkEx.Functions.repeat(col("str"), 3))
  |> DataFrame.with_column("repeated_arr", SparkEx.Functions.array_repeat(col("arr"), 3))
  |> DataFrame.collect()
end)
