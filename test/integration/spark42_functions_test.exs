defmodule SparkEx.Integration.Spark42FunctionsTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{Column, DataFrame, Session}
  alias SparkEx.Functions, as: F

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "TIME conversion precision, variant validity and current path", %{session: session} do
    :ok = Session.config_set(session, [{"spark.sql.timeType.enabled", "true"}])

    df = SparkEx.sql(session, "SELECT 3723000004L AS micros, parse_json('{\"x\":1}') AS v")

    result =
      DataFrame.select(df, [
        Column.alias_(F.time_to_micros(F.time_from_micros("micros")), "micros"),
        Column.alias_(F.time_to_millis(F.time_from_millis(F.lit(3_723_000))), "millis"),
        Column.alias_(F.time_to_seconds(F.time_from_seconds(F.lit(3723))), "seconds"),
        Column.alias_(F.is_valid_variant("v"), "valid"),
        Column.alias_(F.current_path(), "path")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert row["micros"] == 3_723_000_004
    assert row["millis"] == 3_723_000
    assert Decimal.equal?(row["seconds"], Decimal.new("3723.000000"))
    assert row["valid"] == true
    assert row["path"] != nil
  end

  test "top K aggregates preserve the scalar overload", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES ('a',1),('b',2),('c',3) AS t(v,n)")

    result =
      DataFrame.select(df, [
        Column.alias_(F.max_by("v", "n"), "max"),
        Column.alias_(F.max_by("v", "n", 2), "top"),
        Column.alias_(F.min_by("v", "n", 2), "bottom")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert row["max"] == "c"
    assert Enum.sort(row["top"]) == ["b", "c"]
    assert Enum.sort(row["bottom"]) == ["a", "b"]
  end

  test "tuple sketches and KLL merge aggregate round trip", %{session: session} do
    source = SparkEx.range(session, 10)

    sketches =
      DataFrame.select(source, [
        Column.alias_(F.tuple_sketch_agg_double("id", F.expr("CAST(id AS DOUBLE)")), "tuple"),
        Column.alias_(F.kll_sketch_agg_bigint("id"), "kll")
      ])

    assert {:ok, [%{"estimate" => estimate}]} =
             sketches
             |> DataFrame.select([
               Column.alias_(F.tuple_sketch_estimate_double("tuple"), "estimate")
             ])
             |> DataFrame.collect()

    assert_in_delta estimate, 10.0, 0.01

    merged = DataFrame.select(sketches, [Column.alias_(F.kll_merge_agg_bigint("kll"), "merged")])

    assert {:ok, [%{"n" => 10}]} =
             merged
             |> DataFrame.select([Column.alias_(F.kll_sketch_get_n_bigint("merged"), "n")])
             |> DataFrame.collect()
  end
end
