defmodule SparkEx.Integration.Spark42FunctionRegressionsTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{Column, DataFrame, Functions, Session}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  # Expected boundaries are pinned by Spark v4.2.0 DateExpressionsSuite's
  # "time_bucket: day-time interval" cases.
  test "time_bucket handles default/custom origins, NTZ values, boundaries, and nulls", %{
    session: session
  } do
    timezone = "spark.sql.session.timeZone"

    try do
      assert :ok = Session.config_set(session, [{timezone, "UTC"}])

      df =
        SparkEx.sql(
          session,
          """
          SELECT
            TIMESTAMP '2024-01-01 11:27:00' AS ts,
            TIMESTAMP_NTZ '2024-01-01 11:27:00' AS ntz,
            CAST(NULL AS TIMESTAMP) AS missing
          """
        )

      result =
        DataFrame.select(df, [
          as_string(Functions.time_bucket(Functions.expr("INTERVAL 15 MINUTES"), "ts"), "epoch"),
          as_string(
            Functions.time_bucket(
              Functions.expr("INTERVAL 1 HOUR"),
              "ts",
              Functions.expr("TIMESTAMP '1970-01-01 00:05:00'")
            ),
            "custom"
          ),
          as_string(Functions.time_bucket(Functions.expr("INTERVAL 15 MINUTES"), "ntz"), "ntz"),
          Column.alias_(
            Functions.time_bucket(Functions.expr("INTERVAL 15 MINUTES"), "missing"),
            "missing"
          )
        ])

      assert {:ok,
              [
                %{
                  "epoch" => "2024-01-01 11:15:00",
                  "custom" => "2024-01-01 11:05:00",
                  "ntz" => "2024-01-01 11:15:00",
                  "missing" => nil
                }
              ]} = DataFrame.collect(result)
    after
      Session.config_unset(session, [timezone])
    end
  end

  # Spark v4.2.0 DataFrameAggregateSuite pins the k=1, oversized-k, NULL
  # ordering, empty-input, and non-deterministic tie contracts.
  test "top-K max_by/min_by cover k edges, null ordering, empty input, and ties", %{
    session: session
  } do
    df =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('a', 10), ('b', NULL), ('c', 20) AS t(value, ordering)"
      )

    assert {:ok, [row]} =
             DataFrame.select(df, [
               Column.alias_(Functions.max_by("value", "ordering", 1), "max_one"),
               Column.alias_(Functions.min_by("value", "ordering", 5), "min_many")
             ])
             |> DataFrame.collect()

    assert row == %{"max_one" => ["c"], "min_many" => ["a", "c"]}

    empty = DataFrame.filter(df, Functions.lit(false))

    assert {:ok, [%{"top" => nil}]} =
             DataFrame.select(empty, [
               Column.alias_(Functions.max_by("value", "ordering", 2), "top")
             ])
             |> DataFrame.collect()

    ties =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('a', 50), ('b', 50), ('c', 10), ('d', 90) AS t(value, ordering)"
      )

    assert {:ok, [%{"top" => top}]} =
             DataFrame.select(ties, [
               Column.alias_(Functions.max_by("value", "ordering", 3), "top")
             ])
             |> DataFrame.collect()

    assert "d" in top
    assert length(top) == 3
    assert MapSet.subset?(MapSet.new(top), MapSet.new(["a", "b", "d"]))

    assert {:error, %SparkEx.Error.Remote{} = error} =
             DataFrame.select(df, [
               Column.alias_(Functions.max_by("value", "ordering", 0), "invalid")
             ])
             |> DataFrame.collect()

    assert error.error_class == "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE"
    assert error.sql_state == "42K09"
    assert error.message_parameters["currentValue"] == "0"
    assert error.message_parameters["valueRange"] == "[1, 100000]"
  end

  defp as_string(column, name), do: column |> Column.cast("string") |> Column.alias_(name)
end
