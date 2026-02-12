defmodule SparkEx.Integration.WhenOtherwiseTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Functions}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "when/otherwise chains select matching branch", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (5), (15), (25) AS t(score)")

    grade =
      Functions.when_(Column.lt(Functions.col("score"), 10), "low")
      |> Functions.otherwise(
        Functions.when_(Column.lt(Functions.col("score"), 20), "mid")
        |> Functions.otherwise("high")
      )

    projected =
      DataFrame.select(df, [
        Functions.col("score"),
        SparkEx.Column.alias_(grade, "grade")
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert Enum.find(rows, &(&1["score"] == 5))["grade"] == "low"
    assert Enum.find(rows, &(&1["score"] == 15))["grade"] == "mid"
    assert Enum.find(rows, &(&1["score"] == 25))["grade"] == "high"
  end
end
