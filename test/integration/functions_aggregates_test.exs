defmodule SparkEx.Integration.FunctionsAggregatesTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Functions, GroupedData}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "collect_list/collect_set/first/last aggregations", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (1, 'b'), (1, 'b') AS t(id, val)")

    agg =
      df
      |> DataFrame.group_by(["id"])
      |> GroupedData.agg([
        SparkEx.Column.alias_(Functions.collect_list(Functions.col("val")), "vals_list"),
        SparkEx.Column.alias_(Functions.collect_set(Functions.col("val")), "vals_set"),
        SparkEx.Column.alias_(Functions.first(Functions.col("val")), "first_val"),
        SparkEx.Column.alias_(Functions.last(Functions.col("val")), "last_val")
      ])

    assert {:ok, [row]} = DataFrame.collect(agg)

    assert Enum.sort(row["vals_list"]) == ["a", "b", "b"]
    assert Enum.sort(row["vals_set"]) == ["a", "b"]
    assert row["first_val"] in ["a", "b"]
    assert row["last_val"] in ["a", "b"]
  end
end
