defmodule SparkEx.Integration.FunctionCategoryTest do
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

  test "count(*) integration", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")

    agg =
      df
      |> DataFrame.group_by([])
      |> GroupedData.agg([
        SparkEx.Column.alias_(Functions.count(Functions.star()), "cnt")
      ])

    assert {:ok, [row]} = DataFrame.collect(agg)
    assert row["cnt"] == 3
  end

  test "seeded rand/randn are deterministic", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Functions.rand(seed: 7), "r1"),
        SparkEx.Column.alias_(Functions.rand(seed: 7), "r2"),
        SparkEx.Column.alias_(Functions.randn(seed: 11), "n1"),
        SparkEx.Column.alias_(Functions.randn(seed: 11), "n2")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["r1"] == row["r2"]
    assert row["n1"] == row["n2"]
  end

  test "math/string/date function basics", %{session: session} do
    df = SparkEx.sql(session, "SELECT 'abc' AS text, 9 AS num, DATE '2024-01-10' AS dt")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Functions.abs(Functions.col("num")), "abs_num"),
        SparkEx.Column.alias_(Functions.length(Functions.col("text")), "len"),
        SparkEx.Column.alias_(Functions.upper(Functions.col("text")), "upper_text"),
        SparkEx.Column.alias_(Functions.date_add(Functions.col("dt"), 5), "dt_plus")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["abs_num"] == 9
    assert row["len"] == 3
    assert row["upper_text"] == "ABC"
    assert date_to_iso8601(row["dt_plus"]) == "2024-01-15"
  end

  defp date_to_iso8601(%Date{} = d), do: Date.to_iso8601(d)
  defp date_to_iso8601(v) when is_binary(v), do: v
end
