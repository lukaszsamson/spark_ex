defmodule SparkEx.Integration.ColumnEdgeCasesTest do
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

  @tag min_spark: "4.0"
  test "invalid star usage returns structured error", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM (SELECT 1 AS id) t WHERE * = 1")

    assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)

    assert error.error_class in [
             "INVALID_STAR_USAGE",
             "INVALID_USAGE_OF_STAR_OR_REGEX",
             "PARSE_SYNTAX_ERROR",
             "UNRESOLVED_STAR"
           ]
  end

  test "isin edge cases with empty and nil values", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id UNION ALL SELECT NULL AS id")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Column.isin(Functions.col("id"), []), "in_empty"),
        SparkEx.Column.alias_(Column.isin(Functions.col("id"), [nil, 1]), "in_nil")
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert Enum.any?(rows, &(&1["in_nil"] == true))
  end

  test "between with null bounds", %{session: session} do
    df = SparkEx.sql(session, "SELECT 5 AS val, NULL AS low, 10 AS high")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(
          Column.between(Functions.col("val"), Functions.col("low"), Functions.col("high")),
          "in_range"
        )
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["in_range"] == nil
  end

  test "between with column bounds", %{session: session} do
    df = SparkEx.sql(session, "SELECT 5 AS val, 1 AS low, 10 AS high")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(
          Column.between(Functions.col("val"), Functions.col("low"), Functions.col("high")),
          "in_range"
        )
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["in_range"] == true
  end

  test "cast matrix for common types", %{session: session} do
    df = SparkEx.sql(session, "SELECT '123' AS s, 3.5 AS f, 1 AS i")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Column.cast(Functions.col("s"), "INT"), "s_to_int"),
        SparkEx.Column.alias_(Column.cast(Functions.col("f"), "INT"), "f_to_int"),
        SparkEx.Column.alias_(Column.cast(Functions.col("i"), "STRING"), "i_to_str")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["s_to_int"] == 123
    assert row["f_to_int"] == 3
    assert row["i_to_str"] == "1"
  end
end
