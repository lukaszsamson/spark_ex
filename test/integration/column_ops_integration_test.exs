defmodule SparkEx.Integration.ColumnOpsIntegrationTest do
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

  test "string operators filter correctly", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('Alice'), ('Bob'), ('ALPHA') AS t(name)"
      )

    starts = DataFrame.filter(df, Column.starts_with(Functions.col("name"), "A"))
    ends = DataFrame.filter(df, Column.ends_with(Functions.col("name"), "e"))
    like = DataFrame.filter(df, Column.like(Functions.col("name"), Functions.lit("A%")))
    ilike = DataFrame.filter(df, Column.ilike(Functions.col("name"), Functions.lit("a%")))

    assert {:ok, starts_rows} = DataFrame.collect(starts)
    assert Enum.any?(starts_rows, &(&1["name"] == "Alice"))

    assert {:ok, ends_rows} = DataFrame.collect(ends)
    assert Enum.any?(ends_rows, &(&1["name"] == "Alice"))

    assert {:ok, like_rows} = DataFrame.collect(like)
    assert Enum.map(like_rows, & &1["name"]) |> Enum.sort() == ["ALPHA", "Alice"]

    assert {:ok, ilike_rows} = DataFrame.collect(ilike)
    assert Enum.map(ilike_rows, & &1["name"]) |> Enum.sort() == ["ALPHA", "Alice"]
  end

  test "bitwise operators compute expected results", %{session: session} do
    df = SparkEx.sql(session, "SELECT 6 AS a, 3 AS b")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Column.bitwise_and(Functions.col("a"), Functions.col("b")), "and"),
        SparkEx.Column.alias_(Column.bitwise_or(Functions.col("a"), Functions.col("b")), "or"),
        SparkEx.Column.alias_(Column.bitwise_xor(Functions.col("a"), Functions.col("b")), "xor"),
        SparkEx.Column.alias_(Column.bitwise_not(Functions.col("b")), "not_b")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["and"] == 2
    assert row["or"] == 7
    assert row["xor"] == 5
    assert is_integer(row["not_b"])
  end

  test "string/bitwise edge cases", %{session: session} do
    df = SparkEx.sql(session, "SELECT 'Abc123' AS text, 0 AS num")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Column.contains(Functions.col("text"), "123"), "has_digits"),
        SparkEx.Column.alias_(
          Column.rlike(Functions.col("text"), Functions.lit("[A-Z]")),
          "has_upper"
        ),
        SparkEx.Column.alias_(Column.bitwise_not(Functions.col("num")), "not_zero")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["has_digits"] == true
    assert row["has_upper"] == true
    assert row["not_zero"] == -1
  end
end
