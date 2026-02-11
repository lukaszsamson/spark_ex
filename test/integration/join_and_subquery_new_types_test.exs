defmodule SparkEx.Integration.JoinAndSubqueryNewTypesTest do
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

  test "as_of_join executes", %{session: session} do
    left =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id_l, t1)"
      )

    right =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES (1, 9), (2, 19), (3, 29) AS t(id_r, t2)"
      )

    df =
      DataFrame.as_of_join(
        left,
        right,
        Functions.col("t1"),
        Functions.col("t2"),
        on: Column.eq(Functions.col("id_l"), Functions.col("id_r"))
      )
      |> DataFrame.select(["id_l", "t1", "t2"])
      |> DataFrame.order_by(["id_l"])

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.map(rows, & &1["id_l"]) == [1, 2, 3]
  end

  test "lateral_join executes", %{session: session} do
    left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(id_l)")
    right = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id_r)")

    df =
      DataFrame.lateral_join(
        left,
        right,
        Column.eq(Functions.col("id_l"), Functions.col("id_r"))
      )
      |> DataFrame.select(["id_l"])
      |> DataFrame.order_by(["id_l"])

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.map(rows, & &1["id_l"]) == [1, 2]
  end

  test "in_subquery executes", %{session: session} do
    subquery = SparkEx.sql(session, "SELECT * FROM VALUES (1), (3) AS t(id)")

    df =
      SparkEx.range(session, 5)
      |> DataFrame.filter(DataFrame.in_subquery(subquery, [Functions.col("id")]))
      |> DataFrame.order_by(["id"])

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.map(rows, & &1["id"]) == [1, 3]
  end
end
