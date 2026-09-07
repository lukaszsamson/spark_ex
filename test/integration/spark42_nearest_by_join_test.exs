defmodule SparkEx.Integration.Spark42NearestByJoinTest do
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

  defp fixtures(session) do
    left =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES (1, 10.0D), (2, 20.0D), (3, 30.0D) AS t(user_id, score)"
      )

    right =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('A', 11.0D), ('B', 22.0D), ('C', 5.0D) AS t(product, pscore)"
      )

    {left, right}
  end

  defp distance(left, right) do
    left
    |> DataFrame.col("score")
    |> Column.minus(DataFrame.col(right, "pscore"))
    |> Functions.abs()
  end

  test "exact distance returns per-left top K and similarity reverses ranking", %{
    session: session
  } do
    {left, right} = fixtures(session)
    ranking = distance(left, right)

    distance_rows =
      DataFrame.nearest_by_join(left, right, ranking, 2,
        mode: :exact,
        direction: :distance
      )
      |> DataFrame.select(["user_id", "product"])
      |> DataFrame.collect()

    assert {:ok, rows} = distance_rows

    assert Enum.sort(rows) ==
             Enum.sort([
               %{"user_id" => 1, "product" => "A"},
               %{"user_id" => 1, "product" => "C"},
               %{"user_id" => 2, "product" => "A"},
               %{"user_id" => 2, "product" => "B"},
               %{"user_id" => 3, "product" => "A"},
               %{"user_id" => 3, "product" => "B"}
             ])

    assert {:ok, [%{"product" => "B", "user_id" => 1}]} =
             DataFrame.nearest_by_join(left, right, ranking, 1,
               mode: :exact,
               direction: :similarity
             )
             |> DataFrame.filter(Column.eq(DataFrame.col(left, "user_id"), 1))
             |> DataFrame.select(["user_id", "product"])
             |> DataFrame.collect()
  end

  test "left outer retains rows for empty and null-ranked candidate sets", %{session: session} do
    {left, right} = fixtures(session)
    empty = DataFrame.filter(right, Functions.lit(false))

    assert {:ok, rows} =
             DataFrame.nearest_by_join(left, empty, distance(left, empty), 1,
               mode: :exact,
               direction: :distance,
               join_type: :left_outer
             )
             |> DataFrame.select(["user_id", "product"])
             |> DataFrame.collect()

    assert Enum.sort(rows) ==
             Enum.sort([
               %{"user_id" => 1, "product" => nil},
               %{"user_id" => 2, "product" => nil},
               %{"user_id" => 3, "product" => nil}
             ])

    null_right = SparkEx.sql(session, "SELECT CAST(NULL AS DOUBLE) pscore, 'N' product")

    assert {:ok, []} =
             DataFrame.nearest_by_join(left, null_right, distance(left, null_right), 1,
               mode: :exact,
               direction: :distance
             )
             |> DataFrame.filter(Column.eq(DataFrame.col(left, "user_id"), 1))
             |> DataFrame.select(["product"])
             |> DataFrame.collect()

    assert {:ok, [%{"product" => nil}]} =
             DataFrame.nearest_by_join(left, null_right, distance(left, null_right), 1,
               mode: :exact,
               direction: :distance,
               join_type: :left_outer
             )
             |> DataFrame.filter(Column.eq(DataFrame.col(left, "user_id"), 1))
             |> DataFrame.select(["product"])
             |> DataFrame.collect()
  end

  test "aliases and self joins keep both correlated references", %{session: session} do
    base = SparkEx.sql(session, "SELECT * FROM VALUES (1, 10.0D), (2, 12.0D) AS t(id, score)")
    left = DataFrame.alias_(base, "queries")
    right = DataFrame.alias_(base, "candidates")

    ranking =
      left
      |> DataFrame.col("queries.score")
      |> Column.minus(DataFrame.col(right, "candidates.score"))
      |> Functions.abs()

    assert {:ok, rows} =
             DataFrame.nearest_by_join(left, right, ranking, 1,
               mode: :exact,
               direction: :distance
             )
             |> DataFrame.select([
               Column.alias_(DataFrame.col(left, "queries.id"), "query_id"),
               Column.alias_(DataFrame.col(right, "candidates.id"), "candidate_id")
             ])
             |> DataFrame.collect()

    assert Enum.sort(rows) ==
             Enum.sort([
               %{"query_id" => 1, "candidate_id" => 1},
               %{"query_id" => 2, "candidate_id" => 2}
             ])
  end

  test "ties stay within K, approximate mode runs, and exact accepts nondeterminism", %{
    session: session
  } do
    left = SparkEx.sql(session, "SELECT 1 id")
    right = SparkEx.sql(session, "SELECT * FROM VALUES ('a'), ('b'), ('c') AS t(candidate)")

    tie =
      DataFrame.nearest_by_join(left, right, Functions.lit(1), 2,
        mode: :exact,
        direction: :distance
      )

    assert {:ok, 2} = DataFrame.count(tie)

    approx =
      DataFrame.nearest_by_join(left, right, Functions.lit(1), 1,
        mode: :approx,
        direction: :similarity
      )

    assert {:ok, 1} = DataFrame.count(approx)

    nondeterministic =
      DataFrame.nearest_by_join(left, right, Functions.rand(0), 1,
        mode: :exact,
        direction: :similarity
      )

    assert {:ok, 1} = DataFrame.count(nondeterministic)
  end

  test "vector distance is a correlated nearest-by ranking expression", %{session: session} do
    queries =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES (1, CAST(array(0.0, 0.0) AS ARRAY<FLOAT>)), " <>
          "(2, CAST(array(9.0, 9.0) AS ARRAY<FLOAT>)) AS t(query_id, embedding)"
      )
      |> DataFrame.alias_("queries")

    candidates =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('near_zero', CAST(array(1.0, 1.0) AS ARRAY<FLOAT>)), " <>
          "('near_nine', CAST(array(8.0, 8.0) AS ARRAY<FLOAT>)) AS t(candidate, embedding)"
      )
      |> DataFrame.alias_("candidates")

    ranking =
      Functions.vector_l2_distance(
        DataFrame.col(queries, "queries.embedding"),
        DataFrame.col(candidates, "candidates.embedding")
      )

    assert {:ok, rows} =
             DataFrame.nearest_by_join(queries, candidates, ranking, 1,
               mode: :exact,
               direction: :distance
             )
             |> DataFrame.select(["query_id", "candidate"])
             |> DataFrame.collect()

    assert Enum.sort(rows) ==
             Enum.sort([
               %{"query_id" => 1, "candidate" => "near_zero"},
               %{"query_id" => 2, "candidate" => "near_nine"}
             ])
  end
end
