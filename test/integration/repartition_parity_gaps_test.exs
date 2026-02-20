defmodule SparkEx.Integration.RepartitionParityGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Functions}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── repartitionByRange ──

  describe "repartitionByRange behavior" do
    test "repartition_by_range with explicit partition count", %{session: session} do
      df =
        SparkEx.range(session, 100)
        |> DataFrame.repartition_by_range(4, ["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 100
    end

    test "repartition_by_range without partition count", %{session: session} do
      df =
        SparkEx.range(session, 50)
        |> DataFrame.repartition_by_range(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 50
    end

    test "repartition_by_range with multiple columns", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd') AS t(id, name)"
        )
        |> DataFrame.repartition_by_range(3, ["id", "name"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 4
    end

    test "repartition_by_range preserves data", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.repartition_by_range(5, ["id"])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == Enum.to_list(0..19)
    end
  end

  # ── repartition with expressions ──

  describe "repartition with column expressions" do
    test "repartition by computed expression", %{session: session} do
      df =
        SparkEx.range(session, 50)
        |> DataFrame.repartition(4, [Functions.expr("id % 10")])

      assert {:ok, 50} = DataFrame.count(df)
    end

    test "repartition with single column", %{session: session} do
      df =
        SparkEx.range(session, 30)
        |> DataFrame.repartition(3, [Functions.col("id")])

      assert {:ok, 30} = DataFrame.count(df)
    end
  end

  # ── repartition_by_id ──

  describe "repartition_by_id" do
    test "repartition_by_id with valid column", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (0, 'a'), (1, 'b'), (2, 'c') AS t(part_id, value)"
        )
        |> DataFrame.repartition_by_id("part_id")

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
    end

    test "repartition_by_id preserves row values", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (0, 'x'), (1, 'y'), (2, 'z') AS t(part_id, val)"
        )
        |> DataFrame.repartition_by_id("part_id")
        |> DataFrame.order_by(["part_id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["val"]) == ["x", "y", "z"]
    end
  end

  # ── coalesce ──

  describe "coalesce" do
    test "coalesce to 1 partition", %{session: session} do
      df =
        SparkEx.range(session, 100)
        |> DataFrame.repartition(10)
        |> DataFrame.coalesce(1)

      assert {:ok, 100} = DataFrame.count(df)
    end

    test "coalesce preserves data order after sort", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.repartition(5)
        |> DataFrame.coalesce(2)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == Enum.to_list(0..19)
    end
  end

  # ── repartition edge cases ──

  describe "repartition edge cases" do
    test "repartition to 1 partition", %{session: session} do
      df =
        SparkEx.range(session, 50)
        |> DataFrame.repartition(1)

      assert {:ok, 50} = DataFrame.count(df)
    end

    test "repartition to many partitions", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.repartition(100)

      assert {:ok, 10} = DataFrame.count(df)
    end

    test "repartition empty DataFrame", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 1 AS id WHERE 1 = 0")
        |> DataFrame.repartition(4)

      assert {:ok, []} = DataFrame.collect(df)
    end

    test "chained repartition operations", %{session: session} do
      df =
        SparkEx.range(session, 30)
        |> DataFrame.repartition(10)
        |> DataFrame.repartition(3)
        |> DataFrame.repartition(7)

      assert {:ok, 30} = DataFrame.count(df)
    end
  end
end
