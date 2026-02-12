defmodule SparkEx.Integration.FunctionCategoryParityTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Functions, Window, WindowSpec}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "math/string/date functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT 9 AS num, 'Hello' AS text, DATE '2024-01-01' AS dt")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Functions.sqrt(Functions.col("num")), "sqrt"),
        SparkEx.Column.alias_(Functions.log10(Functions.col("num")), "log10"),
        SparkEx.Column.alias_(Functions.upper(Functions.col("text")), "upper"),
        SparkEx.Column.alias_(Functions.length(Functions.col("text")), "len"),
        SparkEx.Column.alias_(Functions.date_add(Functions.col("dt"), 1), "dt_plus")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["sqrt"] == 3.0
    assert row["upper"] == "HELLO"
    assert row["len"] == 5
    assert row["dt_plus"] != nil
  end

  test "collection/map functions execute", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT array(1, 2, 3) AS arr, map('a', 1, 'b', 2) AS mp"
      )

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(
          Functions.array_contains(Functions.col("arr"), Functions.lit(2)),
          "has_two"
        ),
        SparkEx.Column.alias_(Functions.size(Functions.col("arr")), "arr_size"),
        SparkEx.Column.alias_(Functions.map_keys(Functions.col("mp")), "keys"),
        SparkEx.Column.alias_(Functions.map_values(Functions.col("mp")), "vals")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["has_two"] == true
    assert row["arr_size"] == 3
    assert is_list(row["keys"])
    assert is_list(row["vals"])
  end

  test "higher-order collection functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT array(1, 2, 3) AS arr")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(
          Functions.transform(Functions.col("arr"), fn x -> Column.plus(x, Functions.lit(1)) end),
          "plus_one"
        ),
        SparkEx.Column.alias_(
          Functions.filter(Functions.col("arr"), fn x -> Column.gt(x, Functions.lit(1)) end),
          "filtered"
        )
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert row["plus_one"] == [2, 3, 4]
    assert row["filtered"] == [2, 3]
  end

  test "generator/map functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT explode(array(1, 2)) AS v")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(
          Functions.map_from_arrays(
            Functions.array([Functions.lit(1)]),
            Functions.array([Functions.lit(2)])
          ),
          "mp"
        )
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert length(rows) == 2
  end

  test "tvf explode/inline execute", %{session: session} do
    tvf = SparkEx.tvf(session)

    exploded =
      SparkEx.TableValuedFunction.explode(
        tvf,
        Functions.array([Functions.lit(1), Functions.lit(2)])
      )

    assert {:ok, rows} = DataFrame.collect(exploded)
    assert length(rows) == 2
  end

  test "window functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")

    spec =
      Window.partition_by(["val"])
      |> WindowSpec.order_by(["id"])

    projected =
      DataFrame.select(df, [
        Functions.col("id"),
        SparkEx.Column.alias_(Column.over(Functions.row_number(), spec), "row_num")
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert Enum.all?(rows, &is_integer(&1["row_num"]))
  end

  test "misc/hash/type functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT 'abc' AS text, 1 AS num")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Functions.sha2(Functions.col("text"), 256), "sha"),
        SparkEx.Column.alias_(Functions.md5(Functions.col("text")), "md5"),
        SparkEx.Column.alias_(Functions.bit_count(Functions.col("num")), "bits"),
        SparkEx.Column.alias_(Functions.upper(Functions.lit("x")), "upper_x")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert is_binary(row["sha"])
    assert is_binary(row["md5"])
    assert row["bits"] == 1
    assert row["upper_x"] == "X"
  end
end
