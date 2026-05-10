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
    assert date_to_iso8601(row["dt_plus"]) == "2024-01-02"
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

  test "additional map/generator functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT map('a', 1, 'b', 2) AS mp")

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Functions.map_entries(Functions.col("mp")), "entries"),
        SparkEx.Column.alias_(Functions.map_values(Functions.col("mp")), "values")
      ])

    assert {:ok, [row]} = DataFrame.collect(projected)
    assert is_list(row["entries"])
    assert Enum.any?(row["entries"], &is_map/1)
    assert Enum.sort(row["values"]) == [1, 2]

    tvf = SparkEx.tvf(session)

    posed =
      SparkEx.TableValuedFunction.call(tvf, "posexplode", [
        Functions.array([Functions.lit(1), Functions.lit(2)])
      ])

    assert {:ok, rows} = DataFrame.collect(posed)
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

    inline =
      SparkEx.TableValuedFunction.inline(
        tvf,
        Functions.array([
          Functions.struct([Functions.lit(1)]),
          Functions.struct([Functions.lit(2)])
        ])
      )

    stacked =
      SparkEx.TableValuedFunction.stack(
        tvf,
        2,
        [Functions.lit(1), Functions.lit("a"), Functions.lit(2), Functions.lit("b")]
      )

    assert {:ok, inline_rows} = DataFrame.collect(inline)
    assert length(inline_rows) == 2

    assert {:ok, stacked_rows} = DataFrame.collect(stacked)
    assert length(stacked_rows) == 2
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

  test "additional window functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'a') AS t(id, grp)")

    spec = Window.partition_by(["grp"]) |> WindowSpec.order_by(["id"])

    frame_spec =
      Window.partition_by(["grp"])
      |> WindowSpec.order_by(["id"])
      |> WindowSpec.rows_between(-1, 0)

    projected =
      DataFrame.select(df, [
        Functions.col("id"),
        SparkEx.Column.alias_(Column.over(Functions.rank(), spec), "rank"),
        SparkEx.Column.alias_(Column.over(Functions.dense_rank(), spec), "dense_rank"),
        SparkEx.Column.alias_(
          Column.over(Functions.lag(Functions.col("id"), offset: 1), spec),
          "lag_id"
        ),
        SparkEx.Column.alias_(Column.over(Functions.percent_rank(), spec), "percent_rank"),
        SparkEx.Column.alias_(
          Column.over(Functions.sum(Functions.col("id")), frame_spec),
          "sum_id"
        )
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert Enum.all?(rows, &is_integer(&1["rank"]))
    assert Enum.all?(rows, &is_integer(&1["dense_rank"]))
    assert Enum.all?(rows, &is_float(&1["percent_rank"]))
    assert Enum.all?(rows, &is_integer(&1["sum_id"]))
  end

  test "percentile and cume_dist functions execute", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'a') AS t(id, grp)")

    spec = Window.partition_by(["grp"]) |> WindowSpec.order_by(["id"])

    projected =
      DataFrame.select(df, [
        SparkEx.Column.alias_(Column.over(Functions.cume_dist(), spec), "cume_dist"),
        SparkEx.Column.alias_(Column.over(Functions.ntile(2), spec), "ntile")
      ])

    assert {:ok, rows} = DataFrame.collect(projected)
    assert Enum.all?(rows, &is_float(&1["cume_dist"]))
    assert Enum.all?(rows, &is_integer(&1["ntile"]))

    agg =
      df
      |> DataFrame.group_by(["grp"])
      |> SparkEx.GroupedData.agg([
        SparkEx.Column.alias_(Functions.percentile_approx(Functions.col("id"), 0.5, 100), "p50")
      ])

    assert {:ok, [row]} = DataFrame.collect(agg)
    values = Map.values(row)
    assert Enum.any?(values, &is_number/1)
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

  defp date_to_iso8601(%Date{} = d), do: Date.to_iso8601(d)
  defp date_to_iso8601(v) when is_binary(v), do: v
end
