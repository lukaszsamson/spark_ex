defmodule SparkEx.Integration.M11WindowHofTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.Window
  alias SparkEx.WindowSpec

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp window_df(session) do
    SparkEx.sql(
      session,
      "SELECT * FROM VALUES (1, 10), (1, 20), (2, 30) AS t(dept, salary)"
    )
  end

  defp frame_df(session) do
    SparkEx.sql(session, "SELECT * FROM VALUES (1, 10), (1, 20), (1, 30) AS t(dept, salary)")
  end

  defp hof_df(session) do
    SparkEx.sql(
      session,
      "SELECT array(1, 2, 3) AS arr, array(10, 20, 30) AS arr2, map(1, 10, 2, 20) AS m1, map(1, 5, 2, 7) AS m2"
    )
  end

  describe "window functions" do
    test "row_number with partition and order", %{session: session} do
      window = Window.partition_by(["dept"]) |> WindowSpec.order_by(["salary"])

      df =
        window_df(session)
        |> DataFrame.select([
          Functions.col("dept"),
          Functions.col("salary"),
          Column.alias_(Column.over(Functions.row_number(), window), "rn")
        ])
        |> DataFrame.order_by(["dept", "salary"])

      assert {:ok, rows} = DataFrame.collect(df)

      assert Enum.map(rows, &{&1["dept"], &1["salary"], &1["rn"]}) == [
               {1, 10, 1},
               {1, 20, 2},
               {2, 30, 1}
             ]
    end

    test "lag and lead respect offsets and defaults", %{session: session} do
      window = Window.partition_by(["dept"]) |> WindowSpec.order_by(["salary"])

      df =
        window_df(session)
        |> DataFrame.select([
          Functions.col("dept"),
          Functions.col("salary"),
          Column.alias_(
            Column.over(Functions.lag("salary", offset: 1, default: 0), window),
            "prev"
          ),
          Column.alias_(
            Column.over(Functions.lead("salary", offset: 1, default: 0), window),
            "next"
          )
        ])
        |> DataFrame.order_by(["dept", "salary"])

      assert {:ok, rows} = DataFrame.collect(df)

      assert Enum.map(rows, &{&1["dept"], &1["salary"], &1["prev"], &1["next"]}) == [
               {1, 10, 0, 20},
               {1, 20, 10, 0},
               {2, 30, 0, 0}
             ]
    end

    test "rows_between and range_between frame specs execute", %{session: session} do
      rows_window =
        Window.partition_by(["dept"])
        |> WindowSpec.order_by(["salary"])
        |> WindowSpec.rows_between(-1, 1)

      range_window =
        Window.partition_by(["dept"])
        |> WindowSpec.order_by(["salary"])
        |> WindowSpec.range_between(:unbounded, :current_row)

      df =
        frame_df(session)
        |> DataFrame.select([
          Functions.col("salary"),
          Column.alias_(Column.over(Functions.sum("salary"), rows_window), "sum_rows"),
          Column.alias_(Column.over(Functions.sum("salary"), range_window), "sum_range")
        ])
        |> DataFrame.order_by(["salary"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["sum_rows"]) == [30, 60, 50]
      assert Enum.map(rows, & &1["sum_range"]) == [10, 30, 60]
    end
  end

  describe "higher-order functions" do
    test "array lambdas execute", %{session: session} do
      df =
        hof_df(session)
        |> DataFrame.select([
          Column.alias_(
            Functions.element_at(
              Functions.transform("arr", fn x -> Column.plus(x, Functions.lit(1)) end),
              Functions.lit(1)
            ),
            "first_plus"
          ),
          Column.alias_(
            Functions.array_size(
              Functions.filter("arr", fn x -> Column.gt(x, Functions.lit(1)) end)
            ),
            "filtered_size"
          ),
          Column.alias_(
            Functions.exists("arr", fn x -> Column.gt(x, Functions.lit(2)) end),
            "any_gt2"
          ),
          Column.alias_(
            Functions.forall("arr", fn x -> Column.gt(x, Functions.lit(0)) end),
            "all_gt0"
          ),
          Column.alias_(
            Functions.aggregate("arr", 0, fn acc, x -> Column.plus(acc, x) end),
            "sum"
          ),
          Column.alias_(
            Functions.reduce("arr", 0, fn acc, x -> Column.plus(acc, x) end),
            "sum_reduce"
          )
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["first_plus"] == 2
      assert row["filtered_size"] == 2
      assert row["any_gt2"] == true
      assert row["all_gt0"] == true
      assert row["sum"] == 6
      assert row["sum_reduce"] == 6
    end

    test "map and zip lambdas execute", %{session: session} do
      df =
        hof_df(session)
        |> DataFrame.select([
          Column.alias_(
            Functions.element_at(
              Functions.map_filter("m1", fn _k, v -> Column.gt(v, Functions.lit(10)) end),
              Functions.lit(2)
            ),
            "filtered_value"
          ),
          Column.alias_(
            Functions.element_at(
              Functions.map_zip_with("m1", "m2", fn _k, v1, v2 -> Column.plus(v1, v2) end),
              Functions.lit(1)
            ),
            "zip_value"
          ),
          Column.alias_(
            Functions.element_at(
              Functions.transform_keys("m1", fn k, _v -> Column.plus(k, Functions.lit(1)) end),
              Functions.lit(2)
            ),
            "transformed_key_value"
          ),
          Column.alias_(
            Functions.element_at(
              Functions.transform_values("m1", fn _k, v -> Column.plus(v, Functions.lit(1)) end),
              Functions.lit(1)
            ),
            "transformed_value"
          ),
          Column.alias_(
            Functions.element_at(
              Functions.zip_with("arr", "arr2", fn x, y -> Column.plus(x, y) end),
              Functions.lit(1)
            ),
            "zip_first"
          )
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["filtered_value"] == 20
      assert row["zip_value"] == 15
      assert row["transformed_key_value"] == 10
      assert row["transformed_value"] == 11
      assert row["zip_first"] == 11
    end
  end
end
