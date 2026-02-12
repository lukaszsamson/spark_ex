defmodule SparkEx.Integration.ComplexTypesTest do
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

  describe "get_item/get_field" do
    test "extracts array, map, and struct values", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT array(10, 20) AS arr, map('a', 1, 'b', 2) AS mp, named_struct('x', 5, 'y', 'z') AS st"
        )

      projected =
        DataFrame.select(df, [
          Column.alias_(Column.get_item(Functions.col("arr"), 0), "first"),
          Column.alias_(Column.get_item(Functions.col("mp"), "b"), "map_b"),
          Column.alias_(Column.get_field(Functions.col("st"), "y"), "struct_y")
        ])

      assert {:ok, [row]} = DataFrame.collect(projected)
      assert row["first"] == 10
      assert row["map_b"] == 2
      assert row["struct_y"] == "z"
    end

    test "extracts nested values from array -> struct -> map", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT array(named_struct('k', 1, 'v', map('a', 2))) AS arr"
        )

      nested =
        Functions.col("arr")
        |> Column.get_item(0)
        |> Column.get_field("v")
        |> Column.get_item("a")

      projected = DataFrame.select(df, [Column.alias_(nested, "value")])

      assert {:ok, [row]} = DataFrame.collect(projected)
      assert row["value"] == 2
    end
  end
end
