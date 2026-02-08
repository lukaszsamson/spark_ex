defmodule SparkEx.Unit.WriterTest do
  use ExUnit.Case, async: true

  alias SparkEx.Writer
  alias SparkEx.DataFrame

  setup do
    df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
    writer = %Writer{df: df}
    %{df: df, writer: writer}
  end

  describe "builder methods" do
    test "format/2 sets source", %{writer: writer} do
      w = Writer.format(writer, "parquet")
      assert w.source == "parquet"
    end

    test "mode/2 sets save mode", %{writer: writer} do
      for m <- [:append, :overwrite, :error_if_exists, :ignore] do
        w = Writer.mode(writer, m)
        assert w.mode == m
      end
    end

    test "option/3 adds a single option", %{writer: writer} do
      w = writer |> Writer.option("compression", "snappy")
      assert w.options == %{"compression" => "snappy"}
    end

    test "option/3 stringifies primitive values", %{writer: writer} do
      w =
        writer
        |> Writer.option("header", true)
        |> Writer.option("maxColumns", 100)
        |> Writer.option("samplingRatio", 0.5)

      assert w.options == %{
               "header" => "true",
               "maxColumns" => "100",
               "samplingRatio" => "0.5"
             }
    end

    test "options/2 merges options", %{writer: writer} do
      w =
        writer
        |> Writer.option("a", "1")
        |> Writer.options(%{"b" => "2", "c" => "3"})

      assert w.options == %{"a" => "1", "b" => "2", "c" => "3"}
    end

    test "options/2 accepts keyword options", %{writer: writer} do
      w = Writer.options(writer, header: true, maxColumns: 50)
      assert w.options == %{"header" => "true", "maxColumns" => "50"}
    end

    test "partition_by/2 sets partitioning columns", %{writer: writer} do
      w = Writer.partition_by(writer, ["year", "month"])
      assert w.partition_by == ["year", "month"]
    end

    test "sort_by/2 sets sort columns", %{writer: writer} do
      w = Writer.sort_by(writer, ["id", "name"])
      assert w.sort_by == ["id", "name"]
    end

    test "bucket_by/3 sets bucketing", %{writer: writer} do
      w = Writer.bucket_by(writer, 10, ["id"])
      assert w.bucket_by == {10, ["id"]}
    end

    test "cluster_by/2 sets clustering columns", %{writer: writer} do
      w = Writer.cluster_by(writer, ["region"])
      assert w.cluster_by == ["region"]
    end

    test "chaining builders", %{writer: writer} do
      w =
        writer
        |> Writer.format("parquet")
        |> Writer.mode(:overwrite)
        |> Writer.option("compression", "snappy")
        |> Writer.partition_by(["date"])
        |> Writer.sort_by(["id"])
        |> Writer.cluster_by(["region"])

      assert w.source == "parquet"
      assert w.mode == :overwrite
      assert w.options == %{"compression" => "snappy"}
      assert w.partition_by == ["date"]
      assert w.sort_by == ["id"]
      assert w.cluster_by == ["region"]
    end
  end

  describe "DataFrame.write/1" do
    test "returns a Writer struct", %{df: df} do
      w = DataFrame.write(df)
      assert %Writer{} = w
      assert w.df == df
      assert w.mode == :error_if_exists
      assert w.options == %{}
    end
  end
end
