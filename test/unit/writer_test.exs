defmodule SparkEx.Unit.WriterTest do
  use ExUnit.Case, async: true

  alias SparkEx.Writer
  alias SparkEx.DataFrame

  defmodule FakeSession do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {:ok, %{parent: Keyword.fetch!(opts, :parent)}}
    end

    @impl true
    def handle_call({:execute_command, command, exec_opts}, _from, state) do
      send(state.parent, {:execute_command, command, exec_opts})
      {:reply, :ok, state}
    end
  end

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

  describe "format helpers" do
    test "avro/3 creates writer with avro format", %{df: df} do
      writer = %Writer{df: df} |> Writer.format("avro")
      assert writer.source == "avro"
    end

    test "xml/3 creates writer with xml format", %{df: df} do
      writer = %Writer{df: df} |> Writer.format("xml")
      assert writer.source == "xml"
    end

    test "jdbc/4 creates writer with jdbc format", %{df: df} do
      writer = %Writer{df: df} |> Writer.format("jdbc")
      assert writer.source == "jdbc"
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

  describe "convenience option handling" do
    test "parquet merges top-level and nested options into sink options" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = %DataFrame{session: session, plan: {:sql, "SELECT 1", nil}}

      assert :ok =
               Writer.parquet(df, "/tmp/out",
                 mode: :overwrite,
                 compression: "gzip",
                 options: %{"maxRecordsPerFile" => 10},
                 timeout: 1200
               )

      assert_receive {:execute_command, {:write_operation, _, write_opts}, exec_opts}
      assert Keyword.get(write_opts, :mode) == :overwrite
      assert Keyword.get(write_opts, :options)["compression"] == "gzip"
      assert Keyword.get(write_opts, :options)["maxRecordsPerFile"] == "10"
      assert exec_opts == [timeout: 1200]
    end

    test "csv keeps csv-specific options and includes extra top-level options" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = %DataFrame{session: session, plan: {:sql, "SELECT 1", nil}}

      assert :ok =
               Writer.csv(df, "/tmp/out",
                 header: true,
                 separator: "|",
                 quote_all: true,
                 options: %{"escape" => "\\"}
               )

      assert_receive {:execute_command, {:write_operation, _, write_opts}, _exec_opts}
      assert Keyword.get(write_opts, :options)["header"] == "true"
      assert Keyword.get(write_opts, :options)["sep"] == "|"
      assert Keyword.get(write_opts, :options)["quote_all"] == "true"
      assert Keyword.get(write_opts, :options)["escape"] == "\\"
    end
  end
end
