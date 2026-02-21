defmodule SparkEx.Unit.WriterV2Test do
  use ExUnit.Case, async: true

  alias SparkEx.WriterV2
  alias SparkEx.DataFrame
  alias SparkEx.Functions

  setup do
    df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
    writer = %WriterV2{df: df, table_name: "my_table"}
    %{df: df, writer: writer}
  end

  describe "builder methods" do
    test "using/2 sets provider", %{writer: writer} do
      w = WriterV2.using(writer, "parquet")
      assert w.provider == "parquet"
    end

    test "option/3 adds a single option", %{writer: writer} do
      w = WriterV2.option(writer, "key", "value")
      assert w.options == %{"key" => "value"}
    end

    test "option/3 stringifies primitive values", %{writer: writer} do
      w =
        writer
        |> WriterV2.option("enabled", true)
        |> WriterV2.option("numBuckets", 8)
        |> WriterV2.option("samplingRatio", 0.25)

      assert w.options == %{
               "enabled" => "true",
               "numBuckets" => "8",
               "samplingRatio" => "0.25"
             }
    end

    test "option/3 rejects non-primitive values", %{writer: writer} do
      assert_raise ArgumentError, ~r/option value must be a primitive/, fn ->
        WriterV2.option(writer, "bad", %{a: 1})
      end
    end

    test "options/2 merges options", %{writer: writer} do
      w =
        writer
        |> WriterV2.option("a", "1")
        |> WriterV2.options(%{"b" => "2"})

      assert w.options == %{"a" => "1", "b" => "2"}
    end

    test "table_property/3 adds a single property", %{writer: writer} do
      w = WriterV2.table_property(writer, "description", "My table")
      assert w.table_properties == %{"description" => "My table"}
    end

    test "table_properties/2 accepts keyword options", %{writer: writer} do
      w = WriterV2.table_properties(writer, retention_days: 7, audited: true)
      assert w.table_properties == %{"retention_days" => "7", "audited" => "true"}
    end

    test "table_property/3 rejects non-primitive values", %{writer: writer} do
      assert_raise ArgumentError, ~r/option value must be a primitive/, fn ->
        WriterV2.table_property(writer, "bad", [:list])
      end
    end

    test "table_properties/2 merges properties", %{writer: writer} do
      w =
        writer
        |> WriterV2.table_property("a", "1")
        |> WriterV2.table_properties(%{"b" => "2"})

      assert w.table_properties == %{"a" => "1", "b" => "2"}
    end

    test "cluster_by/2 sets clustering columns", %{writer: writer} do
      w = WriterV2.cluster_by(writer, ["region", "date"])
      assert w.cluster_by == ["region", "date"]
    end

    test "partitioned_by/2 sets partitioning expressions", %{writer: writer} do
      w = WriterV2.partitioned_by(writer, ["year", "month"])
      assert w.partitioned_by == [{:col, "year"}, {:col, "month"}]
    end

    test "partitioned_by/2 accepts partitioning helper expressions", %{writer: writer} do
      bucket = Functions.bucket(4, "id")
      years = Functions.years("interval_col")
      months = Functions.months("interval_col")
      days = Functions.days("interval_col")
      hours = Functions.hours("interval_col")

      w = WriterV2.partitioned_by(writer, [bucket, years, months, days, hours])

      assert w.partitioned_by == [
               {:fn, "bucket", [{:lit, 4}, {:col, "id"}], false},
               {:fn, "years", [{:col, "interval_col"}], false},
               {:fn, "months", [{:col, "interval_col"}], false},
               {:fn, "days", [{:col, "interval_col"}], false},
               {:fn, "hours", [{:col, "interval_col"}], false}
             ]
    end

    test "chaining builders", %{writer: writer} do
      w =
        writer
        |> WriterV2.using("delta")
        |> WriterV2.option("mergeSchema", "true")
        |> WriterV2.table_property("description", "My delta table")
        |> WriterV2.cluster_by(["region"])

      assert w.provider == "delta"
      assert w.options == %{"mergeSchema" => "true"}
      assert w.table_properties == %{"description" => "My delta table"}
      assert w.cluster_by == ["region"]
    end
  end

  describe "DataFrame.write_v2/2" do
    test "returns a WriterV2 struct", %{df: df} do
      w = DataFrame.write_v2(df, "catalog.db.table")
      assert %WriterV2{} = w
      assert w.df == df
      assert w.table_name == "catalog.db.table"
      assert w.options == %{}
      assert w.table_properties == %{}
    end
  end

  describe "createOrReplace/2" do
    defmodule CreateOrReplaceSession do
      use GenServer

      def start_link(test_pid) do
        GenServer.start_link(__MODULE__, test_pid, [])
      end

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_call(
            {:execute_command, {:write_operation_v2, _plan, table, v2_opts}, _opts},
            _from,
            test_pid
          ) do
        send(test_pid, {:create_or_replace_called, table, v2_opts})
        {:reply, :ok, test_pid}
      end
    end

    test "delegates to create_or_replace" do
      {:ok, session} = CreateOrReplaceSession.start_link(self())
      df = %DataFrame{session: session, plan: {:sql, "SELECT 1", nil}}

      writer =
        df
        |> DataFrame.write_v2("my_table")
        |> WriterV2.using("parquet")

      assert :ok = WriterV2.createOrReplace(writer)
      assert_receive {:create_or_replace_called, "my_table", v2_opts}
      assert Keyword.get(v2_opts, :mode) == :create_or_replace
    end
  end
end
