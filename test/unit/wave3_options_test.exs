defmodule SparkEx.Unit.Wave3OptionsTest do
  @moduledoc """
  Wave-3 option-handling fixes: T-22, T-23, T-50, T-51, T-52, T-53 and the
  GROK-7 half of T-38 (atom keys accepted by the singular `option/3`).
  """
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.Internal.OptionUtils
  alias SparkEx.Reader
  alias SparkEx.StreamReader
  alias SparkEx.StreamWriter
  alias SparkEx.Writer
  alias SparkEx.WriterV2

  defmodule FakeSession do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts), do: {:ok, %{parent: Keyword.fetch!(opts, :parent)}}

    @impl true
    def handle_call({:execute_command, command, exec_opts}, _from, state) do
      send(state.parent, {:execute_command, command, exec_opts})
      {:reply, :ok, state}
    end
  end

  defp fake_df do
    {:ok, session} = FakeSession.start_link(parent: self())
    DataFrame.new(session, {:sql, "SELECT 1", nil})
  end

  defp written_options do
    assert_receive {:execute_command, {:write_operation, _plan, write_opts}, _exec}
    Keyword.fetch!(write_opts, :options)
  end

  # ── T-23: snake_case atom keys become camelCase ──

  describe "T-23 option key normalization" do
    test "camelizes snake_case atom keys" do
      assert OptionUtils.normalize_option_key(:multi_line) == "multiLine"
      assert OptionUtils.normalize_option_key(:infer_schema) == "inferSchema"
      assert OptionUtils.normalize_option_key(:date_format) == "dateFormat"
      assert OptionUtils.normalize_option_key(:path_glob_filter) == "pathGlobFilter"
      assert OptionUtils.normalize_option_key(:header) == "header"
    end

    test "leaves case-sensitive / dotted / underscore-prefixed atom keys alone" do
      assert OptionUtils.normalize_option_key(:"oracle.net.CONNECT_TIMEOUT") ==
               "oracle.net.CONNECT_TIMEOUT"

      assert OptionUtils.normalize_option_key(:CLIENT_SESSION_KEEP_ALIVE) ==
               "CLIENT_SESSION_KEEP_ALIVE"

      assert OptionUtils.normalize_option_key(:"kafka.group_id") == "kafka.group_id"
      assert OptionUtils.normalize_option_key(:_private) == "_private"
      assert OptionUtils.normalize_option_key(:trailing_) == "trailing_"
      assert OptionUtils.normalize_option_key(:"multi-line") == "multi-line"
    end

    test "leaves string keys untouched" do
      assert OptionUtils.normalize_option_key("multi_line") == "multi_line"
      assert OptionUtils.normalize_option_key("multiLine") == "multiLine"
    end

    test "reader sends camelCase names on the wire" do
      df = Reader.json(self(), "/data/f.json", multi_line: true, path_glob_filter: "*.json")

      assert {:read_data_source, "json", _, nil,
              %{"multiLine" => "true", "pathGlobFilter" => "*.json"}} = unwrap_plan(df)
    end

    test "reader honours verbatim string keys inside :options" do
      df = Reader.json(self(), "/data/f.json", options: %{"multi_line" => "true"})
      assert {:read_data_source, "json", _, nil, %{"multi_line" => "true"}} = unwrap_plan(df)
    end

    test "writer sends camelCase names on the wire" do
      assert :ok = Writer.json(fake_df(), "/tmp/out", ignore_null_fields: true)
      assert written_options()["ignoreNullFields"] == "true"
    end

    test "writer_v2 camelizes atom option keys but not table properties" do
      writer = %WriterV2{df: nil, table_name: "t"}
      assert WriterV2.options(writer, write_mode: "x").options == %{"writeMode" => "x"}

      assert WriterV2.table_properties(writer, transactional_properties: "insert_only").table_properties ==
               %{"transactional_properties" => "insert_only"}
    end

    test "stream reader/writer camelize atom option keys" do
      df = StreamReader.json(:s, "/data/stream", multi_line: true)

      assert {:read_data_source_streaming, "json", _, nil, %{"multiLine" => "true"}} =
               unwrap_plan(df)

      writer = StreamWriter.options(%StreamWriter{df: nil}, checkpoint_location: "/ckpt")
      assert writer.options == %{"checkpointLocation" => "/ckpt"}
    end
  end

  # ── T-22: keyword-list :options and sep/separator ──

  describe "T-22 keyword-list :options" do
    test "Reader.csv accepts a keyword list for :options" do
      df = Reader.csv(self(), "/data/f.csv", options: [multi_line: true, mode: "PERMISSIVE"])

      assert {:read_data_source, "csv", _, nil, %{"multiLine" => "true", "mode" => "PERMISSIVE"}} =
               unwrap_plan(df)
    end

    test "Reader.parquet accepts a keyword list for :options" do
      df = Reader.parquet(self(), "/data/f.parquet", options: [merge_schema: true])
      assert {:read_data_source, "parquet", _, nil, %{"mergeSchema" => "true"}} = unwrap_plan(df)
    end

    test "StreamReader.rate accepts a keyword list for :options" do
      df = StreamReader.rate(:s, options: [rows_per_second: 10])
      assert {:read_data_source_streaming, "rate", [], nil, opts} = unwrap_plan(df)
      assert opts["rowsPerSecond"] == "10"
    end

    test "StreamReader.rate still maps top-level shortcuts" do
      df = StreamReader.rate(:s, rows_per_second: 5, num_partitions: 2, ramp_up_time: 1)
      assert {:read_data_source_streaming, "rate", [], nil, opts} = unwrap_plan(df)

      assert opts == %{"rowsPerSecond" => "5", "numPartitions" => "2", "rampUpTime" => "1"}
    end

    test "Reader.csv accepts agreeing :sep and :separator" do
      df = Reader.csv(self(), "/data/f.csv", sep: ";", separator: ";")
      assert {:read_data_source, "csv", _, nil, %{"sep" => ";"}} = unwrap_plan(df)
    end

    test "Reader.csv rejects disagreeing :sep and :separator" do
      assert_raise ArgumentError, ~r/conflicting :sep and :separator/, fn ->
        Reader.csv(self(), "/data/f.csv", sep: ";", separator: ",")
      end
    end

    test "Reader.csv still rejects a convenience option duplicated in :options" do
      assert_raise ArgumentError, ~r/multiple values for keyword argument/, fn ->
        Reader.csv(self(), "/data/f.csv", header: true, options: %{"header" => "false"})
      end
    end
  end

  describe "review follow-ups" do
    test "jdbc connection properties reach the driver verbatim" do
      df =
        Reader.jdbc(self(), "jdbc:oracle:thin:@h", "t",
          properties: %{
            "oracle.net.CONNECT_TIMEOUT" => "5000",
            "CLIENT_SESSION_KEEP_ALIVE" => "true"
          }
        )

      assert {:read_data_source, "jdbc", [], nil, opts} = unwrap_plan(df)
      assert opts["oracle.net.CONNECT_TIMEOUT"] == "5000"
      assert opts["CLIENT_SESSION_KEEP_ALIVE"] == "true"
    end

    test "jdbc properties given as atoms are not camelized" do
      df = Reader.jdbc(self(), "jdbc:h2:mem", "t", properties: [{:"kafka.group_id", "g"}])
      assert {:read_data_source, "jdbc", [], nil, opts} = unwrap_plan(df)
      assert opts["kafka.group_id"] == "g"
    end

    test "partitioned jdbc properties stay verbatim too" do
      df =
        Reader.jdbc(self(), "jdbc:h2:mem", "t", "id", 0, 10, 2, %{
          "CLIENT_SESSION_KEEP_ALIVE" => "true"
        })

      assert {:read_data_source, "jdbc", [], nil, opts} = unwrap_plan(df)
      assert opts["CLIENT_SESSION_KEEP_ALIVE"] == "true"
      assert opts["numPartitions"] == "2"
    end

    test "Writer.jdbc merges :properties verbatim" do
      assert :ok =
               Writer.jdbc(fake_df(), "jdbc:h2:mem", "t",
                 properties: %{"CLIENT_SESSION_KEEP_ALIVE" => "true"}
               )

      opts = written_options()
      assert opts["CLIENT_SESSION_KEEP_ALIVE"] == "true"
      assert opts["url"] == "jdbc:h2:mem"
    end

    test ":options accepts a list of string-keyed tuples" do
      df = Reader.csv(self(), "/data/f.csv", options: [{"header", "true"}])
      assert {:read_data_source, "csv", _, nil, %{"header" => "true"}} = unwrap_plan(df)
    end

    test "a nil :sep does not conflict with :separator" do
      df = Reader.csv(self(), "/data/f.csv", sep: nil, separator: ";")
      assert {:read_data_source, "csv", _, nil, %{"sep" => ";"}} = unwrap_plan(df)

      assert :ok = Writer.csv(fake_df(), "/tmp/out", sep: nil, separator: ";")
      assert written_options()["sep"] == ";"
    end

    test "duplicate detection is spelling-insensitive" do
      assert_raise ArgumentError, ~r/multiple values for keyword argument/, fn ->
        Reader.json(self(), "/data/f.json",
          multi_line: true,
          options: %{"multi_line" => "false"}
        )
      end
    end
  end

  # ── T-50: explicit nil does not wipe builder state ──

  describe "T-50 explicit nil format/schema" do
    test "Reader.load keeps builder format and schema" do
      df =
        Reader.new(self())
        |> Reader.format("parquet")
        |> Reader.schema("id LONG")
        |> Reader.load("/data/p", format: nil, schema: nil)

      assert {:read_data_source, "parquet", ["/data/p"], "id LONG", %{}} = unwrap_plan(df)
    end

    test "StreamReader.load keeps builder format and schema" do
      df =
        StreamReader.new(:s)
        |> StreamReader.format("json")
        |> StreamReader.schema("id LONG")
        |> StreamReader.load("/data/s", format: nil, schema: nil)

      assert {:read_data_source_streaming, "json", ["/data/s"], "id LONG", %{}} = unwrap_plan(df)
    end

    test "call-time non-nil values still override" do
      df =
        Reader.new(self())
        |> Reader.format("parquet")
        |> Reader.load("/data/p", format: "orc")

      assert {:read_data_source, "orc", _, nil, %{}} = unwrap_plan(df)
    end
  end

  # ── T-51: nil clears an option, [] clears repeated fields ──

  describe "T-51 clearing" do
    test "StreamWriter.option/3 with nil deletes the key" do
      writer =
        %StreamWriter{df: nil}
        |> StreamWriter.option("path", "/a")
        |> StreamWriter.option("path", nil)

      assert writer.options == %{}
    end

    test "StreamWriter.options/2 with a nil value deletes the key" do
      writer =
        %StreamWriter{df: nil, options: %{"checkpointLocation" => "/a"}}
        |> StreamWriter.options(checkpoint_location: nil)

      assert writer.options == %{}
    end

    test "Writer repeated fields accept [] as a reset" do
      writer = %Writer{
        df: nil,
        partition_by: ["a"],
        sort_by: ["b"],
        cluster_by: ["c"],
        bucket_by: {4, ["d"]}
      }

      assert Writer.partition_by(writer, []).partition_by == []
      assert Writer.sort_by(writer, []).sort_by == []
      assert Writer.cluster_by(writer, []).cluster_by == []
      assert Writer.bucket_by(writer, 4, []).bucket_by == nil
    end

    test "Writer repeated fields still reject non-lists" do
      writer = %Writer{df: nil}

      assert_raise ArgumentError, ~r/list of column names/, fn ->
        apply(Writer, :sort_by, [writer, "a"])
      end

      assert_raise ArgumentError, ~r/list of column names/, fn ->
        apply(Writer, :cluster_by, [writer, :a])
      end
    end

    test "StreamWriter repeated fields accept [] and reject non-lists" do
      writer = %StreamWriter{df: nil, partition_by: ["a"], cluster_by: ["b"]}

      assert StreamWriter.partition_by(writer, []).partition_by == []
      assert StreamWriter.cluster_by(writer, []).cluster_by == []

      assert_raise ArgumentError, ~r/list of column names/, fn ->
        apply(StreamWriter, :partition_by, [writer, "a"])
      end
    end
  end

  # ── T-52: streaming duplicate detection + DataType schema ──

  describe "T-52 streaming builders" do
    test "StreamReader raises on a duplicated option" do
      assert_raise ArgumentError, ~r/multiple values for keyword argument/, fn ->
        StreamReader.json(:s, "/data/s",
          max_files_per_trigger: 1,
          options: %{"maxFilesPerTrigger" => "2"}
        )
      end
    end

    test "StreamWriter raises on a duplicated option" do
      writer = %StreamWriter{df: nil}

      assert_raise ArgumentError, ~r/multiple values for keyword argument/, fn ->
        StreamWriter.start(writer,
          checkpoint_location: "/a",
          options: %{"checkpointLocation" => "/b"}
        )
      end
    end

    test "StreamReader.schema accepts a Spark.Connect.DataType" do
      proto =
        SparkEx.Types.struct_type([SparkEx.Types.struct_field("id", :long)])
        |> SparkEx.Types.to_proto()

      reader = StreamReader.schema(StreamReader.new(:s), proto)
      assert reader.schema == SparkEx.Types.data_type_to_json(proto)
    end
  end

  # ── T-53: jdbc properties + Reader.table/3 guard ──

  describe "T-53 jdbc and table guards" do
    test "non-partitioned jdbc merges :properties" do
      df =
        Reader.jdbc(self(), "jdbc:postgresql://h/db", "t",
          properties: %{"user" => "u", "password" => "p"}
        )

      assert {:read_data_source, "jdbc", [], nil, opts} = unwrap_plan(df)
      assert opts["user"] == "u"
      assert opts["password"] == "p"
      assert opts["url"] == "jdbc:postgresql://h/db"
      assert opts["dbtable"] == "t"
    end

    test "non-partitioned jdbc accepts keyword-list :properties" do
      df = Reader.jdbc(self(), "jdbc:h2:mem", "t", properties: [fetch_size: 100])
      assert {:read_data_source, "jdbc", [], nil, opts} = unwrap_plan(df)
      # connection properties are pass-through: no camelization
      assert opts["fetch_size"] == "100"
    end

    test "non-partitioned jdbc rejects a bad :properties value" do
      assert_raise ArgumentError, ~r/properties must be nil, a map, or a keyword list/, fn ->
        Reader.jdbc(self(), "jdbc:h2:mem", "t", properties: "user=u")
      end
    end

    test "Reader.table/3 with a builder and options raises a clear error" do
      reader = Reader.new(self())

      assert_raise ArgumentError, ~r/does not accept call-time/, fn ->
        apply(Reader, :table, [reader, "t", [options: %{"k" => "v"}]])
      end
    end

    test "Reader.table/3 with a builder and no options delegates to table/2" do
      reader = Reader.new(self()) |> Reader.option("k", "v")
      df = Reader.table(reader, "t", [])
      assert {:read_named_table, "t", %{"k" => "v"}} = unwrap_plan(df)
    end
  end

  # ── GROK-7 (T-38): atom keys in the singular option/3 ──

  describe "GROK-7 option/3 accepts atom keys" do
    test "Reader.option/3" do
      reader = Reader.option(Reader.new(self()), :multi_line, true)
      assert reader.options == %{"multiLine" => "true"}
      assert Reader.option(reader, :multi_line, nil).options == %{}
    end

    test "Writer.option/3" do
      writer = Writer.option(%Writer{df: nil}, :compression_codec, "gzip")
      assert writer.options == %{"compressionCodec" => "gzip"}
      assert Writer.option(writer, :compression_codec, nil).options == %{}
    end

    test "WriterV2.option/3 and table_property/3" do
      writer = WriterV2.option(%WriterV2{df: nil, table_name: "t"}, :write_mode, "x")
      assert writer.options == %{"writeMode" => "x"}

      writer = WriterV2.table_property(writer, :retention_days, 7)
      assert writer.table_properties == %{"retention_days" => "7"}
    end

    test "StreamReader.option/3" do
      reader = StreamReader.option(StreamReader.new(:s), :max_files_per_trigger, 1)
      assert reader.options == %{"maxFilesPerTrigger" => "1"}
      assert StreamReader.option(reader, :max_files_per_trigger, nil).options == %{}
    end

    test "StreamWriter.option/3" do
      writer = StreamWriter.option(%StreamWriter{df: nil}, :checkpoint_location, "/ckpt")
      assert writer.options == %{"checkpointLocation" => "/ckpt"}
    end
  end
end
