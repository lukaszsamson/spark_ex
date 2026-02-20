defmodule SparkEx.M14.StreamingTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.CommandEncoder
  alias SparkEx.Connect.PlanEncoder

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
    def handle_call({:execute_command_with_result, command, exec_opts}, _from, state) do
      send(state.parent, {:execute_command_with_result, command, exec_opts})
      result = %{query_id: %{id: "query-id", run_id: "run-id"}, name: ""}
      {:reply, {:ok, {:write_stream_start, result}}, state}
    end
  end

  # ── PlanEncoder: Streaming Read Variants ──

  describe "encode_relation for read_data_source_streaming" do
    test "encodes with is_streaming: true" do
      plan = {:read_data_source_streaming, "rate", [], nil, %{"rowsPerSecond" => "10"}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:data_source, ds} = read.read_type
      assert ds.format == "rate"
      assert ds.options == %{"rowsPerSecond" => "10"}
    end

    test "encodes with paths" do
      plan = {:read_data_source_streaming, "json", ["/data/stream"], nil, %{}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:data_source, ds} = read.read_type
      assert ds.paths == ["/data/stream"]
    end

    test "encodes with schema" do
      plan = {:read_data_source_streaming, "csv", [], "id INT, name STRING", %{}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:data_source, ds} = read.read_type
      assert ds.schema == "id INT, name STRING"
    end

    test "encodes xml streaming source" do
      plan = {:read_data_source_streaming, "xml", ["/data/stream"], nil, %{}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:data_source, ds} = read.read_type
      assert ds.format == "xml"
    end
  end

  describe "encode_relation for read_named_table_streaming" do
    test "encodes with is_streaming: true" do
      plan = {:read_named_table_streaming, "my_table", %{}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:named_table, nt} = read.read_type
      assert nt.unparsed_identifier == "my_table"
    end

    test "encodes with options" do
      plan = {:read_named_table_streaming, "tbl", %{"opt1" => "val1"}}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:read, read} = relation.rel_type
      assert read.is_streaming == true
      assert {:named_table, nt} = read.read_type
      assert nt.options == %{"opt1" => "val1"}
    end
  end

  # ── CommandEncoder: WriteStreamOperationStart ──

  describe "encode_command for write_stream_operation_start" do
    test "encodes basic streaming write" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.format == "console"
      assert proto.output_mode == "append"
      assert proto.input != nil
    end

    test "encodes with processing_time trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: {:processing_time, "5 seconds"},
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.trigger == {:processing_time_interval, "5 seconds"}
    end

    test "encodes with available_now trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: :available_now,
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.trigger == {:available_now, true}
    end

    test "encodes with once trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: :once,
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.trigger == {:once, true}
    end

    test "encodes with continuous trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: {:continuous, "1 second"},
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.trigger == {:continuous_checkpoint_interval, "1 second"}
    end

    test "encodes with path sink destination" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "parquet",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: "/data/output",
        table_name: nil,
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.sink_destination == {:path, "/data/output"}
    end

    test "encodes with table_name sink destination" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "parquet",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: nil,
        table_name: "my_table",
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.sink_destination == {:table_name, "my_table"}
    end

    test "prefers table_name when both path and table_name are provided" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "parquet",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: "/data/output",
        table_name: "my_table",
        partition_by: [],
        cluster_by: []
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.sink_destination == {:table_name, "my_table"}
    end

    test "encodes with query_name, partitioning, and clustering" do
      df_plan = {:sql, "SELECT 1", nil}

      write_opts = [
        format: "console",
        output_mode: "complete",
        options: %{"key" => "val"},
        query_name: "my_query",
        trigger: nil,
        path: nil,
        table_name: nil,
        partition_by: ["col1", "col2"],
        cluster_by: ["col3"]
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.query_name == "my_query"
      assert proto.output_mode == "complete"
      assert proto.partitioning_column_names == ["col1", "col2"]
      assert proto.clustering_column_names == ["col3"]
      assert proto.options == %{"key" => "val"}
    end

    test "encodes with foreach_writer" do
      df_plan = {:sql, "SELECT 1", nil}

      foreach_fn = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{payload: "test_payload"}}
      }

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: [],
        foreach_writer: foreach_fn,
        foreach_batch: nil
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.foreach_writer == foreach_fn
      assert proto.foreach_batch == nil
    end

    test "encodes with foreach_batch" do
      df_plan = {:sql, "SELECT 1", nil}

      foreach_fn = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{payload: "batch_payload"}}
      }

      write_opts = [
        format: "console",
        output_mode: "append",
        options: %{},
        query_name: nil,
        trigger: nil,
        path: nil,
        table_name: nil,
        partition_by: [],
        cluster_by: [],
        foreach_writer: nil,
        foreach_batch: foreach_fn
      ]

      {command, _counter} =
        CommandEncoder.encode_command({:write_stream_operation_start, df_plan, write_opts}, 0)

      assert {:write_stream_operation_start, proto} = command.command_type
      assert proto.foreach_batch == foreach_fn
      assert proto.foreach_writer == nil
    end
  end

  # ── CommandEncoder: StreamingQueryCommand ──

  describe "encode_command for streaming_query_command" do
    test "encodes status command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "query-id-1", "run-id-1", {:status}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.query_id.id == "query-id-1"
      assert cmd.query_id.run_id == "run-id-1"
      assert cmd.command == {:status, true}
    end

    test "encodes stop command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:stop}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.command == {:stop, true}
    end

    test "encodes process_all_available command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:process_all_available}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.command == {:process_all_available, true}
    end

    test "encodes recent_progress command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:recent_progress}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.command == {:recent_progress, true}
    end

    test "encodes last_progress command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:last_progress}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.command == {:last_progress, true}
    end

    test "encodes exception command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:exception}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert cmd.command == {:exception, true}
    end

    test "encodes explain command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:explain, true}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert {:explain, explain} = cmd.command
      assert explain.extended == true
    end

    test "encodes await_termination command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:await_termination, 5000}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert {:await_termination, at} = cmd.command
      assert at.timeout_ms == 5000
    end

    test "encodes await_termination with nil timeout" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_command, "q1", "r1", {:await_termination, nil}},
          0
        )

      assert {:streaming_query_command, cmd} = command.command_type
      assert {:await_termination, at} = cmd.command
      assert at.timeout_ms == nil
    end
  end

  # ── CommandEncoder: StreamingQueryManagerCommand ──

  describe "encode_command for streaming_query_manager_command" do
    test "encodes active command" do
      {command, _counter} =
        CommandEncoder.encode_command({:streaming_query_manager_command, {:active}}, 0)

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert cmd.command == {:active, true}
    end

    test "encodes get_query command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:get_query, "query-id-1"}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert cmd.command == {:get_query, "query-id-1"}
    end

    test "encodes reset_terminated command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:reset_terminated}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert cmd.command == {:reset_terminated, true}
    end

    test "encodes await_any_termination command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:await_any_termination, 10_000}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert {:await_any_termination, at} = cmd.command
      assert at.timeout_ms == 10_000
    end
  end

  # ── CommandEncoder: StreamingQueryManagerCommand — Listener Controls ──

  describe "encode_command for streaming_query_manager_command listener controls" do
    test "encodes add_listener command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:add_listener, "listener-1", "payload-bytes"}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert {:add_listener, listener_cmd} = cmd.command
      assert listener_cmd.id == "listener-1"
      assert listener_cmd.listener_payload == "payload-bytes"
    end

    test "encodes remove_listener command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:remove_listener, "listener-2", "payload"}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert {:remove_listener, listener_cmd} = cmd.command
      assert listener_cmd.id == "listener-2"
      assert listener_cmd.listener_payload == "payload"
    end

    test "encodes list_listeners command" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_manager_command, {:list_listeners}},
          0
        )

      assert {:streaming_query_manager_command, cmd} = command.command_type
      assert cmd.command == {:list_listeners, true}
    end
  end

  # ── CommandEncoder: StreamingQueryListenerBusCommand ──

  describe "encode_command for streaming_query_listener_bus_command" do
    test "encodes add listener bus listener" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_listener_bus_command, :add},
          0
        )

      assert {:streaming_query_listener_bus_command, cmd} = command.command_type
      assert cmd.command == {:add_listener_bus_listener, true}
    end

    test "encodes remove listener bus listener" do
      {command, _counter} =
        CommandEncoder.encode_command(
          {:streaming_query_listener_bus_command, :remove},
          0
        )

      assert {:streaming_query_listener_bus_command, cmd} = command.command_type
      assert cmd.command == {:remove_listener_bus_listener, true}
    end
  end

  # ── StreamReader struct tests ──

  describe "StreamReader" do
    test "new creates reader with session" do
      reader = SparkEx.StreamReader.new(:fake_session)
      assert reader.session == :fake_session
      assert reader.format == nil
      assert reader.schema == nil
      assert reader.options == %{}
    end

    test "format sets the format" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.format("rate")
      assert reader.format == "rate"
    end

    test "schema sets the schema" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.schema("id INT")
      assert reader.schema == "id INT"
    end

    test "option adds an option" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.option("key", "val")
      assert reader.options == %{"key" => "val"}
    end

    test "options merges options" do
      reader =
        SparkEx.StreamReader.new(:s)
        |> SparkEx.StreamReader.option("a", "1")
        |> SparkEx.StreamReader.options(%{"b" => "2", "c" => "3"})

      assert reader.options == %{"a" => "1", "b" => "2", "c" => "3"}
    end

    test "load creates streaming DataFrame" do
      df =
        SparkEx.StreamReader.new(:s)
        |> SparkEx.StreamReader.format("rate")
        |> SparkEx.StreamReader.load()

      assert %SparkEx.DataFrame{} = df
      assert {:read_data_source_streaming, "rate", [], nil, _opts} = df.plan
    end

    test "load with path creates streaming DataFrame" do
      df =
        SparkEx.StreamReader.new(:s)
        |> SparkEx.StreamReader.format("json")
        |> SparkEx.StreamReader.load("/data/stream")

      assert {:read_data_source_streaming, "json", ["/data/stream"], nil, _opts} = df.plan
    end

    test "table creates streaming DataFrame" do
      df = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.table("my_table")
      assert {:read_named_table_streaming, "my_table", _opts} = df.plan
    end

    test "rate convenience creates DataFrame" do
      df = SparkEx.StreamReader.rate(:s, rows_per_second: 10)
      assert {:read_data_source_streaming, "rate", [], nil, opts} = df.plan
      assert opts["rowsPerSecond"] == "10"
    end

    test "convenience methods merge top-level options with nested options" do
      df =
        SparkEx.StreamReader.json(:s, "/data/stream",
          multi_line: true,
          options: %{"mode" => "PERMISSIVE"}
        )

      assert {:read_data_source_streaming, "json", ["/data/stream"], nil, opts} = df.plan
      assert opts["multi_line"] == "true"
      assert opts["mode"] == "PERMISSIVE"
    end

    test "convenience methods accept SparkEx.Types struct schema as JSON" do
      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :long,
            nullable: false,
            metadata: %{"comment" => "stream_id"}
          )
        ])

      df = SparkEx.StreamReader.csv(:s, "/data/stream", schema: schema)
      assert {:read_data_source_streaming, "csv", ["/data/stream"], encoded_schema, _} = df.plan

      decoded = Jason.decode!(encoded_schema)
      assert decoded["type"] == "struct"
      assert Enum.at(decoded["fields"], 0)["nullable"] == false
      assert Enum.at(decoded["fields"], 0)["metadata"]["comment"] == "stream_id"
    end

    test "option with nil value is skipped" do
      reader =
        SparkEx.StreamReader.new(:s)
        |> SparkEx.StreamReader.option("a", "1")
        |> SparkEx.StreamReader.option("b", nil)

      assert reader.options == %{"a" => "1"}
    end

    test "options with nil values are skipped" do
      reader =
        SparkEx.StreamReader.new(:s)
        |> SparkEx.StreamReader.options(%{"a" => "1", "b" => nil})

      assert reader.options == %{"a" => "1"}
    end

    test "load raises on empty path" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.format("json")

      assert_raise ArgumentError, ~r/must not be empty/, fn ->
        SparkEx.StreamReader.load(reader, "")
      end
    end

    test "load raises on blank path" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.format("json")

      assert_raise ArgumentError, ~r/must not be empty/, fn ->
        SparkEx.StreamReader.load(reader, "   ")
      end
    end

    test "load with list raises on blank path in list" do
      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.format("json")

      assert_raise ArgumentError, ~r/must not be empty/, fn ->
        SparkEx.StreamReader.load(reader, ["/valid", "  "])
      end
    end

    test "convenience methods raise on nil path" do
      assert_raise ArgumentError, ~r/non-empty string/, fn ->
        SparkEx.StreamReader.json(:s, nil)
      end
    end

    test "convenience methods raise on blank path" do
      assert_raise ArgumentError, ~r/must not be empty/, fn ->
        SparkEx.StreamReader.json(:s, "   ")
      end
    end
  end

  # ── StreamWriter struct tests ──

  describe "StreamWriter" do
    test "output_mode sets the mode" do
      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.output_mode("append")
      assert writer.output_mode == "append"
    end

    test "format sets the source" do
      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.format("console")
      assert writer.source == "console"
    end

    test "path sets the sink path" do
      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.path("/data/output")
      assert writer.path == "/data/output"
    end

    test "option adds an option" do
      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.option("key", "val")
      assert writer.options == %{"key" => "val"}
    end

    test "query_name sets the name" do
      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.query_name("my_q")
      assert writer.query_name == "my_q"
    end

    test "trigger sets processing_time" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.trigger(processing_time: "5 seconds")

      assert writer.trigger == {:processing_time, "5 seconds"}
    end

    test "trigger sets available_now" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.trigger(available_now: true)

      assert writer.trigger == :available_now
    end

    test "trigger sets once" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.trigger(once: true)

      assert writer.trigger == :once
    end

    test "trigger sets continuous" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.trigger(continuous: "1 second")

      assert writer.trigger == {:continuous, "1 second"}
    end

    test "trigger raises for invalid options" do
      assert_raise ArgumentError, fn ->
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.trigger(invalid: true)
      end
    end

    test "partition_by sets columns" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.partition_by(["a", "b"])

      assert writer.partition_by == ["a", "b"]
    end

    test "cluster_by sets columns" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.cluster_by(["c"])

      assert writer.cluster_by == ["c"]
    end

    test "option with nil value is skipped" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.option("a", "1")
        |> SparkEx.StreamWriter.option("b", nil)

      assert writer.options == %{"a" => "1"}
    end

    test "options with nil values are skipped" do
      writer =
        %SparkEx.StreamWriter{df: nil}
        |> SparkEx.StreamWriter.options(%{"a" => "1", "b" => nil})

      assert writer.options == %{"a" => "1"}
    end

    test "xml sets format and path" do
      df = %SparkEx.DataFrame{session: :s, plan: {:sql, "SELECT 1", nil}}

      writer =
        %SparkEx.StreamWriter{df: df}
        |> SparkEx.StreamWriter.xml("/data/output")

      assert writer.df == df
      assert writer.source == "xml"
      assert writer.path == "/data/output"
    end

    test "start applies call-time writer kwargs and sink options" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = %SparkEx.DataFrame{session: session, plan: {:sql, "SELECT 1", nil}}

      writer =
        %SparkEx.StreamWriter{df: df}
        |> SparkEx.StreamWriter.option("base", "1")

      assert {:ok, %SparkEx.StreamingQuery{query_id: "query-id", run_id: "run-id"}} =
               SparkEx.StreamWriter.start(writer,
                 format: "memory",
                 outputMode: "complete",
                 partitionBy: "part_col",
                 queryName: "q_name",
                 truncate: false,
                 options: %{"numRows" => 5},
                 timeout: 1234
               )

      assert_receive {:execute_command_with_result,
                      {:write_stream_operation_start, _, write_opts}, exec_opts}

      assert Keyword.get(write_opts, :format) == "memory"
      assert Keyword.get(write_opts, :output_mode) == "complete"
      assert Keyword.get(write_opts, :partition_by) == ["part_col"]
      assert Keyword.get(write_opts, :query_name) == "q_name"
      assert Keyword.get(write_opts, :options)["base"] == "1"
      assert Keyword.get(write_opts, :options)["truncate"] == "false"
      assert Keyword.get(write_opts, :options)["numRows"] == "5"
      assert exec_opts == [timeout: 1234]
    end

    test "to_table applies call-time sink options and preserves table destination" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = %SparkEx.DataFrame{session: session, plan: {:sql, "SELECT 1", nil}}

      writer = %SparkEx.StreamWriter{df: df, path: "/tmp/old_path"}

      assert {:ok, %SparkEx.StreamingQuery{query_id: "query-id", run_id: "run-id"}} =
               SparkEx.StreamWriter.to_table(writer, "my_table",
                 checkpointLocation: "/tmp/ckpt",
                 options: %{"mergeSchema" => true},
                 timeout: 2222
               )

      assert_receive {:execute_command_with_result,
                      {:write_stream_operation_start, _, write_opts}, exec_opts}

      assert Keyword.get(write_opts, :table_name) == "my_table"
      assert Keyword.get(write_opts, :path) == nil
      assert Keyword.get(write_opts, :options)["checkpointLocation"] == "/tmp/ckpt"
      assert Keyword.get(write_opts, :options)["mergeSchema"] == "true"
      assert exec_opts == [timeout: 2222]
    end

    test "foreach_writer sets foreach function" do
      func = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{payload: "test"}}
      }

      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.foreach_writer(func)
      assert writer.foreach_writer == func
    end

    test "foreach_batch sets foreach batch function" do
      func = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{payload: "test"}}
      }

      writer = %SparkEx.StreamWriter{df: nil} |> SparkEx.StreamWriter.foreach_batch(func)
      assert writer.foreach_batch == func
    end
  end

  # ── StreamingQuery struct tests ──

  describe "StreamingQuery struct" do
    test "creates with required fields" do
      query = %SparkEx.StreamingQuery{
        session: :fake,
        query_id: "abc",
        run_id: "def",
        name: "my_query"
      }

      assert query.session == :fake
      assert query.query_id == "abc"
      assert query.run_id == "def"
      assert query.name == "my_query"
    end

    test "name defaults to nil" do
      query = %SparkEx.StreamingQuery{
        session: :fake,
        query_id: "abc",
        run_id: "def"
      }

      assert query.name == nil
    end
  end

  # ── DataFrame.write_stream/1 ──

  describe "DataFrame.write_stream/1" do
    test "returns a StreamWriter" do
      df = %SparkEx.DataFrame{session: :s, plan: {:sql, "SELECT 1", nil}}
      writer = SparkEx.DataFrame.write_stream(df)
      assert %SparkEx.StreamWriter{} = writer
      assert writer.df == df
    end
  end

  # ── SparkEx.read_stream/1 ──

  describe "SparkEx.read_stream/1" do
    test "returns a StreamReader" do
      reader = SparkEx.read_stream(:fake_session)
      assert %SparkEx.StreamReader{} = reader
      assert reader.session == :fake_session
    end
  end

  # ── StreamingQueryListener behaviour ──

  describe "StreamingQueryListener behaviour" do
    test "defines the expected callbacks" do
      callbacks = SparkEx.StreamingQueryListener.behaviour_info(:callbacks)
      assert {:on_query_progress, 1} in callbacks
      assert {:on_query_terminated, 1} in callbacks
      assert {:on_query_idle, 1} in callbacks
    end
  end

  # ── SparkEx.Types ──

  describe "SparkEx.Types" do
    import SparkEx.Types

    test "struct_type creates a struct type" do
      schema = struct_type([struct_field("id", :long)])
      assert {:struct, [%{name: "id", type: :long, nullable: true}]} = schema
    end

    test "struct_field defaults nullable to true" do
      field = struct_field("name", :string)
      assert field.nullable == true
    end

    test "struct_field with nullable: false" do
      field = struct_field("name", :string, nullable: false)
      assert field.nullable == false
    end

    test "to_ddl converts struct type to DDL" do
      schema =
        struct_type([
          struct_field("id", :long),
          struct_field("name", :string)
        ])

      assert to_ddl(schema) == "id LONG, name STRING"
    end

    test "to_ddl handles complex types" do
      schema =
        struct_type([
          struct_field("tags", array_type(:string)),
          struct_field("meta", map_type(:string, :long)),
          struct_field("amount", {:decimal, 10, 2})
        ])

      ddl = to_ddl(schema)
      assert ddl =~ "tags ARRAY<STRING>"
      assert ddl =~ "meta MAP<STRING, LONG>"
      assert ddl =~ "amount DECIMAL(10, 2)"
    end

    test "to_json converts struct type to JSON" do
      schema =
        struct_type([
          struct_field("id", :long),
          struct_field("name", :string)
        ])

      json = to_json(schema)
      decoded = Jason.decode!(json)
      assert decoded["type"] == "struct"
      assert length(decoded["fields"]) == 2
      assert Enum.at(decoded["fields"], 0)["name"] == "id"
      assert Enum.at(decoded["fields"], 0)["type"] == "long"
      assert Enum.at(decoded["fields"], 1)["name"] == "name"
      assert Enum.at(decoded["fields"], 1)["type"] == "string"
    end

    test "schema_to_string passes through DDL strings" do
      assert schema_to_string("id LONG") == "id LONG"
    end

    test "schema_to_string converts struct type to DDL" do
      schema = struct_type([struct_field("id", :long)])
      assert schema_to_string(schema) == "id LONG"
    end

    test "Reader.schema accepts struct type as JSON schema" do
      schema =
        struct_type([
          struct_field("id", :long, nullable: false, metadata: %{"comment" => "primary"}),
          struct_field("name", :string)
        ])

      reader = SparkEx.Reader.new(:s) |> SparkEx.Reader.schema(schema)
      decoded = Jason.decode!(reader.schema)
      assert decoded["type"] == "struct"
      assert Enum.at(decoded["fields"], 0)["nullable"] == false
      assert Enum.at(decoded["fields"], 0)["metadata"]["comment"] == "primary"
    end

    test "StreamReader.schema accepts struct type as JSON schema" do
      schema =
        struct_type([
          struct_field("id", :long),
          struct_field("name", :string, nullable: false, metadata: %{"tag" => "required"})
        ])

      reader = SparkEx.StreamReader.new(:s) |> SparkEx.StreamReader.schema(schema)
      decoded = Jason.decode!(reader.schema)
      assert decoded["type"] == "struct"
      assert Enum.at(decoded["fields"], 1)["nullable"] == false
      assert Enum.at(decoded["fields"], 1)["metadata"]["tag"] == "required"
    end
  end
end
