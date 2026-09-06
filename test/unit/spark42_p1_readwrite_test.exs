defmodule SparkEx.Unit.Spark42P1ReadWriteTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias Spark.Connect.{Command, Plan, Relation, WriteOperation, WriteOperationV2}
  alias SparkEx.{DataFrame, Reader, StreamReader, StreamWriter, Writer, WriterV2}
  alias SparkEx.Connect.{CommandEncoder, PlanEncoder}

  test "reader parses DataFrame input lazily with builder schema and options" do
    input = DataFrame.new(self(), {:sql, "SELECT value FROM VALUES ('1,a') AS t(value)", nil})

    schema =
      SparkEx.Types.struct_type([
        SparkEx.Types.struct_field("id", :integer),
        SparkEx.Types.struct_field("name", :string)
      ])

    parsed =
      self()
      |> Reader.new()
      |> Reader.schema(schema)
      |> Reader.option("header", false)
      |> Reader.csv(input, sep: ",")

    assert {:parse, input_plan, :csv, ^schema, %{"header" => "false", "sep" => ","}} =
             unwrap_plan(parsed)

    assert input_plan == unwrap_plan(input)
    assert parsed.session == input.session
  end

  test "DataFrame reader rejects a different session" do
    input = DataFrame.new(:input_session, {:sql, "SELECT 'x' AS value", nil})
    reader = Reader.new(:reader_session)

    assert_raise ArgumentError, ~r/same session/, fn -> Reader.json(reader, input) end
  end

  test "DataFrame reader preserves typed schemas and metadata" do
    input = DataFrame.new(self(), {:sql, "SELECT '{\"id\":1}' AS value", nil})

    schema =
      SparkEx.Types.struct_type([
        SparkEx.Types.struct_field("id", :integer,
          nullable: false,
          metadata: %{"comment" => "primary key"}
        )
      ])

    parsed = Reader.json(Reader.schema(Reader.new(self()), schema), input)
    assert {:parse, _, :json, ^schema, %{}} = unwrap_plan(parsed)

    proto = SparkEx.Types.to_proto(schema)
    parsed = Reader.json(Reader.new(self()), input, schema: proto)
    assert {:parse, _, :json, ^proto, %{}} = unwrap_plan(parsed)
  end

  test "parse XML enum is encoded" do
    {relation, _counter} =
      PlanEncoder.encode_relation(
        {:parse, {:sql, "SELECT '<ROW><id>1</id></ROW>' AS value", nil}, :xml,
         SparkEx.Types.struct_type([SparkEx.Types.struct_field("id", :integer)]),
         %{"rowTag" => "ROW"}},
        0
      )

    assert %Relation{rel_type: {:parse, parse}} = relation
    assert parse.format == :PARSE_FORMAT_XML
    assert parse.options == %{"rowTag" => "ROW"}
    assert %Spark.Connect.DataType{kind: {:struct, struct}} = parse.schema
    assert Enum.map(struct.fields, & &1.name) == ["id"]
  end

  test "named streaming source survives builder changes and encodes presence" do
    df =
      self()
      |> StreamReader.new()
      |> StreamReader.name("events_2026")
      |> StreamReader.option("maxFilesPerTrigger", 2)
      |> StreamReader.format("json")
      |> StreamReader.load("/tmp/input")

    assert {:read_data_source_streaming, "json", ["/tmp/input"], nil,
            %{"maxFilesPerTrigger" => "2"}, "events_2026"} = unwrap_plan(df)

    {relation, _counter} = PlanEncoder.encode_relation(unwrap_plan(df), 0)
    assert %Relation{rel_type: {:read, read}} = relation
    assert {:data_source, source} = read.read_type
    assert source.source_name == "events_2026"
  end

  test "named streaming source survives format-specific load helpers" do
    reader = StreamReader.new(self()) |> StreamReader.name("stable_rate")
    df = StreamReader.rate(reader, rows_per_second: 3)

    assert {:read_data_source_streaming, "rate", [], nil, %{"rowsPerSecond" => "3"},
            "stable_rate"} = unwrap_plan(df)

    json = StreamReader.json(reader, "/tmp/events", multi_line: true)

    assert {:read_data_source_streaming, "json", ["/tmp/events"], nil, %{"multiLine" => "true"},
            "stable_rate"} = unwrap_plan(json)
  end

  test "streaming source names reject blank, punctuation, Unicode, and non-strings" do
    reader = StreamReader.new(self())

    for invalid <- ["", "two words", "dash-name", "źródło", 42] do
      assert_raise ArgumentError, fn -> StreamReader.name(reader, invalid) end
    end
  end

  test "batch and streaming changes encode options and streaming flag" do
    batch =
      self()
      |> Reader.new()
      |> Reader.options(starting_version: 10, ending_bound_inclusive: false)
      |> Reader.changes("catalog.db.events")

    streaming =
      self()
      |> StreamReader.new()
      |> StreamReader.option("startingTimestamp", "2026-09-01T00:00:00Z")
      |> StreamReader.changes("catalog.db.events")

    for {df, expected_streaming} <- [{batch, false}, {streaming, true}] do
      {relation, _counter} = PlanEncoder.encode_relation(unwrap_plan(df), 0)
      assert %Relation{rel_type: {:relation_changes, changes}} = relation
      assert changes.unparsed_identifier == "catalog.db.events"
      assert changes.is_streaming == expected_streaming
      assert changes.options != %{}
    end
  end

  test "changes rejects a reader schema and blank table" do
    assert_raise ArgumentError, ~r/schema/, fn ->
      self() |> Reader.new() |> Reader.schema("id INT") |> Reader.changes("events")
    end

    assert_raise ArgumentError, ~r/table name/, fn ->
      self() |> StreamReader.new() |> StreamReader.changes("  ")
    end
  end

  test "real-time trigger validates and encodes the dedicated oneof" do
    df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
    writer = %StreamWriter{df: df} |> StreamWriter.trigger(real_time: " 5 seconds ")
    assert writer.trigger == {:real_time, "5 seconds"}

    {%Plan{op_type: {:command, %Command{command_type: {:write_stream_operation_start, op}}}}, _} =
      CommandEncoder.encode(
        {:write_stream_operation_start, df.plan, [format: "console", trigger: writer.trigger]},
        0
      )

    assert op.trigger == {:real_time_batch_duration, "5 seconds"}

    assert_raise ArgumentError, fn -> StreamWriter.trigger(writer, real_time: " ") end

    assert_raise ArgumentError, fn ->
      StreamWriter.trigger(writer, real_time: "1 second", once: true)
    end
  end

  test "V1 and V2 writers default evolution off and encode enabled state" do
    df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
    assert %Writer{with_schema_evolution: false} = DataFrame.write(df)
    assert %WriterV2{with_schema_evolution: false} = DataFrame.write_v2(df, "events")

    v1 = df |> DataFrame.write() |> Writer.with_schema_evolution()
    v2 = df |> DataFrame.write_v2("events") |> WriterV2.with_schema_evolution()

    assert v1.with_schema_evolution
    assert v2.with_schema_evolution

    {%Plan{op_type: {:command, %Command{command_type: {:write_operation, op1}}}}, _} =
      CommandEncoder.encode(
        {:write_operation, df.plan,
         [path: "/tmp/out", with_schema_evolution: v1.with_schema_evolution]},
        0
      )

    {%Plan{op_type: {:command, %Command{command_type: {:write_operation_v2, op2}}}}, _} =
      CommandEncoder.encode(
        {:write_operation_v2, df.plan, "events",
         [mode: :append, with_schema_evolution: v2.with_schema_evolution]},
        0
      )

    assert %WriteOperation{with_schema_evolution: true} = op1
    assert %WriteOperationV2{with_schema_evolution: true} = op2
  end

  test "writer schema evolution only accepts booleans" do
    df = DataFrame.new(self(), {:sql, "SELECT 1", nil})

    assert_raise ArgumentError, fn ->
      df |> DataFrame.write() |> Writer.with_schema_evolution(:yes)
    end

    assert_raise ArgumentError, fn ->
      df |> DataFrame.write_v2("events") |> WriterV2.with_schema_evolution(1)
    end
  end
end
