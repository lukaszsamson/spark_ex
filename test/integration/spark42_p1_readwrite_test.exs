defmodule SparkEx.Integration.Spark42P1ReadWriteTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{
    DataFrame,
    Reader,
    Session,
    StreamReader,
    StreamingQuery,
    StreamWriter,
    Writer,
    WriterV2
  }

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "JSON, CSV, and XML DataFrame reads are lazy and preserve parse options", %{
    session: session
  } do
    json_input =
      SparkEx.sql(
        session,
        ~S[SELECT value FROM VALUES ('{"id":1,"name":"Ada"}'), ('{"id":2,"name":"Lin"}') AS t(value)]
      )

    json = Reader.json(Reader.new(session), json_input, schema: "id INT, name STRING")
    assert {:ok, rows} = DataFrame.collect(json)

    assert Enum.sort_by(rows, & &1["id"]) == [
             %{"id" => 1, "name" => "Ada"},
             %{"id" => 2, "name" => "Lin"}
           ]

    csv_input = SparkEx.sql(session, "SELECT value FROM VALUES ('1,Ada'), ('2,Lin') AS t(value)")

    csv =
      Reader.csv(Reader.new(session), csv_input,
        schema: "id INT, name STRING",
        sep: ",",
        mode: "FAILFAST"
      )

    assert {:ok, csv_rows} = DataFrame.collect(csv)

    assert Enum.sort_by(csv_rows, & &1["id"]) == [
             %{"id" => 1, "name" => "Ada"},
             %{"id" => 2, "name" => "Lin"}
           ]

    xml_input =
      SparkEx.sql(
        session,
        "SELECT value FROM VALUES ('<ROW><id>1</id><name>Ada</name></ROW>') AS t(value)"
      )

    xml =
      Reader.xml(Reader.new(session), xml_input, schema: "id INT, name STRING", row_tag: "ROW")

    assert {:ok, [%{"id" => 1, "name" => "Ada"}]} = DataFrame.collect(xml)
  end

  test "DataFrame reads cover inferred and empty input and preserve server validation", %{
    session: session
  } do
    inferred =
      SparkEx.sql(session, ~S[SELECT value FROM VALUES ('{"id":7}') AS t(value)])
      |> then(&Reader.json(Reader.new(session), &1))

    assert {:ok, [%{"id" => 7}]} = DataFrame.collect(inferred)

    empty =
      SparkEx.sql(session, "SELECT CAST(NULL AS STRING) AS value WHERE false")
      |> then(&Reader.json(Reader.new(session), &1, schema: "id INT"))

    assert {:ok, []} = DataFrame.collect(empty)

    multiple_columns = SparkEx.sql(session, "SELECT '1' AS left_value, '2' AS right_value")
    # A schema makes JSON parsing permissive before the input relation is
    # validated on some planner paths, so use inference to exercise the native
    # Parse relation's mandatory single-column validation.
    invalid = Reader.json(Reader.new(session), multiple_columns)
    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(invalid)

    non_string = SparkEx.sql(session, "SELECT 1 AS value")
    invalid = Reader.json(Reader.new(session), non_string, schema: "id INT")
    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(invalid)
  end

  test "unsupported CDC table and malformed bounds surface provider/server errors", %{
    session: session
  } do
    table = "spark42_cdc_negative_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             SparkEx.sql(session, "CREATE TABLE #{table} (id INT) USING parquet")
             |> DataFrame.collect()

    on_exit(fn ->
      SparkEx.sql(session, "DROP TABLE IF EXISTS #{table}") |> DataFrame.collect()
    end)

    unsupported = session |> Reader.new() |> Reader.changes(table)
    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(unsupported)

    malformed =
      session
      |> Reader.new()
      |> Reader.option("startingVersion", "not-a-version")
      |> Reader.changes(table)

    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(malformed)
  end

  test "V1 schema evolution and unsupported real-time combination fail explicitly", %{
    session: session
  } do
    path =
      Path.join(System.tmp_dir!(), "spark42-v1-evolution-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(path) end)

    result =
      SparkEx.sql(session, "SELECT 1L AS id, 'a' AS data")
      |> DataFrame.write()
      |> Writer.format("parquet")
      |> Writer.with_schema_evolution()
      |> Writer.save(path)

    assert {:error, %SparkEx.Error.Remote{error_class: error_class}} = result
    assert error_class =~ "UNSUPPORTED_SCHEMA_EVOLUTION"

    table = "spark42_v2_evolution_negative_#{System.unique_integer([:positive])}"

    result =
      SparkEx.sql(session, "SELECT 1L AS id, 'a' AS data")
      |> DataFrame.write_v2(table)
      |> WriterV2.using("parquet")
      |> WriterV2.with_schema_evolution()
      |> WriterV2.create()

    assert {:error, %SparkEx.Error.Remote{error_class: error_class}} = result
    assert error_class =~ "UNSUPPORTED_SCHEMA_EVOLUTION"

    stream = StreamReader.rate(session, rows_per_second: 1)

    result =
      stream
      |> DataFrame.write_stream()
      |> StreamWriter.format("console")
      |> StreamWriter.trigger(real_time: "1 second")
      |> StreamWriter.start()

    assert {:error, %SparkEx.Error.Remote{}} = result
  end

  test "named file source resumes from the same checkpoint without gaps or duplicates", %{
    session: session
  } do
    sql_ok(session, "SET spark.sql.streaming.queryEvolution.enableSourceEvolution=true")

    suffix = System.unique_integer([:positive, :monotonic])
    root = Path.join(System.tmp_dir!(), "spark42-named-source-#{suffix}")
    input = Path.join(root, "input")
    output = Path.join(root, "output")
    checkpoint = Path.join(root, "checkpoint")
    source_name = "orders_#{suffix}"

    File.mkdir_p!(input)
    File.write!(Path.join(input, "batch-1.json"), ~s({"id":1,"batch":"first"}\n))
    on_exit(fn -> File.rm_rf(root) end)

    stream = fn ->
      session
      |> StreamReader.new()
      |> StreamReader.name(source_name)
      |> StreamReader.json(input, schema: "id LONG, batch STRING")
    end

    run_available_now = fn ->
      {:ok, query} =
        stream.()
        |> DataFrame.write_stream()
        |> StreamWriter.format("parquet")
        |> StreamWriter.output_mode("append")
        |> StreamWriter.option("checkpointLocation", checkpoint)
        |> StreamWriter.trigger(available_now: true)
        |> StreamWriter.start(path: output)

      assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 20)
    end

    run_available_now.()

    assert {:ok, [%{"id" => 1, "batch" => "first"}]} =
             Reader.parquet(session, output) |> DataFrame.collect()

    File.write!(Path.join(input, "batch-2.json"), ~s({"id":2,"batch":"second"}\n))
    run_available_now.()

    assert {:ok, rows} = Reader.parquet(session, output) |> DataFrame.collect()

    assert Enum.sort_by(rows, & &1["id"]) == [
             %{"id" => 1, "batch" => "first"},
             %{"id" => 2, "batch" => "second"}
           ]
  end

  describe "Spark 4.2 upstream provider fixtures" do
    @describetag skip:
                   if(System.get_env("SPARK_EX_TEST_PROVIDERS") == "1",
                     do: false,
                     else: "set SPARK_EX_TEST_PROVIDERS=1 and launch the prepared fixture server"
                   )
    test "upstream V2 fixture evolves append, overwrite, and overwrite-partitions schemas", %{
      session: session
    } do
      for {suffix, action} <- [append: :append, overwrite: :overwrite, partitions: :partitions] do
        table =
          "testcat.p1_evolution_#{suffix}_#{System.unique_integer([:positive])}"

        sql_ok(session, "DROP TABLE IF EXISTS #{table}")

        create =
          if action == :partitions,
            do: "CREATE TABLE #{table} (id BIGINT) USING foo PARTITIONED BY (id)",
            else: "CREATE TABLE #{table} (id BIGINT) USING foo"

        sql_ok(session, create)
        on_exit(fn -> sql_ok(session, "DROP TABLE IF EXISTS #{table}") end)

        writer =
          SparkEx.sql(session, "SELECT 1L AS id, CAST(1 AS STRING) AS data")
          |> DataFrame.write_v2(table)
          |> WriterV2.with_schema_evolution()

        result =
          case action do
            :append -> WriterV2.append(writer)
            :overwrite -> WriterV2.overwrite(writer, SparkEx.Functions.lit(true))
            :partitions -> WriterV2.overwrite_partitions(writer)
          end

        assert :ok = result

        assert {:ok, [%{"id" => 1, "data" => "1"}]} =
                 SparkEx.sql(session, "SELECT * FROM #{table}") |> DataFrame.collect()
      end
    end

    test "upstream changelog fixture returns seeded insert/update/delete history and bounds", %{
      session: session
    } do
      table = "cdc_e2e.p1_cdc"
      sql_ok(session, "DROP TABLE IF EXISTS #{table}")
      sql_ok(session, "CREATE TABLE #{table} (id BIGINT, data STRING) USING foo")
      on_exit(fn -> sql_ok(session, "DROP TABLE IF EXISTS #{table}") end)

      all =
        session
        |> Reader.new()
        |> Reader.option("startingVersion", 1)
        |> Reader.option("endingVersion", 3)
        |> Reader.changes(table)

      assert {:ok, rows} = DataFrame.collect(all)

      assert Enum.map(rows, & &1["_change_type"]) ==
               ["insert", "update_before", "update_after", "delete"]

      bounded =
        session
        |> Reader.new()
        |> Reader.options(
          starting_version: 2,
          ending_version: 2,
          starting_bound_inclusive: true,
          ending_bound_inclusive: true
        )
        |> Reader.changes(table)

      assert {:ok, bounded_rows} = DataFrame.collect(bounded)
      assert Enum.map(bounded_rows, & &1["_change_type"]) == ["update_before", "update_after"]
    end
  end

  defp sql_ok(session, statement) do
    assert {:ok, _} = SparkEx.sql(session, statement) |> DataFrame.collect()
    :ok
  end
end
