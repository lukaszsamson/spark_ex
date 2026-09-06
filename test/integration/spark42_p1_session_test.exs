defmodule SparkEx.Integration.Spark42P1SessionTest do
  use ExUnit.Case

  @moduletag :integration
  alias SparkEx.{DataFrame, Session, Types}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> Session.stop(session) end)
    %{session: session}
  end

  test "empty convenience preserves DDL and struct schemas", %{session: session} do
    for schema <- ["id INT, name STRING", Types.struct_type([Types.struct_field("id", :integer)])] do
      assert {:ok, df} = SparkEx.empty_dataframe(session, schema)
      assert {:ok, []} = DataFrame.collect(df)
      assert {:ok, 0} = DataFrame.count(df)
    end
  end

  @tag min_spark: "4.1"
  test "incremental uploads preserve rows and exact order through tiny batches", %{
    session: session
  } do
    assert :ok =
             Session.config_set(session, [
               {"spark.sql.session.localRelationBatchOfChunksSizeBytes", "4096"}
             ])

    source =
      Explorer.DataFrame.new(%{
        "id" => 0..199,
        "v" => List.duplicate(String.duplicate("x", 100), 200)
      })

    assert {:ok, df} =
             SparkEx.create_dataframe(session, source,
               cache_threshold: 0,
               cache_chunk_size: 4096,
               cache_chunk_rows: 3
             )

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.map(rows, & &1["id"]) == Enum.to_list(0..199)
  end

  @tag min_spark: "4.2"
  test "status observes and cancels active work through the busy session", %{session: session} do
    # Connect itself is local: establish the server session before listing it.
    assert {:ok, _version} = SparkEx.spark_version(session)
    assert {:ok, %{operation_statuses: []}} = SparkEx.get_operation_statuses(session)
    df = SparkEx.sql(session, "SELECT sum(sin(id)) AS n FROM range(0, 1000000000)")
    task = Task.async(fn -> DataFrame.collect(df, timeout: 60_000) end)

    # The server-reported state is the barrier: cancellation is not scheduled
    # using an assumed query duration or by blocking the Session mailbox.
    operation = await_active_operation(session, System.monotonic_time(:millisecond) + 10_000)

    assert {:ok, %{operation_statuses: [selected]}} =
             SparkEx.get_operation_statuses(session, [operation.operation_id])

    assert selected.operation_id == operation.operation_id
    assert selected.state == :OPERATION_STATE_RUNNING

    assert {:ok, ids} = SparkEx.interrupt_operation(session, operation.operation_id)
    assert operation.operation_id in ids
    assert {:error, %SparkEx.Error.Remote{}} = Task.await(task, 15_000)

    assert {:ok, [%{"alive" => 1}]} =
             session |> SparkEx.sql("SELECT 1 AS alive") |> DataFrame.collect()

    assert {:ok, %{operation_statuses: statuses}} =
             SparkEx.get_operation_statuses(session, ["missing"])

    assert Enum.all?(statuses, &(&1.state == :OPERATION_STATE_UNKNOWN))
  end

  defp await_active_operation(session, deadline) do
    assert {:ok, %{operation_statuses: statuses}} = SparkEx.get_operation_statuses(session)

    case Enum.find(statuses, &(&1.state == :OPERATION_STATE_RUNNING)) do
      nil ->
        assert System.monotonic_time(:millisecond) < deadline, "operation never became active"
        Process.sleep(10)
        await_active_operation(session, deadline)

      operation ->
        operation
    end
  end
end
