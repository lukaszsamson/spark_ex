defmodule SparkEx.Integration.Spark42PendingOperationTest do
  use ExUnit.Case

  alias SparkEx.Connect.Client
  alias SparkEx.Internal.SessionSnapshot

  @moduletag :integration
  @moduletag min_spark: "4.2"
  @moduletag skip:
               if(System.get_env("SPARK_EX_TEST_PROVIDERS") == "1",
                 do: false,
                 else: "set SPARK_EX_TEST_PROVIDERS=1 and launch the prepared fixture server"
               )

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> SparkEx.Session.stop(session) end)
    assert {:ok, _} = SparkEx.spark_version(session)
    %{session: session}
  end

  for method <- [:interrupt_tag, :interrupt_operation] do
    test "#{method} cancels a held Pending operation and releases its resources", %{
      session: session
    } do
      operation_id = SparkEx.Internal.UUID.generate_v4()
      tag = "pending-#{operation_id}"
      assert {:ok, _} = fixture(session, "create", operation_id, tag)

      # The fixture asserts the internal Pending state and never starts its
      # runner. GetStatus alone cannot form this barrier: Pending and Started
      # both map to RUNNING on the wire.
      assert_status(session, operation_id, :OPERATION_STATE_RUNNING)
      argument = if unquote(method) == :interrupt_tag, do: tag, else: operation_id

      assert {:ok, ids} = apply(SparkEx, unquote(method), [session, argument])
      assert operation_id in ids
      assert_status(session, operation_id, :OPERATION_STATE_CANCELLED)

      {:ok, snapshot} = SessionSnapshot.fetch(session)
      assert {:ok, _} = Client.release_execute(snapshot, operation_id)
      assert_status(session, operation_id, :OPERATION_STATE_CANCELLED)
      assert {:ok, _} = fixture(session, "assert_removed", operation_id, tag)

      assert {:ok, [%{"alive" => 1}]} =
               session |> SparkEx.sql("SELECT 1 AS alive") |> SparkEx.DataFrame.collect()
    end
  end

  defp assert_status(session, operation_id, state) do
    assert {:ok, %{operation_statuses: [status]}} =
             SparkEx.get_operation_statuses(session, [operation_id])

    assert status.operation_id == operation_id
    assert status.state == state
  end

  defp fixture(session, action, operation_id, tag) do
    state = SparkEx.Session.get_state(session)

    plan = %Spark.Connect.Plan{
      op_type:
        {:command,
         %Spark.Connect.Command{
           command_type:
             {:extension,
              %Google.Protobuf.Any{
                type_url: "spark-ex.test/PendingOperation",
                value: Enum.join([action, operation_id, tag], "|")
              }}
         }}
    }

    Client.execute_plan(state, plan)
  end
end
