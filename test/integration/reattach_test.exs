defmodule SparkEx.Integration.ReattachTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "reattachable execution (default)" do
    test "simple query works with reattachable enabled", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n, 'hello' AS greeting")
      assert {:ok, [row]} = DataFrame.collect(df)
      assert row == %{"n" => 1, "greeting" => "hello"}
    end

    test "range query works with reattachable enabled", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 10
    end

    test "multi-batch query works with reattachable enabled", %{session: session} do
      df = SparkEx.range(session, 1000)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 1000
    end

    test "query with transforms works with reattachable enabled", %{session: session} do
      import SparkEx.Functions

      df =
        SparkEx.range(session, 20)
        |> DataFrame.filter(SparkEx.Column.gt(col("id"), lit(5)))
        |> DataFrame.limit(5)

      assert {:ok, rows} = DataFrame.collect(df)
      assert Kernel.length(rows) == 5
    end

    test "tagged query works with reattachable enabled", %{session: session} do
      df =
        SparkEx.range(session, 5)
        |> DataFrame.tag("reattach-test")

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 5
    end
  end

  describe "reattachable: false" do
    test "query works with reattachable disabled", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, rows} = DataFrame.collect(df, reattachable: false)
      assert length(rows) == 10
    end
  end

  describe "to_explorer with reattachable" do
    test "to_explorer works with reattachable execution", %{session: session} do
      df = SparkEx.range(session, 100)
      assert {:ok, explorer_df} = DataFrame.to_explorer(df)
      assert Explorer.DataFrame.n_rows(explorer_df) == 100
    end
  end

  describe "release_execute telemetry" do
    test "emits release_execute telemetry after successful execution", %{session: session} do
      handler_id = "reattach-release-#{System.unique_integer([:positive])}"
      pid = self()

      :telemetry.attach_many(
        handler_id,
        [[:spark_ex, :rpc, :start], [:spark_ex, :rpc, :stop]],
        fn event, _measurements, metadata, _ ->
          if metadata[:rpc] == :release_execute do
            send(pid, {:telemetry, event, metadata})
          end
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      df = SparkEx.range(session, 5)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 5

      # ReleaseExecute is called asynchronously after successful execution
      assert_receive {:telemetry, [:spark_ex, :rpc, :start], %{rpc: :release_execute}}, 5_000

      assert_receive {:telemetry, [:spark_ex, :rpc, :stop],
                      %{rpc: :release_execute, result: :ok}},
                     5_000
    end

    test "emits reattach-related execute_plan telemetry with operation_id", %{session: session} do
      handler_id = "reattach-exec-#{System.unique_integer([:positive])}"
      pid = self()

      :telemetry.attach(
        handler_id,
        [:spark_ex, :rpc, :start],
        fn _event, _measurements, metadata, _ ->
          if metadata[:rpc] == :execute_plan do
            send(pid, {:execute_start, metadata})
          end
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      df = SparkEx.range(session, 5)
      assert {:ok, _rows} = DataFrame.collect(df)

      assert_receive {:execute_start, metadata}, 5_000
      assert is_binary(metadata.operation_id)
      assert String.length(metadata.operation_id) == 36
    end

    test "emits retry telemetry on transient failure", %{session: session} do
      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{sleep_fun: fn _ -> :ok end, jitter_fun: fn ms -> ms end}
      )

      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "retry-attempt-#{inspect(ref)}",
        [:spark_ex, :retry, :attempt],
        fn _event, measurements, metadata, _ ->
          send(pid, {:retry_attempt, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("retry-attempt-#{inspect(ref)}") end)

      df = SparkEx.sql(session, "SELECT missing_column FROM range(1)")
      assert {:error, %SparkEx.Error.Remote{grpc_status: 13}} = DataFrame.collect(df)

      receive do
        {:retry_attempt, measurements, metadata} ->
          assert is_integer(measurements.backoff_ms)
          assert metadata.grpc_status == 13
      after
        5_000 ->
          assert true
      end
    end
  end

  describe "multiple concurrent reattachable executions" do
    test "independent queries with separate operation_ids", %{session: session} do
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            df = SparkEx.range(session, i * 10)
            DataFrame.collect(df)
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert {:ok, rows1} = Enum.at(results, 0)
      assert {:ok, rows2} = Enum.at(results, 1)
      assert {:ok, rows3} = Enum.at(results, 2)

      assert length(rows1) == 10
      assert length(rows2) == 20
      assert length(rows3) == 30
    end
  end
end
