defmodule SparkEx.TelemetryTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias SparkEx.Connect.ResultDecoder
  alias Spark.Connect.ExecutePlanResponse

  describe "result batch telemetry" do
    test "emits [:spark_ex, :result, :batch] on decoded batch" do
      ref = make_ref()
      pid = self()

      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{sleep_fun: fn _ -> :ok end, jitter_fun: fn ms -> ms end}
      )

      :telemetry.attach(
        "test-batch-#{inspect(ref)}",
        [:spark_ex, :result, :batch],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-batch-#{inspect(ref)}") end)

      # Build a minimal valid Arrow IPC stream for a single-row batch
      # We'll test that the event is at least _attempted_ to be emitted
      # by using a mock stream with a valid arrow batch
      ipc_data = build_test_ipc_data()

      batch = %ExecutePlanResponse.ArrowBatch{
        data: ipc_data,
        row_count: 1,
        start_offset: 0
      }

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type: {:arrow_batch, batch},
           server_side_session_id: "test"
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      assert {:ok, _result} = ResultDecoder.decode_stream(stream)
      assert_receive {:telemetry, ^ref, [:spark_ex, :result, :batch], measurements, metadata}
      assert measurements.row_count == 1
      assert is_integer(measurements.bytes)
      assert is_integer(metadata.batch_index)
    end

    test "emits [:spark_ex, :result, :progress] on execution progress" do
      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "test-progress-#{inspect(ref)}",
        [:spark_ex, :result, :progress],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-progress-#{inspect(ref)}") end)

      progress = %ExecutePlanResponse.ExecutionProgress{
        stages: [
          %ExecutePlanResponse.ExecutionProgress.StageInfo{
            stage_id: 0,
            num_tasks: 10,
            num_completed_tasks: 5,
            input_bytes_read: 1024,
            done: false
          }
        ],
        num_inflight_tasks: 5
      }

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type: {:execution_progress, progress},
           server_side_session_id: "test"
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      {:ok, _result} = ResultDecoder.decode_stream(stream)

      assert_receive {:telemetry, ^ref, [:spark_ex, :result, :progress], measurements, metadata}
      assert measurements.num_inflight_tasks == 5
      assert [stage] = metadata.stages
      assert stage.stage_id == 0
      assert stage.num_tasks == 10
      assert stage.num_completed_tasks == 5
    end
  end

  describe "rpc telemetry" do
    test "emits rpc start/stop with row_count for row results" do
      ref = make_ref()
      pid = self()
      start_id = "test-rpc-start-#{inspect(ref)}"
      stop_id = "test-rpc-stop-#{inspect(ref)}"

      :telemetry.attach(
        start_id,
        [:spark_ex, :rpc, :start],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(start_id) end)

      :telemetry.attach(
        stop_id,
        [:spark_ex, :rpc, :stop],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(stop_id) end)

      metadata = %{rpc: :execute_plan, session_id: "s-1"}
      result = Client.rpc_telemetry_span(metadata, fn -> {:ok, %{rows: [%{"id" => 1}]}} end)
      assert {:ok, %{rows: [%{"id" => 1}]}} = result

      assert_receive {:telemetry, ^ref, [:spark_ex, :rpc, :start], start_measurements, start_meta}
      assert is_integer(start_measurements.system_time)
      assert start_meta.rpc == :execute_plan
      assert start_meta.session_id == "s-1"

      assert_receive {:telemetry, ^ref, [:spark_ex, :rpc, :stop], stop_measurements, stop_meta}
      assert is_integer(stop_measurements.duration)
      assert stop_meta.result == :ok
      assert stop_meta.row_count == 1
    end

    test "emits rpc stop row_count for explorer results" do
      if Code.ensure_loaded?(Explorer.DataFrame) do
        ref = make_ref()
        pid = self()
        stop_id = "test-rpc-stop-explorer-#{inspect(ref)}"

        :telemetry.attach(
          stop_id,
          [:spark_ex, :rpc, :stop],
          fn event, measurements, metadata, _ ->
            send(pid, {:telemetry, ref, event, measurements, metadata})
          end,
          nil
        )

        on_exit(fn -> :telemetry.detach(stop_id) end)

        df = Explorer.DataFrame.new(%{"id" => [1, 2, 3]})

        result =
          Client.rpc_telemetry_span(%{rpc: :execute_plan_explorer, session_id: "s-2"}, fn ->
            {:ok, %{dataframe: df}}
          end)

        assert {:ok, %{dataframe: %Explorer.DataFrame{}}} = result

        assert_receive {:telemetry, ^ref, [:spark_ex, :rpc, :stop], measurements, metadata}
        assert is_integer(measurements.duration)
        assert metadata.result == :ok
        assert metadata.row_count == 3
      end
    end

    test "emits rpc exception and reraises" do
      ref = make_ref()
      pid = self()
      exception_id = "test-rpc-exception-#{inspect(ref)}"

      :telemetry.attach(
        exception_id,
        [:spark_ex, :rpc, :exception],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(exception_id) end)

      assert_raise RuntimeError, "boom", fn ->
        Client.rpc_telemetry_span(%{rpc: :execute_plan, session_id: "s-3"}, fn ->
          raise "boom"
        end)
      end

      assert_receive {:telemetry, ^ref, [:spark_ex, :rpc, :exception], measurements, metadata}
      assert is_integer(measurements.duration)
      assert metadata.rpc == :execute_plan
      assert metadata.kind == :error
      assert %RuntimeError{} = metadata.reason
      assert is_list(metadata.stacktrace)
    end
  end

  describe "retry telemetry" do
    test "emits [:spark_ex, :retry, :attempt] on retry" do
      ref = make_ref()
      pid = self()

      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{sleep_fun: fn _ -> :ok end, jitter_fun: fn ms -> ms end}
      )

      :telemetry.attach(
        "test-retry-#{inspect(ref)}",
        [:spark_ex, :retry, :attempt],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-retry-#{inspect(ref)}") end)

      # Simulate a transient failure followed by success
      call_count = :counters.new(1, [:atomics])

      result =
        SparkEx.Connect.Client.retry_with_backoff(fn ->
          count = :counters.get(call_count, 1)
          :counters.add(call_count, 1, 1)

          if count == 0 do
            {:error, %SparkEx.Error.Remote{grpc_status: 14, message: "unavailable"}}
          else
            {:ok, :success}
          end
        end)

      assert {:ok, :success} = result
      assert_receive {:telemetry, ^ref, [:spark_ex, :retry, :attempt], measurements, metadata}
      assert measurements.attempt == 1
      assert is_integer(measurements.backoff_ms)
      assert metadata.grpc_status == 14
      assert metadata.max_retries == 3
      assert metadata.retry_delay_ms == nil
    end

    test "includes retry_delay_ms in retry metadata" do
      ref = make_ref()
      pid = self()

      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        retry: %{sleep_fun: fn _ -> :ok end, jitter_fun: fn ms -> ms end}
      )

      :telemetry.attach(
        "test-retry-delay-#{inspect(ref)}",
        [:spark_ex, :retry, :attempt],
        fn event, measurements, metadata, _ ->
          send(pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-retry-delay-#{inspect(ref)}") end)

      result =
        Client.retry_with_backoff(fn ->
          {:error, %SparkEx.Error.Remote{grpc_status: 14, retry_delay_ms: 250}}
        end)

      assert {:error, %SparkEx.Error.Remote{grpc_status: 14}} = result
      assert_receive {:telemetry, ^ref, [:spark_ex, :retry, :attempt], measurements, metadata}
      assert measurements.attempt == 1
      assert measurements.backoff_ms == 250
      assert metadata.retry_delay_ms == 250
    end
  end

  # Build a minimal Explorer-compatible IPC stream for testing
  defp build_test_ipc_data do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      df = Explorer.DataFrame.new(%{"a" => [1]})

      case Explorer.DataFrame.dump_ipc_stream(df) do
        {:ok, data} -> data
        _ -> <<>>
      end
    else
      <<>>
    end
  end
end
