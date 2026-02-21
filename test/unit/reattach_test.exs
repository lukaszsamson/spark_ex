defmodule SparkEx.Unit.ReattachTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client

  alias Spark.Connect.{
    ExecutePlanRequest,
    ReattachExecuteRequest,
    ReattachOptions,
    ReleaseExecuteRequest,
    ResultChunkingOptions,
    UserContext,
    ExecutePlanResponse,
    Plan
  }

  describe "ReattachExecuteRequest building" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-123",
        server_side_session_id: "server-side-456",
        user_id: "test_user",
        client_type: "elixir/test"
      }

      %{session: session}
    end

    test "builds request with operation_id and last_response_id", %{session: session} do
      request = %ReattachExecuteRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        operation_id: "op-abc-123",
        last_response_id: "resp-xyz-789"
      }

      assert request.session_id == "test-session-123"
      assert request.operation_id == "op-abc-123"
      assert request.last_response_id == "resp-xyz-789"
      assert request.client_observed_server_side_session_id == "server-side-456"
      assert request.user_context.user_id == "test_user"
      assert request.client_type == "elixir/test"
    end

    test "builds request without last_response_id (initial reattach)", %{session: session} do
      request = %ReattachExecuteRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        operation_id: "op-first",
        last_response_id: nil
      }

      assert request.operation_id == "op-first"
      assert request.last_response_id == nil
    end
  end

  describe "ReleaseExecuteRequest building" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-123",
        server_side_session_id: "server-side-456",
        user_id: "test_user",
        client_type: "elixir/test"
      }

      %{session: session}
    end

    test "builds release_all request", %{session: session} do
      request = %ReleaseExecuteRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        operation_id: "op-done",
        release: {:release_all, %ReleaseExecuteRequest.ReleaseAll{}}
      }

      assert request.session_id == "test-session-123"
      assert request.operation_id == "op-done"
      assert request.release == {:release_all, %ReleaseExecuteRequest.ReleaseAll{}}
    end

    test "builds release_until request", %{session: session} do
      request = %ReleaseExecuteRequest{
        session_id: session.session_id,
        client_observed_server_side_session_id: session.server_side_session_id,
        user_context: %UserContext{user_id: session.user_id},
        client_type: session.client_type,
        operation_id: "op-partial",
        release:
          {:release_until, %ReleaseExecuteRequest.ReleaseUntil{response_id: "resp-halfway"}}
      }

      assert request.operation_id == "op-partial"

      assert {:release_until, %ReleaseExecuteRequest.ReleaseUntil{response_id: "resp-halfway"}} =
               request.release
    end
  end

  describe "ExecutePlanRequest with reattach options" do
    test "includes ReattachOptions and operation_id when reattachable" do
      request = %ExecutePlanRequest{
        session_id: "sess",
        client_type: "test",
        user_context: %UserContext{user_id: "u"},
        operation_id: "op-123",
        request_options: [
          %ExecutePlanRequest.RequestOption{
            request_option:
              {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
          },
          %ExecutePlanRequest.RequestOption{
            request_option: {:reattach_options, %ReattachOptions{reattachable: true}}
          }
        ]
      }

      assert request.operation_id == "op-123"
      assert length(request.request_options) == 2

      reattach_opt =
        Enum.find(request.request_options, fn opt ->
          match?({:reattach_options, _}, opt.request_option)
        end)

      assert {:reattach_options, %ReattachOptions{reattachable: true}} =
               reattach_opt.request_option
    end

    test "does not include ReattachOptions when not reattachable" do
      request = %ExecutePlanRequest{
        session_id: "sess",
        client_type: "test",
        user_context: %UserContext{user_id: "u"},
        operation_id: nil,
        request_options: [
          %ExecutePlanRequest.RequestOption{
            request_option:
              {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
          }
        ]
      }

      assert request.operation_id == nil
      assert length(request.request_options) == 1

      refute Enum.any?(request.request_options, fn opt ->
               match?({:reattach_options, _}, opt.request_option)
             end)
    end
  end

  describe "response_id tracking" do
    test "response_id is present on ExecutePlanResponse" do
      resp = %ExecutePlanResponse{
        session_id: "s",
        server_side_session_id: "ss",
        operation_id: "op-1",
        response_id: "resp-001"
      }

      assert resp.response_id == "resp-001"
      assert resp.operation_id == "op-1"
    end
  end

  describe "reattach behavior with callback hooks" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-123",
        server_side_session_id: "server-side-456",
        user_id: "test_user",
        client_type: "elixir/test"
      }

      %{session: session}
    end

    test "graceful EOF without result_complete triggers reattach continuation", %{
      session: session
    } do
      parent = self()

      execute_stream_fun = fn _request, _timeout ->
        {:ok, [{:ok, %ExecutePlanResponse{response_id: "r1"}}]}
      end

      reattach_stream_fun = fn last_response_id ->
        send(parent, {:reattach_called_with, last_response_id})

        {:ok,
         [
           {:ok,
            %ExecutePlanResponse{
              response_id: "r2",
              response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
            }}
         ]}
      end

      release_execute_fun = fn _opts -> {:ok, nil} end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: release_execute_fun
               )

      assert_receive {:reattach_called_with, "r1"}
    end

    test "operation-not-found during reattach retries initial execute when no partial responses",
         %{
           session: session
         } do
      attempt = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        count = :counters.get(attempt, 1)
        :counters.add(attempt, 1, 1)

        if count == 0 do
          {:ok, []}
        else
          {:ok,
           [
             {:ok,
              %ExecutePlanResponse{
                response_id: "final",
                response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
              }}
           ]}
        end
      end

      reattach_stream_fun = fn _last_response_id ->
        {:error,
         %SparkEx.Error.Remote{
           error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND",
           message: "not found"
         }}
      end

      release_execute_fun = fn _opts -> {:ok, nil} end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: release_execute_fun
               )
    end

    test "operation-not-found during reattach fails after partial responses exist", %{
      session: session
    } do
      execute_stream_fun = fn _request, _timeout ->
        {:ok, [{:ok, %ExecutePlanResponse{response_id: "r1"}}]}
      end

      reattach_stream_fun = fn _last_response_id ->
        {:error,
         %SparkEx.Error.Remote{
           error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND",
           message: "not found"
         }}
      end

      release_execute_fun = fn _opts -> {:ok, nil} end

      assert {:error, %SparkEx.Error.Remote{error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND"}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: release_execute_fun
               )
    end

    test "release_execute is attempted when decode fails", %{session: session} do
      parent = self()

      release_execute_fun = fn _opts ->
        send(parent, :release_called)
        {:ok, nil}
      end

      assert {:error, :decode_failed} =
               Client.execute_plan(
                 session,
                 %Plan{},
                 release_execute_fun: release_execute_fun,
                 execute_stream_fun: fn _request, _timeout ->
                   {:ok,
                    [{:ok, %ExecutePlanResponse{response_id: "x"}}, {:error, :decode_failed}]}
                 end,
                 reattach_stream_fun: fn _last_response_id -> {:ok, []} end
               )

      assert_receive :release_called
    end

    test "release_execute not-found errors are treated as benign", %{session: session} do
      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 release_execute_fun: fn _opts ->
                   {:error,
                    %SparkEx.Error.Remote{
                      error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND",
                      message: "operation missing",
                      grpc_status: 5
                    }}
                 end,
                 execute_stream_fun: fn _request, _timeout ->
                   {:ok,
                    [
                      {:ok,
                       %ExecutePlanResponse{
                         response_id: "done",
                         response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
                       }}
                    ]}
                 end,
                 reattach_stream_fun: fn _last_response_id -> {:ok, []} end
               )
    end

    test "release_execute timeout emits best-effort telemetry", %{session: session} do
      parent = self()
      handler_id = "release-best-effort-timeout-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:spark_ex, :release_execute, :best_effort],
          fn event, measurements, metadata, pid ->
            send(pid, {:release_telemetry, event, measurements, metadata})
          end,
          parent
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 release_execute_timeout: 5,
                 release_execute_fun: fn _opts ->
                   Process.sleep(50)
                   {:ok, nil}
                 end,
                 execute_stream_fun: fn _request, _timeout ->
                   {:ok,
                    [
                      {:ok,
                       %ExecutePlanResponse{
                         response_id: "done",
                         response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
                       }}
                    ]}
                 end,
                 reattach_stream_fun: fn _last_response_id -> {:ok, []} end
               )

      assert_receive {:release_telemetry, [:spark_ex, :release_execute, :best_effort], _m,
                      %{result: :timeout}}
    end

    test "release_execute failures emit best-effort telemetry", %{session: session} do
      parent = self()
      handler_id = "release-best-effort-error-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:spark_ex, :release_execute, :best_effort],
          fn event, measurements, metadata, pid ->
            send(pid, {:release_telemetry, event, measurements, metadata})
          end,
          parent
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 release_execute_fun: fn _opts -> {:error, :release_failed} end,
                 execute_stream_fun: fn _request, _timeout ->
                   {:ok,
                    [
                      {:ok,
                       %ExecutePlanResponse{
                         response_id: "done",
                         response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
                       }}
                    ]}
                 end,
                 reattach_stream_fun: fn _last_response_id -> {:ok, []} end
               )

      assert_receive {:release_telemetry, [:spark_ex, :release_execute, :best_effort], _m,
                      %{result: :error, error: :release_failed}}
    end

    test "reattach retry uses configured reattach sleep_fun", %{session: session} do
      parent = self()

      on_exit(fn -> SparkEx.RetryPolicyRegistry.set_policies(%{}) end)

      SparkEx.RetryPolicyRegistry.set_policies(
        reattach: %{
          initial_backoff_ms: 7,
          max_backoff_ms: 7,
          jitter_fun: fn ms -> ms end,
          sleep_fun: fn ms -> send(parent, {:reattach_slept, ms}) end
        }
      )

      execute_stream_fun = fn _request, _timeout ->
        {:ok, [{:ok, %ExecutePlanResponse{response_id: "r1"}}]}
      end

      reattach_attempts = :counters.new(1, [:atomics])

      reattach_stream_fun = fn _last_response_id ->
        attempt = :counters.get(reattach_attempts, 1)
        :counters.add(reattach_attempts, 1, 1)

        if attempt == 0 do
          {:ok, []}
        else
          {:ok,
           [
             {:ok,
              %ExecutePlanResponse{
                response_id: "r2",
                response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
              }}
           ]}
        end
      end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session, %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _opts -> {:ok, nil} end
               )

      assert_receive {:reattach_slept, 7}
    end
  end
end
