defmodule SparkEx.Unit.Wave2ClientTest do
  @moduledoc """
  Regression tests for the wave-2 client fixes (T-03, T-08, T-09, T-15, T-16,
  T-19, T-41, T-42). Each test reproduces the previously-broken scenario with
  the fake-stub hooks (`execute_stream_fun` / `reattach_stream_fun` /
  `release_execute_fun`) already used by the reattach tests.
  """
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client
  alias SparkEx.Error.Remote
  alias SparkEx.ManagedStream

  alias Spark.Connect.{ExecutePlanResponse, Plan}

  @unavailable 14

  defp session do
    %SparkEx.Session{
      channel: nil,
      session_id: "wave2-session",
      server_side_session_id: "wave2-server",
      user_id: "wave2",
      client_type: "elixir/test",
      # Deterministic, instant backoff for every test in this module.
      retry_policies: %{
        reattach: %{
          max_retries: 3,
          initial_backoff_ms: 1,
          max_backoff_ms: 10,
          jitter_ms: 0,
          sleep_fun: fn _ -> :ok end
        }
      }
    }
  end

  defp resp(id), do: {:ok, %ExecutePlanResponse{response_id: id}}

  defp complete(id) do
    {:ok,
     %ExecutePlanResponse{
       response_id: id,
       response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
     }}
  end

  defp unavailable_rpc_error do
    %GRPC.RPCError{status: @unavailable, message: "transport closed"}
  end

  defp not_found_remote do
    %Remote{
      message: "gone",
      grpc_status: 5,
      error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND"
    }
  end

  defp ids(items), do: for({:ok, %ExecutePlanResponse{response_id: id}} <- items, do: id)

  # ── T-03: controller survives an abnormal owner exit ──────────────────────

  describe "T-03 managed stream controller lifetime" do
    test "abnormal owner exit(:boom) still releases the remote execution" do
      parent = self()

      owner =
        spawn(fn ->
          {:ok, stream} =
            ManagedStream.new([],
              owner: self(),
              release_fun: fn _ ->
                send(parent, :released)
                {:ok, :released}
              end
            )

          send(parent, {:ready, stream.controller})

          receive do
            :crash -> exit(:boom)
          end
        end)

      assert_receive {:ready, controller}, 1_000
      assert Process.alive?(controller)

      send(owner, :crash)

      # Previously the linked controller died with the owner before handling
      # the monitor :DOWN and the release never fired.
      assert_receive :released, 1_000
      refute_receive :released, 50
    end

    test "controller is not linked to the creating process" do
      {:ok, stream} =
        ManagedStream.new(Stream.repeatedly(fn -> :row end),
          release_fun: fn _ -> {:ok, :released} end
        )

      {:links, links} = Process.info(self(), :links)
      refute stream.controller in links

      :ok = ManagedStream.close(stream)
    end

    test "explicit close and normal completion release exactly once" do
      parent = self()

      release = fn _ ->
        send(parent, :released)
        {:ok, :released}
      end

      {:ok, stream} = ManagedStream.new([1, 2], release_fun: release)
      assert Enum.to_list(stream) == [1, 2]
      assert_receive :released, 500
      :ok = ManagedStream.close(stream)
      refute_receive :released, 50

      {:ok, stream2} = ManagedStream.new(Stream.repeatedly(fn -> :row end), release_fun: release)
      :ok = ManagedStream.close(stream2)
      :ok = ManagedStream.close(stream2)
      assert_receive :released, 500
      refute_receive :released, 50
    end
  end

  # ── T-09: managed streams actually reattach ─────────────────────────────────

  describe "T-09 managed stream reattachment" do
    test "graceful EOF on the initial stream reattaches from the last response id" do
      parent = self()

      execute_stream_fun = fn request, _timeout ->
        send(parent, {:execute_request, request})
        {:ok, [resp("r1"), resp("r2")]}
      end

      reattach_stream_fun = fn last_id ->
        send(parent, {:reattach, last_id})

        case last_id do
          "r2" -> {:ok, [resp("r3")]}
          "r3" -> {:ok, [complete("r4")]}
        end
      end

      assert {:ok, %ManagedStream{} = stream} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert ids(Enum.to_list(stream)) == ["r1", "r2", "r3", "r4"]

      assert_received {:execute_request, %Spark.Connect.ExecutePlanRequest{} = request}
      assert request.operation_id != nil

      assert Enum.any?(request.request_options, fn
               %{request_option: {:reattach_options, %{reattachable: true}}} -> true
               _ -> false
             end)

      assert_received {:reattach, "r2"}
      assert_received {:reattach, "r3"}
    end

    test "transient transport error mid-stream reattaches instead of surfacing" do
      execute_stream_fun = fn _request, _timeout ->
        {:ok, [resp("r1"), {:error, unavailable_rpc_error()}]}
      end

      reattach_stream_fun = fn "r1" -> {:ok, [complete("r2")]} end

      assert {:ok, stream} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert ids(Enum.to_list(stream)) == ["r1", "r2"]
    end

    test "initial handshake failure is returned as an error without a controller leak" do
      assert {:error, %Remote{grpc_status: 3}} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: fn _, _ ->
                   {:error, %GRPC.RPCError{status: 3, message: "bad plan"}}
                 end,
                 reattach_stream_fun: fn _ -> {:ok, []} end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )
    end
  end

  # ── T-09 (review): close semantics ──────────────────────────────────────────

  describe "T-09 managed stream close semantics" do
    test "close from the owner while another process is enumerating halts without a fresh ExecutePlan" do
      parent = self()
      executes = :counters.new(1, [:atomics])

      # The upstream yields one response, then blocks until told to EOF —
      # simulating a consumer parked on the gRPC stream when the owner
      # releases the operation.
      execute_stream_fun = fn _request, _timeout ->
        :counters.add(executes, 1, 1)

        upstream =
          Stream.resource(
            fn -> :first end,
            fn
              :first ->
                {[resp("r1")], :blocked}

              :blocked ->
                send(parent, {:producer_blocked, self()})

                receive do
                  :eof -> {:halt, :done}
                end
            end,
            fn _ -> :ok end
          )

        {:ok, upstream}
      end

      assert {:ok, stream} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: fn _ -> flunk("must not reattach after close") end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      consumer = Task.async(fn -> Enum.to_list(stream) end)

      assert_receive {:producer_blocked, producer}, 1_000
      :ok = ManagedStream.close(stream)
      send(producer, :eof)

      assert ids(Task.await(consumer, 1_000)) == ["r1"]
      assert :counters.get(executes, 1) == 1
    end

    test "early halt does not block on release and issues exactly one release_all" do
      parent = self()

      release_execute_fun = fn opts ->
        send(parent, {:release, opts})

        if Keyword.has_key?(opts, :until_response_id) do
          {:ok, nil}
        else
          # A slow release_all must not stall the consumer's halt.
          Process.sleep(300)
          {:ok, nil}
        end
      end

      assert {:ok, stream} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: fn _, _ ->
                   {:ok, Stream.map(Stream.iterate(1, &(&1 + 1)), &resp("r#{&1}"))}
                 end,
                 reattach_stream_fun: fn _ -> flunk("must not reattach") end,
                 release_execute_fun: release_execute_fun
               )

      {elapsed_us, taken} = :timer.tc(fn -> Enum.take(stream, 3) end)
      assert ids(taken) == ["r1", "r2", "r3"]
      assert elapsed_us < 200_000, "halt blocked on release for #{div(elapsed_us, 1000)} ms"

      # Exactly one release_all (the controller's, carrying :timeout); the
      # inner reattach finalizer's release_all is a no-op on this path.
      assert_receive {:release, [timeout: _]}, 1_000
      refute_receive {:release, [timeout: _]}, 400
      refute_receive {:release, []}, 10
    end
  end

  # ── T-08: initial ExecutePlan failure goes through ReattachExecute ─────────

  describe "T-08 initial ExecutePlan retry" do
    test "a retryable initial failure reattaches instead of re-sending the plan" do
      parent = self()
      calls = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        :counters.add(calls, 1, 1)
        {:error, unavailable_rpc_error()}
      end

      # The server may already have registered the operation (Spark 3.5 would
      # answer a re-sent operation_id with OPERATION_ALREADY_EXISTS), so the
      # client must try ReattachExecute first, PySpark-style.
      reattach_stream_fun = fn last_id ->
        send(parent, {:reattach, last_id})
        {:ok, [complete("r1")]}
      end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session(), %Plan{},
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert :counters.get(calls, 1) == 1
      assert_received {:reattach, nil}
    end

    test "OPERATION_NOT_FOUND on that reattach falls back to a fresh ExecutePlan" do
      calls = :counters.new(1, [:atomics])
      reattaches = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) == 1 do
          {:error, unavailable_rpc_error()}
        else
          {:ok, [complete("r1")]}
        end
      end

      reattach_stream_fun = fn nil ->
        :counters.add(reattaches, 1, 1)
        {:error, not_found_remote()}
      end

      assert {:ok, %{rows: []}} =
               Client.execute_plan(session(), %Plan{},
                 reattach_retries: 1,
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert :counters.get(calls, 1) == 2
      assert :counters.get(reattaches, 1) == 1
    end

    test "initial failure recovery honours the reattach retry budget" do
      calls = :counters.new(1, [:atomics])
      reattaches = :counters.new(1, [:atomics])

      execute_stream_fun = fn _request, _timeout ->
        :counters.add(calls, 1, 1)
        {:error, unavailable_rpc_error()}
      end

      reattach_stream_fun = fn nil ->
        :counters.add(reattaches, 1, 1)
        {:error, not_found_remote()}
      end

      assert {:error, {:reattach_incomplete_result, %{responses_received: 0}}} =
               Client.execute_plan(session(), %Plan{},
                 reattach_retries: 1,
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      # initial ExecutePlan -> reattach -> not found -> one fresh ExecutePlan
      # (the single allowed retry) -> budget exhausted.
      assert :counters.get(calls, 1) == 2
      assert :counters.get(reattaches, 1) == 1
    end

    test "a non-retryable initial error is not retried or reattached" do
      calls = :counters.new(1, [:atomics])

      assert {:error, %Remote{grpc_status: 3}} =
               Client.execute_plan(session(), %Plan{},
                 execute_stream_fun: fn _, _ ->
                   :counters.add(calls, 1, 1)
                   {:error, %GRPC.RPCError{status: 3, message: "invalid"}}
                 end,
                 reattach_stream_fun: fn _ -> flunk("must not reattach") end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert :counters.get(calls, 1) == 1
    end
  end

  # ── T-15: fresh ExecutePlan after OPERATION_NOT_FOUND is bounded ────────────

  describe "T-15 operation-not-found retry budget" do
    test "graceful EOF + OPERATION_NOT_FOUND cycle stops once the budget is exceeded" do
      executes = :counters.new(1, [:atomics])
      reattaches = :counters.new(1, [:atomics])

      # Every ExecutePlan ends gracefully with no responses; every reattach
      # reports the operation as gone. Previously this looped forever.
      execute_stream_fun = fn _request, _timeout ->
        :counters.add(executes, 1, 1)
        {:ok, []}
      end

      reattach_stream_fun = fn nil ->
        :counters.add(reattaches, 1, 1)
        {:error, not_found_remote()}
      end

      assert {:error, {:reattach_incomplete_result, %{retries_attempted: 3}}} =
               Client.execute_plan(session(), %Plan{},
                 reattach_retries: 2,
                 execute_stream_fun: execute_stream_fun,
                 reattach_stream_fun: reattach_stream_fun,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      # Each fresh ExecutePlan is one charged retry; it is allowed while the
      # charged count does not exceed max_retries (`>`), so: initial + 3
      # re-executions, then the 4th reattach failure trips the guard.
      assert :counters.get(executes, 1) == 4
      assert :counters.get(reattaches, 1) == 4
    end
  end

  # ── T-16: jitter survives the server retry-delay floor ──────────────────────

  describe "T-16 jitter with server floor" do
    test "jitter is added after max(exponential, server_floor)" do
      parent = self()

      # jitter_ms nil -> the legacy jitter_fun path is used, which is
      # deterministic: +37 ms on top of whatever the un-jittered wait is.
      policy = [
        max_retries: 3,
        initial_backoff_ms: 10,
        max_backoff_ms: 1_000,
        backoff_multiplier: 2.0,
        max_server_retry_delay: 60_000,
        jitter_ms: nil,
        min_jitter_threshold_ms: 0,
        jitter_fun: fn wait -> wait + 37 end,
        sleep_fun: fn ms -> send(parent, {:slept, ms}) end
      ]

      calls = :counters.new(1, [:atomics])

      Client.retry_with_backoff(
        fn ->
          :counters.add(calls, 1, 1)

          if :counters.get(calls, 1) <= 2 do
            {:error, %Remote{message: "busy", grpc_status: @unavailable, retry_delay_ms: 5_000}}
          else
            {:ok, :done}
          end
        end,
        policy
      )

      # The 5000 ms server floor dominates 10 ms / 20 ms exponential waits;
      # previously the jitter was folded into the exponential term and then
      # discarded by the max, yielding exactly 5000 both times.
      assert_received {:slept, 5_037}
      assert_received {:slept, 5_037}
    end

    test "jitter still applies when exponential backoff dominates" do
      parent = self()

      policy = [
        max_retries: 1,
        initial_backoff_ms: 1_000,
        max_backoff_ms: 60_000,
        max_server_retry_delay: 60_000,
        jitter_ms: nil,
        min_jitter_threshold_ms: 0,
        jitter_fun: fn wait -> wait + 37 end,
        sleep_fun: fn ms -> send(parent, {:slept, ms}) end
      ]

      calls = :counters.new(1, [:atomics])

      Client.retry_with_backoff(
        fn ->
          :counters.add(calls, 1, 1)

          if :counters.get(calls, 1) == 1 do
            {:error, %Remote{message: "busy", grpc_status: @unavailable, retry_delay_ms: 10}}
          else
            {:ok, :done}
          end
        end,
        policy
      )

      assert_received {:slept, 1_037}
    end
  end

  # ── T-41: max_rows / max_bytes forwarded on the Arrow path ─────────────────

  describe "T-41 execute_plan_arrow limits" do
    test "max_rows is enforced by the arrow decoder" do
      batches =
        for i <- 0..2 do
          {:ok,
           %ExecutePlanResponse{
             response_id: "r#{i}",
             response_type:
               {:arrow_batch,
                %ExecutePlanResponse.ArrowBatch{
                  row_count: 1,
                  data: <<0, 1, 2, 3>>,
                  start_offset: i,
                  chunk_index: 0,
                  num_chunks_in_batch: 1
                }}
           }}
        end

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :rows}} =
               Client.execute_plan_arrow(session(), %Plan{},
                 max_rows: 1,
                 execute_stream_fun: fn _, _ -> {:ok, batches ++ [complete("done")]} end,
                 reattach_stream_fun: fn _ -> {:ok, []} end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )
    end

    test "max_bytes is enforced by the arrow decoder" do
      batch =
        {:ok,
         %ExecutePlanResponse{
           response_id: "r0",
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{
                row_count: 1,
                data: :binary.copy(<<0>>, 1024),
                start_offset: 0,
                chunk_index: 0,
                num_chunks_in_batch: 1
              }}
         }}

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :bytes}} =
               Client.execute_plan_arrow(session(), %Plan{},
                 max_bytes: 16,
                 execute_stream_fun: fn _, _ -> {:ok, [batch, complete("done")]} end,
                 reattach_stream_fun: fn _ -> {:ok, []} end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )
    end
  end

  # ── T-42: nothing is yielded after ResultComplete ───────────────────────────

  describe "T-42 halt after ResultComplete" do
    test "responses buffered after result_complete are not yielded or pulled" do
      pulled = :counters.new(1, [:atomics])

      upstream =
        Stream.map([resp("r1"), complete("r2"), resp("stale-1"), resp("stale-2")], fn item ->
          :counters.add(pulled, 1, 1)
          item
        end)

      assert {:ok, stream} =
               Client.execute_plan_managed_stream(session(), %Plan{},
                 execute_stream_fun: fn _, _ -> {:ok, upstream} end,
                 reattach_stream_fun: fn _ -> flunk("must not reattach after completion") end,
                 release_execute_fun: fn _ -> {:ok, nil} end
               )

      assert ids(Enum.to_list(stream)) == ["r1", "r2"]
      assert :counters.get(pulled, 1) == 2
    end
  end
end

defmodule SparkEx.Unit.Wave2TaskSupervisorTest do
  # Stops the application's TaskSupervisor briefly; must not overlap with
  # async tests that rely on it.
  use ExUnit.Case, async: false

  alias SparkEx.Connect.Client

  # ── T-19: TaskSupervisor gone during shutdown ───────────────────────────────

  test "supervised task helpers report :noproc instead of exiting" do
    :ok = Supervisor.terminate_child(SparkEx.Supervisor, SparkEx.TaskSupervisor)

    on_exit(fn -> Supervisor.restart_child(SparkEx.Supervisor, SparkEx.TaskSupervisor) end)

    assert {:error, :noproc} = Client.start_supervised_task(fn -> :ok end)
    assert {:error, :noproc} = Client.async_nolink_supervised(fn -> :ok end)

    parent = self()

    # The managed stream controller uses the same helpers: a release while the
    # supervisor is down must be logged, not crash the controller.
    {:ok, stream} =
      SparkEx.ManagedStream.new(Stream.repeatedly(fn -> :row end),
        release_fun: fn _ -> {:ok, :released} end
      )

    :telemetry.attach(
      "wave2-noproc-#{inspect(self())}",
      [:spark_ex, :managed_stream, :release_failed],
      fn _event, _measurements, metadata, _ -> send(parent, {:release_failed, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("wave2-noproc-#{inspect(parent)}") end)

    :ok = SparkEx.ManagedStream.close(stream)
    assert_receive {:release_failed, %{reason: {:task_start_failed, :noproc}}}, 1_000

    {:ok, _} = Supervisor.restart_child(SparkEx.Supervisor, SparkEx.TaskSupervisor)
  end
end
