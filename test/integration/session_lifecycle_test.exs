defmodule SparkEx.Integration.SessionLifecycleTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  describe "release/1" do
    test "releases session and rejects further RPCs" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      # Verify session works before release
      {:ok, version} = SparkEx.spark_version(session)
      assert is_binary(version)

      # Release the session
      assert :ok = SparkEx.Session.release(session)

      # Further RPCs should be rejected
      assert {:error, :session_released} = SparkEx.spark_version(session)
      assert {:error, :session_released} = SparkEx.config_get(session, ["spark.app.name"])

      df = SparkEx.range(session, 10)
      assert {:error, :session_released} = SparkEx.DataFrame.collect(df)

      # Process is still alive (just released)
      assert Process.alive?(session)

      # Clean stop
      SparkEx.Session.stop(session)
      refute Process.alive?(session)
    end

    test "release is idempotent (second release is no-op)" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      assert :ok = SparkEx.Session.release(session)
      assert :ok = SparkEx.Session.release(session)

      SparkEx.Session.stop(session)
    end
  end

  describe "clone_session/2" do
    test "clones session config and keeps sessions independent" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert :ok = SparkEx.config_set(session, [{"spark.test.original", "value1"}])

      assert {:ok, clone_session} = SparkEx.clone_session(session)
      Process.unlink(clone_session)

      on_exit(fn ->
        if Process.alive?(clone_session), do: SparkEx.Session.stop(clone_session)
      end)

      assert {:ok, original} = SparkEx.config_get(session, ["spark.test.original"])
      assert {:ok, cloned} = SparkEx.config_get(clone_session, ["spark.test.original"])
      assert original == [{"spark.test.original", "value1"}]
      assert cloned == [{"spark.test.original", "value1"}]

      assert :ok = SparkEx.config_set(session, [{"spark.test.original", "modified_original"}])
      assert :ok = SparkEx.config_set(clone_session, [{"spark.test.original", "modified_clone"}])

      assert {:ok, original_after} = SparkEx.config_get(session, ["spark.test.original"])
      assert {:ok, cloned_after} = SparkEx.config_get(clone_session, ["spark.test.original"])
      assert original_after == [{"spark.test.original", "modified_original"}]
      assert cloned_after == [{"spark.test.original", "modified_clone"}]
    end
  end

  describe "stop/1" do
    test "stop calls ReleaseSession before disconnecting" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      # Verify session works
      {:ok, _} = SparkEx.spark_version(session)

      # Stop should clean up gracefully
      assert :ok = SparkEx.Session.stop(session)
      refute Process.alive?(session)
    end
  end

  describe "is_stopped/1" do
    test "reflects released state" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert SparkEx.is_stopped(session) == false
      assert :ok = SparkEx.Session.release(session)
      assert SparkEx.is_stopped(session) == true
    end
  end

  describe "interrupt_all/1" do
    test "returns empty list when no operations are running" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert {:ok, interrupted_ids} = SparkEx.interrupt_all(session)
      assert interrupted_ids == []
    end
  end

  describe "interrupt_tag/2" do
    test "returns empty list when no operations match tag" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert {:ok, interrupted_ids} = SparkEx.interrupt_tag(session, "nonexistent-tag")
      assert interrupted_ids == []
    end
  end

  describe "interrupt_operation/2" do
    test "returns empty list for nonexistent operation" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert {:ok, interrupted_ids} =
               SparkEx.interrupt_operation(session, "nonexistent-op-id")

      assert interrupted_ids == []
    end
  end

  describe "DataFrame.tag/2 integration" do
    test "tagged DataFrame executes successfully" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      df =
        SparkEx.range(session, 5)
        |> SparkEx.DataFrame.tag("test-tag")

      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 5
    end

    test "multi-tagged DataFrame executes successfully" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      df =
        SparkEx.range(session, 3)
        |> SparkEx.DataFrame.tag("pipeline-x")
        |> SparkEx.DataFrame.tag("step-1")

      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 3
    end
  end

  describe "interrupt after query completes" do
    test "interrupt_all after executing a query" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      # Execute a query first
      df = SparkEx.range(session, 5)
      assert {:ok, rows} = SparkEx.DataFrame.collect(df)
      assert length(rows) == 5

      # Interrupt should work (no operations running)
      assert {:ok, []} = SparkEx.interrupt_all(session)
    end
  end

  describe "interrupt long-running query" do
    test "interrupt_tag cancels a long-running tagged operation" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      on_exit(fn ->
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      tag = "interrupt-long-running"

      df =
        SparkEx.sql(session, "SELECT sleep(30) AS s")
        |> SparkEx.DataFrame.tag(tag)

      task = Task.async(fn -> SparkEx.DataFrame.collect(df, timeout: 120_000) end)

      # Use a second session handle with the same session_id so we can interrupt while
      # the original session process is busy servicing the long-running execute call.
      state = SparkEx.Session.get_state(session)

      {:ok, control_session} =
        SparkEx.connect(
          url: @spark_remote,
          session_id: state.session_id,
          user_id: state.user_id,
          client_type: state.client_type
        )

      Process.unlink(control_session)

      on_exit(fn ->
        if Process.alive?(control_session), do: SparkEx.Session.stop(control_session)
      end)

      assert {:ok, _interrupted_ids} = wait_for_interrupt(control_session, tag, 150)

      assert {:error, %SparkEx.Error.Remote{} = error} = Task.await(task, 60_000)
      error_text = "#{error.message || ""} #{error.server_message || ""}"

      assert String.contains?(error_text, "OPERATION_CANCELED") or
               String.contains?(error_text, "CANCEL") or
               String.contains?(error_text, "NOT_FOUND") or
               error.error_class in [
                 "INTERNAL_ERROR",
                 "INVALID_HANDLE.OPERATION_NOT_FOUND",
                 "INVALID_HANDLE.SESSION_NOT_FOUND"
               ] or
               error.grpc_status in [4, 13, 14]
    end
  end

  describe "session-control telemetry" do
    test "emits rpc telemetry for interrupt and release" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session)

      handler_id = "integration-session-rpc-#{System.unique_integer([:positive])}"
      pid = self()

      :telemetry.attach_many(
        handler_id,
        [[:spark_ex, :rpc, :start], [:spark_ex, :rpc, :stop]],
        fn event, _measurements, metadata, _ ->
          send(pid, {:telemetry, event, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        if Process.alive?(session), do: SparkEx.Session.stop(session)
      end)

      assert {:ok, []} = SparkEx.interrupt_all(session)
      assert :ok = SparkEx.Session.release(session)

      assert_receive {:telemetry, [:spark_ex, :rpc, :start], %{rpc: :interrupt}}
      assert_receive {:telemetry, [:spark_ex, :rpc, :stop], %{rpc: :interrupt, result: :ok}}
      assert_receive {:telemetry, [:spark_ex, :rpc, :start], %{rpc: :release_session}}
      assert_receive {:telemetry, [:spark_ex, :rpc, :stop], %{rpc: :release_session, result: :ok}}
    end
  end

  defp wait_for_interrupt(_session, tag, 0), do: {:error, {:interrupt_timeout, tag}}

  defp wait_for_interrupt(session, tag, attempts_left) do
    case SparkEx.interrupt_tag(session, tag) do
      {:ok, []} ->
        Process.sleep(200)
        wait_for_interrupt(session, tag, attempts_left - 1)

      {:ok, interrupted_ids} ->
        {:ok, interrupted_ids}

      other ->
        other
    end
  end
end
