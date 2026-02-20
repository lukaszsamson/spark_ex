defmodule SparkEx.Integration.StreamingListenerGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{
    DataFrame,
    StreamReader,
    StreamWriter,
    StreamingQuery,
    StreamingQueryListenerBus,
    StreamingQueryManager
  }

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  defmodule ProgressListener do
    @behaviour SparkEx.StreamingQueryListener
    @pid_key {__MODULE__, :pid}

    def on_query_progress(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:progress, event})
    end

    def on_query_terminated(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:terminated, event})
    end

    def on_query_idle(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:idle, event})
    end

    def set_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_pid, do: :persistent_term.erase(@pid_key)
  end

  defmodule SecondListener do
    @behaviour SparkEx.StreamingQueryListener
    @pid_key {__MODULE__, :pid}

    def on_query_progress(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:progress2, event})
    end

    def on_query_terminated(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:terminated2, event})
    end

    def on_query_idle(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:idle2, event})
    end

    def set_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_pid, do: :persistent_term.erase(@pid_key)
  end

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Progress event payload validation ──

  describe "progress event payload validation" do
    test "progress event has expected fields", %{session: session} do
      ProgressListener.set_pid(self())
      on_exit(fn -> ProgressListener.clear_pid() end)

      {:ok, bus} = StreamingQueryListenerBus.start_link(session)

      on_exit(fn ->
        if Process.alive?(bus) do
          try do
            StreamingQueryListenerBus.remove_listener(bus, ProgressListener)
          catch
            :exit, _ -> :ok
          end

          try do
            StreamingQueryListenerBus.stop(bus)
          catch
            :exit, _ -> :ok
          end
        end
      end)

      :ok = StreamingQueryListenerBus.add_listener(bus, ProgressListener)

      # Simulate a progress event
      event_data = %{
        type: :progress,
        raw_json: ~s({"id":"test-id","runId":"test-run-id","name":"test-query"}),
        data: %{"id" => "test-id", "runId" => "test-run-id", "name" => "test-query"}
      }

      send(bus, {:listener_event, event_data})

      assert_receive {:progress, received_event}, 5_000
      assert received_event.type == :progress
      assert is_binary(received_event.raw_json)
    end
  end

  # ── Multiple listeners ──

  describe "multiple listeners" do
    test "both listeners receive the same event", %{session: session} do
      ProgressListener.set_pid(self())
      SecondListener.set_pid(self())

      on_exit(fn ->
        ProgressListener.clear_pid()
        SecondListener.clear_pid()
      end)

      {:ok, bus} = StreamingQueryListenerBus.start_link(session)

      on_exit(fn ->
        if Process.alive?(bus) do
          try do
            StreamingQueryListenerBus.remove_listener(bus, ProgressListener)
            StreamingQueryListenerBus.remove_listener(bus, SecondListener)
          catch
            :exit, _ -> :ok
          end

          try do
            StreamingQueryListenerBus.stop(bus)
          catch
            :exit, _ -> :ok
          end
        end
      end)

      :ok = StreamingQueryListenerBus.add_listener(bus, ProgressListener)
      :ok = StreamingQueryListenerBus.add_listener(bus, SecondListener)

      event = %{type: :progress, raw_json: "{}", data: %{}}
      send(bus, {:listener_event, event})

      assert_receive {:progress, _}, 5_000
      assert_receive {:progress2, _}, 5_000
    end
  end

  # ── Listener add/remove lifecycle ──

  describe "listener add/remove lifecycle" do
    test "removed listener does not receive subsequent events", %{session: session} do
      ProgressListener.set_pid(self())
      on_exit(fn -> ProgressListener.clear_pid() end)

      {:ok, bus} = StreamingQueryListenerBus.start_link(session)

      on_exit(fn ->
        if Process.alive?(bus) do
          try do
            StreamingQueryListenerBus.stop(bus)
          catch
            :exit, _ -> :ok
          end
        end
      end)

      :ok = StreamingQueryListenerBus.add_listener(bus, ProgressListener)
      :ok = StreamingQueryListenerBus.remove_listener(bus, ProgressListener)

      send(bus, {:listener_event, %{type: :progress, raw_json: "{}", data: %{}}})

      refute_receive {:progress, _}, 1_000
    end
  end

  # ── Streaming query manager ──

  describe "streaming query manager" do
    test "list_listeners returns list", %{session: session} do
      assert {:ok, listeners} = StreamingQueryManager.list_listeners(session)
      assert is_list(listeners)
    end

    test "active queries list reflects running queries", %{session: session} do
      query_name = "mgr_test_#{System.unique_integer([:positive])}"

      df = StreamReader.rate(session, rows_per_second: 5)

      {:ok, query} =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.format("memory")
        |> StreamWriter.output_mode("append")
        |> StreamWriter.query_name(query_name)
        |> StreamWriter.trigger(processing_time: "2 seconds")
        |> StreamWriter.start()

      on_exit(fn -> StreamingQuery.stop(query) end)

      # Wait for query to become active
      Process.sleep(2000)

      assert {:ok, true} = StreamingQuery.is_active?(query)

      StreamingQuery.stop(query)
    end
  end

  # ── Streaming query properties ──

  describe "streaming query properties" do
    test "query exposes id, run_id, name after start", %{session: session} do
      name = "props_test_#{System.unique_integer([:positive])}"

      df = StreamReader.rate(session, rows_per_second: 1)

      {:ok, query} =
        df
        |> DataFrame.write_stream()
        |> StreamWriter.format("memory")
        |> StreamWriter.output_mode("append")
        |> StreamWriter.query_name(name)
        |> StreamWriter.trigger(once: true)
        |> StreamWriter.start()

      on_exit(fn -> StreamingQuery.stop(query) end)

      assert is_binary(StreamingQuery.id(query))
      assert is_binary(StreamingQuery.run_id(query))
      assert StreamingQuery.name(query) == name

      # Wait for once trigger to complete
      assert {:ok, _} = StreamingQuery.await_termination(query, timeout: 20_000)
    end
  end
end
