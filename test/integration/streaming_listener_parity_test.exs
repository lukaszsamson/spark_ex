defmodule SparkEx.Integration.StreamingListenerParityTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{
    DataFrame,
    StreamReader,
    StreamWriter,
    StreamingQuery,
    StreamingQueryListenerBus
  }

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  defmodule ListenerLocalV1 do
    @behaviour SparkEx.StreamingQueryListener
    @pid_key {__MODULE__, :pid}

    def on_query_started(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:listener_v1, :started, event})
    end

    def on_query_progress(event) do
      if pid = :persistent_term.get(@pid_key, nil),
        do: send(pid, {:listener_v1, :progress, event})
    end

    def on_query_terminated(event) do
      if pid = :persistent_term.get(@pid_key, nil),
        do: send(pid, {:listener_v1, :terminated, event})
    end

    def on_query_idle(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:listener_v1, :idle, event})
    end

    def set_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_pid, do: :persistent_term.erase(@pid_key)
  end

  defmodule ListenerLocalV2 do
    @behaviour SparkEx.StreamingQueryListener
    @pid_key {__MODULE__, :pid}

    def on_query_started(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:listener_v2, :started, event})
    end

    def on_query_progress(event) do
      if pid = :persistent_term.get(@pid_key, nil),
        do: send(pid, {:listener_v2, :progress, event})
    end

    def on_query_terminated(event) do
      if pid = :persistent_term.get(@pid_key, nil),
        do: send(pid, {:listener_v2, :terminated, event})
    end

    def on_query_idle(event) do
      if pid = :persistent_term.get(@pid_key, nil), do: send(pid, {:listener_v2, :idle, event})
    end

    def set_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_pid, do: :persistent_term.erase(@pid_key)
  end

  defmodule BadListener do
    @behaviour SparkEx.StreamingQueryListener

    def on_query_started(_event), do: raise("bad listener start")
    def on_query_progress(_event), do: raise("bad listener progress")
    def on_query_terminated(_event), do: raise("bad listener terminate")
    def on_query_idle(_event), do: raise("bad listener idle")
  end

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    {:ok, bus} = StreamingQueryListenerBus.start_link(session)

    ListenerLocalV1.set_pid(self())
    ListenerLocalV2.set_pid(self())

    on_exit(fn ->
      ListenerLocalV1.clear_pid()
      ListenerLocalV2.clear_pid()

      if Process.alive?(bus) do
        for mod <- [ListenerLocalV1, ListenerLocalV2, BadListener] do
          try do
            StreamingQueryListenerBus.remove_listener(bus, mod)
          catch
            :exit, _ -> :ok
          end
        end

        try do
          StreamingQueryListenerBus.stop(bus)
        catch
          :exit, _ -> :ok
        end
      end

      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session, bus: bus}
  end

  defp start_rate_query(session, query_name, trigger_opts \\ [processing_time: "1 second"]) do
    StreamReader.rate(session, rows_per_second: 5)
    |> DataFrame.write_stream()
    |> StreamWriter.format("memory")
    |> StreamWriter.output_mode("append")
    |> StreamWriter.query_name(query_name)
    |> StreamWriter.trigger(trigger_opts)
    |> StreamWriter.start()
  end

  defp assert_query_eventually_has_progress(query, deadline_ms) when deadline_ms > 0 do
    case StreamingQuery.last_progress(query) do
      {:ok, json} when is_binary(json) and json != "" ->
        :ok

      _ ->
        Process.sleep(200)
        assert_query_eventually_has_progress(query, deadline_ms - 200)
    end
  end

  defp assert_query_eventually_has_progress(_query, _deadline_ms) do
    flunk("streaming query never produced progress")
  end

  @tag min_spark: "4.0"
  test "listener management parity (v1/v2) with real stream events", %{session: session, bus: bus} do
    assert StreamingQueryListenerBus.list_listeners(bus) == []

    :ok = StreamingQueryListenerBus.add_listener(bus, ListenerLocalV1)
    :ok = StreamingQueryListenerBus.add_listener(bus, ListenerLocalV2)
    assert length(StreamingQueryListenerBus.list_listeners(bus)) == 2

    {:ok, query} =
      start_rate_query(session, "listener_parity_#{System.unique_integer([:positive])}")

    on_exit(fn -> StreamingQuery.stop(query) end)

    # QueryStarted is expected to be dispatched before start() returns.
    assert_receive {:listener_v1, :started, %{type: :started}}, 2_000
    assert_receive {:listener_v2, :started, %{type: :started}}, 2_000

    assert_query_eventually_has_progress(query, 20_000)
    assert_receive {:listener_v1, :progress, %{type: :progress}}, 10_000
    assert_receive {:listener_v2, :progress, %{type: :progress}}, 10_000

    :ok = StreamingQueryListenerBus.remove_listener(bus, ListenerLocalV1)
    :ok = StreamingQueryListenerBus.remove_listener(bus, ListenerLocalV2)

    # Add listener back and verify terminated event is delivered for stopped query.
    :ok = StreamingQueryListenerBus.add_listener(bus, ListenerLocalV1)

    :ok = StreamingQuery.stop(query)
    assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 20_000)

    assert_receive {:listener_v1, :terminated, %{type: :terminated}}, 20_000
    refute_receive {:listener_v2, :terminated, _}, 1_500

    :ok = StreamingQueryListenerBus.remove_listener(bus, ListenerLocalV1)
    assert StreamingQueryListenerBus.list_listeners(bus) == []
  end

  @tag min_spark: "4.0"
  test "bad listener exceptions do not block good listener", %{session: session, bus: bus} do
    :ok = StreamingQueryListenerBus.add_listener(bus, ListenerLocalV2)
    :ok = StreamingQueryListenerBus.add_listener(bus, BadListener)

    {:ok, query} = start_rate_query(session, "listener_bad_#{System.unique_integer([:positive])}")
    on_exit(fn -> StreamingQuery.stop(query) end)

    assert_receive {:listener_v2, :started, %{type: :started}}, 5_000
    assert_query_eventually_has_progress(query, 20_000)
    assert_receive {:listener_v2, :progress, %{type: :progress}}, 10_000

    :ok = StreamingQuery.stop(query)
    assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 20_000)
    assert_receive {:listener_v2, :terminated, %{type: :terminated}}, 20_000
  end
end
