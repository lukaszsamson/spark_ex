defmodule SparkEx.Integration.StreamingListenerTest do
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

  defmodule TestListener do
    @behaviour SparkEx.StreamingQueryListener
    @listener_pid_key {__MODULE__, :listener_pid}

    def on_query_progress(event) do
      if pid = :persistent_term.get(@listener_pid_key, nil), do: send(pid, {:listener_progress, event})
    end

    def on_query_terminated(event) do
      if pid = :persistent_term.get(@listener_pid_key, nil), do: send(pid, {:listener_terminated, event})
    end

    def on_query_idle(event) do
      if pid = :persistent_term.get(@listener_pid_key, nil), do: send(pid, {:listener_idle, event})
    end

    def set_listener_pid(pid) when is_pid(pid) do
      :persistent_term.put(@listener_pid_key, pid)
    end

    def clear_listener_pid do
      :persistent_term.erase(@listener_pid_key)
    end
  end

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "listener bus lifecycle and progress events", %{session: session} do
    TestListener.set_listener_pid(self())
    on_exit(fn -> TestListener.clear_listener_pid() end)

    {:ok, bus} = StreamingQueryListenerBus.start_link(session)
    :ok = StreamingQueryListenerBus.add_listener(bus, TestListener)
    send(bus, {:listener_event, %{type: :progress, raw_json: "{}", data: %{}}})
    assert_receive {:listener_progress, %{type: :progress, raw_json: "{}"}}, 5_000

    df = StreamReader.rate(session, rows_per_second: 5)

    writer =
      df
      |> DataFrame.write_stream()
      |> StreamWriter.format("memory")
      |> StreamWriter.output_mode("append")
      |> StreamWriter.query_name("listener_test_#{System.unique_integer([:positive])}")
      |> StreamWriter.trigger(processing_time: "1 second")

    {:ok, query} = StreamWriter.start(writer)
    on_exit(fn -> StreamingQuery.stop(query) end)

    Process.sleep(1_500)
    :ok = StreamingQuery.stop(query)
    assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 20_000)

    assert {:ok, listeners} = StreamingQueryManager.list_listeners(session)
    assert is_list(listeners)

    :ok = StreamingQueryListenerBus.remove_listener(bus, TestListener)
    :ok = StreamingQueryListenerBus.stop(bus)
  end

  test "streaming query exposes id/run_id/name", %{session: session} do
    name = "listener_props_#{System.unique_integer([:positive])}"

    df = StreamReader.rate(session, rows_per_second: 1)

    writer =
      df
      |> DataFrame.write_stream()
      |> StreamWriter.format("memory")
      |> StreamWriter.output_mode("append")
      |> StreamWriter.query_name(name)
      |> StreamWriter.trigger(once: true)

    {:ok, query} = StreamWriter.start(writer)
    on_exit(fn -> StreamingQuery.stop(query) end)

    assert is_binary(StreamingQuery.id(query))
    assert is_binary(StreamingQuery.run_id(query))
    assert StreamingQuery.name(query) == name

    assert {:ok, _} = StreamingQuery.await_termination(query, timeout: 20_000)
  end
end
