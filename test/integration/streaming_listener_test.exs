defmodule SparkEx.Integration.StreamingListenerTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, StreamReader, StreamWriter, StreamingQuery, StreamingQueryListenerBus}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  defmodule TestListener do
    @behaviour SparkEx.StreamingQueryListener

    def on_query_progress(event) do
      if pid = Process.get(:listener_pid), do: send(pid, {:listener_progress, event})
    end

    def on_query_terminated(event) do
      if pid = Process.get(:listener_pid), do: send(pid, {:listener_terminated, event})
    end

    def on_query_idle(event) do
      if pid = Process.get(:listener_pid), do: send(pid, {:listener_idle, event})
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
    Process.put(:listener_pid, self())

    {:ok, bus} = StreamingQueryListenerBus.start_link(session)
    :ok = StreamingQueryListenerBus.add_listener(bus, TestListener)

    df = StreamReader.rate(session, rows_per_second: 5)

    writer =
      df
      |> DataFrame.write_stream()
      |> StreamWriter.format("memory")
      |> StreamWriter.output_mode("append")
      |> StreamWriter.query_name("listener_test_#{System.unique_integer([:positive])}")
      |> StreamWriter.trigger(once: true)

    {:ok, query} = StreamWriter.start(writer)
    on_exit(fn -> StreamingQuery.stop(query) end)

    assert {:ok, _} = StreamingQuery.await_termination(query, timeout: 20_000)

    receive do
      {:listener_progress, event} ->
        assert event.type == :progress
        assert is_binary(event.raw_json)
    after
      10_000 ->
        assert StreamingQueryListenerBus.list_listeners(bus) != []
    end

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
