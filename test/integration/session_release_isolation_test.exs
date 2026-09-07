defmodule SparkEx.Integration.SessionReleaseIsolationTest do
  use ExUnit.Case

  alias SparkEx.{ManagedStream, Session}

  @moduletag :integration

  test "stopping one session preserves another session's queued release worker" do
    url = System.fetch_env!("SPARK_REMOTE")
    {:ok, first} = SparkEx.connect(url: url)
    {:ok, second} = SparkEx.connect(url: url)
    Process.unlink(first)
    Process.unlink(second)

    on_exit(fn ->
      Session.stop(first)
      Session.stop(second)
    end)

    parent = self()

    {:ok, stream} =
      ManagedStream.new([],
        owner: second,
        release_fun: fn _opts ->
          send(parent, {:release_started, self()})
          receive do: (:finish_release -> {:ok, :released})
        end
      )

    assert :ok = ManagedStream.close(stream)
    assert_receive {:release_started, worker}, 1_000
    monitor = Process.monitor(worker)
    assert :ok = Session.stop(first)
    assert Process.alive?(worker)
    send(worker, :finish_release)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, 1_000

    assert {:ok, [%{"alive" => 1}]} =
             second |> SparkEx.sql("SELECT 1 AS alive") |> SparkEx.DataFrame.collect()
  end
end
