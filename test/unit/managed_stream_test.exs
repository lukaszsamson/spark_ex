defmodule SparkEx.ManagedStreamTest do
  use ExUnit.Case, async: true

  alias SparkEx.ManagedStream
  import ExUnit.CaptureLog

  test "enumeration closes and releases execute state" do
    parent = self()

    {:ok, stream} =
      ManagedStream.new([1, 2, 3],
        release_fun: fn _opts ->
          send(parent, :released)
          {:ok, :released}
        end
      )

    assert Enum.to_list(stream) == [1, 2, 3]
    assert_receive :released, 500
    :ok = ManagedStream.close(stream)
    refute_receive :released, 50
  end

  test "explicit close releases execute state" do
    parent = self()

    {:ok, stream} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end),
        release_fun: fn _opts ->
          send(parent, :released_explicit)
          {:ok, :released}
        end
      )

    :ok = ManagedStream.close(stream)
    assert_receive :released_explicit, 500
  end

  test "closing one stream does not release another stream's execution" do
    parent = self()

    release = fn label ->
      fn _opts ->
        send(parent, {:released, label})
        {:ok, label}
      end
    end

    {:ok, first} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end), release_fun: release.(:first))

    {:ok, second} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end), release_fun: release.(:second))

    :ok = ManagedStream.close(first)
    assert_receive {:released, :first}, 500
    refute_receive {:released, :second}, 100

    :ok = ManagedStream.close(second)
    assert_receive {:released, :second}, 500
  end

  test "owner exit triggers release cleanup" do
    parent = self()
    owner = spawn(fn -> Process.sleep(:infinity) end)

    {:ok, _stream} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end),
        owner: owner,
        release_fun: fn _opts ->
          send(parent, :released_owner_down)
          {:ok, :released}
        end
      )

    Process.exit(owner, :kill)
    assert_receive :released_owner_down, 1_000
  end

  test "idle timeout triggers release cleanup" do
    parent = self()

    {:ok, _stream} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end),
        idle_timeout: 50,
        release_fun: fn _opts ->
          send(parent, :released_idle_timeout)
          {:ok, :released}
        end
      )

    assert_receive :released_idle_timeout, 1_000
  end

  test "explicit close returns without waiting for release completion" do
    parent = self()

    {:ok, stream} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end),
        release_timeout: 50,
        release_fun: fn _opts ->
          Process.sleep(200)
          send(parent, :released_after_delay)
          {:ok, :released}
        end
      )

    start_time = System.monotonic_time(:millisecond)
    assert :ok = ManagedStream.close(stream)
    duration = System.monotonic_time(:millisecond) - start_time

    assert duration < 100
    refute_receive :released_after_delay, 120
  end

  test "timed out release emits telemetry and does not block close" do
    parent = self()
    handler_id = "managed-stream-timeout-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:spark_ex, :managed_stream, :release_failed],
        fn event, measurements, metadata, _config ->
          send(parent, {:release_failed, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, stream} =
      ManagedStream.new(Stream.repeatedly(fn -> :row end),
        release_timeout: 10,
        release_fun: fn _opts ->
          Process.sleep(100)
          {:ok, :released}
        end
      )

    log =
      capture_log(fn ->
        assert :ok = ManagedStream.close(stream)

        assert_receive {:release_failed, [:spark_ex, :managed_stream, :release_failed], %{},
                        %{reason: :timeout, timeout_ms: 10}},
                       500
      end)

    assert log =~ "managed stream release failed: :timeout"
  end
end
