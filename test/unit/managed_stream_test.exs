defmodule SparkEx.ManagedStreamTest do
  use ExUnit.Case, async: true

  alias SparkEx.ManagedStream

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
end
