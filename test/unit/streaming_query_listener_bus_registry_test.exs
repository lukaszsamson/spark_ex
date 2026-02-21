defmodule SparkEx.StreamingQueryListenerBusRegistryTest do
  use ExUnit.Case, async: false

  alias SparkEx.StreamingQueryListenerBus.Registry

  @registry_table :spark_ex_streaming_listener_buses

  setup do
    :ok = Registry.ensure_started()
    SparkEx.EtsTableOwner.ensure_table!(@registry_table, :bag)
    :ets.delete_all_objects(@registry_table)
    :ok
  end

  test "buses_for_session is read-only and preserves registered entries" do
    session = {:session, make_ref()}
    pid = spawn(fn -> Process.sleep(:infinity) end)

    :ok = Registry.register(session, pid)

    assert Registry.buses_for_session(session) == [pid]
    assert Registry.buses_for_session(session) == [pid]

    assert [{^session, ^pid, _ref}] = :ets.lookup(@registry_table, session)

    :ok = Registry.unregister(session, pid)
    Process.exit(pid, :kill)
  end

  test "dead buses are removed via monitor-driven DOWN cleanup" do
    session = {:session, make_ref()}
    pid = spawn(fn -> Process.sleep(:infinity) end)

    :ok = Registry.register(session, pid)
    assert Registry.buses_for_session(session) == [pid]

    Process.exit(pid, :kill)

    assert_eventually(fn ->
      Registry.buses_for_session(session) == []
    end)
  end

  defp assert_eventually(fun, attempts \\ 20)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      assert true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end
end
