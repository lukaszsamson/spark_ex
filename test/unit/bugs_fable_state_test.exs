defmodule SparkEx.BugsFableStateTest do
  @moduledoc """
  Regression tests for FABLE-28/29/31/51/52/53 — observation routing &
  lifecycle, plan-id allocator name resolution, and ETS reclamation.
  """
  use ExUnit.Case, async: false

  alias SparkEx.{Observation, UserContextExtensions}
  alias SparkEx.Internal.PlanIds

  @obs_table :spark_ex_observations
  @uce_table :spark_ex_user_context_extensions
  @planid_table :spark_ex_plan_id_counters

  setup do
    SparkEx.EtsTableOwner.ensure_table!(@obs_table, :set)
    SparkEx.EtsTableOwner.ensure_table!(@uce_table, :set)
    :ets.delete_all_objects(@obs_table)
    :ok
  end

  describe "FABLE-28 same-named observation collision" do
    test "second live same-named observation in the same session is refused" do
      a = Observation.new("m")
      b = Observation.new("m")

      assert :ok = Observation.register_observation(a, [{:alias, :_, "total"}], "sess-1")

      assert_raise ArgumentError, ~r/AMBIGUOUS_OBSERVATION/, fn ->
        Observation.register_observation(b, [{:alias, :_, "total"}], "sess-1")
      end

      # b's failed attach left no attached marker behind (rolled back).
      refute :ets.member(@obs_table, {:obs_attached, b.id})
    end

    test "same name in different sessions does not collide" do
      a = Observation.new("m")
      b = Observation.new("m")

      assert :ok = Observation.register_observation(a, [{:alias, :_, "total"}], "sess-A")
      assert :ok = Observation.register_observation(b, [{:alias, :_, "total"}], "sess-B")

      Observation.store_observed_metrics(%{"m" => %{"total" => 1}}, "sess-A")
      Observation.store_observed_metrics(%{"m" => %{"total" => 2}}, "sess-B")

      assert Observation.get(a) == %{"total" => 1}
      assert Observation.get(b) == %{"total" => 2}
    end
  end

  describe "FABLE-29 observation reclamation" do
    test "clear/2 removes every row owned by an observation" do
      obs = Observation.new("c")
      Observation.register_observation(obs, [{:alias, :_, "total"}], "sess-c")
      Observation.store_observed_metrics(%{"c" => %{"total" => 5}}, "sess-c")

      assert Observation.get(obs) == %{"total" => 5}

      assert :ok = Observation.clear(obs, "sess-c")

      refute :ets.member(@obs_table, {:obs, obs.id})
      refute :ets.member(@obs_table, {:obs_attached, obs.id})
      refute :ets.member(@obs_table, {:obs_aliases, obs.id})
      refute :ets.member(@obs_table, {:obs_session, "sess-c", obs.id})
      refute :ets.member(@obs_table, {:obs_route, "sess-c", "c"})
    end

    test "clear_session/1 reclaims all observations for a session" do
      o1 = Observation.new("a")
      o2 = Observation.new("b")
      Observation.register_observation(o1, [{:alias, :_, "x"}], "sess-z")
      Observation.register_observation(o2, [{:alias, :_, "y"}], "sess-z")
      # A different session's rows must survive.
      other = Observation.new("a")
      Observation.register_observation(other, [{:alias, :_, "x"}], "sess-other")

      assert :ok = Observation.clear_session("sess-z")

      refute :ets.member(@obs_table, {:obs_attached, o1.id})
      refute :ets.member(@obs_table, {:obs_attached, o2.id})
      assert :ets.member(@obs_table, {:obs_attached, other.id})
    end
  end

  describe "FABLE-51 observation semantics" do
    test "get/1 returns empty map after attach, before any action" do
      obs = Observation.new("pending")
      Observation.register_observation(obs, [{:alias, :_, "total"}])
      assert Observation.get(obs) == %{}
    end

    test "re-execution updates metrics (last execution wins, merge)" do
      obs = Observation.new("upd")
      Observation.register_observation(obs, [], "s")
      Observation.store_observed_metrics(%{"upd" => %{"a" => 1}}, "s")
      Observation.store_observed_metrics(%{"upd" => %{"a" => 2, "b" => 3}}, "s")
      # last value of "a" wins; new key "b" is merged in
      assert Observation.get(obs) == %{"a" => 2, "b" => 3}
    end

    test "never-attached observation still raises NO_OBSERVE_BEFORE_GET" do
      obs = Observation.new("never")

      assert_raise ArgumentError, ~r/NO_OBSERVE_BEFORE_GET/, fn ->
        Observation.get(obs)
      end
    end
  end

  describe "FABLE-31 named session plan-id allocation" do
    test "next/1 resolves an atom-registered process to its shared atomic counter" do
      name = :"plan_ids_named_#{System.unique_integer([:positive])}"

      {:ok, pid} = Agent.start_link(fn -> :ok end, name: name)
      on_exit(fn -> if Process.alive?(pid), do: Agent.stop(pid) end)

      ref = PlanIds.register_session(pid)
      on_exit(fn -> PlanIds.unregister_session(pid) end)

      # Drawing via the atom name must advance the SAME atomic the pid uses,
      # not a per-process fallback counter.
      id_via_name = PlanIds.next(name)
      id_via_pid = PlanIds.next(pid)

      assert id_via_name == 0
      assert id_via_pid == 1
      # The atomic now reflects two allocations.
      assert :atomics.get(ref, 1) == 2
    end
  end

  describe "FABLE-53 plan-id counter row reclamation" do
    test "unregister_session removes the {pid, ref} row" do
      {:ok, pid} = Agent.start_link(fn -> :ok end)
      PlanIds.register_session(pid)

      assert [{^pid, _ref}] = :ets.lookup(@planid_table, pid)

      Agent.stop(pid)
      PlanIds.unregister_session(pid)

      assert :ets.lookup(@planid_table, pid) == []
    end

    test "EtsTableOwner monitor sweeps the row when a registered pid dies" do
      {:ok, pid} = Agent.start_link(fn -> :ok end)
      PlanIds.register_session(pid)
      assert [{^pid, _}] = :ets.lookup(@planid_table, pid)

      ref = Process.monitor(pid)
      Agent.stop(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

      # Give the owner GenServer a moment to process the :DOWN sweep.
      wait_until(fn -> :ets.lookup(@planid_table, pid) == [] end)
      assert :ets.lookup(@planid_table, pid) == []
    end
  end

  describe "FABLE-52 thread-local user-context extension reclamation" do
    test "rows are swept when the registering process dies" do
      type_url = "type.googleapis.com/fable.test.#{System.unique_integer([:positive])}"
      ext = %Google.Protobuf.Any{type_url: type_url, value: <<1, 2, 3>>}

      parent = self()

      {:ok, pid} =
        Task.start(fn ->
          UserContextExtensions.add_threadlocal_user_context_extension(ext)
          send(parent, :registered)
          # stay alive until told
          receive do
            :stop -> :ok
          end
        end)

      assert_receive :registered, 1_000

      scope = {:pid, pid}
      assert [{{^scope, ^type_url}, _}] = :ets.lookup(@uce_table, {scope, type_url})

      mon = Process.monitor(pid)
      send(pid, :stop)
      assert_receive {:DOWN, ^mon, :process, ^pid, _}, 1_000

      wait_until(fn -> :ets.lookup(@uce_table, {scope, type_url}) == [] end)
      assert :ets.lookup(@uce_table, {scope, type_url}) == []
    end
  end

  defp wait_until(fun, attempts \\ 50)
  defp wait_until(_fun, 0), do: :ok

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
