defmodule SparkEx.Wave5SmallTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.Stat

  # ── T-55: Stat.freq_items support validation ──

  describe "Stat.freq_items/3 support validation" do
    defp make_df do
      DataFrame.new(:test_session, :test_plan)
    end

    test "default support (0.01) is accepted" do
      df = Stat.freq_items(make_df(), ["category"])
      assert {:stat_freq_items, :test_plan, ["category"], 0.01} = unwrap_plan(df)
    end

    test "accepts support exactly at the 1e-4 threshold (server bound is >=)" do
      assert {:stat_freq_items, :test_plan, ["category"], 1.0e-4} =
               unwrap_plan(Stat.freq_items(make_df(), ["category"], 1.0e-4))
    end

    test "raises for support below the 1e-4 threshold" do
      assert_raise ArgumentError, ~r/support must be at least/, fn ->
        Stat.freq_items(make_df(), ["category"], 1.0e-5)
      end
    end

    test "raises for support below threshold via keyword option" do
      assert_raise ArgumentError, ~r/support must be at least/, fn ->
        Stat.freq_items(make_df(), ["category"], support: 0.0)
      end
    end

    test "accepts support just above the threshold" do
      df = Stat.freq_items(make_df(), ["category"], 2.0e-4)
      assert {:stat_freq_items, :test_plan, ["category"], support} = unwrap_plan(df)
      assert_in_delta support, 2.0e-4, 1.0e-9
    end

    test "still rejects support above 1.0" do
      assert_raise ArgumentError, ~r/support must be between 0 and 1/, fn ->
        Stat.freq_items(make_df(), ["category"], 1.5)
      end
    end
  end

  # ── T-28: UDTF registration deterministic defaults to false ──

  defmodule FakeUDTFSession do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid, [])

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:execute_command, command, _opts}, _from, test_pid) do
      send(test_pid, {:execute_command_called, command})
      {:reply, :ok, test_pid}
    end
  end

  describe "UDFRegistration.register_udtf/4 deterministic default" do
    test "defaults to false when :deterministic is omitted" do
      {:ok, session} = FakeUDTFSession.start_link(self())

      assert :ok =
               SparkEx.UDFRegistration.register_udtf(
                 session,
                 "my_udtf",
                 <<1, 2, 3>>,
                 eval_type: 300,
                 python_ver: "3.11"
               )

      assert_receive {:execute_command_called,
                      {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 300, "3.11", false}}
    end

    test "honors an explicit :deterministic true" do
      {:ok, session} = FakeUDTFSession.start_link(self())

      assert :ok =
               SparkEx.UDFRegistration.register_udtf(
                 session,
                 "my_udtf",
                 <<1, 2, 3>>,
                 eval_type: 300,
                 python_ver: "3.11",
                 deterministic: true
               )

      assert_receive {:execute_command_called,
                      {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 300, "3.11", true}}
    end
  end

  # ── T-60: on_query_idle is an optional callback ──

  defmodule IdleOptionalListener do
    @behaviour SparkEx.StreamingQueryListener

    @impl true
    def on_query_progress(_event), do: :ok

    @impl true
    def on_query_terminated(_event), do: :ok

    # Deliberately no on_query_idle/1 — must compile without warning since
    # it's an optional callback, and the bus must skip it silently.
  end

  defmodule IdleImplementingListener do
    @behaviour SparkEx.StreamingQueryListener

    @impl true
    def on_query_progress(_event), do: :ok

    @impl true
    def on_query_terminated(_event), do: :ok

    @impl true
    def on_query_idle(event) do
      send(Process.whereis(:wave5_idle_test_pid), {:idle_received, event})
      :ok
    end
  end

  describe "StreamingQueryListener optional on_query_idle callback" do
    test "on_query_idle and on_query_started are listed as optional callbacks" do
      optional = SparkEx.StreamingQueryListener.behaviour_info(:optional_callbacks)
      assert {:on_query_idle, 1} in optional
      assert {:on_query_started, 1} in optional
    end

    test "on_query_idle stays in the full callback list" do
      callbacks = SparkEx.StreamingQueryListener.behaviour_info(:callbacks)
      assert {:on_query_idle, 1} in callbacks
    end

    test "a listener without on_query_idle still implements the other callbacks" do
      assert :ok =
               IdleOptionalListener.on_query_progress(%{
                 type: :progress,
                 data: %{},
                 raw_json: "{}"
               })

      assert :ok =
               IdleOptionalListener.on_query_terminated(%{
                 type: :terminated,
                 data: %{},
                 raw_json: "{}"
               })

      refute function_exported?(IdleOptionalListener, :on_query_idle, 1)
    end

    test "the bus dispatches on_query_idle only when the listener implements it" do
      Process.register(self(), :wave5_idle_test_pid)

      on_exit(fn ->
        try do
          Process.unregister(:wave5_idle_test_pid)
        rescue
          _ -> :ok
        end
      end)

      {:ok, bus} = SparkEx.StreamingQueryListenerBus.start_link(:idle_test_session)

      on_exit(fn ->
        if Process.alive?(bus), do: SparkEx.StreamingQueryListenerBus.stop(bus)
      end)

      # Directly exercise the cast-based dispatch path used for idle events.
      send(
        bus,
        {:"$gen_cast",
         {:dispatch_event, %{type: :idle, data: %{"id" => "q1"}, raw_json: ~s({"id":"q1"})}}}
      )

      # Add both listeners so we can confirm the implementing one gets the
      # event while the non-implementing one is silently skipped (no crash,
      # no warning-triggering exception).
      :sys.replace_state(bus, fn state ->
        %{state | listeners: [IdleOptionalListener, IdleImplementingListener]}
      end)

      send(
        bus,
        {:"$gen_cast",
         {:dispatch_event, %{type: :idle, data: %{"id" => "q2"}, raw_json: ~s({"id":"q2"})}}}
      )

      assert_receive {:idle_received, %{data: %{"id" => "q2"}}}
      refute_receive {:idle_received, %{data: %{"id" => "q1"}}}, 50
    end
  end
end
