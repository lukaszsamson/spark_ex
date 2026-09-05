defmodule SparkEx.Wave6EncoderBusTest do
  # async: false — the bus tests register a named process and tweak
  # application env for the drain timeout.
  use ExUnit.Case, async: false

  alias Spark.Connect.Expression
  alias Spark.Connect.ExecutePlanResponse
  alias Spark.Connect.Plan
  alias Spark.Connect.Relation
  alias Spark.Connect.SQL
  alias Spark.Connect.StreamingQueryListenerEvent
  alias Spark.Connect.StreamingQueryListenerEventsResult
  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.StreamingQueryListenerBus

  # ── T-29(a): rewrite_expr recurses into direct_shuffle_partition_id ──

  describe "T-29(a) cross-plan rewrite of direct_shuffle_partition_id" do
    test "a column of another DataFrame inside the child is hoisted into with_relations" do
      child = {:plan_id, 400, {:sql, "SELECT * FROM t", nil}}
      referenced = {:plan_id, 401, {:sql, "SELECT * FROM other", nil}}

      col = {:alias, {:direct_shuffle_partition_id, {:col, "x", referenced}}, "p"}

      {plan, _} = PlanEncoder.encode({:project, child, [col]}, 0)
      assert %Plan{op_type: {:root, root}} = plan

      assert {:with_relations, wr} = root.rel_type,
             "direct_shuffle_partition_id must hoist the referenced plan"

      assert wr.references != []

      # ... and the inner column now carries the numeric plan_id.
      assert %Relation{rel_type: {:project, project}} = wr.root

      assert [
               %Expression{
                 expr_type:
                   {:alias,
                    %Expression.Alias{
                      expr: %Expression{
                        expr_type:
                          {:direct_shuffle_partition_id,
                           %Expression.DirectShufflePartitionID{child: inner}}
                      }
                    }}
               }
             ] = project.expressions

      assert %Expression{
               expr_type:
                 {:unresolved_attribute,
                  %Expression.UnresolvedAttribute{unparsed_identifier: "x", plan_id: plan_id}}
             } = inner

      assert is_integer(plan_id)
    end

    test "a plain (non cross-plan) child is encoded unchanged" do
      child = {:sql, "SELECT * FROM t", nil}
      plan = {:project, child, [{:direct_shuffle_partition_id, {:col, "x"}}]}
      assert {%Plan{}, _} = PlanEncoder.encode(plan, 0)
    end
  end

  # ── T-29(b): SQL arguments accept every expression tuple ──

  describe "T-29(b) encode_sql_argument delegates known expression tuples" do
    defp sql_args(args) do
      {plan, _} = PlanEncoder.encode({:sql, "SELECT ?", args}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, %SQL{} = sql}}}} = plan
      sql.pos_arguments
    end

    defp sql_named_args(args) do
      {plan, _} = PlanEncoder.encode({:sql, "SELECT :a", args}, 0)
      assert %Plan{op_type: {:root, %Relation{rel_type: {:sql, %SQL{} = sql}}}} = plan
      sql.named_arguments
    end

    test "call_function is encoded as an expression, not a literal" do
      assert [%Expression{expr_type: {:call_function, f}}] =
               sql_args([{:call_function, "my_udf", [{:lit, 1}]}])

      assert f.function_name == "my_udf"
    end

    test "named_arg, outer and update_fields are encoded as expressions" do
      assert [%Expression{expr_type: {:named_argument_expression, _}}] =
               sql_args([{:named_arg, "k", {:lit, 1}}])

      # {:outer, child} encodes as its child.
      assert [%Expression{expr_type: {:unresolved_attribute, %{unparsed_identifier: "x"}}}] =
               sql_args([{:outer, {:col, "x"}}])

      assert [%Expression{expr_type: {:update_fields, _}}] =
               sql_args([{:update_fields, {:col, "s"}, "f", {:lit, 1}}])
    end

    test "direct_shuffle_partition_id is encoded as an expression" do
      assert [%Expression{expr_type: {:direct_shuffle_partition_id, %{child: child}}}] =
               sql_args([{:direct_shuffle_partition_id, {:col, "x"}}])

      assert %Expression{expr_type: {:unresolved_attribute, %{unparsed_identifier: "x"}}} = child
    end

    test "alias with metadata is encoded as an expression" do
      assert [%Expression{expr_type: {:alias, a}}] =
               sql_args([{:alias, {:col, "x"}, "y", ~s({"c":1})}])

      assert a.name == ["y"]
    end

    test "named arguments go through the same path" do
      assert %{"a" => %Expression{expr_type: {:call_function, _}}} =
               sql_named_args(%{a: {:call_function, "my_udf", []}})
    end

    test "literals and unknown tuples still encode as literals" do
      assert [
               %Expression{
                 expr_type: {:literal, %Expression.Literal{literal_type: {:integer, 7}}}
               }
             ] =
               sql_args([{:lit, 7}])

      assert [%Expression{expr_type: {:literal, _}}] = sql_args([42])
      assert [%Expression{expr_type: {:literal, _}}] = sql_args(["s"])

      # An arbitrary tuple is not an expression tuple: it still goes down the
      # literal path (which rejects it), exactly as before.
      assert_raise ArgumentError, fn -> sql_args([{:not_an_expr, 1, 2}]) end
    end
  end

  # ── T-63: draining the reader when the last listener is removed ──

  @listener_sink :wave6_bus_listener_sink

  defmodule SinkListener do
    @behaviour SparkEx.StreamingQueryListener

    @impl true
    def on_query_progress(event), do: notify({:progress, event})
    @impl true
    def on_query_terminated(event), do: notify({:terminated, event})
    @impl true
    def on_query_idle(event), do: notify({:idle, event})

    defp notify(msg) do
      case Process.whereis(:wave6_bus_listener_sink) do
        nil -> :ok
        pid -> send(pid, msg)
      end

      :ok
    end
  end

  # A fake session whose reader stream registers, then blocks until the reader
  # task is told to emit events / EOF. The `:remove` command is what triggers
  # the in-flight event + EOF, mirroring the server closing the stream in
  # response to the removal.
  defmodule DrainFakeSession do
    use GenServer

    def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(opts), do: {:ok, Map.merge(%{reader: nil, on_remove: :eof}, opts)}

    @impl true
    def handle_call({:execute_command_stream, _command, _opts}, {reader, _tag}, state) do
      stream =
        Stream.resource(
          fn -> :added end,
          fn
            :added ->
              response = %ExecutePlanResponse{
                response_type:
                  {:streaming_query_listener_events_result,
                   %StreamingQueryListenerEventsResult{
                     listener_bus_listener_added: true,
                     events: []
                   }}
              }

              {[{:ok, response}], :running}

            :running ->
              receive do
                {:emit, resp} -> {[{:ok, resp}], :running}
                :eof -> {:halt, :running}
              end
          end,
          fn _ -> :ok end
        )

      {:reply, {:ok, stream}, %{state | reader: reader}}
    end

    def handle_call({:execute_command_with_result, _command, _opts}, _from, state) do
      if state.on_remove == :eof and is_pid(state.reader) do
        send(state.reader, {:emit, progress_response()})
        send(state.reader, :eof)
      end

      {:reply, {:ok, {:streaming_query_manager, %{}}}, state}
    end

    def handle_call(:reader, _from, state), do: {:reply, state.reader, state}

    defp progress_response do
      %ExecutePlanResponse{
        response_type:
          {:streaming_query_listener_events_result,
           %StreamingQueryListenerEventsResult{
             listener_bus_listener_added: false,
             events: [
               %StreamingQueryListenerEvent{
                 event_type: :QUERY_PROGRESS_EVENT,
                 event_json: ~s({"id":"q1"})
               }
             ]
           }}
      }
    end
  end

  defp start_bus(opts \\ []) do
    {:ok, session} = DrainFakeSession.start_link(opts)
    {:ok, bus} = StreamingQueryListenerBus.start_link(session)
    on_exit(fn -> if Process.alive?(bus), do: StreamingQueryListenerBus.stop(bus) end)
    {session, bus}
  end

  setup do
    Process.register(self(), @listener_sink)
    prev = Application.get_env(:spark_ex, :listener_bus_drain_timeout_ms)

    on_exit(fn ->
      if prev do
        Application.put_env(:spark_ex, :listener_bus_drain_timeout_ms, prev)
      else
        Application.delete_env(:spark_ex, :listener_bus_drain_timeout_ms)
      end
    end)

    :ok
  end

  describe "T-63 last-listener removal drains the reader stream" do
    test "an in-flight event delivered before EOF still reaches the listener" do
      Application.put_env(:spark_ex, :listener_bus_drain_timeout_ms, 2_000)
      {_session, bus} = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, SinkListener)
      assert %{stream_task: task} = :sys.get_state(bus)
      assert is_pid(task)

      # The remove command makes the fake server emit one last event and close
      # the stream; the drain must dispatch it before killing the reader.
      :ok = StreamingQueryListenerBus.remove_listener(bus, SinkListener)

      assert_received {:progress, %{data: %{"id" => "q1"}}}

      state = :sys.get_state(bus)
      assert state.stream_task == nil
      assert state.stream_token == nil
      refute Process.alive?(task)
    end

    test "the reader is killed after the drain timeout when the stream never ends" do
      Application.put_env(:spark_ex, :listener_bus_drain_timeout_ms, 60)
      {_session, bus} = start_bus(on_remove: :never)
      :ok = StreamingQueryListenerBus.add_listener(bus, SinkListener)
      assert %{stream_task: task} = :sys.get_state(bus)

      started = System.monotonic_time(:millisecond)
      :ok = StreamingQueryListenerBus.remove_listener(bus, SinkListener)
      elapsed = System.monotonic_time(:millisecond) - started

      assert elapsed >= 55, "removal must wait for the drain timeout, waited #{elapsed}ms"
      assert elapsed < 2_000
      refute Process.alive?(task)
      assert :sys.get_state(bus).stream_task == nil
    end

    test "a stale stream-ended message after removal does not trigger a reconnect" do
      Application.put_env(:spark_ex, :listener_bus_drain_timeout_ms, 60)
      {_session, bus} = start_bus(on_remove: :never)
      :ok = StreamingQueryListenerBus.add_listener(bus, SinkListener)
      old_token = :sys.get_state(bus).stream_token

      :ok = StreamingQueryListenerBus.remove_listener(bus, SinkListener)
      assert :sys.get_state(bus).stream_task == nil

      # Re-add a listener: a fresh reader with a fresh token starts.
      :ok = StreamingQueryListenerBus.add_listener(bus, SinkListener)
      new_state = :sys.get_state(bus)
      assert new_state.stream_token != old_token
      new_task = new_state.stream_task
      assert is_pid(new_task)

      # A late EOF from the killed reader must be ignored: it must neither tear
      # down the new reader nor schedule a reconnect.
      send(bus, {:listener_stream_ended, old_token, :normal})
      # Round-trip through the bus so the message above is processed.
      _ = StreamingQueryListenerBus.list_listeners(bus)

      after_state = :sys.get_state(bus)
      assert after_state.stream_task == new_task
      assert after_state.reconnect_attempts == 0
      assert Process.alive?(new_task)
    end
  end
end
