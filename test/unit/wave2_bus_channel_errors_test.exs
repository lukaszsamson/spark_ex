defmodule SparkEx.Wave2BusChannelErrorsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Channel
  alias SparkEx.Error.Remote
  alias SparkEx.StreamingQueryListenerBus

  # A fake session serving a never-ending listener event stream that registers
  # successfully, so the bus keeps a stable reader task we can assert against.
  defmodule StreamingFakeSession do
    use GenServer

    def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, %{}}

    @impl true
    def handle_call({:execute_command_stream, _command, _opts}, _from, state) do
      stream =
        Stream.resource(
          fn -> :added end,
          fn
            :added ->
              response = %Spark.Connect.ExecutePlanResponse{
                response_type:
                  {:streaming_query_listener_events_result,
                   %Spark.Connect.StreamingQueryListenerEventsResult{
                     listener_bus_listener_added: true,
                     events: []
                   }}
              }

              {[{:ok, response}], :idle}

            :idle ->
              Process.sleep(:infinity)
              {:halt, :ok}
          end,
          fn _ -> :ok end
        )

      {:reply, {:ok, stream}, state}
    end

    @impl true
    def handle_call({:execute_command_with_result, _command, _opts}, _from, state) do
      {:reply, {:ok, {:streaming_query_manager, %{}}}, state}
    end
  end

  defmodule NoopListener do
    @behaviour SparkEx.StreamingQueryListener

    @impl true
    def on_query_progress(_event), do: :ok
    @impl true
    def on_query_terminated(_event), do: :ok
    @impl true
    def on_query_idle(_event), do: :ok
  end

  defp start_bus do
    {:ok, session} = StreamingFakeSession.start_link()
    {:ok, bus} = StreamingQueryListenerBus.start_link(session)
    on_exit(fn -> if Process.alive?(bus), do: StreamingQueryListenerBus.stop(bus) end)
    bus
  end

  # ── T-17: stopping the stream must not leave closing_stream? stuck true ──

  describe "T-17 listener bus stream close" do
    test "removing the last listener clears closing_stream? and resets the attempt budget" do
      bus = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      assert %{stream_task: task} = :sys.get_state(bus)
      assert is_pid(task)

      :sys.replace_state(bus, fn state -> %{state | reconnect_attempts: 7} end)
      :ok = StreamingQueryListenerBus.remove_listener(bus, NoopListener)

      state = :sys.get_state(bus)
      assert state.stream_task == nil
      assert state.stream_task_ref == nil
      refute state.closing_stream?
      assert state.reconnect_attempts == 0

      # The killed task's DOWN is demonitored+flushed, so nothing arrives later
      # to re-set the flag; give the bus a beat to prove it stays clear.
      Process.sleep(20)
      refute :sys.get_state(bus).closing_stream?
    end

    test "a later server EOF after a stop still lets add_listener succeed" do
      bus = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      :ok = StreamingQueryListenerBus.remove_listener(bus, NoopListener)

      # A stale EOF from the torn-down stream must not wedge the bus.
      send(bus, {:listener_stream_ended, make_ref(), :normal})
      refute :sys.get_state(bus).closing_stream?

      assert :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      assert :sys.get_state(bus).registered?
    end

    test "a stale EOF processed after a re-add cannot orphan the new reader" do
      bus = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      %{stream_task: reader1, stream_token: token1} = :sys.get_state(bus)
      on_exit(fn -> Process.exit(reader1, :kill) end)

      # reader1 hits server EOF; its message sits in the mailbox unprocessed
      # while remove_listener + add_listener swap in reader2.
      :ok = StreamingQueryListenerBus.remove_listener(bus, NoopListener)
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)

      %{stream_task: reader2, stream_token: token2} = :sys.get_state(bus)
      assert is_pid(reader2) and reader2 != reader1
      assert token2 != token1

      # Now deliver reader1's stale EOF. Untagged, it would call maybe_reconnect,
      # nil out stream_task (orphaning reader2's gRPC stream and its server-side
      # listener) and schedule a third reader — duplicate events for every
      # listener. Tagged with reader1's token, it is dropped.
      send(bus, {:listener_stream_ended, token1, :normal})
      # A stale registration must not answer a pending waiter or flip state.
      send(bus, {:listener_bus_registered, token1})

      state = :sys.get_state(bus)
      assert state.stream_task == reader2
      assert state.stream_token == token2
      assert state.reconnect_attempts == 0

      # No reconnect was scheduled: after more than the first backoff, reader2
      # is still the one and only reader.
      Process.sleep(250)
      assert :sys.get_state(bus).stream_task == reader2
    end
  end

  # ── T-18: an abnormal reader DOWN must reconnect, not go inert ──

  describe "T-18 reader crash reconnects" do
    test "abnormal DOWN from the current reader emits reconnect telemetry" do
      test_pid = self()
      handler_id = {__MODULE__, :crash_reconnect, make_ref()}

      :telemetry.attach(
        handler_id,
        [:spark_ex, :streaming, :listener_bus, :reconnect],
        fn _event, measurements, _meta, _ -> send(test_pid, {:reconnect, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      bus = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      %{stream_task: task} = :sys.get_state(bus)
      # The fake stream parks forever; the injected DOWN is a fabrication, so
      # kill the real reader ourselves rather than leaking it past the test.
      on_exit(fn -> Process.exit(task, :kill) end)

      send(bus, {:DOWN, make_ref(), :process, task, {:badmatch, :boom}})

      assert_receive {:reconnect, %{attempt: 1, delay_ms: delay}}, 500
      assert is_integer(delay) and delay > 0

      state = :sys.get_state(bus)
      assert state.stream_task == nil
      refute state.registered?
      assert state.reconnect_attempts == 1
    end

    test "reconnect actually re-opens the stream after the backoff" do
      bus = start_bus()
      :ok = StreamingQueryListenerBus.add_listener(bus, NoopListener)
      %{stream_task: task} = :sys.get_state(bus)
      on_exit(fn -> Process.exit(task, :kill) end)

      send(bus, {:DOWN, make_ref(), :process, task, :killed})

      # First backoff is 200ms.
      assert eventually(fn ->
               state = :sys.get_state(bus)
               is_pid(state.stream_task) and state.stream_task != task
             end)
    end
  end

  defp eventually(fun, retries \\ 40) do
    cond do
      fun.() ->
        true

      retries == 0 ->
        false

      true ->
        Process.sleep(25)
        eventually(fun, retries - 1)
    end
  end

  # ── T-37: explicit TLS policy ──

  defp tls_opts(tls) do
    opts = %{
      host: "spark.example.com",
      port: 15_002,
      use_ssl: true,
      token: nil,
      auth_transport: :auto,
      extra_params: %{}
    }

    opts = if tls, do: Map.put(opts, :tls, tls), else: opts

    %GRPC.Credential{ssl: ssl} = Keyword.fetch!(Channel.build_grpc_opts(opts), :cred)
    ssl
  end

  describe "T-37 TLS defaults" do
    test "TLS with no tls config pins verify_peer, OS cacerts and https hostname check" do
      ssl = tls_opts(nil)

      assert Keyword.fetch!(ssl, :verify) == :verify_peer
      assert is_list(Keyword.fetch!(ssl, :cacerts))
      assert Keyword.fetch!(ssl, :cacerts) != []
      customize = Keyword.fetch!(ssl, :customize_hostname_check)
      assert is_function(Keyword.fetch!(customize, :match_fun), 2)
    end

    test "ssl_verify=none opts out entirely" do
      ssl = tls_opts(%{verify: :verify_none})

      assert Keyword.fetch!(ssl, :verify) == :verify_none
      refute Keyword.has_key?(ssl, :cacerts)
      refute Keyword.has_key?(ssl, :customize_hostname_check)
    end

    test "an explicit cacertfile keeps the previous bundle-based behaviour" do
      ssl = tls_opts(%{cacertfile: "/etc/ca.pem"})

      assert Keyword.fetch!(ssl, :cacertfile) == ~c"/etc/ca.pem"
      assert Keyword.fetch!(ssl, :verify) == :verify_peer
      refute Keyword.has_key?(ssl, :cacerts)
    end

    test "servername handling is preserved alongside the defaults" do
      ssl = tls_opts(%{servername: "sni.example.com"})

      assert Keyword.fetch!(ssl, :server_name_indication) == ~c"sni.example.com"
      assert Keyword.fetch!(ssl, :verify) == :verify_peer
    end
  end

  # ── T-43: Remote.message/1 prefers the full server message ──

  describe "T-43 Remote.message/1 precedence" do
    test "prefers the fetched server_message over the truncated gRPC message" do
      error = %Remote{
        message: "[TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be fo...",
        server_message: "[TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be found.",
        error_class: "TABLE_OR_VIEW_NOT_FOUND"
      }

      msg = Exception.message(error)
      assert msg =~ "cannot be found."
      refute msg =~ "cannot be fo..."
    end

    test "falls back to the gRPC message when no server_message was fetched" do
      assert Exception.message(%Remote{message: "boom"}) == "boom"
    end
  end
end
