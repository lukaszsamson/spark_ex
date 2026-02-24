defmodule SparkEx.Unit.SessionLifecycleTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Spark.Connect.{CloneSessionRequest, ReleaseSessionRequest, UserContext}

  defmodule InterruptSession do
    use GenServer

    def start_link(parent) do
      GenServer.start_link(__MODULE__, parent, [])
    end

    @impl true
    def init(parent), do: {:ok, parent}

    @impl true
    def handle_call({:interrupt, payload}, _from, parent) do
      send(parent, {:interrupt_called, payload})
      {:reply, {:ok, ["op-1"]}, parent}
    end
  end

  defmodule FakeGrpcConnection do
    use GenServer

    def start_link(ref, state) do
      GenServer.start_link(__MODULE__, state, name: {:global, {GRPC.Client.Connection, ref}})
    end

    @impl true
    def init(state), do: {:ok, state}
  end

  describe "Session interrupt helpers" do
    test "interrupt_all/1 issues :all interrupt payload" do
      {:ok, session} = InterruptSession.start_link(self())
      assert {:ok, ["op-1"]} = SparkEx.Session.interrupt_all(session)
      assert_received {:interrupt_called, :all}
    end

    test "interrupt_tag/2 issues tagged interrupt payload" do
      {:ok, session} = InterruptSession.start_link(self())
      assert {:ok, ["op-1"]} = SparkEx.Session.interrupt_tag(session, "etl-job-42")
      assert_received {:interrupt_called, {:tag, "etl-job-42"}}
    end

    test "interrupt_operation/2 issues operation-id payload" do
      {:ok, session} = InterruptSession.start_link(self())
      assert {:ok, ["op-1"]} = SparkEx.Session.interrupt_operation(session, "op-789")
      assert_received {:interrupt_called, {:operation_id, "op-789"}}
    end
  end

  describe "ReleaseSession request building" do
    test "builds correct ReleaseSessionRequest" do
      request = %ReleaseSessionRequest{
        session_id: "sess-abc",
        user_context: %UserContext{user_id: "spark_ex"},
        client_type: "elixir/test"
      }

      assert request.session_id == "sess-abc"
      assert request.user_context.user_id == "spark_ex"
      assert request.client_type == "elixir/test"
      assert request.allow_reconnect == false
    end
  end

  describe "CloneSession request building" do
    test "builds correct CloneSessionRequest" do
      request = %CloneSessionRequest{
        session_id: "sess-abc",
        client_observed_server_side_session_id: "ssid-1",
        user_context: %UserContext{user_id: "spark_ex"},
        client_type: "elixir/test",
        new_session_id: "clone-xyz"
      }

      assert request.session_id == "sess-abc"
      assert request.client_observed_server_side_session_id == "ssid-1"
      assert request.user_context.user_id == "spark_ex"
      assert request.client_type == "elixir/test"
      assert request.new_session_id == "clone-xyz"
    end
  end

  describe "DataFrame.tag/2" do
    test "adds a tag to a DataFrame" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      tagged = SparkEx.DataFrame.tag(df, "my-tag")

      assert tagged.tags == ["my-tag"]
      assert tagged.plan == df.plan
      assert tagged.session == df.session
    end

    test "accumulates multiple tags" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}

      tagged =
        df
        |> SparkEx.DataFrame.tag("tag-1")
        |> SparkEx.DataFrame.tag("tag-2")

      assert tagged.tags == ["tag-1", "tag-2"]
    end

    test "tags are empty by default" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      assert df.tags == []
    end

    test "tags are preserved through transforms" do
      df =
        %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
        |> SparkEx.DataFrame.tag("etl")
        |> SparkEx.DataFrame.limit(5)

      assert df.tags == ["etl"]
    end

    test "rejects empty tags" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}
      assert_raise ArgumentError, ~r/non-empty string/, fn -> SparkEx.DataFrame.tag(df, "") end
    end

    test "rejects tags containing commas" do
      df = %SparkEx.DataFrame{session: self(), plan: {:range, 0, 10, 1, nil}}

      assert_raise ArgumentError, ~r/cannot contain ','/, fn ->
        SparkEx.DataFrame.tag(df, "a,b")
      end
    end
  end

  describe "Session tag management" do
    defmodule TagSession do
      use GenServer

      def start_link() do
        GenServer.start_link(__MODULE__, [], [])
      end

      @impl true
      def init(_), do: {:ok, []}

      @impl true
      def handle_cast({:add_tag, tag}, tags), do: {:noreply, tags ++ [tag]}

      @impl true
      def handle_cast({:remove_tag, tag}, tags), do: {:noreply, Enum.reject(tags, &(&1 == tag))}

      @impl true
      def handle_cast(:clear_tags, _tags), do: {:noreply, []}

      @impl true
      def handle_call(:get_tags, _from, tags), do: {:reply, tags, tags}
    end

    test "add/remove/get/clear tags" do
      {:ok, session} = TagSession.start_link()

      :ok = SparkEx.Session.add_tag(session, "tag-1")
      :ok = SparkEx.Session.add_tag(session, "tag-2")
      assert SparkEx.Session.get_tags(session) == ["tag-1", "tag-2"]

      :ok = SparkEx.Session.remove_tag(session, "tag-1")
      assert SparkEx.Session.get_tags(session) == ["tag-2"]

      :ok = SparkEx.Session.clear_tags(session)
      assert SparkEx.Session.get_tags(session) == []
    end

    test "rejects invalid tags" do
      {:ok, session} = TagSession.start_link()

      assert_raise ArgumentError, ~r/non-empty string/, fn ->
        SparkEx.Session.add_tag(session, "")
      end

      assert_raise ArgumentError, ~r/cannot contain ','/, fn ->
        SparkEx.Session.add_tag(session, "a,b")
      end
    end
  end

  describe "Session progress handlers" do
    test "registers and removes progress handlers via telemetry" do
      session = %SparkEx.Session{session_id: "progress-session"}

      {:ok, pid} = Agent.start_link(fn -> [] end)

      handler = fn payload ->
        Agent.update(pid, fn payloads -> [payload | payloads] end)
      end

      :ok = SparkEx.Session.register_progress_handler(session, handler)

      :telemetry.execute(
        [:spark_ex, :result, :progress],
        %{num_inflight_tasks: 1},
        %{session_id: "progress-session"}
      )

      :telemetry.execute(
        [:spark_ex, :result, :progress],
        %{num_inflight_tasks: 2},
        %{session_id: "other-session"}
      )

      assert Agent.get(pid, &length/1) == 1

      :ok = SparkEx.Session.remove_progress_handler(session, handler)

      :telemetry.execute(
        [:spark_ex, :result, :progress],
        %{num_inflight_tasks: 3},
        %{session_id: "progress-session"}
      )

      assert Agent.get(pid, &length/1) == 1
    end

    test "clears all progress handlers" do
      session = %SparkEx.Session{session_id: "progress-session-clear"}

      {:ok, pid} = Agent.start_link(fn -> [] end)

      handler_a = fn payload ->
        Agent.update(pid, fn payloads -> [payload | payloads] end)
      end

      handler_b = fn payload ->
        Agent.update(pid, fn payloads -> [payload | payloads] end)
      end

      :ok = SparkEx.Session.register_progress_handler(session, handler_a)
      :ok = SparkEx.Session.register_progress_handler(session, handler_b)

      :ok = SparkEx.Session.clear_progress_handlers(session)

      :telemetry.execute(
        [:spark_ex, :result, :progress],
        %{num_inflight_tasks: 1},
        %{session_id: "progress-session-clear"}
      )

      assert Agent.get(pid, &length/1) == 0
    end
  end

  describe "Session struct released field" do
    test "defaults to false" do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test",
        user_id: "test"
      }

      assert session.released == false
    end
  end

  describe "Session.config_is_modifiable/2 input normalization" do
    defmodule ConfigSession do
      use GenServer

      def start_link(parent) do
        GenServer.start_link(__MODULE__, parent, [])
      end

      @impl true
      def init(parent), do: {:ok, parent}

      @impl true
      def handle_call({:config_is_modifiable, keys}, _from, parent) do
        send(parent, {:config_is_modifiable_called, keys})
        {:reply, {:ok, [{"spark.sql.shuffle.partitions", "true"}]}, parent}
      end

      def handle_call({:config_set, pairs}, _from, parent) do
        send(parent, {:config_set_called, pairs})
        {:reply, :ok, parent}
      end

      def handle_call({:config_get, keys}, _from, parent) do
        send(parent, {:config_get_called, keys})
        {:reply, {:ok, [{"k", "v"}]}, parent}
      end

      def handle_call({:config_get_option, keys}, _from, parent) do
        send(parent, {:config_get_option_called, keys})
        {:reply, {:ok, [{"k", nil}]}, parent}
      end

      def handle_call({:config_get_with_default, pairs}, _from, parent) do
        send(parent, {:config_get_with_default_called, pairs})
        {:reply, {:ok, pairs}, parent}
      end

      def handle_call({:config_get_all, prefix}, _from, parent) do
        send(parent, {:config_get_all_called, prefix})
        {:reply, {:ok, []}, parent}
      end

      def handle_call({:config_unset, keys}, _from, parent) do
        send(parent, {:config_unset_called, keys})
        {:reply, :ok, parent}
      end
    end

    test "wraps bare string key in list" do
      {:ok, session} = ConfigSession.start_link(self())

      assert {:ok, [{"spark.sql.shuffle.partitions", "true"}]} =
               SparkEx.Session.config_is_modifiable(session, "spark.sql.shuffle.partitions")

      assert_receive {:config_is_modifiable_called, ["spark.sql.shuffle.partitions"]}
    end

    test "raises for non-string key list elements" do
      {:ok, session} = ConfigSession.start_link(self())

      assert_raise ArgumentError, ~r/keys must be a list of strings/, fn ->
        SparkEx.Session.config_is_modifiable(session, ["spark.sql.shuffle.partitions", 123])
      end
    end

    test "validates config_set key-value types" do
      {:ok, session} = ConfigSession.start_link(self())
      assert :ok = SparkEx.Session.config_set(session, [{"k", "v"}])
      assert_receive {:config_set_called, [{"k", "v"}]}

      assert_raise ArgumentError, ~r/pairs must be \{string_key, string_value\}/, fn ->
        SparkEx.Session.config_set(session, [{42, "v"}])
      end
    end

    test "validates config_get and config_get_option keys" do
      {:ok, session} = ConfigSession.start_link(self())
      assert {:ok, _} = SparkEx.Session.config_get(session, ["k"])
      assert_receive {:config_get_called, ["k"]}
      assert {:ok, _} = SparkEx.Session.config_get_option(session, ["k"])
      assert_receive {:config_get_option_called, ["k"]}

      assert_raise ArgumentError, ~r/config keys must be strings/, fn ->
        SparkEx.Session.config_get(session, [42])
      end

      assert_raise ArgumentError, ~r/config_get_option\/2 expects a list of string keys/, fn ->
        SparkEx.Session.config_get_option(session, 42)
      end
    end

    test "validates config_get_with_default pair shapes" do
      {:ok, session} = ConfigSession.start_link(self())
      assert {:ok, _} = SparkEx.Session.config_get_with_default(session, [{"k", "v"}])
      assert_receive {:config_get_with_default_called, [{"k", "v"}]}

      assert_raise ArgumentError,
                   ~r/pairs must be \{string_key, string_value\}/,
                   fn ->
                     SparkEx.Session.config_get_with_default(session, [{"k", 42}])
                   end
    end

    test "validates config_get_all prefix and config_unset keys" do
      {:ok, session} = ConfigSession.start_link(self())
      assert {:ok, _} = SparkEx.Session.config_get_all(session, "spark")
      assert_receive {:config_get_all_called, "spark"}
      assert :ok = SparkEx.Session.config_unset(session, ["k"])
      assert_receive {:config_unset_called, ["k"]}

      assert_raise ArgumentError,
                   ~r/config_get_all\/2 expects prefix to be a string or nil/,
                   fn ->
                     SparkEx.Session.config_get_all(session, prefix: "spark")
                   end

      assert_raise ArgumentError, ~r/config_unset\/2 expects a list of string keys/, fn ->
        SparkEx.Session.config_unset(session, 42)
      end
    end
  end

  describe "Session.create_dataframe schema normalization" do
    test "normalizes SparkEx.Types struct schema to DDL string" do
      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :long)
        ])

      request = {:create_dataframe, [%{"id" => 1}], [schema: schema]}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      assert query =~ "from_json(_spark_ex_json, 'id LONG'"
    end
  end

  describe "Session.is_stopped" do
    test "reflects released flag" do
      session = %SparkEx.Session{session_id: "sess-1", released: false}
      refute SparkEx.Session.is_stopped(session)

      released = %SparkEx.Session{session_id: "sess-1", released: true}
      assert SparkEx.Session.is_stopped(released)
    end
  end

  describe "Session.safe_disconnect/1" do
    test "does not raise for invalid channels" do
      assert :ok = SparkEx.Session.safe_disconnect(:invalid_channel)
    end

    test "skips disconnect when grpc connection has unresolved channels" do
      ref = make_ref()

      {:ok, pid} =
        FakeGrpcConnection.start_link(ref, %{
          real_channels: %{"ok" => {:ok, %GRPC.Channel{}}, "err" => {:error, :unreachable}}
        })

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
      end)

      log =
        capture_log(fn ->
          assert :ok = SparkEx.Session.safe_disconnect(%GRPC.Channel{ref: ref})
        end)

      assert log =~ "disconnect skipped due grpc connection state containing unresolved channels"
      assert Process.alive?(pid)
    end
  end
end
