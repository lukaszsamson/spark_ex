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

    # Catch-all: GRPC.Stub.disconnect/1 issues a GenServer.call to the
    # registered grpc connection process. Reply with an error so the call
    # returns cleanly instead of crashing this fake (which would propagate
    # an EXIT to the test via the start_link link).
    @impl true
    def handle_call(_msg, _from, state), do: {:reply, {:error, :not_supported}, state}
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

      assert_raise ArgumentError, ~r/pairs must be \{key, value\}/, fn ->
        SparkEx.Session.config_set(session, [{[1, 2], "v"}])
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
                   ~r/pairs must be \{key, value\}/,
                   fn ->
                     SparkEx.Session.config_get_with_default(session, [{"k", [1, 2]}])
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

      assert query =~ "from_json(_spark_ex_json, 'id BIGINT'"
    end

    test "JSON values containing the literal substring (?) are inlined verbatim" do
      # Regression for HC2-2 / GPT-H4: the previous implementation used
      # String.replace(query, "(?)", "('…')", global: false) per row, so a
      # JSON value that itself contained the substring "(?)" would be
      # consumed as a placeholder and a row would be dropped.
      rows = [%{"note" => "what is (?) here"}, %{"note" => "ok"}]

      request = {:create_dataframe, rows, [schema: "note STRING"]}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      # Both rows must be present in the VALUES clause, with the JSON
      # text — including the embedded "(?)" — escaped into the SQL.
      assert query =~ ~s|('{\"note\":\"what is (?) here\"}')|
      assert query =~ ~s|('{\"note\":\"ok\"}')|
      # No literal placeholders remain.
      refute query =~ "VALUES (?)"
    end

    test "accepts list of tuples with explicit DDL schema" do
      # LC2-11: PySpark accepts a list of tuples paired with an
      # explicit DDL schema. Names come from the schema; values are
      # zipped positionally.
      rows = [{1, "a"}, {2, "b"}]
      request = {:create_dataframe, rows, [schema: "id INT, name STRING"]}

      assert {:reply, {:ok, %SparkEx.DataFrame{}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})
    end

    test "extracts backtick-quoted names with spaces from tuple schema" do
      # parse_schema_field must handle `name with space` STRING so
      # tuple rows can be keyed correctly.
      rows = [{1, "a"}]
      request = {:create_dataframe, rows, [schema: "`id` INT, `first name` STRING"]}

      assert {:reply, {:ok, %SparkEx.DataFrame{}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})
    end

    test "accepts column-name-only list as schema" do
      # LC2-14: PySpark allows passing the schema as a list of column
      # names (no types). Types are inferred from the data.
      rows = [{1, "a"}, {2, "b"}]
      request = {:create_dataframe, rows, [schema: ["id", "name"]]}

      assert {:reply, {:ok, %SparkEx.DataFrame{}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})
    end

    test "tuple rows without a schema infer _1, _2, ... column names (GPT-16)" do
      rows = [{1, "a"}, {2, "b"}]
      request = {:create_dataframe, rows, []}

      # GPT-16: PySpark infers "_1", "_2", ... when no schema is provided.
      # The DataFrame is built via Arrow IPC; the plan is a 3-tuple.
      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:local_relation, ipc, schema}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      assert is_binary(ipc)
      # Schema DDL must contain the inferred _1 and _2 column names.
      assert schema =~ "_1"
      assert schema =~ "_2"
    end

    test "tuple arity mismatch with schema raises ArgumentError" do
      rows = [{1, "a"}, {2}]
      request = {:create_dataframe, rows, [schema: "id INT, name STRING"]}

      assert_raise ArgumentError, ~r/tuple arity/, fn ->
        SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})
      end
    end

    test "rejects DDL with statement terminator or comment markers" do
      # Regression for LC2-13: schema_ddl is interpolated into the SQL
      # `from_json('<ddl>', …)` literal, so unterminated quoting or a
      # stray comment / statement-terminator outside any quoted
      # context indicates a malformed input.
      bad_schemas = [
        "id INT; DROP TABLE users",
        "id INT -- nope",
        "id INT /* comment */",
        "id INT */",
        "id `id\nINT",
        "id ' INT"
      ]

      for ddl <- bad_schemas do
        request = {:create_dataframe, [%{"id" => 1}], [schema: ddl]}

        assert {:reply, {:error, {:invalid_schema_ddl, _}}, %{}} =
                 SparkEx.Session.handle_call(request, {self(), make_ref()}, %{}),
               "expected #{inspect(ddl)} to be rejected"
      end
    end

    test "accepts DDL with comment markers / semicolons inside quoted regions" do
      # Validation is token-aware: characters that would terminate a
      # statement only matter when they appear *outside* a backtick
      # identifier or single-quoted string. Spark itself accepts these.
      good_schemas = [
        "`weird;name` STRING",
        "`has--dashes` INT",
        "`a/*b*/c` STRING",
        "id STRING COMMENT 'has -- dashes; and /* markers */'",
        "id STRING COMMENT 'with \\'escaped\\' quote'",
        "id STRING COMMENT 'doubled '' quote'",
        "`with``backtick` STRING"
      ]

      for ddl <- good_schemas do
        request = {:create_dataframe, [%{}], [schema: ddl]}

        result = SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

        refute match?({:reply, {:error, {:invalid_schema_ddl, _}}, _}, result),
               "expected #{inspect(ddl)} to pass schema validation, got: #{inspect(result)}"
      end
    end
  end

  describe "Session.create_dataframe local inference (GPT-18/67/66)" do
    test "integers always infer as BIGINT in fallback schema inference (GPT-18)" do
      # {:binary, ...} values are not natively handled by Explorer, which
      # causes safe_list_of_maps_to_explorer to fail and the fallback
      # infer_schema_ddl_from_rows path to be used instead.
      # GPT-18: on that path, integers must always infer as BIGINT (not TINYINT/INT).
      rows = [%{"bin" => {:binary, "data"}, "n" => 1}]
      request = {:create_dataframe, rows, []}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, _nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      assert query =~ "BIGINT"
      refute query =~ "TINYINT"
      # Verify the integer column did NOT produce a narrower type.
      refute Regex.match?(~r/\bn SMALLINT\b/, query)
    end

    test "Decimal infers as DECIMAL(38, 18) in fallback schema inference (GPT-67)" do
      # {:binary, ...} values force the fallback path where our Decimal
      # inference applies. GPT-67: Decimal always infers as DECIMAL(38, 18).
      rows = [%{"bin" => {:binary, "data"}, "d" => Decimal.new("1.5")}]
      request = {:create_dataframe, rows, []}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, _nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      assert query =~ "DECIMAL(38, 18)"
      refute query =~ "DECIMAL(2,"
    end

    test "Date values are JSON-encoded as ISO-8601 strings (GPT-66)" do
      # The JSON-relation path must not raise a Jason.EncodeError for date values.
      rows = [%{"d" => ~D[2024-01-15]}]
      request = {:create_dataframe, rows, [schema: "d DATE"]}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, _nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      # The ISO-8601 date string must appear in the generated SQL.
      assert query =~ "2024-01-15"
    end

    test "NaiveDateTime values are JSON-encoded as ISO-8601 strings (GPT-66)" do
      rows = [%{"ts" => ~N[2024-06-01 12:00:00]}]
      request = {:create_dataframe, rows, [schema: "ts TIMESTAMP_NTZ"]}

      assert {:reply, {:ok, %SparkEx.DataFrame{plan: {:sql, query, _nil}}}, %{}} =
               SparkEx.Session.handle_call(request, {self(), make_ref()}, %{})

      assert query =~ "2024-06-01T12:00:00"
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

    test "does best-effort disconnect even when grpc raises" do
      # The removed bug-risk hack inspected grpc-elixir private state. Verify
      # the replacement: on any failure path, safe_disconnect logs and returns
      # :ok without leaking the exception to the caller.
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

      assert log =~ "spark_ex session channel disconnect"
    end

    test "no-ops on a nil channel" do
      assert :ok = SparkEx.Session.safe_disconnect(nil)
    end
  end
end
