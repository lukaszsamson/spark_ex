defmodule SparkEx.Unit.Wave3SessionTest do
  @moduledoc """
  Regression tests for the wave-3 triaged session fixes:
  T-11, T-13, T-14, T-39, T-44, T-45, T-46.
  """
  use ExUnit.Case, async: false

  alias SparkEx.Connect.ResultDecoder
  alias SparkEx.DataFrame
  alias SparkEx.Internal.SessionSnapshot
  alias SparkEx.Session
  alias Spark.Connect.DataType
  alias Spark.Connect.ExecutePlanResponse

  # Starts a Session GenServer connected to a throwaway local TCP listener.
  # The gRPC connection is lazy, so no real Spark server is needed for the
  # local paths exercised here.
  defp start_fake_session(opts \\ []) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
    {:ok, port} = :inet.port(listener)

    acceptor =
      spawn_link(fn ->
        case :gen_tcp.accept(listener) do
          {:ok, _socket} -> Process.sleep(:infinity)
          _ -> :ok
        end
      end)

    {:ok, session} =
      SparkEx.connect(Keyword.put_new(opts, :url, "sc://localhost:#{port}"))

    Process.unlink(session)

    on_exit(fn ->
      # A graceful stop would wait on ReleaseSession against a listener that
      # never answers; nothing here needs the release, so kill outright.
      Process.exit(session, :kill)

      Process.exit(acceptor, :kill)
      :gen_tcp.close(listener)
    end)

    session
  end

  # Casts are async: a sync call afterwards guarantees they were processed.
  defp sync(session), do: Session.get_tags(session)

  defp t(kind_atom, kind_struct), do: %DataType{kind: {kind_atom, kind_struct}}
  defp string_type, do: t(:string, %DataType.String{})
  defp long_type, do: t(:long, %DataType.Long{})
  defp map_type(kt, vt), do: t(:map, %DataType.Map{key_type: kt, value_type: vt})

  defp struct_schema(fields) do
    t(:struct, %DataType.Struct{
      fields:
        Enum.map(fields, fn {name, dt} ->
          %DataType.StructField{name: name, data_type: dt, nullable: true}
        end)
    })
  end

  # A stand-in Session GenServer that records the execute_count / execute_show
  # requests DataFrame sends it.
  defmodule RecordingSession do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:execute_count, plan, opts}, _from, test_pid) do
      send(test_pid, {:execute_count, plan, opts})
      {:reply, {:ok, 42}, test_pid}
    end

    def handle_call({:execute_show, plan, opts}, _from, test_pid) do
      send(test_pid, {:execute_show, plan, opts})
      {:reply, {:ok, "+---+"}, test_pid}
    end

    def handle_call({:execute_collect, plan, opts}, _from, test_pid) do
      send(test_pid, {:execute_collect, plan, opts})
      {:reply, {:ok, []}, test_pid}
    end

    # to_local_iterator/2 asks for the session struct after the stream is set up.
    def handle_call(:get_state, _from, test_pid), do: {:reply, nil, test_pid}

    def handle_call({:execute_plan_reattachable_stream, plan, opts}, _from, test_pid) do
      send(test_pid, {:execute_plan_stream, plan, opts})
      {:reply, {:ok, []}, test_pid}
    end
  end

  describe "T-11: out-of-band interrupt observes the server session id via the integrity path" do
    test "the first id learned via interrupt republishes the ETS snapshot" do
      session = start_fake_session()
      assert {:ok, %{server_side_session_id: nil}} = SessionSnapshot.fetch(session)

      # This is the cast interrupt/2 issues after a successful Interrupt RPC.
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      sync(session)

      assert {:ok, %{server_side_session_id: "srv-1"}} = SessionSnapshot.fetch(session)
      assert %Session{server_side_session_id: "srv-1", closed: false} = Session.get_state(session)
    end

    test "a matching id is a no-op" do
      session = start_fake_session()
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      sync(session)

      assert {:ok, %{server_side_session_id: "srv-1"}} = SessionSnapshot.fetch(session)
      assert %Session{closed: false} = Session.get_state(session)
      assert Process.alive?(session)
    end

    test "a conflicting id closes the session and drops the snapshot" do
      session = start_fake_session()
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      sync(session)

      GenServer.cast(session, {:observe_server_session_id, "srv-2"})
      sync(session)

      assert SessionSnapshot.fetch(session) == :error
      assert {:error, :session_closed} = Session.execute_collect(session, {:range, 0, 1, 1, nil})
      # interrupt now falls back to the GenServer, which replies with the lifecycle error
      assert {:error, :session_closed} = Session.interrupt_all(session)
    end

    test "a session-changed error from the snapshot interrupt path closes the session" do
      session = start_fake_session()
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      sync(session)

      # Non-integrity errors leave the session open...
      GenServer.cast(session, {:observe_rpc_error, {:error, :timeout}})
      sync(session)
      assert {:ok, %{server_side_session_id: "srv-1"}} = SessionSnapshot.fetch(session)

      # ...a SESSION_CHANGED signal closes it, mirroring reply_error/2.
      error = {:error, {:server_session_changed, %{pinned: "srv-1", got: "srv-2"}}}
      GenServer.cast(session, {:observe_rpc_error, error})
      sync(session)

      assert SessionSnapshot.fetch(session) == :error
      assert {:error, :session_closed} = Session.interrupt_all(session)
    end

    test "a late observed id does not republish the snapshot of a closed session" do
      session = start_fake_session()
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      sync(session)

      error = {:error, {:server_session_changed, %{pinned: "srv-1", got: "srv-2"}}}
      GenServer.cast(session, {:observe_rpc_error, error})
      sync(session)
      assert SessionSnapshot.fetch(session) == :error

      # An interrupt that was in flight while the session closed reports back late.
      GenServer.cast(session, {:observe_server_session_id, "srv-1"})
      GenServer.cast(session, {:observe_server_session_id, "srv-3"})
      sync(session)

      assert SessionSnapshot.fetch(session) == :error
      assert {:error, :session_closed} = Session.interrupt_all(session)
    end
  end

  describe "T-13: map_format: :map after the unique-column-name retry" do
    test "renamed columns get converters positionally" do
      schema =
        struct_schema([
          {"m", map_type(string_type(), long_type())},
          {"m", map_type(string_type(), long_type())},
          {"s", string_type()}
        ])

      renamed = Session.__unique_columns_map_format_schema__(schema)
      %DataType{kind: {:struct, %DataType.Struct{fields: fields}}} = renamed
      assert Enum.map(fields, & &1.name) == ["m", "m_1", "s"]

      rows = [
        %{
          "m" => [%{"key" => "a", "value" => 1}],
          "m_1" => [%{"key" => "b", "value" => 2}],
          "s" => "x"
        }
      ]

      assert [%{"m" => %{"a" => 1}, "m_1" => %{"b" => 2}, "s" => "x"}] =
               ResultDecoder.convert_map_columns(rows, renamed)
    end

    test "duplicate names of different types do not bind a map converter to the wrong column" do
      schema =
        struct_schema([
          {"x", t(:array, %DataType.Array{element_type: string_type()})},
          {"x", map_type(string_type(), long_type())}
        ])

      renamed = Session.__unique_columns_map_format_schema__(schema)
      rows = [%{"x" => ["a", "b"], "x_1" => [%{"key" => "k", "value" => 1}]}]

      assert [%{"x" => ["a", "b"], "x_1" => %{"k" => 1}}] =
               ResultDecoder.convert_map_columns(rows, renamed)

      # And even with a mismatched schema, a non-map value degrades instead of raising.
      mismatched = struct_schema([{"x", map_type(string_type(), long_type())}])

      assert [%{"x" => ["a", "b"]}] =
               ResultDecoder.convert_map_columns([%{"x" => ["a", "b"]}], mismatched)
    end

    test "schemas without duplicates and non-struct inputs pass through" do
      schema = struct_schema([{"m", map_type(string_type(), long_type())}])
      assert Session.__unique_columns_map_format_schema__(schema) == schema
      assert Session.__unique_columns_map_format_schema__(nil) == nil
    end
  end

  describe "T-14: count/show/html_string forward DataFrame tags and :timeout" do
    setup do
      {:ok, session} = RecordingSession.start_link(self())
      df = DataFrame.new(session, {:range, 0, 10, 1, nil}) |> DataFrame.tag("t1")
      %{session: session, df: df}
    end

    test "count/2", %{df: df} do
      assert {:ok, 42} = DataFrame.count(df, timeout: 1_000)
      assert_receive {:execute_count, _plan, opts}
      assert Keyword.fetch!(opts, :tags) == ["t1"]
      assert Keyword.fetch!(opts, :timeout) == 1_000

      assert {:ok, 42} = DataFrame.count(df)
      assert_receive {:execute_count, _plan, [tags: ["t1"]]}
    end

    test "show/2 forwards only request options", %{df: df} do
      assert {:ok, _} = DataFrame.show(df, num_rows: 5, timeout: 2_000)
      assert_receive {:execute_show, {:show_string, _, 5, 20, false}, opts}
      assert Keyword.fetch!(opts, :tags) == ["t1"]
      assert Keyword.fetch!(opts, :timeout) == 2_000
      refute Keyword.has_key?(opts, :num_rows)
    end

    test "html_string/2", %{df: df} do
      assert {:ok, _} = DataFrame.html_string(df, timeout: 3_000)
      assert_receive {:execute_show, {:html_string, _, 20, 20}, opts}
      assert Keyword.fetch!(opts, :tags) == ["t1"]
      assert Keyword.fetch!(opts, :timeout) == 3_000
    end

    test "Session.execute_count/2 and execute_show/2 still delegate", %{session: session} do
      assert {:ok, 42} = Session.execute_count(session, {:range, 0, 1, 1, nil})
      assert_receive {:execute_count, _, []}
      assert {:ok, _} = Session.execute_show(session, {:show_string, :p, 1, 1, false})
      assert_receive {:execute_show, _, []}
    end
  end

  describe "T-39: :map_format validation and streaming conversion" do
    test "collect/2 and to_local_iterator/2 reject an invalid :map_format before calling the session" do
      {:ok, session} = RecordingSession.start_link(self())
      df = DataFrame.new(session, {:range, 0, 10, 1, nil})

      assert {:error, {:invalid_option, {:map_format, :dict}}} =
               DataFrame.collect(df, map_format: :dict)

      assert {:error, {:invalid_option, {:map_format, "map"}}} =
               DataFrame.to_local_iterator(df, map_format: "map")

      refute_receive {:execute_collect, _, _}
      refute_receive {:execute_plan_stream, _, _}
    end

    test "valid formats are forwarded" do
      {:ok, session} = RecordingSession.start_link(self())
      df = DataFrame.new(session, {:range, 0, 10, 1, nil})

      assert {:ok, []} = DataFrame.collect(df, map_format: :map)
      assert_receive {:execute_collect, _, [map_format: :map]}
      assert {:ok, _} = DataFrame.to_local_iterator(df, map_format: :key_value_pairs)
      assert_receive {:execute_plan_stream, _, [map_format: :key_value_pairs]}
    end

    @tag :explorer
    test "rows_stream/3 converts MAP columns per batch with map_format: :map" do
      series =
        Explorer.Series.from_list([
          [%{"key" => "a", "value" => 1}],
          [%{"key" => "b", "value" => 2}, %{"key" => "c", "value" => 3}]
        ])

      ipc = Explorer.DataFrame.dump_ipc_stream!(Explorer.DataFrame.new(%{"m" => series}))
      schema = struct_schema([{"m", map_type(string_type(), long_type())}])

      stream = [
        {:ok,
         %ExecutePlanResponse{
           schema: schema,
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{data: ipc, row_count: 2, start_offset: 0}}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch,
              %ExecutePlanResponse.ArrowBatch{data: ipc, row_count: 2, start_offset: 2}}
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
         }}
      ]

      converted = Enum.to_list(ResultDecoder.rows_stream(stream, nil, map_format: :map))

      assert converted == [
               {:ok, %{"m" => %{"a" => 1}}},
               {:ok, %{"m" => %{"b" => 2, "c" => 3}}},
               {:ok, %{"m" => %{"a" => 1}}},
               {:ok, %{"m" => %{"b" => 2, "c" => 3}}}
             ]

      assert [{:ok, %{"m" => [%{"key" => "a", "value" => 1}]}} | _] =
               Enum.to_list(ResultDecoder.rows_stream(stream))
    end
  end

  describe "T-44: real_session_process?/1 does not touch the GenServer mailbox" do
    test "a live Session is detected via its snapshot, a closed one via $initial_call" do
      session = start_fake_session()
      assert Session.__real_session_process__?(session)

      # Suspended processes cannot serve calls; the probe must still answer.
      :ok = :sys.suspend(session)
      assert Session.__real_session_process__?(session)
      :ok = :sys.resume(session)

      SessionSnapshot.delete(session)
      assert Session.__real_session_process__?(session)
    end

    test "non-Session processes and dead pids are not real sessions" do
      {:ok, other} = RecordingSession.start_link(self())
      refute Session.__real_session_process__?(other)

      pid = spawn(fn -> :ok end)
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}
      refute Session.__real_session_process__?(pid)
      refute Session.__real_session_process__?(:wave3_no_such_name)
    end
  end

  describe "T-45: is_stopped/1 after the process is gone" do
    test "returns true for a stopped pid and an unknown name" do
      session = start_fake_session()
      refute Session.is_stopped(session)

      :ok = GenServer.stop(session)
      assert Session.is_stopped(session)
      assert Session.is_stopped(:wave3_no_such_session)
    end
  end

  describe "T-46: session tags are deduplicated" do
    test "add_tag of an existing tag keeps a single copy, preserving order" do
      session = start_fake_session()
      :ok = Session.add_tag(session, "a")
      :ok = Session.add_tag(session, "b")
      :ok = Session.add_tag(session, "a")

      assert Session.get_tags(session) == ["a", "b"]
    end
  end
end
