defmodule SparkEx.Unit.BugsFableSessionTest do
  @moduledoc """
  Regression tests for the FABLE-* fixes owned by the session/reader/writer
  module group: FABLE-04, 22, 23, 32, 43, 44, 45, 46, 47, 48, 53.
  """
  use ExUnit.Case, async: false

  alias SparkEx.Reader
  alias SparkEx.Writer

  # DataFrame.new/2 wraps the logical plan in a {:plan_id, n, plan} envelope.
  defp unwrap_plan({:plan_id, _id, plan}), do: plan
  defp unwrap_plan(plan), do: plan

  # Starts a Session GenServer connected to a throwaway local TCP listener.
  # The gRPC connection is lazy, so no real Spark server is needed for the
  # local-plan-building paths exercised here (create_dataframe, tags, etc.).
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

    # Unlink so the (trapping-exits) session isn't perturbed by the test
    # process shutting down before on_exit cleanup runs.
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session) do
        try do
          GenServer.stop(session, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
      end

      Process.exit(acceptor, :kill)
      :gen_tcp.close(listener)
    end)

    session
  end

  describe "FABLE-04: malformed createDataFrame input does not crash the session" do
    test "tuple arity mismatch returns {:error, _} and keeps the session alive" do
      session = start_fake_session()

      result =
        SparkEx.create_dataframe(session, [{1, 2}, {3}], schema: ["a", "b"])

      assert match?({:error, _}, result)
      assert Process.alive?(session)
      refute SparkEx.is_stopped(session)
    end

    test "heterogeneous inferred types return {:error, _} and keep the session alive" do
      session = start_fake_session()

      result =
        SparkEx.create_dataframe(session, [%{"a" => 1}, %{"a" => "x"}])

      assert match?({:error, _}, result)
      assert Process.alive?(session)
      refute SparkEx.is_stopped(session)
    end
  end

  describe "FAB-2: keyword-list rows are treated as map rows" do
    test "list of keyword lists builds a local relation with named columns" do
      session = start_fake_session()

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, [[a: 1, b: "x"], [a: 2, b: "y"]])

      assert {:local_relation, _ipc, schema_ddl} = unwrap_plan(plan)
      assert schema_ddl =~ "a"
      assert schema_ddl =~ "b"
    end

    test "keyword rows honor an explicit DDL schema" do
      session = start_fake_session()

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, [[id: 1]], schema: "id INT")

      # Explicit-DDL map rows go through the from_json SQL plan; the row must
      # have been converted to a JSON object (not a tuple of {key, value}).
      assert {:sql, sql, nil} = unwrap_plan(plan)
      assert sql =~ "id INT"
      assert sql =~ ~s({"id":1})
    end
  end

  describe "FABLE-22: column-name-list schema for Explorer / column-map data" do
    test "Explorer.DataFrame with a column-name list renames columns" do
      session = start_fake_session()
      df = Explorer.DataFrame.new(%{"x" => [1, 2], "y" => ["a", "b"]})

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, df, schema: ["id", "name"])

      assert {:local_relation, _ipc, schema_ddl} = unwrap_plan(plan)
      assert is_binary(schema_ddl)
      assert schema_ddl =~ "id"
      assert schema_ddl =~ "name"
      # The {:column_names, ...} tuple must not leak into the schema field.
      refute schema_ddl =~ "column_names"
    end

    test "column-map data with a column-name list renames columns" do
      session = start_fake_session()

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, %{"a" => [1, 2], "b" => [3, 4]},
                 schema: ["c1", "c2"]
               )

      assert {:local_relation, _ipc, schema_ddl} = unwrap_plan(plan)
      assert is_binary(schema_ddl)
      assert schema_ddl =~ "c1"
      assert schema_ddl =~ "c2"
    end

    test "mismatched column-name count returns an error" do
      session = start_fake_session()
      df = Explorer.DataFrame.new(%{"x" => [1, 2], "y" => ["a", "b"]})

      assert {:error, {:invalid_schema, _}} =
               SparkEx.create_dataframe(session, df, schema: ["only_one"])
    end

    test "Explorer.DataFrame with a metadata-bearing struct schema keeps the JSON form" do
      session = start_fake_session()
      df = Explorer.DataFrame.new(%{"id" => [1, 2]})

      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :long,
            nullable: false,
            metadata: %{"comment" => "range_id"}
          )
        ])

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, df, schema: schema)

      assert {:local_relation, _ipc, schema_str} = unwrap_plan(plan)
      # The JSON form (not the DDL form) must reach the local relation so
      # field metadata and nullability round-trip to the server.
      assert {:ok, decoded} = Jason.decode(schema_str)
      assert [field] = decoded["fields"]
      assert field["metadata"] == %{"comment" => "range_id"}
      assert field["nullable"] == false
    end

    test "column-map data with a metadata-bearing struct schema keeps the JSON form" do
      session = start_fake_session()

      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :long,
            nullable: false,
            metadata: %{"comment" => "range_id"}
          )
        ])

      assert {:ok, %SparkEx.DataFrame{plan: plan}} =
               SparkEx.create_dataframe(session, %{"id" => [1, 2]}, schema: schema)

      assert {:local_relation, _ipc, schema_str} = unwrap_plan(plan)
      assert {:ok, decoded} = Jason.decode(schema_str)
      assert [field] = decoded["fields"]
      assert field["metadata"] == %{"comment" => "range_id"}
    end
  end

  describe "FABLE-23: Reader builder propagates :predicates" do
    test "load via builder with predicates emits the 6-tuple read_data_source plan" do
      session = start_fake_session()

      %SparkEx.DataFrame{plan: plan} =
        session
        |> SparkEx.read()
        |> Reader.format("jdbc")
        |> Reader.option("url", "jdbc:postgresql://h/db")
        |> Reader.option("dbtable", "t")
        |> Reader.load(nil, predicates: ["id < 100", "id >= 100"])

      assert {:read_data_source, "jdbc", _paths, _schema, _options, predicates} =
               unwrap_plan(plan)

      assert predicates == ["id < 100", "id >= 100"]
    end

    test "load via builder without predicates emits the 5-tuple plan" do
      session = start_fake_session()

      %SparkEx.DataFrame{plan: plan} =
        session
        |> SparkEx.read()
        |> Reader.format("parquet")
        |> Reader.load("/data/x.parquet")

      assert {:read_data_source, "parquet", _paths, _schema, _options} = unwrap_plan(plan)
    end
  end

  describe "FABLE-32: SPARK_REMOTE is only a fallback; explicit :url wins" do
    setup do
      prev = System.get_env("SPARK_REMOTE")

      on_exit(fn ->
        if prev, do: System.put_env("SPARK_REMOTE", prev), else: System.delete_env("SPARK_REMOTE")
      end)

      :ok
    end

    test "explicit :url is not overridden by SPARK_REMOTE" do
      System.put_env("SPARK_REMOTE", "sc://override-host:9999")
      # The explicit (bad) URL must still be used and rejected, proving the
      # env var did not redirect it to the structurally valid override.
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "http://bad")
    end

    test "SPARK_REMOTE is used as fallback when :url is omitted" do
      System.put_env("SPARK_REMOTE", "sc://fallback:1234")
      # Bad fallback URL surfaces via the same validation path.
      System.put_env("SPARK_REMOTE", "http://bad-fallback")
      assert {:error, {:invalid_uri, _}} = SparkEx.connect([])
    end

    test "missing :url and unset SPARK_REMOTE raises" do
      System.delete_env("SPARK_REMOTE")

      assert_raise ArgumentError, ~r/requires the :url option/, fn ->
        SparkEx.connect([])
      end
    end
  end

  describe "FABLE-43: Writer partition_by accepts a bare string" do
    test "builder partition_by/2 accepts a string" do
      writer = %Writer{partition_by: nil}
      assert %Writer{partition_by: ["year"]} = Writer.partition_by(writer, "year")
    end

    test "builder partition_by/2 still accepts a list" do
      writer = %Writer{partition_by: nil}
      assert %Writer{partition_by: ["a", "b"]} = Writer.partition_by(writer, ["a", "b"])
    end

    test "empty list still raises" do
      empty = Enum.filter(["x"], fn _ -> false end)

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        Writer.partition_by(%Writer{}, empty)
      end
    end
  end

  describe "FABLE-45: progress/lifecycle APIs accept named sessions" do
    test "is_stopped/1 works with a registered name" do
      session = start_fake_session()
      name = :"fable45_#{System.unique_integer([:positive])}"
      Process.register(session, name)

      refute SparkEx.is_stopped(name)
    end

    test "progress handler APIs work with a registered name" do
      session = start_fake_session()
      name = :"fable45h_#{System.unique_integer([:positive])}"
      Process.register(session, name)

      handler = fn _ -> :ok end
      assert :ok = SparkEx.register_progress_handler(name, handler)
      assert :ok = SparkEx.remove_progress_handler(name, handler)
      assert :ok = SparkEx.clear_progress_handlers(name)
    end

    test "is_stopped/1 raises a clear error for an unknown name" do
      assert_raise ArgumentError, ~r/no process associated/, fn ->
        SparkEx.is_stopped(:fable45_nonexistent_name)
      end
    end
  end

  describe "FABLE-48: remove_tag/2 validates the tag" do
    test "remove_tag rejects a tag with a comma" do
      session = start_fake_session()

      assert_raise ArgumentError, fn ->
        SparkEx.remove_tag(session, "a,b")
      end
    end

    test "remove_tag rejects an empty tag" do
      session = start_fake_session()

      assert_raise ArgumentError, fn ->
        SparkEx.remove_tag(session, "")
      end
    end
  end

  describe "FABLE-53: session traps exits" do
    test "session has trap_exit enabled" do
      session = start_fake_session()
      {:trap_exit, true} = Process.info(session, :trap_exit)
    end

    test "an abnormal linked exit stops the session so terminate/2 runs" do
      session = start_fake_session()
      ref = Process.monitor(session)

      # Spawn a process, link it to the session, then crash it abnormally.
      # The propagated {:EXIT, _, reason} should stop the trapping session
      # (running terminate/2 cleanup) rather than being silently ignored.
      test_pid = self()

      crasher =
        spawn(fn ->
          Process.link(session)
          send(test_pid, :linked)
          Process.sleep(:infinity)
        end)

      assert_receive :linked, 1_000
      Process.exit(crasher, :kill)

      # terminate/2 does a best-effort release_session with up to a 5s yield
      # against the (non-responsive) fake server before the process exits.
      assert_receive {:DOWN, ^ref, :process, ^session, _reason}, 8_000
    end

    test "a graceful linked exit does not stop the session" do
      session = start_fake_session()
      ref = Process.monitor(session)
      test_pid = self()

      graceful =
        spawn(fn ->
          Process.link(session)
          send(test_pid, :linked)
          # exit normally
          :ok
        end)

      assert_receive :linked, 1_000
      _ = graceful
      refute_receive {:DOWN, ^ref, :process, ^session, _}, 500
    end
  end

  describe "FABLE-53 follow-up: stop/1 tolerates a concurrently shutting-down session" do
    # With trap_exit set, OTP's parent-exit protocol gracefully terminates the
    # session when its starting process exits, so Session.stop/1 can observe a
    # :shutdown (instead of :normal) exit mid-termination. Synthesize that by
    # stopping a server whose terminate/2 exits with a shutdown reason.
    defmodule ShutdownOnStop do
      use GenServer
      def init(reason), do: {:ok, reason}
      def handle_call(:ping, _from, s), do: {:reply, :pong, s}
      def terminate(_reason, exit_reason), do: exit(exit_reason)
    end

    test "returns :ok when the process exits :shutdown during stop" do
      {:ok, pid} = GenServer.start(ShutdownOnStop, :shutdown)
      assert SparkEx.Session.stop(pid) == :ok
      refute Process.alive?(pid)
    end

    test "returns :ok when the process exits {:shutdown, term} during stop" do
      {:ok, pid} = GenServer.start(ShutdownOnStop, {:shutdown, :server_session_changed})
      assert SparkEx.Session.stop(pid) == :ok
      refute Process.alive?(pid)
    end

    test "returns :ok for an already-dead session" do
      {:ok, pid} = GenServer.start(ShutdownOnStop, :shutdown)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
      assert SparkEx.Session.stop(pid) == :ok
    end

    test "propagates an abnormal exit reason" do
      {:ok, pid} = GenServer.start(ShutdownOnStop, :boom)
      assert catch_exit(SparkEx.Session.stop(pid))
    end
  end
end
