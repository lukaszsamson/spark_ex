defmodule SparkEx.SessionTest do
  use ExUnit.Case, async: true

  alias SparkEx.Internal.UUID

  describe "connect/1" do
    test "rejects invalid URI scheme before attempting connection" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "http://bad")
    end

    test "rejects URI without host" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "sc://")
    end

    test "rejects missing scheme" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "localhost:15002")
    end

    test "raises on invalid user_id type" do
      assert_raise ArgumentError, ~r/user_id must be a string/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", user_id: 123)
      end
    end

    test "raises on invalid client_type type" do
      assert_raise ArgumentError, ~r/client_type must be a string/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", client_type: 123)
      end
    end

    test "raises on invalid session_id type" do
      assert_raise ArgumentError, ~r/session_id must be a UUID string/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", session_id: 123)
      end
    end

    test "raises on invalid session_id format" do
      assert_raise ArgumentError, ~r/session_id must be a UUID string/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", session_id: "not-a-uuid")
      end
    end

    test "rejects invalid session_id in the URI" do
      assert {:error, {:invalid_param, "session_id=not-a-uuid"}} =
               SparkEx.connect(url: "sc://localhost:15002/;session_id=not-a-uuid")
    end

    test "uses identity parameters from the URI when explicit opts are absent" do
      {:ok, listener} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
      {:ok, port} = :inet.port(listener)

      acceptor =
        spawn_link(fn ->
          {:ok, socket} = :gen_tcp.accept(listener)
          Process.sleep(:infinity)
          :gen_tcp.close(socket)
        end)

      on_exit(fn ->
        Process.exit(acceptor, :kill)
        :gen_tcp.close(listener)
      end)

      session_id = UUID.generate_v4()

      assert {:ok, session} =
               SparkEx.connect(
                 url:
                   "sc://localhost:#{port}/;user_id=uri-user;user_agent=uri-agent;session_id=#{session_id}"
               )

      state = SparkEx.Session.get_state(session)
      assert state.user_id == "uri-user"
      assert state.client_type == "uri-agent"
      assert state.session_id == session_id

      SparkEx.Session.stop(session)
    end

    test "raises on invalid server_side_session_id type" do
      assert_raise ArgumentError, ~r/server_side_session_id must be a string or nil/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", server_side_session_id: 123)
      end
    end
  end

  describe "clone_session/2" do
    test "raises on non-string new_session_id" do
      assert_raise ArgumentError, ~r/new_session_id must be a string or nil/, fn ->
        SparkEx.clone_session(self(), 123)
      end
    end
  end
end
