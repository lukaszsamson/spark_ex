defmodule SparkEx.SessionTest do
  use ExUnit.Case, async: true

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
      assert_raise ArgumentError, ~r/session_id must be a string/, fn ->
        SparkEx.connect(url: "sc://localhost:15002", session_id: 123)
      end
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
