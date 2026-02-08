defmodule SparkEx.Connect.ErrorsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Errors
  alias SparkEx.Error.Remote

  describe "from_grpc_error/2 without error details" do
    test "creates basic Remote error from plain gRPC error" do
      grpc_error = %GRPC.RPCError{status: 3, message: "bad request"}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)

      assert %Remote{} = error
      assert error.message == "bad request"
      assert error.grpc_status == 3
      assert error.error_class == nil
    end

    test "creates basic Remote error when details is nil" do
      grpc_error = %GRPC.RPCError{status: 13, message: "internal", details: nil}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)
      assert %Remote{message: "internal", grpc_status: 13} = error
    end
  end

  describe "SparkEx.Error.Remote exception" do
    test "message/1 formats error with class and sql_state" do
      error = %Remote{
        message: "Table not found",
        error_class: "TABLE_OR_VIEW_NOT_FOUND",
        sql_state: "42P01"
      }

      msg = Exception.message(error)
      assert msg =~ "Table not found"
      assert msg =~ "TABLE_OR_VIEW_NOT_FOUND"
      assert msg =~ "42P01"
    end

    test "message/1 with only message" do
      error = %Remote{message: "Something failed"}
      msg = Exception.message(error)
      assert msg == "Something failed"
    end

    test "message/1 with no message falls back to server_message" do
      error = %Remote{server_message: "Server says no"}
      msg = Exception.message(error)
      assert msg =~ "Server says no"
    end
  end

  defp build_fake_session do
    %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test",
      plan_id_counter: 0
    }
  end
end
