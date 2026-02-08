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

  describe "from_grpc_error/2 with ErrorInfo details" do
    test "maps metadata into structured remote error without fetch call" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "spark",
        domain: "spark",
        metadata: %{
          "errorClass" => "ANALYSIS.ERROR",
          "sqlState" => "42000",
          "message" => "analysis failed",
          "messageParameters" => ~s({"a":"1"})
        }
      }

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(error_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 3, message: "bad request", details: details}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)

      assert %Remote{} = error
      assert error.error_class == "ANALYSIS.ERROR"
      assert error.sql_state == "42000"
      assert error.server_message == "analysis failed"
      assert error.message_parameters == %{"a" => "1"}
      assert error.grpc_status == 3
    end

    test "falls back to base metadata when errorId exists but fetch is unavailable" do
      error_info = %Google.Rpc.ErrorInfo{
        reason: "spark",
        domain: "spark",
        metadata: %{
          "errorId" => "id-1",
          "errorClass" => "ANALYSIS.ERROR"
        }
      }

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(error_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 3, message: "bad request", details: details}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)

      assert %Remote{} = error
      assert error.error_class == "ANALYSIS.ERROR"
      assert error.grpc_status == 3
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
