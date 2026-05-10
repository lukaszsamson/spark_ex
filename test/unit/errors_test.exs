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
      assert error.retry_delay_ms == nil
    end

    test "creates basic Remote error when details is nil" do
      grpc_error = %GRPC.RPCError{status: 13, message: "internal", details: nil}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)
      assert %Remote{message: "internal", grpc_status: 13} = error
      assert error.retry_delay_ms == nil
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

    test "extracts RetryInfo retry_delay_ms when present" do
      retry_info = %Google.Rpc.RetryInfo{
        retry_delay: %Google.Protobuf.Duration{seconds: 1, nanos: 250_000_000}
      }

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.RetryInfo",
          value: Protobuf.encode(retry_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 14, message: "unavailable", details: details}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)

      assert %Remote{} = error
      assert error.grpc_status == 14
      assert error.retry_delay_ms == 1_250
    end

    test "RetryInfo with nil retry_delay leaves retry_delay_ms nil (no hint)" do
      # Empty RetryInfo (no retry_delay) must not collapse to 0; the
      # retry loop has to distinguish "no hint" from "retry immediately".
      retry_info = %Google.Rpc.RetryInfo{retry_delay: nil}

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.RetryInfo",
          value: Protobuf.encode(retry_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 14, message: "unavailable", details: details}
      session = build_fake_session()

      error = Errors.from_grpc_error(grpc_error, session)

      assert %Remote{} = error
      assert error.grpc_status == 14
      assert error.retry_delay_ms == nil
    end
  end

  describe "ErrorInfo metadata enrichment" do
    test "parses `classes` JSON list and `stackTrace` string from metadata" do
      error_info = %Google.Rpc.ErrorInfo{
        metadata: %{
          "classes" => ~s(["org.apache.spark.SparkException","java.lang.RuntimeException"]),
          "stackTrace" => "org.apache.spark.SparkException: boom\n\tat A.m(F.java:1)"
        }
      }

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(error_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 13, message: "internal", details: details}
      error = Errors.from_grpc_error(grpc_error, build_fake_session())

      assert error.classes == ["org.apache.spark.SparkException", "java.lang.RuntimeException"]
      assert error.stack_trace_inline =~ "org.apache.spark.SparkException: boom"
    end

    test "non-map messageParameters JSON is preserved as raw string instead of dropped" do
      error_info = %Google.Rpc.ErrorInfo{
        metadata: %{
          "messageParameters" => ~s(["unexpected","list"])
        }
      }

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(error_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 3, message: "bad", details: details}
      error = Errors.from_grpc_error(grpc_error, build_fake_session())

      assert error.message_parameters == ~s(["unexpected","list"])
    end

    test "malformed `classes` JSON yields nil rather than crashing" do
      error_info = %Google.Rpc.ErrorInfo{metadata: %{"classes" => "not-json"}}

      details = [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(error_info)
        }
      ]

      grpc_error = %GRPC.RPCError{status: 3, message: "bad", details: details}
      error = Errors.from_grpc_error(grpc_error, build_fake_session())

      assert error.classes == nil
    end
  end

  describe "cause-chain walking (walk_cause_chain/2)" do
    alias Spark.Connect.FetchErrorDetailsResponse

    test "returns a chain entry for each cause in order" do
      root = %FetchErrorDetailsResponse.Error{
        message: "query failed",
        error_type_hierarchy: ["org.apache.spark.SparkException"],
        stack_trace: [
          %FetchErrorDetailsResponse.StackTraceElement{
            declaring_class: "org.apache.spark.Foo",
            method_name: "run",
            file_name: "Foo.scala",
            line_number: 42
          }
        ],
        cause_idx: 1,
        spark_throwable: nil
      }

      cause = %FetchErrorDetailsResponse.Error{
        message: "root cause",
        error_type_hierarchy: ["java.lang.RuntimeException"],
        stack_trace: [],
        cause_idx: nil,
        spark_throwable: nil
      }

      errors_tuple = {root, cause}
      chain = Errors.walk_cause_chain(errors_tuple, 0)

      assert length(chain) == 2
      [root_entry, cause_entry] = chain
      assert root_entry.message == "query failed"
      assert root_entry.error_type_hierarchy == ["org.apache.spark.SparkException"]
      assert length(root_entry.stack_trace) == 1
      assert hd(root_entry.stack_trace).declaring_class == "org.apache.spark.Foo"
      assert cause_entry.message == "root cause"
      assert cause_entry.error_type_hierarchy == ["java.lang.RuntimeException"]
    end

    test "cycle in cause_idx chain terminates safely" do
      err0 = %FetchErrorDetailsResponse.Error{
        message: "a",
        error_type_hierarchy: [],
        stack_trace: [],
        cause_idx: 1,
        spark_throwable: nil
      }

      err1 = %FetchErrorDetailsResponse.Error{
        message: "b",
        error_type_hierarchy: [],
        stack_trace: [],
        cause_idx: 0,
        spark_throwable: nil
      }

      chain = Errors.walk_cause_chain({err0, err1}, 0)
      assert length(chain) == 2
    end

    test "out-of-bounds cause_idx terminates without crash" do
      err = %FetchErrorDetailsResponse.Error{
        message: "a",
        error_type_hierarchy: [],
        stack_trace: [],
        cause_idx: 99,
        spark_throwable: nil
      }

      chain = Errors.walk_cause_chain({err}, 0)
      assert length(chain) == 1
    end
  end

  describe "format_jvm_stacktrace/1" do
    test "first line includes exception class and message" do
      chain = [
        %{
          message: "query failed",
          error_type_hierarchy: ["org.apache.spark.SparkException"],
          stack_trace: [
            %{declaring_class: "Foo", method_name: "bar", file_name: "Foo.scala", line_number: 1}
          ]
        },
        %{
          message: "root cause",
          error_type_hierarchy: ["java.lang.RuntimeException"],
          stack_trace: []
        }
      ]

      result = Errors.format_jvm_stacktrace(chain)

      assert result =~ "org.apache.spark.SparkException: query failed"
      assert result =~ "\tat Foo.bar(Foo.scala:1)"
      assert result =~ "Caused by: java.lang.RuntimeException: root cause"
    end

    test "returns nil when error_type_hierarchy is empty" do
      chain = [%{message: "x", error_type_hierarchy: [], stack_trace: []}]
      assert Errors.format_jvm_stacktrace(chain) == nil
    end

    test "returns nil for empty chain" do
      assert Errors.format_jvm_stacktrace([]) == nil
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

    test "message/1 falls back to unknown message when both message fields are nil" do
      error = %Remote{message: nil, server_message: nil}
      assert Exception.message(error) == "Unknown Spark error"
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
