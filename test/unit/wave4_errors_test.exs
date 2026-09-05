defmodule SparkEx.Unit.Wave4ErrorsTest do
  @moduledoc """
  T-35: `FetchErrorDetails` enrichment must be lazy.

  Every failed RPC attempt used to trigger a `FetchErrorDetails` round-trip
  (up to 5 s) even when the attempt was about to be retried. The decode pass
  (`Errors.decode_grpc_error/1`) is now RPC-free and only the terminal error
  handed back to the caller is enriched (`Errors.enrich/2`), exactly once.

  The tests drive a fake gRPC adapter that counts `FetchErrorDetails` calls
  by messaging the test process.
  """
  use ExUnit.Case, async: true

  alias Spark.Connect.FetchErrorDetailsResponse
  alias SparkEx.Connect.{Client, Errors}
  alias SparkEx.Error.Remote

  @unavailable 14
  @invalid_argument 3

  defmodule FakeAdapter do
    @moduledoc false

    def send_request(%{channel: channel} = stream, _message, _opts) do
      %{test_pid: pid, response: response} = channel.adapter_payload
      send(pid, :fetch_error_details_called)

      %{stream | __interface__: %{receive_data: fn _s, _o -> {:ok, response} end}}
    end
  end

  defp fake_channel(response) do
    %GRPC.Channel{
      adapter: FakeAdapter,
      adapter_payload: %{test_pid: self(), response: response},
      codec: GRPC.Codec.Proto,
      interceptors: [],
      compressor: nil,
      accepted_compressors: []
    }
  end

  defp session(response) do
    %SparkEx.Session{
      channel: fake_channel(response),
      session_id: "wave4-session",
      server_side_session_id: "wave4-server",
      user_id: "wave4",
      client_type: "elixir/test",
      plan_id_counter: 0,
      retry_policies: %{
        retry: %{
          max_retries: 3,
          initial_backoff_ms: 1,
          max_backoff_ms: 2,
          jitter_ms: 0,
          sleep_fun: fn _ -> :ok end
        }
      }
    }
  end

  defp details_response do
    %FetchErrorDetailsResponse{
      root_error_idx: 0,
      errors: [
        %FetchErrorDetailsResponse.Error{
          message: "full server side message",
          error_type_hierarchy: ["org.apache.spark.sql.AnalysisException"],
          stack_trace: [
            %FetchErrorDetailsResponse.StackTraceElement{
              declaring_class: "org.apache.spark.sql.Analyzer",
              method_name: "check",
              file_name: "Analyzer.scala",
              line_number: 77
            }
          ],
          cause_idx: nil,
          spark_throwable: %FetchErrorDetailsResponse.SparkThrowable{
            error_class: "TABLE_OR_VIEW_NOT_FOUND",
            sql_state: "42P01",
            message_parameters: %{"relationName" => "t"}
          }
        }
      ]
    }
  end

  defp grpc_error(status, opts \\ []) do
    metadata =
      %{"errorClass" => "TABLE_OR_VIEW_NOT_FOUND"}
      |> then(fn m ->
        case Keyword.get(opts, :error_id, "err-1") do
          nil -> m
          id -> Map.put(m, "errorId", id)
        end
      end)

    info = %Google.Rpc.ErrorInfo{reason: "spark", domain: "spark", metadata: metadata}

    %GRPC.RPCError{
      status: status,
      message: "truncated message",
      details: [
        %Google.Protobuf.Any{
          type_url: "type.googleapis.com/google.rpc.ErrorInfo",
          value: Protobuf.encode(info)
        }
      ]
    }
  end

  describe "decode_grpc_error/1 (cheap pass)" do
    test "decodes status, ErrorInfo and RetryInfo without any FetchErrorDetails call" do
      retry_info = %Google.Rpc.RetryInfo{
        retry_delay: %Google.Protobuf.Duration{seconds: 1, nanos: 0}
      }

      error = grpc_error(@unavailable)

      error = %{
        error
        | details:
            error.details ++
              [
                %Google.Protobuf.Any{
                  type_url: "type.googleapis.com/google.rpc.RetryInfo",
                  value: Protobuf.encode(retry_info)
                }
              ]
      }

      remote = Errors.decode_grpc_error(error)

      assert %Remote{} = remote
      assert remote.grpc_status == @unavailable
      assert remote.error_class == "TABLE_OR_VIEW_NOT_FOUND"
      assert remote.retry_delay_ms == 1_000
      assert remote.has_retry_info == true
      assert remote.error_id == "err-1"
      refute remote.enriched?
      assert Client.retryable_error?(remote)
      refute_received :fetch_error_details_called
    end

    test "an error without ErrorInfo needs no enrichment at all" do
      remote = Errors.decode_grpc_error(%GRPC.RPCError{status: 13, message: "boom"})

      assert remote.error_id == nil
      assert remote.enriched? == true
    end
  end

  describe "lazy enrichment through the retry loop" do
    test "retryable failure followed by success performs zero FetchErrorDetails calls" do
      session = session(details_response())
      counter = :counters.new(1, [])

      result =
        Client.retry_with_backoff(
          fn ->
            :counters.add(counter, 1, 1)

            if :counters.get(counter, 1) < 3 do
              {:error, Errors.decode_grpc_error(grpc_error(@unavailable))}
            else
              {:ok, :done}
            end
          end,
          session: session
        )

      assert result == {:ok, :done}
      assert :counters.get(counter, 1) == 3
      refute_received :fetch_error_details_called
    end

    test "terminal failure performs exactly one FetchErrorDetails call and is enriched" do
      session = session(details_response())

      assert {:error, %Remote{} = error} =
               Client.retry_with_backoff(
                 fn -> {:error, Errors.decode_grpc_error(grpc_error(@invalid_argument))} end,
                 session: session
               )

      assert_received :fetch_error_details_called
      refute_received :fetch_error_details_called

      # Enriched details survive: full server message, error class, sqlstate,
      # structured + inline JVM stack trace.
      assert error.enriched?
      assert error.server_message == "full server side message"
      assert error.error_class == "TABLE_OR_VIEW_NOT_FOUND"
      assert error.sql_state == "42P01"
      assert error.error_type_hierarchy == ["org.apache.spark.sql.AnalysisException"]
      assert [%{declaring_class: "org.apache.spark.sql.Analyzer"}] = error.stacktrace
      assert error.stack_trace_inline =~ "org.apache.spark.sql.AnalysisException"
      # Wave 2: message/1 prefers the (full) server message over the gRPC one.
      assert Exception.message(error) =~ "full server side message"
    end

    test "retries then a terminal failure still enrich only once" do
      session = session(details_response())
      counter = :counters.new(1, [])

      assert {:error, %Remote{enriched?: true}} =
               Client.retry_with_backoff(
                 fn ->
                   :counters.add(counter, 1, 1)
                   {:error, Errors.decode_grpc_error(grpc_error(@unavailable))}
                 end,
                 session: session
               )

      # 1 initial + 3 retries, but a single enrichment RPC.
      assert :counters.get(counter, 1) == 4
      assert_received :fetch_error_details_called
      refute_received :fetch_error_details_called
    end

    test "enrich/2 is a no-op for an already-enriched error and for non-Remote terms" do
      session = session(details_response())
      enriched = %Remote{message: "x", error_id: "err-1", enriched?: true}

      assert Errors.enrich(enriched, session) == enriched
      assert Errors.enrich(:some_other_error, session) == :some_other_error
      refute_received :fetch_error_details_called
    end

    test "an error without errorId is never enriched over the wire" do
      session = session(details_response())
      remote = Errors.decode_grpc_error(grpc_error(@invalid_argument, error_id: nil))

      enriched = Errors.enrich(remote, session)

      assert enriched.enriched?
      assert enriched.error_class == "TABLE_OR_VIEW_NOT_FOUND"
      refute_received :fetch_error_details_called
    end
  end

  describe "from_grpc_error/2 compatibility" do
    test "still returns an eagerly enriched error (single fetch)" do
      session = session(details_response())

      error = Errors.from_grpc_error(grpc_error(@invalid_argument), session)

      assert error.server_message == "full server side message"
      assert error.enriched?
      assert_received :fetch_error_details_called
      refute_received :fetch_error_details_called
    end
  end
end
