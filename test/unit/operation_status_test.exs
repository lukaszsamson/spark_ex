defmodule SparkEx.OperationStatusTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{GetStatusRequest, GetStatusResponse}
  alias SparkEx.Connect.Client
  alias SparkEx.Internal.SessionSnapshot

  defmodule Adapter do
    def send_request(%{channel: channel} = stream, request, opts) do
      %{owner: owner, response: response} = channel.adapter_payload
      request = request |> IO.iodata_to_binary() |> Spark.Connect.GetStatusRequest.decode()
      send(owner, {:status_rpc, stream.method_name, request, opts})
      %{stream | __interface__: %{receive_data: fn _, _ -> response.() end}}
    end
  end

  defp session(response) do
    %SparkEx.Session{
      session_id: "status-client",
      server_side_session_id: "status-server",
      user_id: "user",
      client_type: "test",
      channel: %GRPC.Channel{
        adapter: Adapter,
        adapter_payload: %{owner: self(), response: response},
        codec: GRPC.Codec.Proto,
        interceptors: []
      },
      retry_policies: %{
        retry: %{max_retries: 1, initial_backoff_ms: 1, max_backoff_ms: 1, jitter_ms: 0}
      }
    }
  end

  defp response do
    %GetStatusResponse{session_id: "status-client", server_side_session_id: "status-server"}
  end

  test "all operations has present submessage and preserves extensions and unknown enum states" do
    extension = %Google.Protobuf.Any{type_url: "test/status", value: <<1, 2>>}

    response = %{
      response()
      | extensions: [extension],
        operation_statuses: [
          %GetStatusResponse.OperationStatus{
            operation_id: "op",
            state: 999,
            extensions: [extension]
          }
        ]
    }

    session = session(fn -> {:ok, response} end)

    assert {:ok, ^response, "status-server"} =
             Client.get_operation_statuses(session, [],
               extensions: [extension],
               operation_extensions: [extension]
             )

    assert_receive {:status_rpc, "GetStatus", %GetStatusRequest{} = request, _}
    assert request.session_id == session.session_id
    assert request.user_context.user_id == "user"
    assert request.client_type == "test"
    assert request.client_observed_server_side_session_id == "status-server"
    assert request.operation_status.operation_ids == []
    assert request.operation_status.extensions == [extension]
    assert request.extensions == [extension]
    assert GetStatusResponse.decode(GetStatusResponse.encode(response)) == response
  end

  test "status can execute while the owner cannot service its mailbox" do
    owner = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(owner, :stop) end)
    response = response()
    SessionSnapshot.put(owner, Map.from_struct(session(fn -> {:ok, response} end)))

    assert {:ok, ^response} = SparkEx.get_operation_statuses(owner, ["one", "two"], timeout: 500)
    assert_receive {:status_rpc, "GetStatus", request, opts}
    assert request.operation_status.operation_ids == ["one", "two"]
    assert opts[:timeout] == 500
  end

  test "stale identity is rejected and released sessions issue no RPC" do
    stale = %{response() | server_side_session_id: "rotated"}

    assert {:error, {:server_session_changed, _}} =
             Client.get_operation_statuses(session(fn -> {:ok, stale} end))

    state = %SparkEx.Session{released: true}

    assert {:reply, {:error, :session_released}, ^state} =
             SparkEx.Session.handle_call({:get_operation_statuses, [], []}, nil, state)

    assert {:error, {:invalid_operation_ids, [1]}} =
             Client.get_operation_statuses(session(fn -> flunk("must not send") end), [1])
  end

  test "UNIMPLEMENTED is returned and transient status failures are retried" do
    unsupported = %GRPC.RPCError{status: 12, message: "unsupported"}

    assert {:error, %SparkEx.Error.Remote{grpc_status: 12}} =
             Client.get_operation_statuses(session(fn -> {:error, unsupported} end))

    counter = :atomics.new(1, [])
    response = response()

    session =
      session(fn ->
        if :atomics.add_get(counter, 1, 1) == 1,
          do: {:error, %GRPC.RPCError{status: 14, message: "retry"}},
          else: {:ok, response}
      end)

    assert {:ok, ^response, _} = Client.get_operation_statuses(session)
    assert :atomics.get(counter, 1) == 2
  end
end
