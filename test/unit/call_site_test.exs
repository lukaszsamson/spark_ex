defmodule SparkEx.Unit.CallSiteTest do
  use ExUnit.Case, async: false

  alias SparkEx.{CallSite, Session}
  alias SparkEx.Connect.Client
  alias Spark.Connect.{AnalyzePlanRequest, AnalyzePlanResponse, ConfigRequest, ConfigResponse}

  defmodule Adapter do
    def send_request(%{channel: channel} = stream, bytes, _opts) do
      {request_type, response} =
        case stream.method_name do
          "Config" ->
            {ConfigRequest,
             %ConfigResponse{session_id: "client", server_side_session_id: "server"}}

          "AnalyzePlan" ->
            {AnalyzePlanRequest,
             %AnalyzePlanResponse{
               session_id: "client",
               server_side_session_id: "server",
               result: {:schema, %AnalyzePlanResponse.Schema{}}
             }}
        end

      send(channel.adapter_payload, {:request, request_type.decode(IO.iodata_to_binary(bytes))})
      %{stream | __interface__: %{receive_data: fn _, _ -> {:ok, response} end}}
    end
  end

  defmodule Proxy do
    use GenServer
    def start_link(state), do: GenServer.start_link(__MODULE__, state)
    @impl true
    def init(state), do: {:ok, state}
    @impl true
    def handle_call(message, from, state), do: Session.handle_call(message, from, state)
  end

  setup do
    old = Application.fetch_env(:spark_ex, :debug_client_call_stack)
    env = System.get_env("SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK")
    System.delete_env("SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK")
    Application.put_env(:spark_ex, :debug_client_call_stack, false)

    on_exit(fn ->
      case old do
        {:ok, value} -> Application.put_env(:spark_ex, :debug_client_call_stack, value)
        :error -> Application.delete_env(:spark_ex, :debug_client_call_stack)
      end

      if env,
        do: System.put_env("SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK", env),
        else: System.delete_env("SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK")
    end)

    state = %Session{
      session_id: "client",
      server_side_session_id: "server",
      user_id: "user",
      client_type: "test",
      channel: %GRPC.Channel{
        adapter: Adapter,
        adapter_payload: self(),
        codec: GRPC.Codec.Proto,
        interceptors: []
      }
    }

    %{state: state}
  end

  test "disabled tracing adds no extension", %{state: state} do
    assert CallSite.capture() == nil
    request = Client.build_execute_request(state, %Spark.Connect.Plan{}, [], nil, false)
    assert request.user_context.extensions == []
  end

  test "config captures the actual caller before crossing the session boundary", %{state: state} do
    Application.put_env(:spark_ex, :debug_client_call_stack, true)
    session = start_supervised!({Proxy, state})
    assert {:ok, []} = config_at_caller(session)
    assert_receive {:request, request}
    [extension] = request.user_context.extensions

    assert extension.type_url ==
             "type.googleapis.com/spark.connect.FetchErrorDetailsResponse.Error"

    error = Spark.Connect.FetchErrorDetailsResponse.Error.decode(extension.value)

    assert Enum.any?(
             error.stack_trace,
             &(&1.method_name == "config_at_caller" and &1.line_number > 0)
           )

    refute Enum.any?(error.stack_trace, &(&1.declaring_class == "SparkEx.Session"))

    Application.put_env(:spark_ex, :debug_client_call_stack, false)
    assert {:ok, []} = Session.config_get(session, [])
    assert_receive {:request, request}
    assert request.user_context.extensions == []
  end

  test "execute and analyze pack the scoped trace and restore it on failure", %{state: state} do
    trace = %Google.Protobuf.Any{type_url: "test/trace", value: "caller"}

    CallSite.with_trace(trace, fn ->
      request = Client.build_execute_request(state, %Spark.Connect.Plan{}, [], nil, false)
      assert request.user_context.extensions == [trace]
      assert {:ok, nil, "server"} = Client.analyze_schema(state, %Spark.Connect.Plan{})
      assert_receive {:request, request}
      assert request.user_context.extensions == [trace]
    end)

    assert_raise RuntimeError, fn -> CallSite.with_trace(trace, fn -> raise "failure" end) end
    assert CallSite.extensions() == []
  end

  test "cross-process lazy re-execution retains the creator trace", %{state: state} do
    parent = self()
    attempts = :counters.new(1, [:atomics])
    creator_trace = %Google.Protobuf.Any{type_url: "test/trace", value: "creator"}
    consumer_trace = %Google.Protobuf.Any{type_url: "test/trace", value: "consumer"}

    execute = fn request, _timeout ->
      send(parent, {:execute_trace, self(), request.user_context.extensions})
      :counters.add(attempts, 1, 1)

      responses =
        if :counters.get(attempts, 1) == 1 do
          []
        else
          [
            {:ok,
             %Spark.Connect.ExecutePlanResponse{
               session_id: "client",
               server_side_session_id: "server",
               response_id: "complete",
               response_type:
                 {:result_complete, %Spark.Connect.ExecutePlanResponse.ResultComplete{}}
             }}
          ]
        end

      {:ok, responses}
    end

    {:ok, stream} =
      CallSite.with_trace(creator_trace, fn ->
        Client.execute_plan_reattachable_response_stream(state, %Spark.Connect.Plan{},
          execute_stream_fun: execute,
          reattach_stream_fun: fn _ ->
            {:error,
             %SparkEx.Error.Remote{
               error_class: "INVALID_HANDLE.OPERATION_NOT_FOUND",
               message: "not found"
             }}
          end,
          release_execute_fun: fn _ -> {:ok, nil} end
        )
      end)

    assert_receive {:execute_trace, ^parent, [^creator_trace]}
    assert CallSite.extensions() == []

    task =
      Task.async(fn ->
        assert CallSite.extensions() == []
        result = CallSite.with_trace(consumer_trace, fn -> Enum.to_list(stream) end)
        assert CallSite.extensions() == []
        result
      end)

    assert [{:ok, %{response_id: "complete"}}] = Task.await(task)
    consumer = task.pid
    assert_receive {:execute_trace, ^consumer, [^creator_trace]}
    assert :counters.get(attempts, 1) == 2
    assert CallSite.extensions() == []
  end

  test "nested trace scopes restore their parent after throws and exits" do
    outer = %Google.Protobuf.Any{type_url: "test/trace", value: "outer"}
    inner = %Google.Protobuf.Any{type_url: "test/trace", value: "inner"}

    CallSite.with_trace(outer, fn ->
      assert catch_throw(CallSite.with_trace(inner, fn -> throw(:thrown) end)) == :thrown
      assert CallSite.extensions() == [outer]
      assert catch_exit(CallSite.with_trace(inner, fn -> exit(:exited) end)) == :exited
      assert CallSite.extensions() == [outer]
    end)

    assert CallSite.extensions() == []
  end

  defp config_at_caller(session) do
    result = Session.config_get(session, [])
    send(self(), :caller_completed)
    result
  end
end
