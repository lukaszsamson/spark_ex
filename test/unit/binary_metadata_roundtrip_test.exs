defmodule SparkEx.Unit.BinaryMetadataRoundtripTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Channel

  defmodule EchoService do
    use GRPC.Service, name: "spark_ex.test.MetadataEcho"
    rpc(:Echo, Google.Protobuf.StringValue, Google.Protobuf.StringValue)
  end

  defmodule EchoStub do
    use GRPC.Stub, service: EchoService
  end

  defmodule EchoServer do
    use GRPC.Server, service: EchoService

    def echo(_request, stream) do
      headers = Map.take(stream.http_request_headers, ["x-text", "x-raw-bin", "x-encoded-bin"])
      %Google.Protobuf.StringValue{value: Jason.encode!(headers)}
    end
  end

  test "URI metadata reaches a local gRPC server with binary values encoded exactly once" do
    servers = %{EchoService.__meta__(:name) => EchoServer}
    assert {:ok, _pid, port} = GRPC.Server.Adapters.Cowboy.start(nil, servers, 0, [])
    on_exit(fn -> GRPC.Server.Adapters.Cowboy.stop(nil, servers) end)

    assert {:ok, opts} =
             Channel.parse_uri(
               "sc://localhost:#{port}/;x-text=hello;x-raw-bin=%FF%00%01;x-encoded-bin=%2FwAB"
             )

    assert {:ok, channel} = Channel.connect(opts)
    on_exit(fn -> Channel.disconnect(channel) end)

    assert {:ok, reply} = EchoStub.echo(channel, %Google.Protobuf.StringValue{})

    assert Jason.decode!(reply.value) == %{
             "x-text" => "hello",
             "x-raw-bin" => Base.encode64(<<255, 0, 1>>),
             "x-encoded-bin" => Base.encode64("/wAB")
           }
  end
end
