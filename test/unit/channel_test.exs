defmodule SparkEx.Connect.ChannelTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Channel

  describe "parse_uri/1" do
    test "parses basic URI with host and port" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002")
      assert opts.host == "localhost"
      assert opts.port == 15002
      assert opts.use_ssl == false
      assert opts.token == nil
      assert opts.extra_params == %{}
    end

    test "uses default port 15002 when not specified" do
      assert {:ok, opts} = Channel.parse_uri("sc://spark-host")
      assert opts.host == "spark-host"
      assert opts.port == 15002
    end

    test "parses URI with trailing slash" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/")
      assert opts.host == "localhost"
      assert opts.port == 15002
    end

    test "parses use_ssl parameter" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;use_ssl=true")
      assert opts.use_ssl == true
    end

    test "parses token parameter" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;token=mytoken123")
      assert opts.token == "mytoken123"
    end

    test "parses auth_transport parameter" do
      assert {:ok, opts} =
               Channel.parse_uri("sc://localhost:15002/;token=mytoken123;auth_transport=metadata")

      assert opts.auth_transport == :metadata
    end

    test "rejects invalid auth_transport parameter" do
      assert {:error, {:invalid_auth_transport, "invalid"}} =
               Channel.parse_uri("sc://localhost:15002/;auth_transport=invalid")
    end

    test "parses multiple parameters" do
      uri = "sc://spark-host:15002/;use_ssl=true;token=abc123;custom_key=custom_value"
      assert {:ok, opts} = Channel.parse_uri(uri)
      assert opts.host == "spark-host"
      assert opts.port == 15002
      assert opts.use_ssl == true
      assert opts.token == "abc123"
      assert opts.auth_transport == :auto
      assert opts.extra_params == %{"custom_key" => "custom_value"}
    end

    test "rejects non-sc scheme" do
      assert {:error, {:invalid_uri, _}} = Channel.parse_uri("http://localhost:15002")
    end

    test "rejects missing scheme" do
      assert {:error, {:invalid_uri, _}} = Channel.parse_uri("localhost:15002")
    end

    test "rejects non-empty path component" do
      assert {:error, {:invalid_uri, _}} = Channel.parse_uri("sc://localhost:15002/not-allowed")
    end

    test "rejects parameter without equals sign" do
      assert {:error, {:invalid_param, "parm1"}} = Channel.parse_uri("sc://host/;parm1;param2=ok")
    end

    test "treats non-true use_ssl value as false" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;use_ssl=abcs")
      assert opts.use_ssl == false
    end

    test "decodes percent-encoded params" do
      assert {:ok, opts} =
               Channel.parse_uri(
                 "sc://host/;x-my-header=hello%20world;user_agent=Agent123%20%2F3.4"
               )

      assert opts.extra_params["x-my-header"] == "hello world"
      assert opts.extra_params["user_agent"] == "Agent123 /3.4"
    end

    test "rejects empty token parameter" do
      assert {:error, {:invalid_param, "token="}} =
               Channel.parse_uri("sc://localhost:15002/;token=")
    end

    test "rejects non-numeric port" do
      assert {:error, {:invalid_uri, _}} = Channel.parse_uri("sc://localhost:abc")
    end

    test "parses IPv6 URI with port" do
      assert {:ok, opts} = Channel.parse_uri("sc://[::1]:15002")
      assert opts.host == "::1"
      assert opts.port == 15002
    end

    test "parses IPv6 URI without port uses default" do
      assert {:ok, opts} = Channel.parse_uri("sc://[::1]")
      assert opts.host == "::1"
      assert opts.port == 15002
    end
  end

  describe "build_grpc_opts/1" do
    test "includes custom metadata headers and authorization" do
      opts = %{
        host: "host",
        port: 15002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{"x-my-header" => "v1", "custom" => "v2"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{metadata: md} = Enum.into(grpc_opts, %{})
      assert md["authorization"] == "Bearer abc"
      assert md["x-my-header"] == "v1"
      assert md["custom"] == "v2"
    end

    test "filters reserved metadata keys" do
      opts = %{
        host: "host",
        port: 15002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{
          "session_id" => "123",
          "user_agent" => "ua",
          "user_id" => "u",
          "use_ssl" => "true",
          "token" => "zzz",
          "x-keep" => "ok"
        }
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{metadata: md} = Enum.into(grpc_opts, %{})
      assert md["x-keep"] == "ok"
      refute Map.has_key?(md, "session_id")
      refute Map.has_key?(md, "user_agent")
      refute Map.has_key?(md, "user_id")
      refute Map.has_key?(md, "use_ssl")
      assert md["authorization"] == "Bearer abc"
    end

    test "token implies secure credentials even when use_ssl is false" do
      opts = %{
        host: "host",
        port: 15002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %GRPC.Credential{} = Keyword.fetch!(grpc_opts, :cred)
    end

    test "token authorization overrides custom authorization header" do
      opts = %{
        host: "host",
        port: 15002,
        use_ssl: true,
        token: "abc",
        auth_transport: :metadata,
        extra_params: %{"authorization" => "Basic 123"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{metadata: md} = Enum.into(grpc_opts, %{})
      assert md["authorization"] == "Bearer abc"
    end
  end
end
