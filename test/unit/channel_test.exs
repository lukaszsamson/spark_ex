defmodule SparkEx.Connect.ChannelTest do
  use ExUnit.Case, async: false

  alias SparkEx.Connect.Channel

  describe "parse_uri/1" do
    test "parses basic URI with host and port" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002")
      assert opts.host == "localhost"
      assert opts.port == 15_002
      assert opts.use_ssl == false
      assert opts.token == nil
      assert opts.extra_params == %{}
    end

    test "uses default port 15_002 when not specified" do
      assert {:ok, opts} = Channel.parse_uri("sc://spark-host")
      assert opts.host == "spark-host"
      assert opts.port == 15_002
    end

    test "parses URI with trailing slash" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/")
      assert opts.host == "localhost"
      assert opts.port == 15_002
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
      assert opts.port == 15_002
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

    test "use_ssl parsing is case-insensitive" do
      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;use_ssl=TRUE")
      assert opts.use_ssl == true

      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;use_ssl=True")
      assert opts.use_ssl == true

      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002/;use_ssl=FALSE")
      assert opts.use_ssl == false
    end

    test "grpc_keepalive_time_ms parses; timeout_ms and permit_without_calls are ignored" do
      assert {:ok, opts} =
               Channel.parse_uri("sc://localhost:15002/;grpc_keepalive_time_ms=30000")

      assert opts.keepalive == %{time_ms: 30_000}

      # The gun HTTP/2 adapter exposes only a ping interval (`keepalive`) plus a
      # count-based `keepalive_tolerance` — there is no ms ack-timeout or
      # idle-permit equivalent. These two params are accepted for Spark Connect
      # URI compatibility but stripped and ignored (never leaked into metadata).
      assert {:ok, opts} =
               Channel.parse_uri(
                 "sc://localhost:15002/;grpc_keepalive_timeout_ms=5000;grpc_keepalive_permit_without_calls=true"
               )

      assert opts.keepalive == %{}
      refute Map.has_key?(opts.extra_params, "grpc_keepalive_timeout_ms")
      refute Map.has_key?(opts.extra_params, "grpc_keepalive_permit_without_calls")
    end

    test "decodes percent-encoded params" do
      session_id = "550e8400-e29b-41d4-a716-446655440000"

      assert {:ok, opts} =
               Channel.parse_uri(
                 "sc://host/;x-my-header=hello%20world;user_agent=Agent123%20%2F3.4;user_id=alice;session_id=#{session_id}"
               )

      assert opts.extra_params["x-my-header"] == "hello world"
      assert opts.user_agent == "Agent123 /3.4"
      assert opts.user_id == "alice"
      assert opts.session_id == session_id
      refute Map.has_key?(opts.extra_params, "user_agent")
      refute Map.has_key?(opts.extra_params, "user_id")
      refute Map.has_key?(opts.extra_params, "session_id")
    end

    test "rejects empty token parameter" do
      assert {:error, {:invalid_param, "token="}} =
               Channel.parse_uri("sc://localhost:15002/;token=")
    end

    test "rejects invalid session_id parameter" do
      assert {:error, {:invalid_param, "session_id=not-a-uuid"}} =
               Channel.parse_uri("sc://localhost:15002/;session_id=not-a-uuid")
    end

    test "uses SPARK_CONNECT_AUTHENTICATE_TOKEN when URI token is absent" do
      previous = System.get_env("SPARK_CONNECT_AUTHENTICATE_TOKEN")
      System.put_env("SPARK_CONNECT_AUTHENTICATE_TOKEN", "env-token")

      on_exit(fn ->
        if previous do
          System.put_env("SPARK_CONNECT_AUTHENTICATE_TOKEN", previous)
        else
          System.delete_env("SPARK_CONNECT_AUTHENTICATE_TOKEN")
        end
      end)

      assert {:ok, opts} = Channel.parse_uri("sc://localhost:15002")
      assert opts.token == "env-token"
    end

    test "rejects non-numeric port" do
      assert {:error, {:invalid_uri, _}} = Channel.parse_uri("sc://localhost:abc")
    end

    test "parses IPv6 URI with port" do
      assert {:ok, opts} = Channel.parse_uri("sc://[::1]:15002")
      assert opts.host == "::1"
      assert opts.port == 15_002
    end

    test "parses IPv6 URI without port uses default" do
      assert {:ok, opts} = Channel.parse_uri("sc://[::1]")
      assert opts.host == "::1"
      assert opts.port == 15_002
    end

    test "parses TLS params" do
      assert {:ok, opts} =
               Channel.parse_uri(
                 "sc://host/;use_ssl=true;ssl_cacert=%2Fetc%2Fca.pem;ssl_servername=spark.example.com;ssl_verify=peer"
               )

      assert opts.tls == %{
               cacertfile: "/etc/ca.pem",
               servername: "spark.example.com",
               verify: :verify_peer
             }
    end

    test "rejects invalid ssl_verify" do
      assert {:error, {:invalid_param, "ssl_verify=bogus"}} =
               Channel.parse_uri("sc://host/;ssl_verify=bogus")
    end
  end

  describe "build_grpc_opts/1" do
    test "gRPC binary metadata is base64 encoded exactly once by the adapter" do
      # Spark Connect URI metadata is text, while grpc-elixir owns the wire
      # representation. Its contract is to accept raw bytes and encode -bin
      # values when constructing HTTP/2 headers.
      assert %{"x-ascii-bin" => "YWJj", "x-raw-bin" => "/wAB"} =
               GRPC.Transport.HTTP2.encode_metadata(%{
                 "x-ascii-bin" => "abc",
                 "x-raw-bin" => <<255, 0, 1>>
               })
    end

    test "gRPC binary metadata rejects malformed base64 in received headers" do
      assert_raise ArgumentError, fn ->
        GRPC.Transport.HTTP2.decode_headers([{"x-payload-bin", "not-base64!"}])
      end
    end

    test "includes custom metadata headers and authorization" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{"x-my-header" => "v1", "custom" => "v2"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{headers: md} = Enum.into(grpc_opts, %{})
      assert md["authorization"] == "Bearer abc"
      assert md["x-my-header"] == "v1"
      assert md["custom"] == "v2"
    end

    test "filters reserved metadata keys" do
      opts = %{
        host: "host",
        port: 15_002,
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
      assert %{headers: md} = Enum.into(grpc_opts, %{})
      assert md["x-keep"] == "ok"
      refute Map.has_key?(md, "session_id")
      refute Map.has_key?(md, "user_agent")
      refute Map.has_key?(md, "user_id")
      refute Map.has_key?(md, "use_ssl")
      assert md["authorization"] == "Bearer abc"
    end

    test "remote token implies secure credentials even when use_ssl is false" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %GRPC.Credential{} = Keyword.fetch!(grpc_opts, :cred)
    end

    test "localhost token does not force tls when use_ssl is false" do
      opts = %{
        host: "localhost",
        port: 15_002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      refute Keyword.has_key?(grpc_opts, :cred)
      assert %{headers: %{"authorization" => "Bearer abc"}} = Enum.into(grpc_opts, %{})
    end

    test "expanded IPv6 loopback hosts do not force tls when use_ssl is false" do
      for host <- ["0:0:0:0:0:0:0:1", "::ffff:127.0.0.1", "::ffff:127.0.0.55"] do
        opts = %{
          host: host,
          port: 15_002,
          use_ssl: false,
          token: "abc",
          auth_transport: :auto,
          extra_params: %{}
        }

        grpc_opts = Channel.build_grpc_opts(opts)
        refute Keyword.has_key?(grpc_opts, :cred), "expected #{host} to be treated as loopback"
        assert %{headers: %{"authorization" => "Bearer abc"}} = Enum.into(grpc_opts, %{})
      end
    end

    test "non-loopback IPv4-mapped address forces tls for remote tokens" do
      opts = %{
        host: "::ffff:8.8.8.8",
        port: 15_002,
        use_ssl: false,
        token: "abc",
        auth_transport: :auto,
        extra_params: %{}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %GRPC.Credential{} = Keyword.fetch!(grpc_opts, :cred)
    end

    test "ssl_cacert without explicit ssl_verify defaults to verify_peer" do
      opts = %{
        host: "remote",
        port: 15_002,
        use_ssl: true,
        token: nil,
        auth_transport: :auto,
        extra_params: %{},
        tls: %{cacertfile: "/etc/ca.pem"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      %GRPC.Credential{ssl: ssl} = Keyword.fetch!(grpc_opts, :cred)
      assert Keyword.fetch!(ssl, :cacertfile) == ~c"/etc/ca.pem"
      assert Keyword.fetch!(ssl, :verify) == :verify_peer
    end

    test "env token is used for auth metadata and tls selection" do
      previous = System.get_env("SPARK_CONNECT_AUTHENTICATE_TOKEN")
      System.put_env("SPARK_CONNECT_AUTHENTICATE_TOKEN", "env-token")

      on_exit(fn ->
        if previous do
          System.put_env("SPARK_CONNECT_AUTHENTICATE_TOKEN", previous)
        else
          System.delete_env("SPARK_CONNECT_AUTHENTICATE_TOKEN")
        end
      end)

      remote_opts = %{
        host: "remote-host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{}
      }

      local_opts = %{remote_opts | host: "localhost"}

      remote_grpc_opts = Channel.build_grpc_opts(remote_opts)
      assert %GRPC.Credential{} = Keyword.fetch!(remote_grpc_opts, :cred)

      assert %{headers: %{"authorization" => "Bearer env-token"}} =
               Enum.into(remote_grpc_opts, %{})

      local_grpc_opts = Channel.build_grpc_opts(local_opts)
      refute Keyword.has_key?(local_grpc_opts, :cred)

      assert %{headers: %{"authorization" => "Bearer env-token"}} =
               Enum.into(local_grpc_opts, %{})
    end

    test "adapter http2_opts clamp frame size to the HTTP/2 valid range" do
      # Below the HTTP/2 minimum (16,384) — must clamp UP, not produce an
      # invalid SETTINGS value that breaks gun negotiation.
      tiny_opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{},
        max_message_size: 8_000
      }

      grpc_opts = Channel.build_grpc_opts(tiny_opts)
      adapter_opts = Keyword.fetch!(grpc_opts, :adapter_opts)
      http2_opts = Keyword.fetch!(adapter_opts, :http2_opts)
      assert http2_opts.max_frame_size_received == 16_384

      # Above the HTTP/2 maximum (16,777,215) — must clamp DOWN.
      huge_opts = %{tiny_opts | max_message_size: 256 * 1024 * 1024}
      grpc_opts = Channel.build_grpc_opts(huge_opts)
      adapter_opts = Keyword.fetch!(grpc_opts, :adapter_opts)
      http2_opts = Keyword.fetch!(adapter_opts, :http2_opts)
      assert http2_opts.max_frame_size_received == 16_777_215

      # Window size scales with the configured max — large responses
      # shouldn't stall on the default 64 KiB window.
      assert http2_opts.initial_stream_window_size == 256 * 1024 * 1024
      assert http2_opts.initial_connection_window_size == 256 * 1024 * 1024
    end

    test "normalises metadata keys: lowercases and collapses underscores" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{"X-My-Header" => "v1", "trace_id" => "t1"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{headers: md} = Enum.into(grpc_opts, %{})
      assert md["x-my-header"] == "v1"
      assert md["trace-id"] == "t1"
      refute Map.has_key?(md, "X-My-Header")
      refute Map.has_key?(md, "trace_id")
    end

    test "drops metadata keys that contain illegal characters after normalisation" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{"bad key!" => "v1", "x-good" => "v2"}
      }

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          grpc_opts = Channel.build_grpc_opts(opts)
          assert %{headers: md} = Enum.into(grpc_opts, %{})
          assert md["x-good"] == "v2"
          refute Map.has_key?(md, "bad key!")
        end)

      assert log =~ "bad key!"
    end

    test "wires keepalive time_ms into http2_opts" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{},
        keepalive: %{time_ms: 30_000}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      adapter_opts = Keyword.fetch!(grpc_opts, :adapter_opts)
      http2_opts = Keyword.fetch!(adapter_opts, :http2_opts)
      assert http2_opts.keepalive == 30_000
    end

    test "wires user_agent into HTTP/2 user-agent header" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: false,
        token: nil,
        auth_transport: :auto,
        extra_params: %{},
        user_agent: "MyApp spark/connect-1 os/linux"
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{headers: md} = Enum.into(grpc_opts, %{})
      assert md["user-agent"] == "MyApp spark/connect-1 os/linux"
    end

    test "wires TLS opts into credential ssl" do
      opts = %{
        host: "remote",
        port: 15_002,
        use_ssl: true,
        token: nil,
        auth_transport: :auto,
        extra_params: %{},
        tls: %{cacertfile: "/etc/ca.pem", servername: "spark.example.com", verify: :verify_peer}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      %GRPC.Credential{ssl: ssl} = Keyword.fetch!(grpc_opts, :cred)
      assert Keyword.fetch!(ssl, :cacertfile) == ~c"/etc/ca.pem"
      assert Keyword.fetch!(ssl, :server_name_indication) == ~c"spark.example.com"
      assert Keyword.fetch!(ssl, :verify) == :verify_peer
    end

    test "token authorization overrides custom authorization header" do
      opts = %{
        host: "host",
        port: 15_002,
        use_ssl: true,
        token: "abc",
        auth_transport: :metadata,
        extra_params: %{"authorization" => "Basic 123"}
      }

      grpc_opts = Channel.build_grpc_opts(opts)
      assert %{headers: md} = Enum.into(grpc_opts, %{})
      assert md["authorization"] == "Bearer abc"
    end
  end
end
