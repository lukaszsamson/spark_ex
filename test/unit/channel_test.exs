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

    test "parses multiple parameters" do
      uri = "sc://spark-host:15002/;use_ssl=true;token=abc123;custom_key=custom_value"
      assert {:ok, opts} = Channel.parse_uri(uri)
      assert opts.host == "spark-host"
      assert opts.port == 15002
      assert opts.use_ssl == true
      assert opts.token == "abc123"
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
  end
end
