defmodule SparkEx.Connect.Channel do
  @moduledoc """
  Parses Spark Connect URIs (`sc://host:port/;key=value`) and opens gRPC channels.
  """

  @default_port 15002

  @type connect_opts :: %{
          host: String.t(),
          port: non_neg_integer(),
          use_ssl: boolean(),
          token: String.t() | nil,
          extra_params: %{String.t() => String.t()}
        }

  @doc """
  Parses a Spark Connect URI string into connection options.

  ## Format

      sc://host:port/;key1=value1;key2=value2

  Supported parameters:
  - `use_ssl` — `"true"` enables TLS (default: `false`)
  - `token` — bearer token for auth

  ## Examples

      iex> SparkEx.Connect.Channel.parse_uri("sc://localhost:15002")
      {:ok, %{host: "localhost", port: 15002, use_ssl: false, token: nil, extra_params: %{}}}

      iex> SparkEx.Connect.Channel.parse_uri("sc://spark-host:15002/;use_ssl=true;token=abc123")
      {:ok, %{host: "spark-host", port: 15002, use_ssl: true, token: "abc123", extra_params: %{}}}
  """
  @spec parse_uri(String.t()) :: {:ok, connect_opts()} | {:error, term()}
  def parse_uri(uri_string) when is_binary(uri_string) do
    with {:ok, {host, port, params_string}} <- split_uri(uri_string),
         {:ok, params} <- parse_params(params_string) do
      {token, rest} = Map.pop(params, "token")
      {use_ssl_str, rest} = Map.pop(rest, "use_ssl", "false")

      {:ok,
       %{
         host: host,
         port: port,
         use_ssl: use_ssl_str == "true",
         token: token,
         extra_params: rest
       }}
    end
  end

  @doc """
  Opens a gRPC channel to the given connection options.
  """
  @spec connect(connect_opts()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def connect(opts) do
    address = "#{opts.host}:#{opts.port}"

    cred =
      if opts.use_ssl do
        GRPC.Credential.new(ssl: [])
      end

    grpc_opts =
      if cred do
        [cred: cred]
      else
        []
      end

    grpc_opts =
      if opts.token do
        Keyword.put(grpc_opts, :metadata, %{"authorization" => "Bearer #{opts.token}"})
      else
        grpc_opts
      end

    GRPC.Stub.connect(address, grpc_opts)
  end

  @doc """
  Disconnects a gRPC channel.
  """
  @spec disconnect(GRPC.Channel.t()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def disconnect(channel) do
    GRPC.Stub.disconnect(channel)
  end

  # --- Private ---

  defp split_uri(uri_string) do
    case URI.parse(uri_string) do
      %URI{scheme: "sc", host: host, port: port, path: path}
      when is_binary(host) and host != "" ->
        port = port || @default_port

        params_string =
          case path do
            nil -> ""
            "/" -> ""
            "/;" <> rest -> rest
            _ -> ""
          end

        {:ok, {host, port, params_string}}

      %URI{scheme: nil} ->
        {:error, {:invalid_uri, "missing sc:// scheme"}}

      %URI{scheme: scheme} when scheme != "sc" ->
        {:error, {:invalid_uri, "expected sc:// scheme, got #{scheme}://"}}

      _ ->
        {:error, {:invalid_uri, "could not parse host from URI"}}
    end
  end

  defp parse_params(""), do: {:ok, %{}}

  defp parse_params(params_string) do
    pairs =
      params_string
      |> String.split(";", trim: true)
      |> Enum.reduce_while(%{}, fn pair, acc ->
        case String.split(pair, "=", parts: 2) do
          [key, value] -> {:cont, Map.put(acc, key, value)}
          _ -> {:halt, {:error, {:invalid_param, pair}}}
        end
      end)

    case pairs do
      {:error, _} = err -> err
      map when is_map(map) -> {:ok, map}
    end
  end
end
