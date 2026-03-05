defmodule SparkEx.Connect.Channel do
  @moduledoc false

  @default_port 15002

  @type connect_opts :: %{
          host: String.t(),
          port: non_neg_integer(),
          use_ssl: boolean(),
          token: String.t() | nil,
          user_id: String.t() | nil,
          user_agent: String.t() | nil,
          session_id: String.t() | nil,
          auth_transport: :auto | :metadata,
          extra_params: %{String.t() => String.t()}
        }

  @reserved_metadata_keys ~w(token use_ssl user_id user_agent session_id)

  @doc """
  Parses a Spark Connect URI string into connection options.

  ## Format

      sc://host:port/;key1=value1;key2=value2

  Supported parameters:
  - `use_ssl` — `"true"` enables TLS (default: `false`)
  - `token` — bearer token for auth
  - `auth_transport` — `"auto"` (default) or `"metadata"`

  ## Examples

      iex> SparkEx.Connect.Channel.parse_uri("sc://localhost:15002")
      {:ok, %{host: "localhost", port: 15002, use_ssl: false, token: nil, extra_params: %{}}}

      iex> SparkEx.Connect.Channel.parse_uri("sc://spark-host:15002/;use_ssl=true;token=abc123")
      {:ok, %{host: "spark-host", port: 15002, use_ssl: true, token: "abc123", extra_params: %{}}}
  """
  @spec parse_uri(String.t()) :: {:ok, connect_opts()} | {:error, term()}
  def parse_uri(uri_string) when is_binary(uri_string) do
    with {:ok, {host, port, params_string}} <- split_uri(uri_string),
         {:ok, params} <- parse_params(params_string),
         {:ok, auth_transport, params} <- pop_auth_transport(params),
         :ok <- validate_token(params),
         :ok <- validate_session_id(params) do
      {token, rest} = Map.pop(params, "token")
      {use_ssl_str, rest} = Map.pop(rest, "use_ssl", "false")
      {user_id, rest} = Map.pop(rest, "user_id")
      {user_agent, rest} = Map.pop(rest, "user_agent")
      {session_id, rest} = Map.pop(rest, "session_id")

      {:ok,
       %{
         host: host,
         port: port,
         use_ssl: use_ssl_str == "true",
         token: token,
         user_id: user_id,
         user_agent: user_agent,
         session_id: session_id,
         auth_transport: auth_transport,
         extra_params: rest
       }}
    end
  end

  @doc """
  Opens a gRPC channel to the given connection options.
  """
  @spec connect(connect_opts(), keyword()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def connect(opts, extra_grpc_opts \\ []) do
    address = "#{opts.host}:#{opts.port}"
    grpc_opts = build_grpc_opts(opts)
    grpc_opts = Keyword.merge(grpc_opts, extra_grpc_opts)
    GRPC.Stub.connect(address, grpc_opts)
  end

  @doc false
  @spec build_grpc_opts(connect_opts()) :: keyword()
  def build_grpc_opts(opts) do
    secure? = opts.use_ssl or remote_token_requires_tls?(opts)

    cred =
      if secure? do
        GRPC.Credential.new(ssl: [])
      end

    grpc_opts =
      if cred do
        [cred: cred]
      else
        []
      end

    extra_metadata =
      opts.extra_params
      |> Enum.reject(fn {k, _v} -> k in @reserved_metadata_keys end)
      |> Enum.into(%{})

    grpc_opts =
      case {opts.token, map_size(extra_metadata)} do
        {nil, 0} ->
          grpc_opts

        {token, 0} when is_binary(token) ->
          auth_metadata = auth_metadata_fallback(opts, token)
          Keyword.put(grpc_opts, :metadata, auth_metadata)

        {nil, _} ->
          Keyword.put(grpc_opts, :metadata, extra_metadata)

        {token, _} ->
          md = Map.merge(extra_metadata, auth_metadata_fallback(opts, token))
          Keyword.put(grpc_opts, :metadata, md)
      end

    grpc_opts
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
    authority =
      uri_string
      |> String.replace_prefix("sc://", "")
      |> String.split("/", parts: 2)
      |> hd()

    case URI.parse(uri_string) do
      %URI{scheme: "sc", host: host, port: port, path: path}
      when is_binary(host) and host != "" ->
        if malformed_port?(authority, port) do
          {:error, {:invalid_uri, "port must be numeric"}}
        else
          port = port || @default_port

          case path do
            nil ->
              {:ok, {host, port, ""}}

            "/" ->
              {:ok, {host, port, ""}}

            "/;" <> rest ->
              {:ok, {host, port, rest}}

            other ->
              {:error, {:invalid_uri, "path component '#{other}' must be empty"}}
          end
        end

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
          [key, value] ->
            decoded_key = URI.decode_www_form(key)
            decoded_value = URI.decode_www_form(value)
            {:cont, Map.put(acc, decoded_key, decoded_value)}

          _ ->
            {:halt, {:error, {:invalid_param, pair}}}
        end
      end)

    case pairs do
      {:error, _} = err -> err
      map when is_map(map) -> {:ok, map}
    end
  end

  defp pop_auth_transport(params) do
    {value, rest} = Map.pop(params, "auth_transport", "auto")

    case String.downcase(value) do
      "auto" -> {:ok, :auto, rest}
      "metadata" -> {:ok, :metadata, rest}
      invalid -> {:error, {:invalid_auth_transport, invalid}}
    end
  end

  defp auth_metadata_fallback(opts, token) when is_binary(token) do
    case Map.get(opts, :auth_transport, :auto) do
      :metadata ->
        %{"authorization" => "Bearer #{token}"}

      :auto ->
        # grpc-elixir currently has no call-credential support; use metadata fallback.
        %{"authorization" => "Bearer #{token}"}
    end
  end

  defp validate_token(%{"token" => ""}), do: {:error, {:invalid_param, "token="}}
  defp validate_token(_params), do: :ok

  defp validate_session_id(%{"session_id" => session_id}) do
    if SparkEx.Internal.UUID.valid_v4?(session_id) do
      :ok
    else
      {:error, {:invalid_param, "session_id=#{session_id}"}}
    end
  end

  defp validate_session_id(_params), do: :ok

  defp remote_token_requires_tls?(%{token: nil}), do: false
  defp remote_token_requires_tls?(%{use_ssl: true}), do: true

  defp remote_token_requires_tls?(%{host: host}) do
    not localhost?(host)
  end

  defp localhost?(host) when host in ["localhost", "::1"], do: true

  defp localhost?(host) when is_binary(host) do
    case :inet.parse_strict_address(String.to_charlist(host)) do
      {:ok, {127, _, _, _}} -> true
      {:ok, _addr} -> false
      {:error, _reason} -> false
    end
  end

  defp malformed_port?(authority, nil) do
    # For bracketed IPv6 hosts like [::1], strip the bracket prefix
    # and only check for a port after the closing bracket.
    case Regex.run(~r/^\[.*\](?::(.+))?$/, authority) do
      [_full] ->
        # Bracketed host, no port — valid
        false

      [_full, maybe_port] ->
        # Bracketed host with port
        not String.match?(maybe_port, ~r/^\d+$/)

      nil ->
        # Non-bracketed host
        case String.split(authority, ":", parts: 2) do
          [_host] ->
            false

          [_host, maybe_port] ->
            maybe_port != "" and not String.match?(maybe_port, ~r/^\d+$/)
        end
    end
  end

  defp malformed_port?(_authority, _port), do: false
end
