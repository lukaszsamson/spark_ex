defmodule SparkEx.Connect.Channel do
  @moduledoc false

  @default_port 15_002
  # Spark Connect / PySpark default: 128 MiB for both directions.
  @default_max_message_size 128 * 1024 * 1024

  @type connect_opts :: %{
          host: String.t(),
          port: non_neg_integer(),
          use_ssl: boolean(),
          token: String.t() | nil,
          user_id: String.t() | nil,
          user_agent: String.t() | nil,
          session_id: String.t() | nil,
          auth_transport: :auto | :metadata,
          extra_params: %{String.t() => String.t()},
          max_message_size: pos_integer(),
          keepalive: %{optional(atom()) => term()},
          tls: %{optional(atom()) => term()}
        }

  @reserved_metadata_keys ~w(token use_ssl user_id user_agent session_id grpc_max_message_size grpc_keepalive_time_ms grpc_keepalive_timeout_ms grpc_keepalive_permit_without_calls ssl_cacert ssl_servername ssl_verify)

  @doc """
  Default max gRPC message size (bytes).
  """
  @spec default_max_message_size() :: pos_integer()
  def default_max_message_size, do: @default_max_message_size

  @doc """
  Parses a Spark Connect URI string into connection options.

  ## Format

      sc://host:port/;key1=value1;key2=value2

  Supported parameters:
  - `use_ssl` — `"true"` enables TLS (default: `false`)
  - `token` — bearer token for auth
  - `auth_transport` — `"auto"` (default) or `"metadata"`
  - `grpc_max_message_size` — max gRPC message size in bytes (default: 128 MiB)
  - `grpc_keepalive_time_ms`, `grpc_keepalive_timeout_ms`,
    `grpc_keepalive_permit_without_calls` — keepalive surface for follow-up

  ## Examples

      iex> {:ok, opts} = SparkEx.Connect.Channel.parse_uri("sc://localhost:15002")
      iex> {opts.host, opts.port, opts.use_ssl, opts.token, opts.extra_params}
      {"localhost", 15_002, false, nil, %{}}

      iex> {:ok, opts} = SparkEx.Connect.Channel.parse_uri("sc://spark-host:15002/;use_ssl=true;token=abc123")
      iex> {opts.host, opts.port, opts.use_ssl, opts.token}
      {"spark-host", 15_002, true, "abc123"}
  """
  @spec parse_uri(String.t()) :: {:ok, connect_opts()} | {:error, term()}
  def parse_uri(uri_string) when is_binary(uri_string) do
    with {:ok, {host, port, params_string}} <- split_uri(uri_string),
         {:ok, params} <- parse_params(params_string),
         {:ok, auth_transport, params} <- pop_auth_transport(params),
         :ok <- validate_token(params),
         :ok <- validate_session_id(params),
         {:ok, max_message_size, params} <- pop_max_message_size(params),
         {:ok, keepalive, params} <- pop_keepalive(params),
         {:ok, tls, params} <- pop_tls(params) do
      {parsed_token, rest} = Map.pop(params, "token")
      {use_ssl_str, rest} = Map.pop(rest, "use_ssl", "false")
      {user_id, rest} = Map.pop(rest, "user_id")
      {user_agent, rest} = Map.pop(rest, "user_agent")
      {session_id, rest} = Map.pop(rest, "session_id")
      token = resolve_token(parsed_token)

      {:ok,
       %{
         host: host,
         port: port,
         use_ssl: String.downcase(use_ssl_str) == "true",
         token: token,
         user_id: user_id,
         user_agent: user_agent,
         session_id: session_id,
         auth_transport: auth_transport,
         extra_params: rest,
         max_message_size: max_message_size,
         keepalive: keepalive,
         tls: tls
       }}
    end
  end

  @doc """
  Opens a gRPC channel to the given connection options.

  Explicit `extra_grpc_opts` win over the channel-level defaults built from
  `opts` (e.g. you can override `:adapter_opts`, `:cred`, or `:metadata`).
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
    token = resolve_token(opts.token)
    secure? = opts.use_ssl or remote_token_requires_tls?(opts, token)

    cred =
      if secure? do
        GRPC.Credential.new(ssl: build_ssl_opts(opts))
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
      |> Enum.reduce(%{}, fn {k, v}, acc ->
        case normalize_metadata_key(k) do
          {:ok, normalized} ->
            if Map.has_key?(acc, normalized) do
              require Logger

              Logger.warning(
                "dropping extra_param #{inspect(k)}: normalises to duplicate key #{inspect(normalized)}"
              )

              acc
            else
              Map.put(acc, normalized, v)
            end

          {:error, reason} ->
            require Logger
            Logger.warning("dropping extra_param #{inspect(k)}: #{reason}")
            acc
        end
      end)

    user_agent_md = user_agent_metadata(opts)

    base_metadata =
      case token do
        nil -> %{}
        resolved_token when is_binary(resolved_token) -> auth_metadata_fallback(opts, resolved_token)
      end

    merged_metadata =
      extra_metadata
      |> Map.merge(user_agent_md)
      |> Map.merge(base_metadata)

    grpc_opts =
      if map_size(merged_metadata) == 0 do
        grpc_opts
      else
        Keyword.put(grpc_opts, :metadata, merged_metadata)
      end

    grpc_opts
    |> Keyword.put(:adapter_opts, build_adapter_opts(opts))
  end

  defp user_agent_metadata(opts) do
    case Map.get(opts, :user_agent) do
      ua when is_binary(ua) and ua != "" -> %{"user-agent" => ua}
      _ -> %{}
    end
  end

  defp build_ssl_opts(opts) do
    tls = Map.get(opts, :tls) || %{}

    base = []

    base =
      case Map.get(tls, :cacertfile) do
        path when is_binary(path) and path != "" ->
          [{:cacertfile, String.to_charlist(path)}, {:verify, :verify_peer} | base]

        _ ->
          base
      end

    base =
      case Map.get(tls, :verify) do
        nil -> base
        verify -> Keyword.put(base, :verify, verify)
      end

    case Map.get(tls, :servername) do
      sni when is_binary(sni) and sni != "" ->
        Keyword.put(base, :server_name_indication, String.to_charlist(sni))

      _ ->
        base
    end
  end

  @doc """
  Disconnects a gRPC channel.
  """
  @spec disconnect(GRPC.Channel.t()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def disconnect(channel) do
    GRPC.Stub.disconnect(channel)
  end

  # --- Private ---

  # HTTP/2 SETTINGS_MAX_FRAME_SIZE valid range (RFC 7540 §6.5.2):
  #   min 16,384 (2^14), max 16,777,215 (2^24 - 1).
  @http2_min_frame_size 16_384
  @http2_max_frame_size 16_777_215

  defp build_adapter_opts(opts) do
    max_size = Map.get(opts, :max_message_size) || @default_max_message_size

    # Frame size advertised to the peer. Independently clamped to the
    # HTTP/2 valid range so an unusually small `grpc_max_message_size`
    # can't produce an invalid SETTINGS value and break negotiation.
    frame_size =
      max_size
      |> min(@http2_max_frame_size)
      |> max(@http2_min_frame_size)

    # Window size advertised by us controls how much the peer can send
    # without an explicit WINDOW_UPDATE; raise it to the max message
    # size so large server responses (Arrow batches, plan results)
    # don't stall on default 64 KiB windows.
    window_size = max(max_size, 65_535)

    keepalive = Map.get(opts, :keepalive) || %{}

    http2_opts =
      %{
        max_frame_size_received: frame_size,
        initial_connection_window_size: window_size,
        initial_stream_window_size: window_size
      }
      |> maybe_put_map(:keepalive, Map.get(keepalive, :time_ms))

    [http2_opts: http2_opts]
  end

  defp maybe_put_map(map, _key, nil), do: map
  defp maybe_put_map(map, key, value), do: Map.put(map, key, value)

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

  defp pop_max_message_size(params) do
    case Map.pop(params, "grpc_max_message_size") do
      {nil, rest} ->
        {:ok, @default_max_message_size, rest}

      {value, rest} ->
        case Integer.parse(value) do
          {bytes, ""} when bytes > 0 ->
            {:ok, bytes, rest}

          _ ->
            {:error, {:invalid_param, "grpc_max_message_size=#{value}"}}
        end
    end
  end

  defp pop_keepalive(params) do
    {time_ms, params} = pop_pos_int(params, "grpc_keepalive_time_ms")
    {timeout_ms, params} = pop_pos_int(params, "grpc_keepalive_timeout_ms")
    {permit, params} = pop_bool(params, "grpc_keepalive_permit_without_calls")

    case {time_ms, timeout_ms, permit} do
      {{:error, _} = err, _, _} ->
        err

      {_, {:error, _} = err, _} ->
        err

      {_, _, {:error, _} = err} ->
        err

      {{:ok, t}, {:ok, to}, {:ok, p}} ->
        keepalive =
          %{}
          |> maybe_put(:time_ms, t)
          |> maybe_put(:timeout_ms, to)
          |> maybe_put(:permit_without_calls, p)

        {:ok, keepalive, params}
    end
  end

  defp pop_tls(params) do
    {cacert, params} = Map.pop(params, "ssl_cacert")
    {servername, params} = Map.pop(params, "ssl_servername")
    {verify_raw, params} = Map.pop(params, "ssl_verify")

    with {:ok, verify} <- parse_ssl_verify(verify_raw) do
      tls =
        %{}
        |> maybe_put(:cacertfile, cacert)
        |> maybe_put(:servername, servername)
        |> maybe_put(:verify, verify)

      {:ok, tls, params}
    end
  end

  defp parse_ssl_verify(nil), do: {:ok, nil}

  defp parse_ssl_verify(value) when is_binary(value) do
    case String.downcase(value) do
      "peer" -> {:ok, :verify_peer}
      "verify_peer" -> {:ok, :verify_peer}
      "none" -> {:ok, :verify_none}
      "verify_none" -> {:ok, :verify_none}
      _ -> {:error, {:invalid_param, "ssl_verify=#{value}"}}
    end
  end

  defp pop_pos_int(params, key) do
    case Map.pop(params, key) do
      {nil, rest} ->
        {{:ok, nil}, rest}

      {value, rest} ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> {{:ok, n}, rest}
          _ -> {{:error, {:invalid_param, "#{key}=#{value}"}}, rest}
        end
    end
  end

  defp pop_bool(params, key) do
    case Map.pop(params, key) do
      {nil, rest} ->
        {{:ok, nil}, rest}

      {value, rest} ->
        case String.downcase(value) do
          "true" -> {{:ok, true}, rest}
          "false" -> {{:ok, false}, rest}
          _ -> {{:error, {:invalid_param, "#{key}=#{value}"}}, rest}
        end
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  # gRPC metadata keys are HTTP/2 header names: lower-case ASCII
  # alphanumerics and `-`. Underscores are accepted by some proxies but
  # rejected by others, so collapse `_` to `-` to match the headers
  # users typically copy from Spark/PySpark docs.
  defp normalize_metadata_key(key) when is_binary(key) do
    normalized =
      key
      |> String.downcase()
      |> String.replace("_", "-")

    cond do
      normalized == "" ->
        {:error, "empty key"}

      String.match?(normalized, ~r/^[a-z0-9\-]+$/) ->
        {:ok, normalized}

      true ->
        {:error, "key must contain only [a-z0-9-] after normalization"}
    end
  end

  defp normalize_metadata_key(key),
    do: {:error, "key must be a binary string, got: #{inspect(key)}"}

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
    if SparkEx.Internal.UUID.valid_uuid?(session_id) do
      :ok
    else
      {:error, {:invalid_param, "session_id=#{session_id}"}}
    end
  end

  defp validate_session_id(_params), do: :ok

  defp resolve_token(token) when is_binary(token), do: token

  defp resolve_token(nil) do
    case System.get_env("SPARK_CONNECT_AUTHENTICATE_TOKEN") do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp remote_token_requires_tls?(_opts, nil), do: false
  defp remote_token_requires_tls?(%{use_ssl: true}, _token), do: true

  defp remote_token_requires_tls?(%{host: host}, _token) do
    not localhost?(host)
  end

  defp localhost?(host) when host in ["localhost", "::1"], do: true

  defp localhost?(host) when is_binary(host) do
    case :inet.parse_strict_address(String.to_charlist(host)) do
      {:ok, {127, _, _, _}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      {:ok, {0, 0, 0, 0, 0, 0xFFFF, ab, cd}} -> ip4_mapped_loopback?(ab, cd)
      {:ok, _addr} -> false
      {:error, _reason} -> false
    end
  end

  defp ip4_mapped_loopback?(ab, _cd), do: Bitwise.bsr(ab, 8) == 127

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
