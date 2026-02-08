defmodule SparkEx.Session do
  @moduledoc """
  Manages a Spark Connect session as a GenServer process.

  Holds the gRPC channel, session ID, server-side session ID tracking,
  and a monotonic plan ID counter.
  """

  use GenServer

  alias SparkEx.Connect.Channel
  alias SparkEx.Connect.Client

  defstruct [
    :channel,
    :session_id,
    :server_side_session_id,
    :user_id,
    :client_type,
    plan_id_counter: 0
  ]

  @type t :: %__MODULE__{
          channel: GRPC.Channel.t() | nil,
          session_id: String.t(),
          server_side_session_id: String.t() | nil,
          user_id: String.t(),
          client_type: String.t(),
          plan_id_counter: non_neg_integer()
        }

  # --- Public API ---

  @doc """
  Starts a session process connected to a Spark Connect endpoint.

  ## Options

  - `:url` — Spark Connect URI (required), e.g. `"sc://localhost:15002"`
  - `:user_id` — user identifier (default: `"spark_ex"`)
  - `:client_type` — client type string (default: auto-generated)
  - `:session_id` — custom session UUID (default: auto-generated)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, session_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, session_opts, gen_opts)
  end

  @doc """
  Returns the session state (for building requests).
  """
  @spec get_state(GenServer.server()) :: t()
  def get_state(session) do
    GenServer.call(session, :get_state)
  end

  @doc """
  Generates the next plan ID and returns it.
  """
  @spec next_plan_id(GenServer.server()) :: non_neg_integer()
  def next_plan_id(session) do
    GenServer.call(session, :next_plan_id)
  end

  @doc """
  Updates the server-side session ID (called after every response).
  """
  @spec update_server_side_session_id(GenServer.server(), String.t()) :: :ok
  def update_server_side_session_id(session, server_side_session_id) do
    GenServer.cast(session, {:update_server_side_session_id, server_side_session_id})
  end

  @doc """
  Fetches the Spark version from the connected server.
  """
  @spec spark_version(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def spark_version(session) do
    GenServer.call(session, :spark_version)
  end

  @doc """
  Stops the session and releases server resources.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(session) do
    GenServer.stop(session)
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    user_id = Keyword.get(opts, :user_id, "spark_ex")
    client_type = Keyword.get(opts, :client_type, default_client_type())
    session_id = Keyword.get(opts, :session_id, generate_uuid())

    with {:ok, connect_opts} <- Channel.parse_uri(url),
         {:ok, channel} <- Channel.connect(connect_opts) do
      state = %__MODULE__{
        channel: channel,
        session_id: session_id,
        user_id: user_id,
        client_type: client_type
      }

      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:next_plan_id, _from, state) do
    id = state.plan_id_counter
    {:reply, id, %{state | plan_id_counter: id + 1}}
  end

  def handle_call(:spark_version, _from, state) do
    case Client.analyze_spark_version(state) do
      {:ok, version, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, version}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast({:update_server_side_session_id, id}, state) do
    {:noreply, %{state | server_side_session_id: id}}
  end

  @impl true
  def terminate(_reason, %{channel: nil}), do: :ok

  def terminate(_reason, %{channel: channel}) do
    Channel.disconnect(channel)
    :ok
  end

  # --- Private ---

  defp maybe_update_server_session(state, nil), do: state
  defp maybe_update_server_session(state, ""), do: state

  defp maybe_update_server_session(state, id) do
    %{state | server_side_session_id: id}
  end

  defp generate_uuid do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)

    <<a::48, 4::4, b::12, 2::2, c::62>>
    |> encode_uuid()
  end

  defp encode_uuid(<<a::32, b::16, c::16, d::16, e::48>>) do
    hex = &Base.encode16(&1, case: :lower)

    [
      hex.(<<a::32>>),
      "-",
      hex.(<<b::16>>),
      "-",
      hex.(<<c::16>>),
      "-",
      hex.(<<d::16>>),
      "-",
      hex.(<<e::48>>)
    ]
    |> IO.iodata_to_binary()
  end

  defp default_client_type do
    otp_release = :erlang.system_info(:otp_release) |> List.to_string()
    "elixir/#{System.version()}/otp#{otp_release}/spark_ex/#{Mix.Project.config()[:version]}"
  end
end
