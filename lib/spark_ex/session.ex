defmodule SparkEx.Session do
  @moduledoc """
  Manages a Spark Connect session as a GenServer process.

  Holds the gRPC channel, session ID, server-side session ID tracking,
  and a monotonic plan ID counter.
  """

  use GenServer

  alias SparkEx.Connect.Channel
  alias SparkEx.Connect.Client
  alias SparkEx.Connect.PlanEncoder

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
  Executes a plan and collects rows.
  """
  @spec execute_collect(GenServer.server(), term(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def execute_collect(session, plan, opts \\ []) do
    GenServer.call(session, {:execute_collect, plan, opts}, call_timeout(opts))
  end

  @doc """
  Executes a plan wrapped in a count(*) aggregate and returns the count.
  """
  @spec execute_count(GenServer.server(), term()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_count(session, plan) do
    GenServer.call(session, {:execute_count, plan}, :timer.seconds(60))
  end

  @doc """
  Returns the schema for a plan via AnalyzePlan.
  """
  @spec analyze_schema(GenServer.server(), term()) :: {:ok, term()} | {:error, term()}
  def analyze_schema(session, plan) do
    GenServer.call(session, {:analyze_schema, plan})
  end

  @doc """
  Returns the explain string for a plan via AnalyzePlan.
  """
  @spec analyze_explain(GenServer.server(), term(), atom()) ::
          {:ok, String.t()} | {:error, term()}
  def analyze_explain(session, plan, mode \\ :simple) do
    GenServer.call(session, {:analyze_explain, plan, mode})
  end

  @doc """
  Sets Spark configuration key-value pairs.
  """
  @spec config_set(GenServer.server(), [{String.t(), String.t()}]) ::
          :ok | {:error, term()}
  def config_set(session, pairs) do
    GenServer.call(session, {:config_set, pairs})
  end

  @doc """
  Gets Spark configuration values for the given keys.
  """
  @spec config_get(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get(session, keys) do
    GenServer.call(session, {:config_get, keys})
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

  def handle_call({:execute_collect, plan, opts}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan(state, proto_plan, opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        {:reply, {:ok, result.rows}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_count, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode_count(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan(state, proto_plan) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        count = extract_count(result.rows)
        {:reply, {:ok, count}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_schema, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, schema}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_explain, plan, mode}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_explain(state, proto_plan, mode) do
      {:ok, explain_str, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, explain_str}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_set, pairs}, _from, state) do
    case Client.config_set(state, pairs) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_get, keys}, _from, state) do
    case Client.config_get(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

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

  @spark_ex_version Mix.Project.config()[:version]

  defp default_client_type do
    otp_release = :erlang.system_info(:otp_release) |> List.to_string()
    "elixir/#{System.version()}/otp#{otp_release}/spark_ex/#{@spark_ex_version}"
  end

  defp extract_count([%{"count(1)" => n}]), do: n
  defp extract_count([row]) when is_map(row), do: row |> Map.values() |> hd()
  defp extract_count(_), do: 0

  defp call_timeout(opts) do
    Keyword.get(opts, :timeout, 60_000) + 5_000
  end
end
