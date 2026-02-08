defmodule SparkEx.Session do
  @moduledoc """
  Manages a Spark Connect session as a GenServer process.

  Holds the gRPC channel, session ID, server-side session ID tracking,
  and a monotonic plan ID counter.
  """

  use GenServer

  @compile {:no_warn_undefined, Explorer.DataFrame}

  alias SparkEx.Connect.Channel
  alias SparkEx.Connect.Client
  alias SparkEx.Connect.PlanEncoder

  defstruct [
    :channel,
    :connect_opts,
    :session_id,
    :server_side_session_id,
    :user_id,
    :client_type,
    plan_id_counter: 0,
    released: false
  ]

  @type t :: %__MODULE__{
          channel: GRPC.Channel.t() | nil,
          connect_opts: SparkEx.Connect.Channel.connect_opts() | nil,
          session_id: String.t(),
          server_side_session_id: String.t() | nil,
          user_id: String.t(),
          client_type: String.t(),
          plan_id_counter: non_neg_integer(),
          released: boolean()
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
  Clones the current server-side session and returns a new Session process.

  The cloned session inherits server-side state (configs/temp views/etc.) and
  uses a new session ID unless one is explicitly provided.
  """
  @spec clone(GenServer.server(), String.t() | nil) :: {:ok, pid()} | {:error, term()}
  def clone(session, new_session_id \\ nil) do
    GenServer.call(session, {:clone_session, new_session_id})
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
  Executes a plan and returns an `Explorer.DataFrame`.

  Pushes a LIMIT into the plan unless `unsafe: true`. Enforces row/byte bounds.
  `unsafe: true` skips only LIMIT injection; decoder bounds still apply unless
  explicitly overridden.

  ## Options

  - `:max_rows` — maximum rows (default: 10_000)
  - `:max_bytes` — maximum bytes (default: 64 MB)
  - `:unsafe` — skip LIMIT injection only (default: false)
  - `:timeout` — gRPC timeout in ms (default: 60_000)
  """
  @spec execute_explorer(GenServer.server(), term(), keyword()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, term()}
  def execute_explorer(session, plan, opts \\ []) do
    GenServer.call(session, {:execute_explorer, plan, opts}, call_timeout(opts))
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
  Executes a ShowString plan and returns the formatted string.
  """
  @spec execute_show(GenServer.server(), term()) ::
          {:ok, String.t()} | {:error, term()}
  def execute_show(session, plan) do
    GenServer.call(session, {:execute_show, plan}, :timer.seconds(60))
  end

  @doc """
  Releases the server-side session via the `ReleaseSession` RPC.

  After release, all further RPC calls through this session will return
  `{:error, :session_released}`. The GenServer process remains alive but
  the gRPC channel is disconnected.
  """
  @spec release(GenServer.server()) :: :ok | {:error, term()}
  def release(session) do
    GenServer.call(session, :release_session)
  end

  @doc """
  Interrupts all running operations on this session.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_all(GenServer.server()) :: {:ok, [String.t()]} | {:error, term()}
  def interrupt_all(session) do
    GenServer.call(session, {:interrupt, :all})
  end

  @doc """
  Interrupts operations matching the given tag.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_tag(GenServer.server(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def interrupt_tag(session, tag) when is_binary(tag) do
    GenServer.call(session, {:interrupt, {:tag, tag}})
  end

  @doc """
  Interrupts a specific operation by its ID.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_operation(GenServer.server(), String.t()) ::
          {:ok, [String.t()]} | {:error, term()}
  def interrupt_operation(session, operation_id) when is_binary(operation_id) do
    GenServer.call(session, {:interrupt, {:operation_id, operation_id}})
  end

  @doc """
  Stops the session process. Calls `ReleaseSession` if not already released,
  then disconnects the gRPC channel.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(session) do
    GenServer.stop(session)
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(opts) do
    connect_opts_opt = Keyword.get(opts, :connect_opts)
    url_opt = Keyword.get(opts, :url)
    user_id = Keyword.get(opts, :user_id, "spark_ex")
    client_type = Keyword.get(opts, :client_type, default_client_type())
    session_id = Keyword.get(opts, :session_id, generate_uuid())
    observed_server_session_id = Keyword.get(opts, :server_side_session_id, nil)

    with {:ok, connect_opts} <- resolve_connect_opts(url_opt, connect_opts_opt),
         {:ok, channel} <- Channel.connect(connect_opts) do
      state = %__MODULE__{
        channel: channel,
        connect_opts: connect_opts,
        session_id: session_id,
        server_side_session_id: observed_server_session_id,
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

  def handle_call({:clone_session, _new_session_id}, _from, %{released: true} = state) do
    {:reply, {:error, :session_released}, state}
  end

  def handle_call({:clone_session, new_session_id}, _from, state) do
    case Client.clone_session(state, new_session_id) do
      {:ok, clone_info} ->
        state =
          maybe_update_server_session(state, clone_info.source_server_side_session_id)

        clone_opts = [
          connect_opts: state.connect_opts,
          user_id: state.user_id,
          client_type: state.client_type,
          session_id: clone_info.new_session_id,
          server_side_session_id: clone_info.new_server_side_session_id
        ]

        case __MODULE__.start_link(clone_opts) do
          {:ok, clone_session} ->
            {:reply, {:ok, clone_session}, state}

          {:error, _} = error ->
            {:reply, error, state}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:next_plan_id, _from, state) do
    id = state.plan_id_counter
    {:reply, id, %{state | plan_id_counter: id + 1}}
  end

  def handle_call(:release_session, _from, %{released: true} = state) do
    {:reply, :ok, state}
  end

  # --- Released guard: reject RPCs after session release ---

  def handle_call(_msg, _from, %{released: true} = state) do
    {:reply, {:error, :session_released}, state}
  end

  # --- Session lifecycle handlers ---

  def handle_call(:release_session, _from, state) do
    case Client.release_session(state) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        Channel.disconnect(state.channel)
        state = %{state | released: true, channel: nil}
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:interrupt, type}, _from, state) do
    case Client.interrupt(state, type) do
      {:ok, interrupted_ids, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, interrupted_ids}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
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

  def handle_call({:execute_explorer, plan, opts}, _from, state) do
    max_rows = Keyword.get(opts, :max_rows, 10_000)
    unsafe = Keyword.get(opts, :unsafe, false)

    {effective_plan, decoder_opts} =
      if unsafe do
        # Skip remote LIMIT injection only; local decoder limits stay active unless overridden.
        {plan, opts}
      else
        {{:limit, plan, max_rows}, opts}
      end

    {proto_plan, counter} = PlanEncoder.encode(effective_plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan_explorer(state, proto_plan, decoder_opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        {:reply, {:ok, result.dataframe}, state}

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

        case extract_count(result.rows) do
          {:ok, count} -> {:reply, {:ok, count}, state}
          {:error, _} = error -> {:reply, error, state}
        end

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

  def handle_call({:execute_show, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan(state, proto_plan) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)

        case extract_show_string(result.rows) do
          {:ok, str} -> {:reply, {:ok, str}, state}
          {:error, _} = error -> {:reply, error, state}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast({:update_server_side_session_id, id}, state) do
    {:noreply, %{state | server_side_session_id: id}}
  end

  # Silently discard gun messages that arrive after session release
  @impl true
  def handle_info({:gun_data, _, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_trailers, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_error, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_down, _, _, _, _}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    require Logger

    Logger.error(
      "#{inspect(__MODULE__)} #{inspect(self())} received unexpected message in handle_info/2: #{inspect(msg)}"
    )

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{released: true}), do: :ok
  def terminate(_reason, %{channel: nil}), do: :ok

  def terminate(_reason, %{channel: channel} = state) do
    # Best-effort release before disconnect
    _ = Client.release_session(state)
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

  defp extract_count([%{"count(1)" => n}]) when is_integer(n) and n >= 0, do: {:ok, n}

  defp extract_count([row]) when is_map(row) and map_size(row) == 1 do
    case Map.values(row) do
      [n] when is_integer(n) and n >= 0 -> {:ok, n}
      _ -> {:error, {:invalid_count_response, row}}
    end
  end

  defp extract_count(rows), do: {:error, {:invalid_count_response, rows}}

  defp extract_show_string([%{"show_string" => str}]) when is_binary(str), do: {:ok, str}

  defp extract_show_string([row]) when is_map(row) and map_size(row) == 1 do
    case Map.values(row) do
      [str] when is_binary(str) -> {:ok, str}
      _ -> {:error, {:invalid_show_response, row}}
    end
  end

  defp extract_show_string(rows), do: {:error, {:invalid_show_response, rows}}

  defp call_timeout(opts) do
    Keyword.get(opts, :timeout, 60_000) + 5_000
  end

  defp resolve_connect_opts(url, nil) when is_binary(url), do: Channel.parse_uri(url)
  defp resolve_connect_opts(nil, connect_opts) when is_map(connect_opts), do: {:ok, connect_opts}
  defp resolve_connect_opts(url, _connect_opts) when is_binary(url), do: Channel.parse_uri(url)

  defp resolve_connect_opts(_url, _connect_opts) do
    {:error, {:invalid_connect_opts, "expected :url or :connect_opts"}}
  end
end
