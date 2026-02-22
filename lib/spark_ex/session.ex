defmodule SparkEx.Session do
  @moduledoc """
  Manages a Spark Connect session as a GenServer process.

  Holds the gRPC channel, session ID, server-side session ID tracking,
  and a monotonic plan ID counter.
  """

  use GenServer

  @compile {:no_warn_undefined, [Explorer.DataFrame, Explorer.Series]}

  alias SparkEx.Connect.Channel
  alias SparkEx.Connect.Client
  alias SparkEx.Connect.PlanEncoder
  alias SparkEx.Connect.TypeMapper
  alias SparkEx.Internal.Tag
  alias SparkEx.Internal.UUID
  require Logger

  defstruct [
    :channel,
    :connect_opts,
    :session_id,
    :server_side_session_id,
    :user_id,
    :client_type,
    allow_arrow_batch_chunking: true,
    preferred_arrow_chunk_size: nil,
    plan_id_counter: 0,
    last_execution_metrics: %{},
    tags: [],
    released: false
  ]

  @type t :: %__MODULE__{
          channel: GRPC.Channel.t() | nil,
          connect_opts: SparkEx.Connect.Channel.connect_opts() | nil,
          session_id: String.t(),
          server_side_session_id: String.t() | nil,
          user_id: String.t(),
          client_type: String.t(),
          allow_arrow_batch_chunking: boolean(),
          preferred_arrow_chunk_size: non_neg_integer() | nil,
          plan_id_counter: non_neg_integer(),
          last_execution_metrics: map(),
          tags: [String.t()],
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
  - `:allow_arrow_batch_chunking` — allow server-side Arrow chunk splitting (default: `true`)
  - `:preferred_arrow_chunk_size` — preferred chunk size in bytes (default: `nil`)
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
  Executes a plan and returns Arrow IPC data.
  """
  @spec execute_arrow(GenServer.server(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def execute_arrow(session, plan, opts \\ []) do
    GenServer.call(session, {:execute_arrow, plan, opts}, call_timeout(opts))
  end

  @doc """
  Generates the next plan ID and returns it.
  """
  @spec next_plan_id(GenServer.server()) :: non_neg_integer() | {:error, :session_released}
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
  Adds a tag to be applied to all subsequent operations in this session.
  """
  @spec add_tag(GenServer.server(), String.t()) :: :ok
  def add_tag(session, tag) when is_binary(tag) do
    Tag.validate!(tag)
    GenServer.cast(session, {:add_tag, tag})
  end

  @doc """
  Removes a tag from the session.
  """
  @spec remove_tag(GenServer.server(), String.t()) :: :ok
  def remove_tag(session, tag) when is_binary(tag) do
    GenServer.cast(session, {:remove_tag, tag})
  end

  @doc """
  Returns all tags set on the session.
  """
  @spec get_tags(GenServer.server()) :: [String.t()]
  def get_tags(session) do
    GenServer.call(session, :get_tags)
  end

  @doc """
  Clears all tags from the session.
  """
  @spec clear_tags(GenServer.server()) :: :ok
  def clear_tags(session) do
    GenServer.cast(session, :clear_tags)
  end

  @doc """
  Registers a progress handler callback for this session.

  The handler receives a map with `:event`, `:measurements`, and `:metadata`.
  """
  @spec register_progress_handler(GenServer.server(), (map() -> any())) :: :ok
  def register_progress_handler(session, handler) when is_function(handler, 1) do
    session_id = session_id_for(session)
    SparkEx.ProgressHandlerRegistry.register(session_id, handler)
  end

  @doc """
  Removes a previously registered progress handler for this session.
  """
  @spec remove_progress_handler(GenServer.server(), (map() -> any())) :: :ok
  def remove_progress_handler(session, handler) when is_function(handler, 1) do
    session_id = session_id_for(session)
    SparkEx.ProgressHandlerRegistry.remove(session_id, handler)
  end

  @doc """
  Clears all progress handlers registered for this session.
  """
  @spec clear_progress_handlers(GenServer.server()) :: :ok
  def clear_progress_handlers(session) do
    session_id = session_id_for(session)
    SparkEx.ProgressHandlerRegistry.clear(session_id)
  end

  @doc """
  Returns whether the session has been released/stopped.
  """
  @spec is_stopped(GenServer.server() | t()) :: boolean()
  def is_stopped(session) do
    if is_pid(session) do
      GenServer.call(session, :is_stopped)
    else
      %__MODULE__{released: released} = session
      released
    end
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
  Executes a plan and returns a managed response stream handle.

  Used by APIs that need incremental row consumption.

  The returned stream handle is enumerable and also supports
  `SparkEx.ManagedStream.close/1` for explicit early cleanup.
  """
  @spec execute_plan_stream(GenServer.server(), term(), keyword()) ::
          {:ok, SparkEx.ManagedStream.t()} | {:error, term()}
  def execute_plan_stream(session, plan, opts \\ []) do
    timeout = call_timeout(opts)

    case safe_get_state(session, timeout) do
      {:ok, %__MODULE__{} = state} ->
        {proto_plan, _counter} = PlanEncoder.encode(plan, 0)
        opts = merge_session_tags(opts, state.tags)

        Client.execute_plan_managed_stream(
          state,
          proto_plan,
          Keyword.put(opts, :stream_owner, self())
        )

      _ ->
        GenServer.call(session, {:execute_plan_stream, plan, opts}, timeout)
    end
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
  Returns execution metrics captured from the last action on this session.
  """
  @spec last_execution_metrics(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def last_execution_metrics(session) do
    GenServer.call(session, :last_execution_metrics)
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
  Gets Spark configuration values with fallback defaults.
  """
  @spec config_get_with_default(GenServer.server(), [{String.t(), String.t()}]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_with_default(session, pairs) do
    GenServer.call(session, {:config_get_with_default, pairs})
  end

  @doc """
  Gets optional Spark configuration values (returns nil for unset keys).
  """
  @spec config_get_option(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_option(session, keys) do
    GenServer.call(session, {:config_get_option, keys})
  end

  @doc """
  Gets all Spark configuration values, optionally filtered by prefix.
  """
  @spec config_get_all(GenServer.server(), String.t() | nil) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_all(session, prefix \\ nil) do
    GenServer.call(session, {:config_get_all, prefix})
  end

  @doc """
  Unsets Spark configuration values.
  """
  @spec config_unset(GenServer.server(), [String.t()]) :: :ok | {:error, term()}
  def config_unset(session, keys) do
    GenServer.call(session, {:config_unset, keys})
  end

  @doc """
  Checks whether configuration keys are modifiable at runtime.
  """
  @spec config_is_modifiable(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  def config_is_modifiable(session, keys) do
    GenServer.call(session, {:config_is_modifiable, keys})
  end

  @doc """
  Returns the tree-string representation of a plan.
  """
  @spec analyze_tree_string(GenServer.server(), term(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def analyze_tree_string(session, plan, opts \\ []) do
    GenServer.call(session, {:analyze_tree_string, plan, opts})
  end

  @doc """
  Checks if a plan is local (i.e., can be computed locally without Spark).
  """
  @spec analyze_is_local(GenServer.server(), term()) :: {:ok, boolean()} | {:error, term()}
  def analyze_is_local(session, plan) do
    GenServer.call(session, {:analyze_is_local, plan})
  end

  @doc """
  Checks if a plan represents a streaming query.
  """
  @spec analyze_is_streaming(GenServer.server(), term()) :: {:ok, boolean()} | {:error, term()}
  def analyze_is_streaming(session, plan) do
    GenServer.call(session, {:analyze_is_streaming, plan})
  end

  @doc """
  Returns the input files for a plan.
  """
  @spec analyze_input_files(GenServer.server(), term()) ::
          {:ok, [String.t()]} | {:error, term()}
  def analyze_input_files(session, plan) do
    GenServer.call(session, {:analyze_input_files, plan})
  end

  @doc """
  Parses a DDL string into a DataType.
  """
  @spec analyze_ddl_parse(GenServer.server(), String.t()) :: {:ok, term()} | {:error, term()}
  def analyze_ddl_parse(session, ddl_string) do
    GenServer.call(session, {:analyze_ddl_parse, ddl_string})
  end

  @doc """
  Converts a JSON schema string to DDL format.
  """
  @spec analyze_json_to_ddl(GenServer.server(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def analyze_json_to_ddl(session, json_string) do
    GenServer.call(session, {:analyze_json_to_ddl, json_string})
  end

  @doc """
  Checks if two plans have the same semantics.
  """
  @spec analyze_same_semantics(GenServer.server(), term(), term()) ::
          {:ok, boolean()} | {:error, term()}
  def analyze_same_semantics(session, plan1, plan2) do
    GenServer.call(session, {:analyze_same_semantics, plan1, plan2})
  end

  @doc """
  Returns the semantic hash of a plan.
  """
  @spec analyze_semantic_hash(GenServer.server(), term()) ::
          {:ok, integer()} | {:error, term()}
  def analyze_semantic_hash(session, plan) do
    GenServer.call(session, {:analyze_semantic_hash, plan})
  end

  @doc """
  Persists a DataFrame's underlying relation with optional storage level.
  """
  @spec analyze_persist(GenServer.server(), term(), keyword()) :: :ok | {:error, term()}
  def analyze_persist(session, plan, opts \\ []) do
    GenServer.call(session, {:analyze_persist, plan, opts})
  end

  @doc """
  Unpersists a DataFrame's underlying relation.
  """
  @spec analyze_unpersist(GenServer.server(), term(), keyword()) :: :ok | {:error, term()}
  def analyze_unpersist(session, plan, opts \\ []) do
    GenServer.call(session, {:analyze_unpersist, plan, opts})
  end

  @doc """
  Returns the storage level of a persisted relation.
  """
  @spec analyze_get_storage_level(GenServer.server(), term()) ::
          {:ok, Spark.Connect.StorageLevel.t()} | {:error, term()}
  def analyze_get_storage_level(session, plan) do
    GenServer.call(session, {:analyze_get_storage_level, plan})
  end

  @doc """
  Checks existence of artifacts on the server.

  Returns a map of artifact name to boolean.
  """
  @spec artifact_status(GenServer.server(), [String.t()]) ::
          {:ok, %{String.t() => boolean()}} | {:error, term()}
  def artifact_status(session, names) do
    GenServer.call(session, {:artifact_status, names})
  end

  @doc """
  Uploads artifacts to the server.

  Artifacts are provided as a list of `{name, data}` tuples.
  Returns a list of `{name, crc_successful?}` tuples.
  """
  @spec add_artifacts(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_artifacts(session, artifacts) do
    GenServer.call(session, {:add_artifacts, artifacts})
  end

  @doc """
  Uploads JAR artifacts to the server.

  Artifact names are automatically prefixed with `jars/`.
  """
  @spec add_jars(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_jars(session, artifacts) do
    prefixed = Enum.map(artifacts, fn {name, data} -> {"jars/#{name}", data} end)
    add_artifacts(session, prefixed)
  end

  @doc """
  Uploads file artifacts to the server.

  Artifact names are automatically prefixed with `files/`.
  """
  @spec add_files(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_files(session, artifacts) do
    prefixed = Enum.map(artifacts, fn {name, data} -> {"files/#{name}", data} end)
    add_artifacts(session, prefixed)
  end

  @doc """
  Uploads archive artifacts to the server.

  Artifact names are automatically prefixed with `archives/`.
  """
  @spec add_archives(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_archives(session, artifacts) do
    prefixed = Enum.map(artifacts, fn {name, data} -> {"archives/#{name}", data} end)
    add_artifacts(session, prefixed)
  end

  @doc """
  Copies a local file to the Spark driver filesystem.

  Reads the file at `local_path` and uploads it as a Spark Connect
  forward-to-filesystem artifact.
  """
  @spec copy_from_local_to_fs(GenServer.server(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def copy_from_local_to_fs(session, local_path, dest_path) do
    with :ok <- validate_forward_dest_path(dest_path),
         {:ok, data} <- read_local_file(local_path),
         {:ok, _summaries} <-
           add_artifacts(session, [{forward_to_fs_artifact_name(dest_path), data}]) do
      :ok
    end
  end

  @doc """
  Creates a DataFrame from local data.

  For small data (under the cache threshold), the data is embedded directly
  in the plan as a `LocalRelation`. For larger data, the Arrow IPC bytes
  are uploaded to the server via `AddArtifacts` and referenced via
  `CachedLocalRelation`.

  ## Options

  - `:schema` — DDL schema string (e.g. `"id INT, name STRING"`). If omitted,
    inferred from the Explorer.DataFrame or from the data.
  - `:cache_threshold` — byte size threshold above which data is cached on the
    server instead of inlined (default: 4 MB)
  """
  @spec create_dataframe(GenServer.server(), term(), keyword()) ::
          {:ok, SparkEx.DataFrame.t()} | {:error, term()}
  def create_dataframe(session, data, opts \\ []) do
    GenServer.call(session, {:create_dataframe, data, opts}, call_timeout(opts))
  end

  @doc """
  Executes a command (write, create view, etc.) and returns :ok or error.

  Commands are side-effecting operations that don't return data rows.
  """
  @spec execute_command(GenServer.server(), term(), keyword()) :: :ok | {:error, term()}
  def execute_command(session, command, opts \\ []) do
    GenServer.call(session, {:execute_command, command, opts}, call_timeout(opts))
  end

  @doc """
  Executes a command and returns the command result data from the response.

  Unlike `execute_command/3` which returns `:ok`, this returns the raw
  command result from the `ExecutePlanResponse` stream (e.g. streaming
  query IDs, status results, etc.).
  """
  @spec execute_command_with_result(GenServer.server(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def execute_command_with_result(session, command, opts \\ []) do
    GenServer.call(session, {:execute_command_with_result, command, opts}, call_timeout(opts))
  end

  @doc """
  Executes a command and returns a managed response stream handle.

  Used for long-lived streaming operations like the listener bus.
  The caller is responsible for consuming/closing the stream.
  """
  @spec execute_command_stream(GenServer.server(), term(), keyword()) ::
          {:ok, SparkEx.ManagedStream.t()} | {:error, term()}
  def execute_command_stream(session, command, opts \\ []) do
    GenServer.call(session, {:execute_command_stream, command, opts}, call_timeout(opts))
  end

  @doc """
  Registers a Java UDF. Convenience delegate to `SparkEx.UDFRegistration.register_java/4`.
  """
  @spec register_java_udf(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, term()}
  def register_java_udf(session, name, class_name, opts \\ []) do
    SparkEx.UDFRegistration.register_java(session, name, class_name, opts)
  end

  @doc """
  Registers a UDTF. Convenience delegate to `SparkEx.UDFRegistration.register_udtf/4`.
  """
  @spec register_udtf(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def register_udtf(session, name, python_command, opts \\ []) do
    SparkEx.UDFRegistration.register_udtf(session, name, python_command, opts)
  end

  @doc """
  Registers a data source. Convenience delegate to `SparkEx.UDFRegistration.register_data_source/4`.
  """
  @spec register_data_source(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def register_data_source(session, name, python_command, opts \\ []) do
    SparkEx.UDFRegistration.register_data_source(session, name, python_command, opts)
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
    session_id = Keyword.get(opts, :session_id, UUID.generate_v4())
    observed_server_session_id = Keyword.get(opts, :server_side_session_id, nil)
    allow_arrow_batch_chunking = Keyword.get(opts, :allow_arrow_batch_chunking, true)
    preferred_arrow_chunk_size = Keyword.get(opts, :preferred_arrow_chunk_size, nil)

    with {:ok, connect_opts} <- resolve_connect_opts(url_opt, connect_opts_opt),
         {:ok, channel} <- Channel.connect(connect_opts) do
      state = %__MODULE__{
        channel: channel,
        connect_opts: connect_opts,
        session_id: session_id,
        server_side_session_id: observed_server_session_id,
        user_id: user_id,
        client_type: client_type,
        allow_arrow_batch_chunking: allow_arrow_batch_chunking,
        preferred_arrow_chunk_size: preferred_arrow_chunk_size
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

  def handle_call(:get_tags, _from, state) do
    {:reply, state.tags, state}
  end

  def handle_call(:get_session_id, _from, state) do
    {:reply, state.session_id, state}
  end

  def handle_call(:is_stopped, _from, state) do
    {:reply, state.released, state}
  end

  def handle_call(:last_execution_metrics, _from, state) do
    {:reply, {:ok, state.last_execution_metrics}, state}
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
          server_side_session_id: clone_info.new_server_side_session_id,
          allow_arrow_batch_chunking: state.allow_arrow_batch_chunking,
          preferred_arrow_chunk_size: state.preferred_arrow_chunk_size
        ]

        case __MODULE__.start_link(clone_opts) do
          {:ok, clone_session} ->
            {:reply, {:ok, clone_session}, state}

          {:error, _} = error ->
            case cleanup_cloned_session_on_start_failure(state, clone_info) do
              :ok ->
                :ok

              {:error, cleanup_error} ->
                Logger.warning(
                  "failed to cleanup orphaned cloned session #{inspect(clone_info.new_session_id)} after start_link failure: #{inspect(cleanup_error)}"
                )
            end

            {:reply, error, state}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:next_plan_id, _from, %{released: true} = state) do
    {:reply, {:error, :session_released}, state}
  end

  def handle_call(:next_plan_id, _from, state) do
    id = state.plan_id_counter
    {:reply, id, %{state | plan_id_counter: id + 1}}
  end

  def handle_call(:release_session, _from, state) do
    if state.released do
      {:reply, :ok, state}
    else
      case Client.release_session(state) do
        {:ok, server_side_session_id} ->
          state = maybe_update_server_session(state, server_side_session_id)
          safe_disconnect(state.channel)
          state = %{state | released: true, channel: nil}
          {:reply, :ok, state}

        {:error, _} = error ->
          # Disconnect channel even on RPC error to prevent resource leak
          safe_disconnect(state.channel)
          state = %{state | released: true, channel: nil}
          {:reply, error, state}
      end
    end
  end

  # --- Released guard: reject RPCs after session release ---

  def handle_call(_msg, _from, %{released: true} = state) do
    {:reply, {:error, :session_released}, state}
  end

  # --- Session lifecycle handlers ---

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
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        opts = merge_session_tags(opts, state.tags)

        case Client.execute_plan(state, proto_plan, opts) do
          {:ok, result} ->
            state = maybe_update_server_session(state, result.server_side_session_id)

            {:ok, result, state} =
              maybe_retry_collect_with_json_projection(state, plan, proto_plan, opts, result)

            state = %{state | last_execution_metrics: result.execution_metrics}
            SparkEx.Observation.store_observed_metrics(result.observed_metrics)
            {:reply, {:ok, result.rows}, state}

          {:error, {:arrow_decode_failed, _reason}} = error ->
            case retry_collect_with_unique_columns(state, plan, proto_plan, opts) do
              {:ok, result, state} ->
                state = maybe_update_server_session(state, result.server_side_session_id)
                {result, state} = maybe_decode_retry_result_rows(state, plan, proto_plan, result)
                state = %{state | last_execution_metrics: result.execution_metrics}
                SparkEx.Observation.store_observed_metrics(result.observed_metrics)
                {:reply, {:ok, result.rows}, state}

              {:error, state} ->
                {:reply, error, state}
            end

          {:error, %SparkEx.Error.Remote{} = remote} = error ->
            case retry_collect_with_legacy_fallbacks(state, plan, opts, remote) do
              {:ok, result, state} ->
                state = maybe_update_server_session(state, result.server_side_session_id)
                state = %{state | last_execution_metrics: result.execution_metrics}
                SparkEx.Observation.store_observed_metrics(result.observed_metrics)
                {:reply, {:ok, result.rows}, state}

              :error ->
                {:reply, error, state}
            end

          {:error, _} = error ->
            {:reply, error, state}
        end

      {nil, error} ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_plan_stream, plan, opts}, {owner, _tag}, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan_managed_stream(
           state,
           proto_plan,
           Keyword.put(opts, :stream_owner, owner)
         ) do
      {:ok, stream} ->
        {:reply, {:ok, stream}, state}

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

    case safe_encode(effective_plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        decoder_opts = merge_session_tags(decoder_opts, state.tags)

        case Client.execute_plan_explorer(state, proto_plan, decoder_opts) do
          {:ok, result} ->
            state = maybe_update_server_session(state, result.server_side_session_id)
            state = %{state | last_execution_metrics: result.execution_metrics}
            SparkEx.Observation.store_observed_metrics(result.observed_metrics)
            {:reply, {:ok, result.dataframe}, state}

          {:error, _} = error ->
            {:reply, error, state}
        end

      {nil, error} ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_arrow, plan, opts}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        opts = merge_session_tags(opts, state.tags)

        case Client.execute_plan_arrow(state, proto_plan, opts) do
          {:ok, result} ->
            state = maybe_update_server_session(state, result.server_side_session_id)
            state = %{state | last_execution_metrics: result.execution_metrics}
            SparkEx.Observation.store_observed_metrics(result.observed_metrics)
            {:reply, {:ok, result.arrow}, state}

          {:error, _} = error ->
            {:reply, error, state}
        end

      {nil, error} ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_count, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode_count(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan(state, proto_plan, merge_session_tags([], state.tags)) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        state = %{state | last_execution_metrics: result.execution_metrics}
        SparkEx.Observation.store_observed_metrics(result.observed_metrics)

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

  def handle_call({:config_get_with_default, pairs}, _from, state) do
    case Client.config_get_with_default(state, pairs) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_get_option, keys}, _from, state) do
    case Client.config_get_option(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_get_all, prefix}, _from, state) do
    case Client.config_get_all(state, prefix) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_unset, keys}, _from, state) do
    case Client.config_unset(state, keys) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:config_is_modifiable, keys}, _from, state) do
    case Client.config_is_modifiable(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_tree_string, plan, opts}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_tree_string(state, proto_plan, opts) do
      {:ok, str, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, str}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_is_local, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_is_local(state, proto_plan) do
      {:ok, is_local, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, is_local}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_is_streaming, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_is_streaming(state, proto_plan) do
      {:ok, is_streaming, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, is_streaming}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_input_files, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_input_files(state, proto_plan) do
      {:ok, files, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, files}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_ddl_parse, ddl_string}, _from, state) do
    case Client.analyze_ddl_parse(state, ddl_string) do
      {:ok, parsed, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, parsed}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_json_to_ddl, json_string}, _from, state) do
    case Client.analyze_json_to_ddl(state, json_string) do
      {:ok, ddl, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, ddl}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_same_semantics, plan1, plan2}, _from, state) do
    {proto_plan1, counter} = PlanEncoder.encode(plan1, state.plan_id_counter)
    {proto_plan2, counter} = PlanEncoder.encode(plan2, counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_same_semantics(state, proto_plan1, proto_plan2) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_semantic_hash, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_semantic_hash(state, proto_plan) do
      {:ok, hash, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, hash}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_persist, plan, opts}, _from, state) do
    {relation, counter} = PlanEncoder.encode_relation(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_persist(state, relation, opts) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_unpersist, plan, opts}, _from, state) do
    {relation, counter} = PlanEncoder.encode_relation(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_unpersist(state, relation, opts) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:analyze_get_storage_level, plan}, _from, state) do
    {relation, counter} = PlanEncoder.encode_relation(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.analyze_get_storage_level(state, relation) do
      {:ok, storage_level, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, storage_level}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:artifact_status, names}, _from, state) do
    case Client.artifact_status(state, names) do
      {:ok, statuses, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, statuses}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:add_artifacts, artifacts}, _from, state) do
    case Client.add_artifacts(state, artifacts) do
      {:ok, summaries, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, summaries}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:create_dataframe, data, opts}, from, state) do
    case prepare_local_data(data, opts) do
      {:ok, {:local_relation, arrow_ipc, schema_ddl}} ->
        cache_threshold = Keyword.get(opts, :cache_threshold, 4 * 1024 * 1024)

        if byte_size(arrow_ipc) <= cache_threshold do
          plan = {:local_relation, arrow_ipc, schema_ddl}
          df = %SparkEx.DataFrame{session: self(), plan: plan}
          {:reply, {:ok, df}, state}
        else
          handle_call({:create_dataframe_chunked_cache, arrow_ipc, schema_ddl}, from, state)
        end

      {:ok, {:sql_relation, query, args}} ->
        df = %SparkEx.DataFrame{session: self(), plan: {:sql, query, args}}
        {:reply, {:ok, df}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:create_dataframe_chunked_cache, arrow_ipc, schema_ddl}, _from, state) do
    # Upload data as cache artifact
    data_hash = :crypto.hash(:sha256, arrow_ipc) |> Base.encode16(case: :lower)
    data_artifact = {"cache/#{data_hash}", arrow_ipc}

    # Upload schema DDL as separate cache artifact (no "schema_" prefix —
    # the server looks up schemaHash directly in the cache key space)
    schema_bytes = if schema_ddl, do: schema_ddl, else: ""
    schema_hash = :crypto.hash(:sha256, schema_bytes) |> Base.encode16(case: :lower)
    schema_artifact = {"cache/#{schema_hash}", schema_bytes}

    artifacts = [data_artifact, schema_artifact]

    case upload_missing_cache_artifacts(state, artifacts) do
      {:ok, state} ->
        plan = {:chunked_cached_local_relation, [data_hash], schema_hash}
        df = %SparkEx.DataFrame{session: self(), plan: plan}
        {:reply, {:ok, df}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_command, command, opts}, _from, state) do
    alias SparkEx.Connect.CommandEncoder

    {proto_plan, counter} = CommandEncoder.encode(command, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan(state, proto_plan, opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        state = %{state | last_execution_metrics: result.execution_metrics}
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_command_with_result, command, opts}, _from, state) do
    alias SparkEx.Connect.CommandEncoder

    {proto_plan, counter} = CommandEncoder.encode(command, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan(state, proto_plan, opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        state = %{state | last_execution_metrics: result.execution_metrics}
        {:reply, {:ok, result.command_result}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_command_stream, command, opts}, {owner, _tag}, state) do
    alias SparkEx.Connect.CommandEncoder

    {proto_plan, counter} = CommandEncoder.encode(command, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan_managed_stream(
           state,
           proto_plan,
           Keyword.put(opts, :stream_owner, owner)
         ) do
      {:ok, stream} ->
        {:reply, {:ok, stream}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_show, plan}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    case Client.execute_plan(state, proto_plan, merge_session_tags([], state.tags)) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        state = %{state | last_execution_metrics: result.execution_metrics}

        case extract_show_string(result.rows) do
          {:ok, str} -> {:reply, {:ok, str}, state}
          {:error, _} = error -> {:reply, error, state}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast({:add_tag, tag}, state) do
    Tag.validate!(tag)
    {:noreply, %{state | tags: state.tags ++ [tag]}}
  end

  @impl true
  def handle_cast({:remove_tag, tag}, state) do
    {:noreply, %{state | tags: Enum.reject(state.tags, &(&1 == tag))}}
  end

  @impl true
  def handle_cast(:clear_tags, state) do
    {:noreply, %{state | tags: []}}
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
    # Best-effort release before disconnect with timeout to prevent blocking
    task = Task.async(fn -> Client.release_session(state) end)
    Task.yield(task, 5_000) || Task.shutdown(task)
    safe_disconnect(channel)
    :ok
  end

  @doc false
  @spec safe_disconnect(term()) :: :ok
  def safe_disconnect(%GRPC.Channel{} = channel) do
    require Logger

    if grpc_disconnect_bug_risk?(channel) do
      Logger.warning(
        "spark_ex session channel disconnect skipped due grpc connection state containing unresolved channels"
      )

      :ok
    else
      do_safe_disconnect(channel)
    end
  end

  def safe_disconnect(channel) do
    do_safe_disconnect(channel)
  end

  defp do_safe_disconnect(channel) do
    require Logger

    try do
      case Channel.disconnect(channel) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning("spark_ex session channel disconnect failed: #{inspect(reason)}")
          :ok
      end
    rescue
      exception ->
        Logger.warning(
          "spark_ex session channel disconnect raised #{inspect(exception.__struct__)}: #{Exception.message(exception)}"
        )

        :ok
    catch
      kind, reason ->
        Logger.warning("spark_ex session channel disconnect #{kind}: #{inspect(reason)}")

        :ok
    end
  end

  # grpc-elixir 0.11.5 can crash during disconnect when the connection manager
  # state contains unresolved real_channels entries ({:error, reason}).
  # Detect that shape and skip disconnect to avoid noisy error logs on release.
  defp grpc_disconnect_bug_risk?(%GRPC.Channel{ref: ref}) when is_reference(ref) do
    case :global.whereis_name({GRPC.Client.Connection, ref}) do
      pid when is_pid(pid) ->
        case safe_connection_state(pid) do
          %{real_channels: real_channels} when is_map(real_channels) ->
            Enum.any?(real_channels, fn {_k, value} -> match?({:error, _}, value) end)

          _ ->
            false
        end

      _ ->
        false
    end
  end

  defp grpc_disconnect_bug_risk?(_), do: false

  defp safe_connection_state(pid) do
    :sys.get_state(pid)
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  # --- Private ---

  defp retry_collect_with_unique_columns(state, plan, proto_plan, opts) do
    with {:ok, schema, server_side_session_id} <- Client.analyze_schema(state, proto_plan) do
      state = maybe_update_server_session(state, server_side_session_id)

      case unique_schema_column_names(schema) do
        unique_names when is_list(unique_names) ->
          case execute_retry_plan(state, {:to_df, plan, unique_names}, opts) do
            {:ok, result, state} ->
              {:ok, result, state}

            {:error, state} ->
              retry_collect_with_json_projection(state, plan, schema, opts)
          end

        _ ->
          retry_collect_with_json_projection(state, plan, schema, opts)
      end
    else
      _ ->
        {:error, state}
    end
  end

  defp maybe_retry_collect_with_json_projection(state, plan, proto_plan, opts, primary_result) do
    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)

        primary_result =
          if maybe_json_relation_plan?(plan) do
            primary_result
          else
            Map.update!(primary_result, :rows, &decode_rows_from_json_projection(&1, schema))
          end

        if schema_has_nested_map?(schema) and
             rows_show_nested_map_decode_loss?(primary_result.rows, schema) do
          case retry_collect_with_json_projection(state, plan, schema, opts) do
            {:ok, retry_result, state} ->
              retry_result =
                if maybe_json_relation_plan?(plan) do
                  retry_result
                else
                  Map.update!(retry_result, :rows, &decode_rows_from_json_projection(&1, schema))
                end

              {:ok, retry_result, state}

            _ ->
              {:ok, primary_result, state}
          end
        else
          {:ok, primary_result, state}
        end

      _ ->
        {:ok, primary_result, state}
    end
  end

  defp maybe_json_relation_plan?({:sql, query, _args}) when is_binary(query) do
    String.contains?(query, "from_json(_spark_ex_json")
  end

  defp maybe_json_relation_plan?(_), do: false

  defp maybe_decode_retry_result_rows(state, plan, proto_plan, result) do
    if maybe_json_relation_plan?(plan) do
      {result, state}
    else
      case Client.analyze_schema(state, proto_plan) do
        {:ok, schema, server_side_session_id} ->
          state = maybe_update_server_session(state, server_side_session_id)
          result = Map.update!(result, :rows, &decode_rows_from_json_projection(&1, schema))
          {result, state}

        _ ->
          {result, state}
      end
    end
  end

  defp execute_retry_plan(state, retry_plan, opts) do
    with {{retry_proto_plan, counter}, nil} <- safe_encode(retry_plan, state.plan_id_counter) do
      state = %{state | plan_id_counter: counter}

      case Client.execute_plan(state, retry_proto_plan, opts) do
        {:ok, result} ->
          {:ok, result, state}

        {:error, {:arrow_decode_failed, _reason}} ->
          retry_collect_with_unique_columns(state, retry_plan, retry_proto_plan, opts)

        {:error, _} ->
          {:error, state}
      end
    else
      _ ->
        {:error, state}
    end
  end

  defp retry_collect_with_legacy_fallbacks(
         state,
         plan,
         opts,
         %SparkEx.Error.Remote{message: message}
       )
       when is_binary(message) do
    cond do
      String.contains?(message, "Unknown Group Type UNRECOGNIZED") ->
        with {:ok, rewritten_plan} <- rewrite_grouping_sets_collect_plan(plan),
             {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
          {:ok, result, state}
        else
          _ -> :error
        end

      String.contains?(message, "Does not support convert UNPARSED to catalyst types.") ->
        with {:ok, rewritten_plan} <- rewrite_parse_collect_plan(state, plan),
             {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
          {:ok, result, state}
        else
          _ -> :error
        end

      String.contains?(message, "Expected Relation to be set, but is empty.") ->
        retry_collect_for_empty_relation_errors(state, plan, opts)

      true ->
        :error
    end
  end

  defp retry_collect_with_legacy_fallbacks(_state, _plan, _opts, _remote), do: :error

  defp retry_collect_for_empty_relation_errors(state, plan, opts) do
    with {:ok, rewritten_plan} <- rewrite_transpose_collect_plan(plan),
         {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
      {:ok, result, state}
    else
      _ ->
        with {:ok, rewritten_plan} <- rewrite_table_function_collect_plan(plan),
             {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
          {:ok, result, state}
        else
          _ ->
            with {:ok, rewritten_plan} <- rewrite_as_of_join_collect_plan(plan),
                 {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
              {:ok, result, state}
            else
              _ ->
                with {:ok, rewritten_plan} <- rewrite_subquery_collect_plan(plan),
                     {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
                  {:ok, result, state}
                else
                  _ -> :error
                end
            end
        end
    end
  end

  defp rewrite_transpose_collect_plan({:transpose, child_plan, index_columns}) do
    case transpose_emulation_plan(child_plan, index_columns) do
      {:ok, rewritten} -> {:ok, rewritten}
      :error -> :error
    end
  end

  defp rewrite_transpose_collect_plan({:sort, child_plan, sort_orders}) do
    with {:ok, rewritten_child} <- rewrite_transpose_collect_plan(child_plan) do
      {:ok, {:sort, rewritten_child, sort_orders}}
    end
  end

  defp rewrite_transpose_collect_plan({:sort, child_plan, sort_orders, is_global}) do
    with {:ok, rewritten_child} <- rewrite_transpose_collect_plan(child_plan) do
      {:ok, {:sort, rewritten_child, sort_orders, is_global}}
    end
  end

  defp rewrite_transpose_collect_plan(_plan), do: :error

  defp rewrite_table_function_collect_plan({:table_valued_function, function_name, arg_exprs})
       when is_binary(function_name) and is_list(arg_exprs) do
    with {:ok, args_sql} <- expr_list_to_sql(arg_exprs) do
      sql =
        if args_sql == [] do
          "SELECT * FROM #{function_name}()"
        else
          "SELECT * FROM #{function_name}(#{Enum.join(args_sql, ", ")})"
        end

      {:ok, {:sql, sql, []}}
    end
  end

  defp rewrite_table_function_collect_plan(_plan), do: :error

  defp rewrite_as_of_join_collect_plan(
         {:as_of_join, left_plan, right_plan, _left_as_of, _right_as_of, join_expr, using_columns,
          _join_type, _tolerance, _allow_exact_matches, _direction}
       ) do
    condition =
      case join_expr do
        {:lit, nil} -> nil
        other -> other
      end

    {:ok, {:join, left_plan, right_plan, condition, :left, using_columns || []}}
  end

  defp rewrite_as_of_join_collect_plan(_plan), do: :error

  defp transpose_emulation_plan(child_plan, [index_expr]) do
    with {:ok, index_name} <- extract_col_name(index_expr) do
      unpivot_plan =
        {:unpivot, child_plan, [{:col, index_name}], nil, "__spark_ex_transpose_key",
         "__spark_ex_transpose_value"}

      pivot_plan =
        {:aggregate, unpivot_plan, :pivot, [{:col, "__spark_ex_transpose_key"}],
         [{:fn, "first", [{:col, "__spark_ex_transpose_value"}], false}], {:col, index_name}, nil}

      {:ok,
       {:sort, pivot_plan,
        [{:sort_order, {:col, "__spark_ex_transpose_key"}, :asc, :nulls_first}]}}
    end
  end

  defp transpose_emulation_plan(_child_plan, _index_columns), do: :error

  defp rewrite_grouping_sets_collect_plan({:sort, child_plan, sort_orders}) do
    with {:ok, rewritten_child} <- rewrite_grouping_sets_collect_plan(child_plan) do
      {:ok, {:sort, rewritten_child, sort_orders}}
    end
  end

  defp rewrite_grouping_sets_collect_plan({:sort, child_plan, sort_orders, is_global}) do
    with {:ok, rewritten_child} <- rewrite_grouping_sets_collect_plan(child_plan) do
      {:ok, {:sort, rewritten_child, sort_orders, is_global}}
    end
  end

  defp rewrite_grouping_sets_collect_plan(
         {:aggregate, child_plan, :grouping_sets, grouping_exprs, agg_exprs, grouping_sets}
       ) do
    with {:ok, grouping_names} <- extract_col_names(grouping_exprs),
         {:ok, agg_aliases} <- extract_agg_alias_names(agg_exprs),
         {:ok, set_specs} <- extract_grouping_set_specs(grouping_sets) do
      set_plans =
        Enum.map(set_specs, fn {set_exprs, set_names} ->
          grouped_plan = {:aggregate, child_plan, :groupby, set_exprs, agg_exprs}
          set_name_set = MapSet.new(set_names)

          grouping_projections =
            Enum.map(grouping_names, fn grouping_name ->
              expr =
                if MapSet.member?(set_name_set, grouping_name),
                  do: {:col, grouping_name},
                  else: {:lit, nil}

              {:alias, expr, grouping_name}
            end)

          agg_projections =
            Enum.map(agg_aliases, fn alias_name ->
              {:alias, {:col, alias_name}, alias_name}
            end)

          {:project, grouped_plan, grouping_projections ++ agg_projections}
        end)

      case set_plans do
        [first_plan | rest_plans] ->
          rewritten =
            Enum.reduce(rest_plans, first_plan, fn set_plan, acc ->
              {:set_operation, acc, set_plan, :union, true}
            end)

          {:ok, rewritten}

        [] ->
          :error
      end
    end
  end

  defp rewrite_grouping_sets_collect_plan(_plan), do: :error

  defp rewrite_subquery_collect_plan(plan) do
    {rewritten, changed?} = rewrite_subquery_plan(plan)
    if changed?, do: {:ok, rewritten}, else: :error
  end

  defp rewrite_parse_collect_plan(
         state,
         {:parse, child_plan, format, schema, options}
       )
       when format in [:csv, :json] do
    with {:ok, source_column} <- first_schema_column_name(state, child_plan),
         {:ok, parsed_field_names} <- parse_schema_field_names(state, schema),
         {:ok, parse_expr} <- build_parse_expression(format, source_column, schema, options) do
      parsed_alias = "__spark_ex_parsed"
      parsed_plan = {:project, child_plan, [{:alias, parse_expr, parsed_alias}]}

      projected_fields =
        Enum.map(parsed_field_names, fn field_name ->
          {:alias, {:unresolved_extract_value, {:col, parsed_alias}, {:lit, field_name}}, field_name}
        end)

      {:ok, {:project, parsed_plan, projected_fields}}
    end
  end

  defp rewrite_parse_collect_plan(_state, _plan), do: :error

  defp first_schema_column_name(state, plan) do
    with {{proto_plan, _counter}, nil} <- safe_encode(plan, 0),
         {:ok, schema, _server_side_session_id} <- Client.analyze_schema(state, proto_plan),
         %Spark.Connect.DataType{kind: {:struct, %Spark.Connect.DataType.Struct{fields: [first | _]}}} <-
           schema,
         name when is_binary(name) <- first.name do
      {:ok, name}
    else
      _ -> :error
    end
  end

  defp parse_schema_field_names(_state, nil), do: :error

  defp parse_schema_field_names(_state, %Spark.Connect.DataType{kind: {:struct, struct}}) do
    {:ok, Enum.map(struct.fields, & &1.name)}
  end

  defp parse_schema_field_names(state, schema) when is_binary(schema) do
    with {:ok, parsed, _server_side_session_id} <- Client.analyze_ddl_parse(state, schema),
         %Spark.Connect.DataType{kind: {:struct, struct}} <- parsed do
      {:ok, Enum.map(struct.fields, & &1.name)}
    else
      _ -> :error
    end
  end

  defp parse_schema_field_names(_state, _schema), do: :error

  defp build_parse_expression(_format, _source_column, nil, _options), do: :error

  defp build_parse_expression(format, source_column, schema, options) do
    function_name =
      case format do
        :csv -> "from_csv"
        :json -> "from_json"
      end

    schema_expr =
      case schema do
        %Spark.Connect.DataType{} = data_type -> {:lit, SparkEx.Types.data_type_to_json(data_type)}
        s when is_binary(s) -> {:lit, s}
      end

    args =
      case options_to_map_expression(options) do
        nil -> [{:col, source_column}, schema_expr]
        opts_expr -> [{:col, source_column}, schema_expr, opts_expr]
      end

    {:ok, {:fn, function_name, args, false}}
  end

  defp options_to_map_expression(nil), do: nil
  defp options_to_map_expression(%{} = options) when map_size(options) == 0, do: nil

  defp options_to_map_expression(%{} = options) do
    kvs =
      options
      |> Enum.flat_map(fn {k, v} -> [{:lit, to_string(k)}, {:lit, to_string(v)}] end)

    {:fn, "map", kvs, false}
  end

  defp options_to_map_expression(_), do: nil

  defp rewrite_subquery_plan({:sort, child_plan, sort_orders}) do
    {child_plan, child_changed?} = rewrite_subquery_plan(child_plan)
    {sort_orders, sort_changed?} = rewrite_subquery_expr_list(sort_orders)
    {{:sort, child_plan, sort_orders}, child_changed? or sort_changed?}
  end

  defp rewrite_subquery_plan({:sort, child_plan, sort_orders, is_global}) do
    {child_plan, child_changed?} = rewrite_subquery_plan(child_plan)
    {sort_orders, sort_changed?} = rewrite_subquery_expr_list(sort_orders)
    {{:sort, child_plan, sort_orders, is_global}, child_changed? or sort_changed?}
  end

  defp rewrite_subquery_plan({:filter, child_plan, condition}) do
    {child_plan, child_changed?} = rewrite_subquery_plan(child_plan)
    {condition, cond_changed?} = rewrite_subquery_expr(condition)
    {{:filter, child_plan, condition}, child_changed? or cond_changed?}
  end

  defp rewrite_subquery_plan({:project, child_plan, expressions}) do
    {child_plan, child_changed?} = rewrite_subquery_plan(child_plan)
    {expressions, expr_changed?} = rewrite_subquery_expr_list(expressions)
    {{:project, child_plan, expressions}, child_changed? or expr_changed?}
  end

  defp rewrite_subquery_plan({:limit, child_plan, limit}) do
    {child_plan, changed?} = rewrite_subquery_plan(child_plan)
    {{:limit, child_plan, limit}, changed?}
  end

  defp rewrite_subquery_plan(plan), do: {plan, false}

  defp rewrite_subquery_expr({:subquery, subquery_type, referenced_plan, opts} = expr)
       when is_list(opts) do
    case subquery_to_sql_expression(subquery_type, referenced_plan, opts) do
      {:ok, sql_expr} -> {sql_expr, true}
      :error -> {expr, false}
    end
  end

  defp rewrite_subquery_expr({:fn, name, args, is_distinct}) do
    {args, changed?} = rewrite_subquery_expr_list(args)
    {{:fn, name, args, is_distinct}, changed?}
  end

  defp rewrite_subquery_expr({:alias, expr, name}) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {{:alias, expr, name}, changed?}
  end

  defp rewrite_subquery_expr({:alias, expr, name, metadata}) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {{:alias, expr, name, metadata}, changed?}
  end

  defp rewrite_subquery_expr({:cast, expr, type_str}) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {{:cast, expr, type_str}, changed?}
  end

  defp rewrite_subquery_expr({:cast, expr, type_str, mode}) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {{:cast, expr, type_str, mode}, changed?}
  end

  defp rewrite_subquery_expr({:sort_order, expr, direction, null_order}) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {{:sort_order, expr, direction, null_order}, changed?}
  end

  defp rewrite_subquery_expr(%SparkEx.Column{expr: expr} = col) do
    {expr, changed?} = rewrite_subquery_expr(expr)
    {%{col | expr: expr}, changed?}
  end

  defp rewrite_subquery_expr(expr), do: {expr, false}

  defp rewrite_subquery_expr_list(values) when is_list(values) do
    Enum.map_reduce(values, false, fn value, acc_changed? ->
      {value, value_changed?} = rewrite_subquery_expr(value)
      {value, acc_changed? or value_changed?}
    end)
  end

  defp subquery_to_sql_expression(subquery_type, referenced_plan, opts) when is_list(opts) do
    if explicit_subquery_reference_plan?(referenced_plan) do
      :error
    else
      referenced_plan = normalize_subquery_reference_plan(referenced_plan)

      with {:ok, subquery_sql} <- subquery_plan_to_sql(referenced_plan) do
        case subquery_type do
          :scalar ->
            {:ok, {:expr, "(#{subquery_sql})"}}

          :exists ->
            {:ok, {:expr, "EXISTS (#{subquery_sql})"}}

          :in ->
            case Keyword.get(opts, :in_values, []) do
              [] ->
                :error

              in_values ->
                with {:ok, in_values_sql} <- expr_list_to_sql(in_values) do
                  left_expr_sql =
                    case in_values_sql do
                      [single] -> single
                      many -> "(" <> Enum.join(many, ", ") <> ")"
                    end

                  {:ok, {:expr, "#{left_expr_sql} IN (#{subquery_sql})"}}
                end
            end

          _ ->
            :error
        end
      end
    end
  end

  defp explicit_subquery_reference_plan?(%{plan_id: plan_id, plan: _plan})
       when is_integer(plan_id),
       do: true

  defp explicit_subquery_reference_plan?({plan_id, _plan}) when is_integer(plan_id), do: true
  defp explicit_subquery_reference_plan?({:plan_id, plan_id, _plan}) when is_integer(plan_id), do: true
  defp explicit_subquery_reference_plan?(_), do: false

  defp normalize_subquery_reference_plan({:plan_id, _plan_id, plan}), do: plan
  defp normalize_subquery_reference_plan(%{plan: plan}), do: plan
  defp normalize_subquery_reference_plan(plan), do: plan

  defp subquery_plan_to_sql({:sql, query, args}) when args in [nil, []] and is_binary(query),
    do: {:ok, query}

  defp subquery_plan_to_sql({:project, child_plan, expressions}) when is_list(expressions) do
    with {:ok, child_sql} <- subquery_plan_to_sql(child_plan),
         {:ok, select_items} <- select_items_to_sql(expressions) do
      {:ok, "SELECT #{Enum.join(select_items, ", ")} FROM (#{child_sql}) spark_ex_sub"}
    end
  end

  defp subquery_plan_to_sql({:filter, child_plan, condition}) do
    with {:ok, child_sql} <- subquery_plan_to_sql(child_plan),
         {:ok, condition_sql} <- expr_to_sql(condition) do
      {:ok, "SELECT * FROM (#{child_sql}) spark_ex_sub WHERE #{condition_sql}"}
    end
  end

  defp subquery_plan_to_sql({:sort, child_plan, sort_orders}) do
    with {:ok, child_sql} <- subquery_plan_to_sql(child_plan),
         {:ok, order_sql} <- sort_orders_to_sql(sort_orders) do
      {:ok, "SELECT * FROM (#{child_sql}) spark_ex_sub ORDER BY #{Enum.join(order_sql, ", ")}"}
    end
  end

  defp subquery_plan_to_sql({:sort, child_plan, sort_orders, _is_global}) do
    subquery_plan_to_sql({:sort, child_plan, sort_orders})
  end

  defp subquery_plan_to_sql({:limit, child_plan, limit}) when is_integer(limit) and limit >= 0 do
    with {:ok, child_sql} <- subquery_plan_to_sql(child_plan) do
      {:ok, "SELECT * FROM (#{child_sql}) spark_ex_sub LIMIT #{limit}"}
    end
  end

  defp subquery_plan_to_sql(_plan), do: :error

  defp select_items_to_sql(expressions) do
    sql_items =
      Enum.map(expressions, fn
        {:alias, expr, name} when is_binary(name) ->
          with {:ok, expr_sql} <- expr_to_sql(expr) do
            {:ok, "#{expr_sql} AS #{quote_sql_identifier(name)}"}
          end

        expr ->
          expr_to_sql(expr)
      end)

    if Enum.all?(sql_items, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(sql_items, fn {:ok, sql} -> sql end)}
    else
      :error
    end
  end

  defp sort_orders_to_sql(sort_orders) when is_list(sort_orders) do
    sql_items =
      Enum.map(sort_orders, fn
        {:sort_order, expr, direction, null_ordering} ->
          with {:ok, expr_sql} <- expr_to_sql(expr) do
            dir =
              case direction do
                :desc -> "DESC"
                _ -> "ASC"
              end

            nulls =
              case null_ordering do
                :nulls_last -> "NULLS LAST"
                _ -> "NULLS FIRST"
              end

            {:ok, "#{expr_sql} #{dir} #{nulls}"}
          end

        _ ->
          :error
      end)

    if Enum.all?(sql_items, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(sql_items, fn {:ok, sql} -> sql end)}
    else
      :error
    end
  end

  defp expr_list_to_sql(exprs) when is_list(exprs) do
    sql_items = Enum.map(exprs, &expr_to_sql/1)

    if Enum.all?(sql_items, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(sql_items, fn {:ok, sql} -> sql end)}
    else
      :error
    end
  end

  defp expr_to_sql(%SparkEx.Column{expr: expr}), do: expr_to_sql(expr)

  defp expr_to_sql({:col, name}) when is_binary(name), do: {:ok, quote_sql_identifier(name)}

  defp expr_to_sql({:col, name, _plan_ref}) when is_binary(name),
    do: {:ok, quote_sql_identifier(name)}

  defp expr_to_sql({:lit, value}), do: {:ok, literal_to_sql(value)}
  defp expr_to_sql({:expr, sql}) when is_binary(sql), do: {:ok, "(#{sql})"}

  defp expr_to_sql({:fn, name, args, is_distinct}) when is_binary(name) and is_list(args) do
    with {:ok, arg_sql} <- expr_list_to_sql(args) do
      case {name, arg_sql} do
        {"==", [left, right]} -> {:ok, "(#{left} = #{right})"}
        {"!=", [left, right]} -> {:ok, "(#{left} <> #{right})"}
        {"<>", [left, right]} -> {:ok, "(#{left} <> #{right})"}
        {">", [left, right]} -> {:ok, "(#{left} > #{right})"}
        {"<", [left, right]} -> {:ok, "(#{left} < #{right})"}
        {">=", [left, right]} -> {:ok, "(#{left} >= #{right})"}
        {"<=", [left, right]} -> {:ok, "(#{left} <= #{right})"}
        {"and", [left, right]} -> {:ok, "(#{left} AND #{right})"}
        {"or", [left, right]} -> {:ok, "(#{left} OR #{right})"}
        {"not", [arg]} -> {:ok, "(NOT #{arg})"}
        {"+", [left, right]} -> {:ok, "(#{left} + #{right})"}
        {"-", [left, right]} -> {:ok, "(#{left} - #{right})"}
        {"*", [left, right]} -> {:ok, "(#{left} * #{right})"}
        {"/", [left, right]} -> {:ok, "(#{left} / #{right})"}
        _ -> {:ok, sql_function_call(name, arg_sql, is_distinct)}
      end
    end
  end

  defp expr_to_sql(_expr), do: :error

  defp sql_function_call(name, args_sql, is_distinct) do
    args_sql =
      case {is_distinct, args_sql} do
        {true, [first | rest]} -> ["DISTINCT " <> first | rest]
        _ -> args_sql
      end

    "#{name}(#{Enum.join(args_sql, ", ")})"
  end

  defp literal_to_sql(nil), do: "NULL"
  defp literal_to_sql(true), do: "TRUE"
  defp literal_to_sql(false), do: "FALSE"
  defp literal_to_sql(value) when is_integer(value), do: Integer.to_string(value)
  defp literal_to_sql(value) when is_float(value), do: Float.to_string(value)
  defp literal_to_sql(%Decimal{} = value), do: Decimal.to_string(value)

  defp literal_to_sql(%Date{} = value), do: "'#{Date.to_iso8601(value)}'"

  defp literal_to_sql(%NaiveDateTime{} = value),
    do: "'#{NaiveDateTime.to_iso8601(value)}'"

  defp literal_to_sql(%DateTime{} = value), do: "'#{DateTime.to_iso8601(value)}'"

  defp literal_to_sql(value) when is_binary(value) do
    escaped =
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("'", "''")

    "'#{escaped}'"
  end

  defp literal_to_sql(other), do: "'#{to_string(other)}'"

  defp quote_sql_identifier(name) when is_binary(name) do
    name
    |> String.split(".")
    |> Enum.map(fn part ->
      escaped = String.replace(part, "`", "``")
      "`#{escaped}`"
    end)
    |> Enum.join(".")
  end

  defp extract_grouping_set_specs(grouping_sets) when is_list(grouping_sets) do
    set_specs =
      Enum.map(grouping_sets, fn set_exprs ->
        with {:ok, set_names} <- extract_col_names(set_exprs) do
          {:ok, {set_exprs, set_names}}
        end
      end)

    if Enum.all?(set_specs, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(set_specs, fn {:ok, spec} -> spec end)}
    else
      :error
    end
  end

  defp extract_grouping_set_specs(_), do: :error

  defp extract_col_names(exprs) when is_list(exprs) do
    names = Enum.map(exprs, &extract_col_name/1)

    if Enum.all?(names, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(names, fn {:ok, name} -> name end)}
    else
      :error
    end
  end

  defp extract_col_names(_), do: :error

  defp extract_col_name({:col, name}) when is_binary(name), do: {:ok, name}
  defp extract_col_name({:col, name, _plan_ref}) when is_binary(name), do: {:ok, name}
  defp extract_col_name(_expr), do: :error

  defp extract_agg_alias_names(agg_exprs) when is_list(agg_exprs) do
    names =
      Enum.map(agg_exprs, fn
        {:alias, _expr, name} when is_binary(name) -> {:ok, name}
        {:alias, _expr, [name | _]} when is_binary(name) -> {:ok, name}
        _ -> :error
      end)

    if Enum.all?(names, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(names, fn {:ok, name} -> name end)}
    else
      :error
    end
  end

  defp extract_agg_alias_names(_), do: :error

  defp retry_collect_with_json_projection(state, plan, schema, opts) do
    case json_fallback_projection_plan(plan, schema) do
      nil -> {:error, state}
      retry_plan -> execute_retry_plan(state, retry_plan, opts)
    end
  end

  defp schema_has_nested_map?(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    Enum.any?(struct.fields, fn field ->
      nested_map_type?(field.data_type, 1)
    end)
  end

  defp schema_has_nested_map?(_), do: false

  defp nested_map_type?(
         %Spark.Connect.DataType{
           kind:
             {:map,
              %Spark.Connect.DataType.Map{key_type: key_type, value_type: value_type}}
         },
         depth
       ) do
    depth > 1 or nested_map_type?(key_type, depth + 1) or nested_map_type?(value_type, depth + 1)
  end

  defp nested_map_type?(%Spark.Connect.DataType{kind: {:array, array}}, depth) do
    nested_map_type?(array.element_type, depth + 1)
  end

  defp nested_map_type?(%Spark.Connect.DataType{kind: {:struct, struct}}, depth) do
    Enum.any?(struct.fields, fn field ->
      nested_map_type?(field.data_type, depth + 1)
    end)
  end

  defp nested_map_type?(_other, _depth), do: false

  defp rows_show_nested_map_decode_loss?(rows, %Spark.Connect.DataType{kind: {:struct, struct}})
       when is_list(rows) do
    map_paths = nested_map_paths(struct, [])

    map_paths != [] and
      Enum.any?(rows, fn row ->
        Enum.any?(map_paths, fn path ->
          row
          |> map_value_at_path(path)
          |> suspicious_nested_map_decode_value?()
        end)
      end)
  end

  defp rows_show_nested_map_decode_loss?(_rows, _schema), do: false

  defp suspicious_nested_map_decode_value?(value) when is_binary(value), do: true
  defp suspicious_nested_map_decode_value?([]), do: true
  defp suspicious_nested_map_decode_value?([[]]), do: true
  defp suspicious_nested_map_decode_value?(_value), do: false

  defp nested_map_paths(%Spark.Connect.DataType.Struct{fields: fields}, prefix) do
    Enum.flat_map(fields, fn field ->
      nested_map_paths(field.data_type, prefix ++ [field.name])
    end)
  end

  defp nested_map_paths(%Spark.Connect.DataType{kind: {:map, _}}, prefix), do: [prefix]

  defp nested_map_paths(%Spark.Connect.DataType{kind: {:array, array}}, prefix) do
    nested_map_paths(array.element_type, prefix)
  end

  defp nested_map_paths(%Spark.Connect.DataType{kind: {:struct, struct}}, prefix) do
    nested_map_paths(struct, prefix)
  end

  defp nested_map_paths(_other, _prefix), do: []

  defp map_value_at_path(current, []), do: current

  defp map_value_at_path(current, [key | rest]) when is_map(current) do
    current
    |> Map.get(key)
    |> map_value_at_path(rest)
  end

  defp map_value_at_path(_current, _path), do: nil

  defp decode_rows_from_json_projection(
         rows,
         %Spark.Connect.DataType{kind: {:struct, %Spark.Connect.DataType.Struct{fields: fields}}}
       )
       when is_list(rows) do
    field_types =
      fields
      |> Enum.filter(fn field -> complex_field_type?(field.data_type) end)
      |> Map.new(fn field -> {field.name, field.data_type} end)

    Enum.map(rows, fn
      row when is_map(row) ->
        Enum.reduce(field_types, row, fn {name, data_type}, acc ->
          Map.update(acc, name, nil, &decode_complex_json_value(&1, data_type))
        end)

      other ->
        other
    end)
  end

  defp decode_rows_from_json_projection(rows, _schema), do: rows

  defp decode_complex_json_value(nil, _data_type), do: nil

  defp decode_complex_json_value(value, data_type) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> coerce_complex_decoded_value(decoded, data_type)
      _ -> value
    end
  end

  defp decode_complex_json_value(value, _data_type), do: value

  defp coerce_complex_decoded_value(
         value,
         %Spark.Connect.DataType{kind: {:array, %Spark.Connect.DataType.Array{element_type: element_type}}}
       )
       when is_list(value) do
    Enum.map(value, &coerce_complex_decoded_value(&1, element_type))
  end

  defp coerce_complex_decoded_value(
         value,
         %Spark.Connect.DataType{
           kind:
             {:map,
              %Spark.Connect.DataType.Map{key_type: key_type, value_type: value_type}}
         }
       )
       when is_map(value) do
    Enum.map(value, fn {key, item} ->
      %{
        "key" => coerce_json_map_key(key, key_type),
        "value" => coerce_complex_decoded_value(item, value_type)
      }
    end)
  end

  defp coerce_complex_decoded_value(
         value,
         %Spark.Connect.DataType{kind: {:struct, %Spark.Connect.DataType.Struct{fields: fields}}}
       )
       when is_map(value) do
    Enum.reduce(fields, %{}, fn field, acc ->
      Map.put(
        acc,
        field.name,
        coerce_complex_decoded_value(Map.get(value, field.name), field.data_type)
      )
    end)
  end

  defp coerce_complex_decoded_value(value, _data_type), do: value

  defp coerce_json_map_key(key, %Spark.Connect.DataType{kind: {tag, _}})
       when tag in [:byte, :short, :integer, :long] do
    case Integer.parse(to_string(key)) do
      {parsed, ""} -> parsed
      _ -> key
    end
  end

  defp coerce_json_map_key(key, %Spark.Connect.DataType{kind: {tag, _}})
       when tag in [:float, :double] do
    case Float.parse(to_string(key)) do
      {parsed, ""} -> parsed
      _ -> key
    end
  end

  defp coerce_json_map_key(key, _key_type), do: key

  defp json_fallback_projection_plan(
         plan,
         %Spark.Connect.DataType{kind: {:struct, %{fields: fields}}}
       ) do
    has_fallback_fields? =
      Enum.any?(fields, fn field ->
        fallback_projection_field_type?(field.data_type)
      end)

    if has_fallback_fields? do
      expressions =
        Enum.map(fields, fn field ->
          name = field.name

          expr = fallback_projection_expression(name, field.data_type)

          {:alias, expr, name}
        end)

      {:project, plan, expressions}
    else
      nil
    end
  end

  defp json_fallback_projection_plan(_plan, _schema), do: nil

  defp fallback_projection_expression(name, data_type) do
    cond do
      complex_field_type?(data_type) ->
        {:fn, "to_json", [{:col, name}], false}

      unsupported_arrow_scalar_type?(data_type) ->
        {:cast, {:col, name}, "STRING"}

      true ->
        {:col, name}
    end
  end

  defp fallback_projection_field_type?(data_type) do
    complex_field_type?(data_type) or unsupported_arrow_scalar_type?(data_type)
  end

  defp complex_field_type?(%Spark.Connect.DataType{kind: {tag, _}}),
    do: tag in [:array, :struct, :map]

  defp complex_field_type?(_), do: false

  defp unsupported_arrow_scalar_type?(%Spark.Connect.DataType{kind: {tag, _}}),
    do: tag in [:year_month_interval, :day_time_interval, :calendar_interval]

  defp unsupported_arrow_scalar_type?(_), do: false

  defp unique_schema_column_names(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    names = Enum.map(struct.fields, & &1.name)
    unique_names = dedupe_column_names(names)

    if unique_names == names do
      :no_duplicates
    else
      unique_names
    end
  end

  defp unique_schema_column_names(_), do: :no_duplicates

  defp dedupe_column_names(names) when is_list(names) do
    {unique_names, _used} =
      Enum.map_reduce(names, MapSet.new(), fn name, used ->
        candidate = next_unique_column_name(name, used, 0)
        {candidate, MapSet.put(used, candidate)}
      end)

    unique_names
  end

  defp next_unique_column_name(name, used, attempt) do
    candidate =
      if attempt == 0 do
        name
      else
        "#{name}_#{attempt}"
      end

    if MapSet.member?(used, candidate) do
      next_unique_column_name(name, used, attempt + 1)
    else
      candidate
    end
  end

  defp safe_encode(plan, counter) do
    {PlanEncoder.encode(plan, counter), nil}
  rescue
    e -> {nil, {:error, {:plan_encode_error, Exception.message(e)}}}
  end

  defp maybe_update_server_session(state, nil), do: state
  defp maybe_update_server_session(state, ""), do: state

  defp maybe_update_server_session(state, id) do
    %{state | server_side_session_id: id}
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
    case Keyword.get(opts, :timeout, 60_000) do
      nil ->
        :infinity

      :infinity ->
        :infinity

      timeout when is_integer(timeout) and timeout > 0 ->
        timeout + 5_000

      other ->
        raise ArgumentError, "timeout must be a positive integer or nil, got: #{inspect(other)}"
    end
  end

  defp safe_get_state(session, timeout) do
    {:ok, GenServer.call(session, :get_state, timeout)}
  catch
    :exit, _ -> :error
  end

  # --- Local data preparation ---

  defp prepare_local_data(data, opts) when is_struct(data, Explorer.DataFrame) do
    schema_ddl = Keyword.get(opts, :schema, nil) || explorer_to_ddl(data)

    if normalize_local_relation_arrow?(opts) and dataframe_contains_complex_dtype?(data) do
      prepare_sql_json_relation(data, schema_ddl)
    else
      case Explorer.DataFrame.dump_ipc_stream(data) do
        {:ok, ipc_bytes} -> {:ok, {:local_relation, ipc_bytes, schema_ddl}}
        {:error, reason} -> {:error, {:arrow_encode_error, reason}}
      end
    end
  end

  defp prepare_local_data(data, opts) when is_list(data) do
    schema = Keyword.get(opts, :schema, nil)

    cond do
      is_binary(schema) ->
        # User provided DDL schema string — convert list of maps to Explorer.DataFrame
        prepare_list_data_with_schema(data, schema, opts)

      true ->
        # No schema — try to infer from data
        prepare_list_data_inferred(data, opts)
    end
  end

  defp prepare_local_data(data, opts) when is_map(data) and not is_struct(data) do
    # Column-oriented data: %{"col1" => [1,2,3], "col2" => ["a","b","c"]}
    schema_ddl = Keyword.get(opts, :schema, nil)

    try do
      explorer_df = Explorer.DataFrame.new(data)
      effective_schema = schema_ddl || explorer_to_ddl(explorer_df)
      prepare_local_data(explorer_df, Keyword.put(opts, :schema, effective_schema))
    rescue
      e -> {:error, {:data_conversion_error, Exception.message(e)}}
    end
  end

  defp prepare_local_data(_data, _opts) do
    {:error, {:invalid_data, "expected Explorer.DataFrame, list of maps, or column map"}}
  end

  defp prepare_list_data_with_schema(data, schema_ddl, opts) when is_binary(schema_ddl) do
    if normalize_local_relation_arrow?(opts) do
      non_string_map_fields = non_string_top_level_map_fields(schema_ddl)
      binary_fields = binary_top_level_fields(schema_ddl)

      if rows_contain_null_byte_text?(data, binary_fields) do
        case prepare_list_data_with_schema_arrow_fallback(data, schema_ddl, opts) do
          {:ok, _} = ok ->
            ok

          {:error, _} ->
            prepare_list_data_with_schema_json_relation(
              data,
              schema_ddl,
              non_string_map_fields,
              binary_fields
            )
        end
      else
        prepare_list_data_with_schema_json_relation(
          data,
          schema_ddl,
          non_string_map_fields,
          binary_fields
        )
      end
    else
      try do
        explorer_df = list_of_maps_to_explorer(data)
        prepare_local_data(explorer_df, Keyword.put(opts, :schema, schema_ddl))
      rescue
        e -> {:error, {:data_conversion_error, Exception.message(e)}}
      end
    end
  end

  defp prepare_list_data_with_schema_json_relation(
         data,
         schema_ddl,
         non_string_map_fields,
         binary_fields
       ) do
    with {:ok, normalized_rows} <-
           normalize_rows_for_schema(
             data,
             Enum.map(non_string_map_fields, & &1.name),
             binary_fields
           ),
         {:ok, row_json} <- encode_rows_as_json(normalized_rows) do
      if non_string_map_fields == [] do
        query =
          json_rows_to_sql_query(length(row_json), schema_ddl)
          |> inline_sql_json_args(row_json)

        {:ok, {:sql_relation, query, nil}}
      else
        query =
          json_rows_to_sql_query_with_projection(
            length(row_json),
            helper_schema_for_non_string_map_fields(schema_ddl, non_string_map_fields),
            projected_select_list_for_non_string_map_fields(schema_ddl, non_string_map_fields)
          )
          |> inline_sql_json_args(row_json)

        {:ok, {:sql_relation, query, nil}}
      end
    end
  end

  defp prepare_list_data_with_schema_arrow_fallback(data, schema_ddl, opts) do
    try do
      explorer_df = list_of_maps_to_explorer(data)

      prepare_local_data(
        explorer_df,
        opts
        |> Keyword.put(:schema, schema_ddl)
        |> Keyword.put(:normalize_local_relation_arrow, false)
      )
    rescue
      _ -> {:error, :arrow_fallback_failed}
    end
  end

  defp prepare_list_data_inferred(data, opts) do
    if Enum.empty?(data) do
      {:error, {:invalid_data, "cannot infer schema from empty list"}}
    else
      try do
        explorer_df = list_of_maps_to_explorer(data)
        prepare_local_data(explorer_df, opts)
      rescue
        e ->
          if normalize_local_relation_arrow?(opts) do
            with {:ok, normalized_rows} <- normalize_rows_for_schema(data),
                 {:ok, inferred_schema_ddl} <- infer_schema_ddl_from_rows(normalized_rows),
                 {:ok, row_json} <- encode_rows_as_json(normalized_rows) do
              query =
                json_rows_to_sql_query(length(row_json), inferred_schema_ddl)
                |> inline_sql_json_args(row_json)

              {:ok, {:sql_relation, query, nil}}
            else
              {:error, _} = error -> error
            end
          else
            {:error, {:data_conversion_error, Exception.message(e)}}
          end
      end
    end
  end

  defp list_of_maps_to_explorer([]) do
    Explorer.DataFrame.new(%{})
  end

  defp list_of_maps_to_explorer([first | _] = data) when is_map(first) do
    normalized_rows =
      Enum.map(data, fn row ->
        Map.new(row, fn {key, value} -> {to_string(key), value} end)
      end)

    {_seen, ordered_keys_rev} =
      Enum.reduce(normalized_rows, {MapSet.new(), []}, fn row, {seen, keys_rev} ->
        Enum.reduce(row, {seen, keys_rev}, fn {key, _value}, {seen_acc, keys_acc} ->
          if MapSet.member?(seen_acc, key) do
            {seen_acc, keys_acc}
          else
            {MapSet.put(seen_acc, key), [key | keys_acc]}
          end
        end)
      end)

    ordered_keys = Enum.reverse(ordered_keys_rev)

    columns =
      ordered_keys
      |> Enum.map(fn key ->
        values = Enum.map(normalized_rows, fn row -> Map.get(row, key) end)
        {key, values}
      end)
      |> Map.new()

    Explorer.DataFrame.new(columns)
  end

  defp normalize_rows_for_schema(rows, non_string_map_fields \\ [], binary_fields \\ [])
       when is_list(rows) do
    non_string_map_fields_map = Map.new(non_string_map_fields, &{&1, true})
    binary_fields_map = Map.new(binary_fields, &{&1, true})

    Enum.reduce_while(rows, {:ok, []}, fn
      row, {:ok, acc} when is_map(row) and not is_struct(row) ->
        normalized =
          row
          |> Enum.map(fn {key, value} ->
            key_string = to_string(key)

            value =
              if Map.has_key?(non_string_map_fields_map, key_string) do
                normalize_non_string_map_value(value)
              else
                if Map.has_key?(binary_fields_map, key_string) do
                  normalize_binary_field_value(value)
                else
                  normalize_json_value(value)
                end
              end

            {key_string, value}
          end)
          |> Map.new()

        {:cont, {:ok, [normalized | acc]}}

      row, _acc ->
        {:halt,
         {:error,
          {:invalid_data,
           "expected list of maps for schema-based create_dataframe, got: #{inspect(row)}"}}}
    end)
    |> case do
      {:ok, rows_rev} -> {:ok, Enum.reverse(rows_rev)}
      {:error, _} = error -> error
    end
  end

  defp normalize_non_string_map_value(nil), do: nil

  defp normalize_non_string_map_value(value) when is_map(value) and not is_struct(value) do
    Enum.map(value, fn {k, v} ->
      %{"key" => k, "value" => normalize_json_value(v)}
    end)
  end

  defp normalize_non_string_map_value(value), do: normalize_json_value(value)

  defp normalize_binary_field_value(nil), do: nil
  defp normalize_binary_field_value(value) when is_binary(value), do: Base.encode64(value)
  defp normalize_binary_field_value(value), do: normalize_json_value(value)

  defp rows_contain_null_byte_text?(rows, binary_fields) when is_list(rows) do
    binary_fields_set = MapSet.new(binary_fields)

    Enum.any?(rows, fn
      row when is_map(row) and not is_struct(row) ->
        Enum.any?(row, fn {key, value} ->
          key_string = to_string(key)
          not MapSet.member?(binary_fields_set, key_string) and value_contains_null_byte_text?(value)
        end)

      _ ->
        false
    end)
  end

  defp rows_contain_null_byte_text?(_rows, _binary_fields), do: false

  defp value_contains_null_byte_text?(value) when is_binary(value) do
    :binary.match(value, <<0>>) != :nomatch
  end

  defp value_contains_null_byte_text?(value) when is_list(value) do
    Enum.any?(value, &value_contains_null_byte_text?/1)
  end

  defp value_contains_null_byte_text?(value) when is_map(value) and not is_struct(value) do
    Enum.any?(value, fn {k, v} ->
      value_contains_null_byte_text?(k) or value_contains_null_byte_text?(v)
    end)
  end

  defp value_contains_null_byte_text?(_value), do: false

  defp binary_top_level_fields(schema_ddl) do
    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.flat_map(fn field ->
      case parse_schema_field(field) do
        {name, type} ->
          if String.upcase(String.trim(type)) == "BINARY", do: [name], else: []

        :error ->
          []
      end
    end)
  end

  defp non_string_top_level_map_fields(schema_ddl) do
    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.flat_map(fn field ->
      case parse_schema_field(field) do
        {name, type} ->
          case parse_map_type(type) do
            {:ok, key_type, value_type} ->
              if String.upcase(String.trim(key_type)) == "STRING" do
                []
              else
                [
                  %{
                    name: name,
                    key_type: String.trim(key_type),
                    value_type: String.trim(value_type)
                  }
                ]
              end

            :error ->
              []
          end

        :error ->
          []
      end
    end)
  end

  defp helper_schema_for_non_string_map_fields(schema_ddl, non_string_map_fields) do
    replacements =
      Map.new(non_string_map_fields, fn %{name: name, key_type: key_type, value_type: value_type} ->
        {name, "ARRAY<STRUCT<key: #{key_type}, value: #{value_type}>>"}
      end)

    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.map(fn field ->
      case parse_schema_field(field) do
        {name, _type} ->
          replacement = Map.get(replacements, name)
          "#{name} #{replacement || schema_field_type(field)}"

        :error ->
          field
      end
    end)
    |> Enum.join(", ")
  end

  defp projected_select_list_for_non_string_map_fields(schema_ddl, non_string_map_fields) do
    map_fields = MapSet.new(Enum.map(non_string_map_fields, & &1.name))

    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.map(fn field ->
      case parse_schema_field(field) do
        {name, _type} ->
          if MapSet.member?(map_fields, name) do
            "map_from_entries(parsed.`#{name}`) AS `#{name}`"
          else
            "parsed.`#{name}` AS `#{name}`"
          end

        :error ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join(", ")
  end

  defp split_top_level_schema_fields(schema_ddl) do
    {parts, current, _depth} =
      schema_ddl
      |> String.graphemes()
      |> Enum.reduce({[], "", 0}, fn
        "<", {parts, current, depth} ->
          {parts, current <> "<", depth + 1}

        ">", {parts, current, depth} when depth > 0 ->
          {parts, current <> ">", depth - 1}

        ",", {parts, current, 0} ->
          {[String.trim(current) | parts], "", 0}

        ch, {parts, current, depth} ->
          {parts, current <> ch, depth}
      end)

    parts = [String.trim(current) | parts]

    parts
    |> Enum.reverse()
    |> Enum.reject(&(&1 == ""))
  end

  defp parse_schema_field(field) do
    case Regex.run(~r/^\s*(`?[^`\s]+`?)\s+(.+)$/, field) do
      [_, raw_name, raw_type] ->
        name = raw_name |> String.trim_leading("`") |> String.trim_trailing("`")
        {name, String.trim(raw_type)}

      _ ->
        :error
    end
  end

  defp schema_field_type(field) do
    case parse_schema_field(field) do
      {_name, type} -> type
      :error -> field
    end
  end

  defp parse_map_type(type) do
    trimmed = String.trim(type)

    if String.starts_with?(String.upcase(trimmed), "MAP<") and String.ends_with?(trimmed, ">") do
      inner = String.slice(trimmed, 4, String.length(trimmed) - 5)

      case split_map_type_inner(inner) do
        {key_type, value_type} ->
          {:ok, String.trim(key_type), String.trim(value_type)}

        :error ->
          :error
      end
    else
      :error
    end
  end

  defp split_map_type_inner(inner) do
    {key, value, angle_depth, paren_depth, split?} =
      inner
      |> String.graphemes()
      |> Enum.reduce({"", "", 0, 0, false}, fn
        "<", {k, v, a, p, s} -> {append_part(k, v, s, "<"), v_if_needed(v, s, "<"), a + 1, p, s}
        ">", {k, v, a, p, s} -> {append_part(k, v, s, ">"), v_if_needed(v, s, ">"), max(a - 1, 0), p, s}
        "(", {k, v, a, p, s} -> {append_part(k, v, s, "("), v_if_needed(v, s, "("), a, p + 1, s}
        ")", {k, v, a, p, s} -> {append_part(k, v, s, ")"), v_if_needed(v, s, ")"), a, max(p - 1, 0), s}
        ",", {k, v, 0, 0, false} -> {k, v, 0, 0, true}
        ch, {k, v, a, p, s} -> {append_part(k, v, s, ch), v_if_needed(v, s, ch), a, p, s}
      end)

    cond do
      not split? -> :error
      angle_depth != 0 or paren_depth != 0 -> :error
      String.trim(key) == "" or String.trim(value) == "" -> :error
      true -> {key, value}
    end
  end

  defp append_part(k, _v, false, ch), do: k <> ch
  defp append_part(k, _v, true, _ch), do: k
  defp v_if_needed(v, false, _ch), do: v
  defp v_if_needed(v, true, ch), do: v <> ch

  defp infer_schema_ddl_from_rows(rows) when is_list(rows) do
    ordered_keys = collect_ordered_keys(rows)

    fields =
      Enum.map(ordered_keys, fn key ->
        values = Enum.map(rows, &Map.get(&1, key))
        {key, infer_value_type(values)}
      end)

    {:ok,
     fields
     |> Enum.map(fn {name, type} -> "#{name} #{type_to_inferred_ddl(type)}" end)
     |> Enum.join(", ")}
  end

  defp collect_ordered_keys(rows) do
    {_seen, ordered_keys_rev} =
      Enum.reduce(rows, {MapSet.new(), []}, fn row, {seen, keys_rev} ->
        Enum.reduce(row, {seen, keys_rev}, fn {key, _value}, {seen_acc, keys_acc} ->
          if MapSet.member?(seen_acc, key) do
            {seen_acc, keys_acc}
          else
            {MapSet.put(seen_acc, key), [key | keys_acc]}
          end
        end)
      end)

    Enum.reverse(ordered_keys_rev)
  end

  defp infer_value_type(values) do
    values
    |> Enum.map(&infer_single_type/1)
    |> Enum.reduce(:null, &merge_inferred_types/2)
  end

  defp infer_single_type(nil), do: :null
  defp infer_single_type(v) when is_boolean(v), do: :boolean
  defp infer_single_type(v) when is_integer(v), do: :long
  defp infer_single_type(v) when is_float(v), do: :double
  defp infer_single_type(%Decimal{}), do: :double
  defp infer_single_type(%Date{}), do: :date
  defp infer_single_type(%DateTime{}), do: :timestamp
  defp infer_single_type(%NaiveDateTime{}), do: :timestamp
  defp infer_single_type(v) when is_binary(v), do: :string

  defp infer_single_type(v) when is_list(v) do
    {:array, infer_value_type(v)}
  end

  defp infer_single_type(v) when is_map(v) and not is_struct(v) do
    fields =
      v
      |> Enum.map(fn {k, value} -> {to_string(k), infer_single_type(value)} end)
      |> Map.new()

    {:struct, fields}
  end

  defp infer_single_type(_), do: :string

  defp merge_inferred_types(:null, type), do: type
  defp merge_inferred_types(type, :null), do: type
  defp merge_inferred_types(type, type), do: type
  defp merge_inferred_types(:long, :double), do: :double
  defp merge_inferred_types(:double, :long), do: :double
  defp merge_inferred_types({:array, a}, {:array, b}), do: {:array, merge_inferred_types(a, b)}

  defp merge_inferred_types({:struct, a}, {:struct, b}) do
    keys = (Map.keys(a) ++ Map.keys(b)) |> Enum.uniq()

    merged =
      Enum.map(keys, fn key ->
        {key, merge_inferred_types(Map.get(a, key, :null), Map.get(b, key, :null))}
      end)
      |> Map.new()

    {:struct, merged}
  end

  defp merge_inferred_types(_a, _b), do: :string

  defp type_to_inferred_ddl(:null), do: "STRING"
  defp type_to_inferred_ddl(:boolean), do: "BOOLEAN"
  defp type_to_inferred_ddl(:long), do: "BIGINT"
  defp type_to_inferred_ddl(:double), do: "DOUBLE"
  defp type_to_inferred_ddl(:date), do: "DATE"
  defp type_to_inferred_ddl(:timestamp), do: "TIMESTAMP"
  defp type_to_inferred_ddl(:string), do: "STRING"

  defp type_to_inferred_ddl({:array, element_type}) do
    "ARRAY<#{type_to_inferred_ddl(element_type)}>"
  end

  defp type_to_inferred_ddl({:struct, fields}) do
    inner =
      fields
      |> Enum.sort_by(fn {name, _} -> name end)
      |> Enum.map(fn {name, type} -> "#{name}: #{type_to_inferred_ddl(type)}" end)
      |> Enum.join(", ")

    "STRUCT<#{inner}>"
  end

  defp normalize_json_value(value) when is_map(value) and not is_struct(value) do
    value
    |> Enum.map(fn {k, v} -> {to_string(k), normalize_json_value(v)} end)
    |> Map.new()
  end

  defp normalize_json_value(value) when is_list(value) do
    Enum.map(value, &normalize_json_value/1)
  end

  defp normalize_json_value(value), do: value

  defp explorer_to_ddl(explorer_df) do
    dtypes = Explorer.DataFrame.dtypes(explorer_df)

    ordered_dtypes =
      explorer_df
      |> Explorer.DataFrame.names()
      |> Enum.map(fn name -> {name, Map.fetch!(dtypes, name)} end)

    TypeMapper.explorer_schema_to_ddl(ordered_dtypes)
  end

  defp prepare_sql_json_relation(explorer_df, schema_ddl) do
    rows = Explorer.DataFrame.to_rows(explorer_df)

    with {:ok, row_json} <- encode_rows_as_json(rows) do
      query =
        json_rows_to_sql_query(length(row_json), schema_ddl)
        |> inline_sql_json_args(row_json)

      {:ok, {:sql_relation, query, nil}}
    end
  end

  defp encode_rows_as_json(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case Jason.encode(row) do
        {:ok, json} -> {:cont, {:ok, [json | acc]}}
        {:error, reason} -> {:halt, {:error, {:data_conversion_error, Exception.message(reason)}}}
      end
    end)
    |> case do
      {:ok, json_rows_rev} -> {:ok, Enum.reverse(json_rows_rev)}
      {:error, _} = error -> error
    end
  end

  defp json_rows_to_sql_query(0, schema_ddl) do
    escaped_schema = sql_escape_string(schema_ddl)

    """
    SELECT parsed.*
    FROM (
      SELECT from_json(NULL, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
    ) _spark_ex_parsed
    WHERE 1 = 0
    """
    |> String.trim()
  end

  defp json_rows_to_sql_query(row_count, schema_ddl) when row_count > 0 do
    escaped_schema = sql_escape_string(schema_ddl)

    placeholders =
      1..row_count
      |> Enum.map(fn _ -> "(?)" end)
      |> Enum.join(", ")

    """
    SELECT parsed.*
    FROM (
      SELECT from_json(_spark_ex_json, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
      FROM VALUES #{placeholders} AS _spark_ex_input(_spark_ex_json)
    ) _spark_ex_parsed
    """
    |> String.trim()
  end

  defp json_rows_to_sql_query_with_projection(0, schema_ddl, select_list) do
    escaped_schema = sql_escape_string(schema_ddl)

    """
    SELECT #{select_list}
    FROM (
      SELECT from_json(NULL, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
    ) _spark_ex_parsed
    WHERE 1 = 0
    """
    |> String.trim()
  end

  defp json_rows_to_sql_query_with_projection(row_count, schema_ddl, select_list)
       when row_count > 0 do
    escaped_schema = sql_escape_string(schema_ddl)

    placeholders =
      1..row_count
      |> Enum.map(fn _ -> "(?)" end)
      |> Enum.join(", ")

    """
    SELECT #{select_list}
    FROM (
      SELECT from_json(_spark_ex_json, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
      FROM VALUES #{placeholders} AS _spark_ex_input(_spark_ex_json)
    ) _spark_ex_parsed
    """
    |> String.trim()
  end

  defp sql_escape_string(value) when is_binary(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
  end

  defp inline_sql_json_args(query, []), do: query

  defp inline_sql_json_args(query, json_rows) when is_list(json_rows) do
    Enum.reduce(json_rows, query, fn json, acc ->
      escaped = sql_escape_string(json)
      String.replace(acc, "(?)", "('#{escaped}')", global: false)
    end)
  end

  defp normalize_local_relation_arrow?(opts) do
    Keyword.get(opts, :normalize_local_relation_arrow, true)
  end

  defp dataframe_contains_complex_dtype?(explorer_df) do
    explorer_df
    |> Explorer.DataFrame.dtypes()
    |> Map.values()
    |> Enum.any?(&dtype_requires_json_relation?/1)
  end

  defp dtype_requires_json_relation?({:list, _inner}), do: true
  defp dtype_requires_json_relation?({:struct, _fields}), do: true
  defp dtype_requires_json_relation?({:map, _key_dtype, _value_dtype}), do: true

  defp dtype_requires_json_relation?({_tag, inner}) do
    dtype_requires_json_relation?(inner)
  end

  defp dtype_requires_json_relation?(_other), do: false

  defp read_local_file(local_path) do
    case File.read(local_path) do
      {:ok, data} -> {:ok, data}
      {:error, reason} -> {:error, {:file_read_error, local_path, reason}}
    end
  end

  defp validate_forward_dest_path(dest_path) when is_binary(dest_path) do
    case URI.parse(dest_path) do
      %URI{scheme: nil} ->
        if Path.type(dest_path) == :absolute do
          :ok
        else
          {:error, {:invalid_destination_path, "destination path must be absolute"}}
        end

      _uri ->
        {:error, {:invalid_destination_path, "destination path must not include a URI scheme"}}
    end
  end

  defp validate_forward_dest_path(_dest_path) do
    {:error, {:invalid_destination_path, "destination path must be a string"}}
  end

  defp forward_to_fs_artifact_name(dest_path), do: "forward_to_fs" <> dest_path

  defp upload_missing_cache_artifacts(state, artifacts) do
    names = Enum.map(artifacts, fn {name, _data} -> name end)

    case Client.artifact_status(state, names) do
      {:ok, statuses, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        missing = Enum.reject(artifacts, fn {name, _data} -> Map.get(statuses, name, false) end)
        maybe_upload_cache_artifacts(state, missing)

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_upload_cache_artifacts(state, []), do: {:ok, state}

  defp maybe_upload_cache_artifacts(state, missing) do
    case Client.add_artifacts(state, missing) do
      {:ok, _summaries, server_side_session_id} ->
        {:ok, maybe_update_server_session(state, server_side_session_id)}

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve_connect_opts(url, nil) when is_binary(url), do: Channel.parse_uri(url)
  defp resolve_connect_opts(nil, connect_opts) when is_map(connect_opts), do: {:ok, connect_opts}
  defp resolve_connect_opts(url, _connect_opts) when is_binary(url), do: Channel.parse_uri(url)

  defp resolve_connect_opts(_url, _connect_opts) do
    {:error, {:invalid_connect_opts, "expected :url or :connect_opts"}}
  end

  defp session_id_for(session) do
    if is_pid(session) do
      GenServer.call(session, :get_session_id)
    else
      %__MODULE__{session_id: session_id} = session
      session_id
    end
  end

  @doc false
  @spec cleanup_cloned_session_on_start_failure(
          t(),
          %{
            required(:new_session_id) => String.t(),
            optional(:new_server_side_session_id) => String.t() | nil,
            optional(:source_server_side_session_id) => String.t() | nil
          },
          (t() -> {:ok, String.t() | nil} | {:error, term()})
        ) :: :ok | {:error, term()}
  def cleanup_cloned_session_on_start_failure(
        %__MODULE__{} = state,
        %{new_session_id: new_session_id} = clone_info,
        release_fun \\ &Client.release_session/1
      )
      when is_binary(new_session_id) and is_function(release_fun, 1) do
    cleanup_session = %__MODULE__{
      channel: state.channel,
      session_id: new_session_id,
      server_side_session_id: Map.get(clone_info, :new_server_side_session_id),
      user_id: state.user_id,
      client_type: state.client_type
    }

    case release_fun.(cleanup_session) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error

      other ->
        {:error, {:unexpected_release_session_result, other}}
    end
  end

  defp merge_session_tags(opts, session_tags) do
    request_tags = Keyword.get(opts, :tags, [])
    combined = request_tags ++ session_tags

    if combined == [] do
      opts
    else
      Keyword.put(opts, :tags, combined)
    end
  end
end
