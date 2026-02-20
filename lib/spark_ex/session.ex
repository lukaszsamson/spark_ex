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
  Executes a plan and returns the raw gRPC response stream.

  Used by APIs that need incremental row consumption.
  """
  @spec execute_plan_stream(GenServer.server(), term(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def execute_plan_stream(session, plan, opts \\ []) do
    timeout = call_timeout(opts)

    case safe_get_state(session, timeout) do
      {:ok, %__MODULE__{} = state} ->
        {proto_plan, _counter} = PlanEncoder.encode(plan, 0)
        opts = merge_session_tags(opts, state.tags)
        Client.execute_plan_raw_stream(state, proto_plan, opts)

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
  Executes a command and returns a raw gRPC response stream.

  Used for long-lived streaming operations like the listener bus.
  The caller is responsible for consuming the stream.
  """
  @spec execute_command_stream(GenServer.server(), term(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
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

  def handle_call(:release_session, _from, state) do
    if state.released do
      {:reply, :ok, state}
    else
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
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan(state, proto_plan, opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        state = %{state | last_execution_metrics: result.execution_metrics}
        SparkEx.Observation.store_observed_metrics(result.observed_metrics)
        {:reply, {:ok, result.rows}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execute_plan_stream, plan, opts}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan_raw_stream(state, proto_plan, opts) do
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

    {proto_plan, counter} = PlanEncoder.encode(effective_plan, state.plan_id_counter)
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
  end

  def handle_call({:execute_arrow, plan, opts}, _from, state) do
    {proto_plan, counter} = PlanEncoder.encode(plan, state.plan_id_counter)
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

  def handle_call({:execute_command_stream, command, opts}, _from, state) do
    alias SparkEx.Connect.CommandEncoder

    {proto_plan, counter} = CommandEncoder.encode(command, state.plan_id_counter)
    state = %{state | plan_id_counter: counter}

    opts = merge_session_tags(opts, state.tags)

    case Client.execute_plan_raw_stream(state, proto_plan, opts) do
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

  defp safe_get_state(session, timeout) do
    {:ok, GenServer.call(session, :get_state, timeout)}
  catch
    :exit, _ -> :error
  end

  # --- Local data preparation ---

  defp prepare_local_data(data, opts) when is_struct(data, Explorer.DataFrame) do
    schema_ddl = Keyword.get(opts, :schema, nil) || explorer_to_ddl(data)

    if normalize_local_relation_arrow?(opts) and dataframe_contains_list_dtype?(data) do
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
    try do
      explorer_df = list_of_maps_to_explorer(data)
      prepare_local_data(explorer_df, Keyword.put(opts, :schema, schema_ddl))
    rescue
      e -> {:error, {:data_conversion_error, Exception.message(e)}}
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
        e -> {:error, {:data_conversion_error, Exception.message(e)}}
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
      query = json_rows_to_sql_query(length(row_json), schema_ddl)
      {:ok, {:sql_relation, query, row_json}}
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
    FROM (SELECT from_json(NULL, '#{escaped_schema}') AS parsed) _spark_ex_parsed
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
      SELECT from_json(_spark_ex_json, '#{escaped_schema}') AS parsed
      FROM VALUES #{placeholders} AS _spark_ex_input(_spark_ex_json)
    ) _spark_ex_parsed
    """
    |> String.trim()
  end

  defp sql_escape_string(value) when is_binary(value) do
    String.replace(value, "'", "''")
  end

  defp normalize_local_relation_arrow?(opts) do
    Keyword.get(opts, :normalize_local_relation_arrow, true)
  end

  defp dataframe_contains_list_dtype?(explorer_df) do
    explorer_df
    |> Explorer.DataFrame.dtypes()
    |> Map.values()
    |> Enum.any?(&dtype_contains_list?/1)
  end

  defp dtype_contains_list?({:list, _inner}), do: true

  defp dtype_contains_list?({:struct, fields}) when is_list(fields) do
    Enum.any?(fields, fn
      {_name, field_dtype} -> dtype_contains_list?(field_dtype)
      _other -> false
    end)
  end

  defp dtype_contains_list?({:map, key_dtype, value_dtype}) do
    dtype_contains_list?(key_dtype) or dtype_contains_list?(value_dtype)
  end

  defp dtype_contains_list?({_tag, inner}) do
    dtype_contains_list?(inner)
  end

  defp dtype_contains_list?(_other), do: false

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
