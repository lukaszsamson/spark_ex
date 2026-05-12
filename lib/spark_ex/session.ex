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
    released: false,
    closed: false,
    retry_policies: nil
  ]

  @type t :: %__MODULE__{
          channel: GRPC.Channel.t() | nil,
          connect_opts: map() | nil,
          session_id: String.t(),
          server_side_session_id: String.t() | nil,
          user_id: String.t(),
          client_type: String.t(),
          allow_arrow_batch_chunking: boolean(),
          preferred_arrow_chunk_size: non_neg_integer() | nil,
          plan_id_counter: non_neg_integer(),
          last_execution_metrics: map(),
          tags: [String.t()],
          released: boolean(),
          closed: boolean(),
          retry_policies: %{atom() => map()} | nil
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
  - `:retry_policies` — per-session retry policy overrides (default: `nil`,
    falling back to `SparkEx.RetryPolicyRegistry`'s global policies). Accepts
    a map or keyword list keyed by `:retry`, `:reattach`, and/or `:streaming`,
    where each value is a partial map/keyword of the same keys accepted by
    `SparkEx.RetryPolicyRegistry.set_policies/1`. Only the keys you supply
    override the global policy; everything else is inherited unchanged.
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

  @doc false
  # Internal helper that bypasses `SessionIntegrity` validation by design —
  # it exists only to let integration tests force a server-session-id
  # mismatch and exercise the failure path. The `__` prefix and `@doc
  # false` make it clear this is not a public API; production code uses
  # `maybe_update_server_session/2` which validates against the pinned id
  # before accepting any change.
  @spec __update_server_side_session_id__(GenServer.server(), String.t()) :: :ok
  def __update_server_side_session_id__(session, server_side_session_id) do
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
  def clone(session, new_session_id \\ nil)

  def clone(session, nil) do
    GenServer.call(session, {:clone_session, nil})
  end

  def clone(session, new_session_id) when is_binary(new_session_id) do
    GenServer.call(session, {:clone_session, new_session_id})
  end

  def clone(_session, new_session_id) do
    raise ArgumentError, "new_session_id must be a string or nil, got: #{inspect(new_session_id)}"
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
  Executes a plan and returns a raw response stream.

  Used by APIs that need incremental row consumption.

  Used by APIs that need incremental row consumption.
  """
  @spec execute_plan_stream(GenServer.server(), term(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def execute_plan_stream(session, plan, opts \\ []) do
    if real_session_process?(session) do
      case GenServer.call(
             session,
             {:prepare_execute_plan_stream, plan, opts},
             call_timeout(opts)
           ) do
        {:ok, stream_state, proto_plan, stream_opts} ->
          Client.execute_plan_raw_stream(stream_state, proto_plan, stream_opts)

        {:error, _} = error ->
          error
      end
    else
      GenServer.call(session, {:execute_plan_stream, plan, opts}, call_timeout(opts))
    end
  end

  @doc """
  Like `execute_plan_stream/3`, but returns a reattachable response stream.

  The returned enumerable yields `{:ok, ExecutePlanResponse.t()} | {:error, term()}`
  and survives graceful-EOF reattach + retryable transient errors. Used by
  long-lived consumers like `DataFrame.to_local_iterator/2` so a mid-stream
  disconnect does not lose the in-progress result.
  """
  @spec execute_plan_reattachable_stream(GenServer.server(), term(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def execute_plan_reattachable_stream(session, plan, opts \\ []) do
    if real_session_process?(session) do
      case GenServer.call(
             session,
             {:prepare_execute_plan_stream, plan, opts},
             call_timeout(opts)
           ) do
        {:ok, stream_state, proto_plan, stream_opts} ->
          Client.execute_plan_reattachable_response_stream(stream_state, proto_plan, stream_opts)

        {:error, _} = error ->
          error
      end
    else
      GenServer.call(session, {:execute_plan_reattachable_stream, plan, opts}, call_timeout(opts))
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
    validate_config_pairs!(pairs, "config_set/2")
    GenServer.call(session, {:config_set, pairs})
  end

  @doc """
  Gets Spark configuration values for the given keys.
  """
  @spec config_get(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get(session, keys) do
    validate_config_keys!(keys, "config_get/2")
    GenServer.call(session, {:config_get, keys})
  end

  @doc """
  Gets Spark configuration values with fallback defaults.
  """
  @spec config_get_with_default(GenServer.server(), [{String.t(), String.t() | nil}]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_with_default(session, pairs) do
    validate_config_default_pairs!(pairs)
    GenServer.call(session, {:config_get_with_default, pairs})
  end

  @doc """
  Gets optional Spark configuration values (returns nil for unset keys).
  """
  @spec config_get_option(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_option(session, keys) do
    validate_config_keys!(keys, "config_get_option/2")
    GenServer.call(session, {:config_get_option, keys})
  end

  @doc """
  Gets all Spark configuration values, optionally filtered by prefix.
  """
  @spec config_get_all(GenServer.server(), String.t() | nil) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_all(session, prefix \\ nil) do
    validate_config_prefix!(prefix)
    GenServer.call(session, {:config_get_all, prefix})
  end

  @doc """
  Unsets Spark configuration values.
  """
  @spec config_unset(GenServer.server(), [String.t()]) :: :ok | {:error, term()}
  def config_unset(session, keys) do
    validate_config_keys!(keys, "config_unset/2")
    GenServer.call(session, {:config_unset, keys})
  end

  @doc """
  Checks whether configuration keys are modifiable at runtime.
  """
  @spec config_is_modifiable(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean() | nil}]} | {:error, term()}
  def config_is_modifiable(session, key) when is_binary(key) do
    config_is_modifiable(session, [key])
  end

  def config_is_modifiable(session, keys) when is_list(keys) do
    unless Enum.all?(keys, &is_binary/1) do
      raise ArgumentError, "keys must be a list of strings"
    end

    GenServer.call(session, {:config_is_modifiable, keys})
  end

  defp validate_config_default_pairs!(pairs) when is_list(pairs) do
    if Enum.all?(pairs, fn
         {k, nil} -> coercible_config_key?(k)
         {k, v} -> coercible_config_key?(k) and coercible_config_value?(v)
         _ -> false
       end) do
      :ok
    else
      raise ArgumentError,
            "config_get_with_default/2 pairs must be {key, value} where key is a string or atom and value is a string, boolean, integer, float, atom, or nil, got: #{inspect(pairs, charlists: :as_lists)}"
    end
  end

  defp validate_config_default_pairs!(pairs) do
    raise ArgumentError,
          "config_get_with_default/2 expects a list of {key, value} pairs, got: #{inspect(pairs)}"
  end

  defp validate_config_pairs!(pairs, _fun_name) when is_list(pairs) do
    if Enum.all?(pairs, fn
         {k, v} -> coercible_config_key?(k) and coercible_config_value?(v)
         _ -> false
       end) do
      :ok
    else
      raise ArgumentError,
            "config_set/config_get_with_default pairs must be {key, value} where key/value are strings, booleans, integers, floats, or atoms, got: #{inspect(pairs, charlists: :as_lists)}"
    end
  end

  defp validate_config_pairs!(pairs, fun_name) do
    raise ArgumentError,
          "#{fun_name} expects a list of {key, value} pairs, got: #{inspect(pairs)}"
  end

  defp validate_config_keys!(keys, _fun_name) when is_list(keys) do
    if Enum.all?(keys, &coercible_config_key?/1) do
      :ok
    else
      raise ArgumentError,
            "config keys must be strings or atoms, got: #{inspect(keys, charlists: :as_lists)}"
    end
  end

  defp validate_config_keys!(keys, fun_name) do
    raise ArgumentError, "#{fun_name} expects a list of string keys, got: #{inspect(keys)}"
  end

  defp coercible_config_key?(key) when is_binary(key), do: true
  defp coercible_config_key?(key) when is_atom(key) and not is_nil(key), do: true
  defp coercible_config_key?(_), do: false

  defp coercible_config_value?(value)
       when is_binary(value) or is_boolean(value) or is_integer(value) or is_float(value),
       do: true

  defp coercible_config_value?(value) when is_atom(value) and not is_nil(value), do: true
  defp coercible_config_value?(_), do: false

  defp validate_config_prefix!(nil), do: :ok
  defp validate_config_prefix!(prefix) when is_binary(prefix), do: :ok

  defp validate_config_prefix!(prefix) do
    raise ArgumentError,
          "config_get_all/2 expects prefix to be a string or nil, got: #{inspect(prefix)}"
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
          {:ok, SparkEx.Types.storage_level()} | {:error, term()}
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

  Artifacts are provided as a list of `{name, data}` tuples where
  `data` is either:

    * a `binary()` containing the artifact contents in memory; or
    * `{:file, path, size}` for a path on disk that should be streamed
      lazily in chunks (peak memory ≈ chunk size, not file size).

  Returns a list of `{name, crc_successful?}` tuples.
  """
  @spec add_artifacts(
          GenServer.server(),
          [{String.t(), binary() | {:file, Path.t(), non_neg_integer()}}]
        ) :: {:ok, [{String.t(), boolean()}]} | {:error, term()}
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

  Uploads the file at `local_path` as a Spark Connect forward-to-filesystem
  artifact. The file is streamed to the server in chunks rather than read
  into memory all at once.
  """
  @spec copy_from_local_to_fs(GenServer.server(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def copy_from_local_to_fs(session, local_path, dest_path) do
    with :ok <- validate_forward_dest_path(dest_path),
         {:ok, size} <- stat_local_file(local_path),
         {:ok, _summaries} <-
           add_artifacts(session, [
             {forward_to_fs_artifact_name(dest_path), {:file, local_path, size}}
           ]) do
      :ok
    end
  end

  @doc """
  Creates a DataFrame from local data.

  For small data (under the cache threshold), the data is embedded directly
  in the plan as a `LocalRelation`. For larger data, the Arrow IPC bytes
  are split into one or more chunks, each uploaded to the server via
  `AddArtifacts` and referenced together via `ChunkedCachedLocalRelation`
  (mirroring PySpark's `_chunk_local_relation`).

  ## Options

  - `:schema` — DDL schema string (e.g. `"id INT, name STRING"`). If omitted,
    inferred from the Explorer.DataFrame or from the data.
  - `:cache_threshold` — byte size threshold above which data is cached on the
    server instead of inlined (default: 4 MB)
  - `:cache_chunk_size` — maximum byte size of each Arrow IPC chunk uploaded as
    a separate cache artifact when the payload exceeds `:cache_threshold`
    (default: same as `:cache_threshold`). Mirrors PySpark's
    `_chunk_local_relation` behaviour, which splits a large Arrow stream into
    multiple per-chunk cache entries instead of uploading a single blob.
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
    if real_session_process?(session) do
      case GenServer.call(
             session,
             {:prepare_execute_command_stream, command, opts},
             call_timeout(opts)
           ) do
        {:ok, stream_state, proto_plan, stream_opts} ->
          Client.execute_plan_managed_stream(
            stream_state,
            proto_plan,
            Keyword.put(stream_opts, :stream_owner, self())
          )

        {:error, _} = error ->
          error
      end
    else
      GenServer.call(session, {:execute_command_stream, command, opts}, call_timeout(opts))
    end
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
  catch
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(opts) do
    connect_opts_opt = Keyword.get(opts, :connect_opts)
    url_opt = Keyword.get(opts, :url)
    observed_server_session_id = Keyword.get(opts, :server_side_session_id, nil)
    allow_arrow_batch_chunking = Keyword.get(opts, :allow_arrow_batch_chunking, true)
    preferred_arrow_chunk_size = Keyword.get(opts, :preferred_arrow_chunk_size, nil)
    retry_policies = normalize_retry_policies_opt(Keyword.get(opts, :retry_policies))

    grpc_opts = Keyword.get(opts, :grpc_opts, [])

    with {:ok, connect_opts} <- resolve_connect_opts(url_opt, connect_opts_opt),
         {:ok, session_identity} <- resolve_session_identity(opts, connect_opts),
         connect_opts <- Map.put(connect_opts, :user_agent, session_identity.client_type),
         {:ok, channel} <- Channel.connect(connect_opts, grpc_opts) do
      # Register a session-scoped plan_id allocator in ETS so DataFrames
      # constructed in caller processes draw plan_ids from the same
      # namespace as encoder-allocated synthetic ids; prevents
      # cross-process collisions (see SparkEx.Internal.PlanIds).
      SparkEx.Internal.PlanIds.register_session(self())

      state = %__MODULE__{
        channel: channel,
        connect_opts: connect_opts,
        session_id: session_identity.session_id,
        server_side_session_id: observed_server_session_id,
        user_id: session_identity.user_id,
        client_type: session_identity.client_type,
        allow_arrow_batch_chunking: allow_arrow_batch_chunking,
        preferred_arrow_chunk_size: preferred_arrow_chunk_size,
        retry_policies: retry_policies
      }

      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp normalize_retry_policies_opt(nil), do: nil

  defp normalize_retry_policies_opt(policies) when is_map(policies) or is_list(policies) do
    SparkEx.RetryPolicyRegistry.normalize_session_policies!(policies)
  end

  defp normalize_retry_policies_opt(other) do
    raise ArgumentError,
          "expected :retry_policies to be nil, a map, or a keyword list keyed by " <>
            ":retry | :reattach | :streaming, got: #{inspect(other)}"
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

            reply_error(error, state)
        end

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call(:next_plan_id, _from, %{released: true} = state) do
    {:reply, {:error, :session_released}, state}
  end

  def handle_call(:next_plan_id, _from, state) do
    # The authoritative allocator is the session-scoped `:atomics` ref
    # registered in `SparkEx.Internal.PlanIds`. `DataFrame.new/2` from
    # caller processes and `PlanEncoder.next_id/1` in this process both
    # draw from it, so `state.plan_id_counter` would lag and could
    # return duplicate ids. Pull from the atomic instead and keep
    # `state.plan_id_counter` in sync only for legacy reads.
    id = SparkEx.Internal.PlanIds.next(self())
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
          reply_error(error, state)
      end
    end
  end

  # --- Closed guard: reject RPCs after server session change ---
  # Allow :release_session, :get_state, :is_stopped, :get_session_id, :get_tags
  # so callers can still introspect and dispose of the session cleanly.

  def handle_call(msg, _from, %{closed: true} = state)
      when msg not in [
             :release_session,
             :get_state,
             :is_stopped,
             :get_session_id,
             :get_tags,
             :last_execution_metrics
           ] do
    {:reply, {:error, :session_closed}, state}
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
        reply_error(error, state)
    end
  end

  def handle_call(:spark_version, _from, state) do
    case Client.analyze_spark_version(state) do
      {:ok, version, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, version}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  # execute_collect retry cascade (in order of precedence):
  #
  # 1. Pre-execution: if the plan contains a JSON relation with nested maps,
  #    proactively re-execute with JSON projection to avoid Arrow decode issues.
  # 2. Normal execution via Client.execute_plan.
  # 3. Post-execution: if the result has nested maps with decode loss,
  #    retry with JSON projection.
  # 4. Arrow decode failure: retry with unique column names, then JSON projection.
  # 5. Remote error (SparkEx.Error.Remote): try legacy plan rewrites based on
  #    error message patterns (grouping sets, UNPARSED types, empty relations).
  # 6. Empty relation errors: cascade through transpose, table function,
  #    as-of join, and subquery plan rewrites.
  def handle_call({:execute_collect, plan, opts}, _from, state) do
    operation_telemetry_span(:execute_collect, state.session_id, fn ->
      case safe_encode(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}
          opts = merge_session_tags(opts, state.tags)

          case maybe_execute_collect_with_json_projection(state, plan, proto_plan, opts) do
            {:ok, result, state} ->
              reply_collect_result(result, state)

            {:no_fallback, state} ->
              execute_collect_no_fallback(state, plan, proto_plan, opts)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:prepare_execute_plan_stream, plan, opts}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}
        opts = merge_session_tags(opts, state.tags)
        {:reply, {:ok, state, proto_plan, opts}, state}

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:execute_explorer, plan, opts}, _from, state) do
    operation_telemetry_span(:execute_explorer, state.session_id, fn ->
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

              SparkEx.Observation.store_observed_metrics(
                result.observed_metrics,
                state.session_id
              )

              {:reply, {:ok, result.dataframe}, state}

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:execute_arrow, plan, opts}, _from, state) do
    operation_telemetry_span(:execute_arrow, state.session_id, fn ->
      case safe_encode(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          opts = merge_session_tags(opts, state.tags)

          case Client.execute_plan_arrow(state, proto_plan, opts) do
            {:ok, result} ->
              state = maybe_update_server_session(state, result.server_side_session_id)
              state = %{state | last_execution_metrics: result.execution_metrics}

              SparkEx.Observation.store_observed_metrics(
                result.observed_metrics,
                state.session_id
              )

              {:reply, {:ok, result.arrow}, state}

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:execute_count, plan}, _from, state) do
    operation_telemetry_span(:execute_count, state.session_id, fn ->
      case safe_encode_count(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          case Client.execute_plan(state, proto_plan, merge_session_tags([], state.tags)) do
            {:ok, result} ->
              reply_execute_count_result(result, state)

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:analyze_schema, plan}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_schema(state, proto_plan) do
          {:ok, schema, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, schema}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_explain, plan, mode}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_explain(state, proto_plan, mode) do
          {:ok, explain_str, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, explain_str}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_set, pairs}, _from, state) do
    case Client.config_set(state, pairs) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_get, keys}, _from, state) do
    case Client.config_get(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_get_with_default, pairs}, _from, state) do
    case Client.config_get_with_default(state, pairs) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_get_option, keys}, _from, state) do
    case Client.config_get_option(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_get_all, prefix}, _from, state) do
    case Client.config_get_all(state, prefix) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_unset, keys}, _from, state) do
    case Client.config_unset(state, keys) do
      {:ok, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, :ok, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:config_is_modifiable, keys}, _from, state) do
    case Client.config_is_modifiable(state, keys) do
      {:ok, result, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, result}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_tree_string, plan, opts}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_tree_string(state, proto_plan, opts) do
          {:ok, str, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, str}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_is_local, plan}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_is_local(state, proto_plan) do
          {:ok, is_local, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, is_local}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_is_streaming, plan}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_is_streaming(state, proto_plan) do
          {:ok, is_streaming, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, is_streaming}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_input_files, plan}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_input_files(state, proto_plan) do
          {:ok, files, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, files}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_ddl_parse, ddl_string}, _from, state) do
    case Client.analyze_ddl_parse(state, ddl_string) do
      {:ok, parsed, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, parsed}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_json_to_ddl, json_string}, _from, state) do
    case Client.analyze_json_to_ddl(state, json_string) do
      {:ok, ddl, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, ddl}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_same_semantics, plan1, plan2}, _from, state) do
    case safe_encode(plan1, state.plan_id_counter) do
      {{proto_plan1, counter}, nil} ->
        case safe_encode(plan2, counter) do
          {{proto_plan2, counter}, nil} ->
            state = %{state | plan_id_counter: counter}

            case Client.analyze_same_semantics(state, proto_plan1, proto_plan2) do
              {:ok, result, server_side_session_id} ->
                state = maybe_update_server_session(state, server_side_session_id)
                {:reply, {:ok, result}, state}

              {:error, _} = error ->
                reply_error(error, state)
            end

          {nil, error} ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_semantic_hash, plan}, _from, state) do
    case safe_encode(plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_semantic_hash(state, proto_plan) do
          {:ok, hash, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, hash}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_persist, plan, opts}, _from, state) do
    case safe_encode_relation(plan, state.plan_id_counter) do
      {{relation, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_persist(state, relation, opts) do
          {:ok, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, :ok, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_unpersist, plan, opts}, _from, state) do
    case safe_encode_relation(plan, state.plan_id_counter) do
      {{relation, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_unpersist(state, relation, opts) do
          {:ok, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, :ok, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:analyze_get_storage_level, plan}, _from, state) do
    case safe_encode_relation(plan, state.plan_id_counter) do
      {{relation, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.analyze_get_storage_level(state, relation) do
          {:ok, storage_level, server_side_session_id} ->
            state = maybe_update_server_session(state, server_side_session_id)
            {:reply, {:ok, storage_level}, state}

          {:error, _} = error ->
            reply_error(error, state)
        end

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:artifact_status, names}, _from, state) do
    case Client.artifact_status(state, names) do
      {:ok, statuses, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, statuses}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:add_artifacts, artifacts}, _from, state) do
    case Client.add_artifacts(state, artifacts) do
      {:ok, summaries, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        {:reply, {:ok, summaries}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:create_dataframe, data, opts}, from, state) do
    case prepare_local_data(data, opts) do
      {:ok, {:local_relation, arrow_ipc, schema_ddl, source_df}} ->
        cache_threshold = Keyword.get(opts, :cache_threshold, 4 * 1024 * 1024)

        cond do
          not (is_integer(cache_threshold) and cache_threshold >= 0) ->
            {:reply, {:error, {:invalid_option, {:cache_threshold, cache_threshold}}}, state}

          byte_size(arrow_ipc) <= cache_threshold ->
            plan = {:local_relation, arrow_ipc, schema_ddl}
            df = SparkEx.DataFrame.new(self(), plan)
            {:reply, {:ok, df}, state}

          true ->
            chunk_size =
              Keyword.get(opts, :cache_chunk_size, max(cache_threshold, 1))

            if is_integer(chunk_size) and chunk_size > 0 do
              # Drop arrow_ipc from this scope before re-encoding per chunk so
              # peak memory stays close to the size of the source DataFrame
              # rather than 2x that for very large payloads.
              handle_call(
                {:create_dataframe_chunked_cache, source_df, schema_ddl, chunk_size},
                from,
                state
              )
            else
              {:reply, {:error, {:invalid_option, {:cache_chunk_size, chunk_size}}}, state}
            end
        end

      {:ok, {:sql_relation, query, args}} ->
        df = SparkEx.DataFrame.new(self(), {:sql, query, args})
        {:reply, {:ok, df}, state}

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call(
        {:create_dataframe_chunked_cache, source_df, schema_ddl, chunk_size},
        _from,
        state
      ) do
    case split_explorer_dataframe_for_cache(source_df, chunk_size) do
      {:ok, data_chunks} ->
        {data_hashes, data_artifacts} =
          data_chunks
          |> Enum.map(fn chunk ->
            hash = :crypto.hash(:sha256, chunk) |> Base.encode16(case: :lower)
            {hash, {"cache/#{hash}", chunk}}
          end)
          |> Enum.unzip()

        # Upload schema DDL as separate cache artifact (no "schema_" prefix —
        # the server looks up schemaHash directly in the cache key space)
        schema_bytes = if schema_ddl, do: schema_ddl, else: ""
        schema_hash = :crypto.hash(:sha256, schema_bytes) |> Base.encode16(case: :lower)
        schema_artifact = {"cache/#{schema_hash}", schema_bytes}

        # Dedupe by artifact name so identical chunks (or chunk == schema bytes
        # by coincidence) only get uploaded once. The plan still references
        # each hash in original order via `data_hashes`.
        artifacts =
          (data_artifacts ++ [schema_artifact])
          |> Enum.uniq_by(fn {name, _data} -> name end)

        case upload_missing_cache_artifacts(state, artifacts) do
          {:ok, state} ->
            plan = {:chunked_cached_local_relation, data_hashes, schema_hash}
            df = SparkEx.DataFrame.new(self(), plan)
            {:reply, {:ok, df}, state}

          {:error, _reason} = error ->
            {:reply, error, state}
        end

      {:error, _reason} = error ->
        reply_error(error, state)
    end
  end

  def handle_call({:execute_command, command, opts}, _from, state) do
    operation_telemetry_span(:execute_command, state.session_id, fn ->
      case safe_encode_command(command, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          opts = merge_session_tags(opts, state.tags)

          case Client.execute_plan(state, proto_plan, opts) do
            {:ok, result} ->
              state = maybe_update_server_session(state, result.server_side_session_id)
              state = %{state | last_execution_metrics: result.execution_metrics}
              {:reply, :ok, state}

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:execute_command_with_result, command, opts}, _from, state) do
    operation_telemetry_span(:execute_command_with_result, state.session_id, fn ->
      case safe_encode_command(command, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          opts = merge_session_tags(opts, state.tags)

          case Client.execute_plan(state, proto_plan, opts) do
            {:ok, result} ->
              state = maybe_update_server_session(state, result.server_side_session_id)
              state = %{state | last_execution_metrics: result.execution_metrics}
              {:reply, {:ok, result.command_result}, state}

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:prepare_execute_command_stream, command, opts}, _from, state) do
    case safe_encode_command(command, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}
        opts = merge_session_tags(opts, state.tags)
        {:reply, {:ok, state, proto_plan, opts}, state}

      {nil, error} ->
        reply_error(error, state)
    end
  end

  def handle_call({:execute_show, plan}, _from, state) do
    operation_telemetry_span(:execute_show, state.session_id, fn ->
      case safe_encode(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          case Client.execute_plan(state, proto_plan, merge_session_tags([], state.tags)) do
            {:ok, result} ->
              reply_execute_show_result(result, state)

            {:error, _} = error ->
              reply_error(error, state)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  defp reply_execute_show_result(result, state) do
    state = maybe_update_server_session(state, result.server_side_session_id)
    state = %{state | last_execution_metrics: result.execution_metrics}

    case extract_show_string(result.rows) do
      {:ok, str} -> {:reply, {:ok, str}, state}
      {:error, _} = error -> reply_error(error, state)
    end
  end

  defp execute_collect_no_fallback(state, plan, proto_plan, opts) do
    case Client.execute_plan(state, proto_plan, opts) do
      {:ok, result} ->
        state = maybe_update_server_session(state, result.server_side_session_id)

        {:ok, result, state} =
          maybe_retry_collect_with_json_projection(state, plan, proto_plan, opts, result)

        reply_collect_result(result, state)

      {:error, {:arrow_decode_failed, _reason}} = error ->
        execute_collect_retry_unique_columns(state, plan, proto_plan, opts, error)

      {:error, %SparkEx.Error.Remote{} = remote} = error ->
        execute_collect_retry_legacy(state, plan, opts, remote, error)

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  defp execute_collect_retry_unique_columns(state, plan, proto_plan, opts, error) do
    case retry_collect_with_unique_columns(state, plan, proto_plan, opts) do
      {:ok, result, state} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        {result, state} = maybe_decode_retry_result_rows(state, plan, proto_plan, result)
        reply_collect_result(result, state)

      {:error, state} ->
        reply_error(error, state)
    end
  end

  defp execute_collect_retry_legacy(state, plan, opts, remote, error) do
    case retry_collect_with_legacy_fallbacks(state, plan, opts, remote) do
      {:ok, result, state} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        reply_collect_result(result, state)

      :error ->
        reply_error(error, state)
    end
  end

  defp reply_collect_result(result, state) do
    state = %{state | last_execution_metrics: result.execution_metrics}
    SparkEx.Observation.store_observed_metrics(result.observed_metrics, state.session_id)
    {:reply, {:ok, result.rows}, state}
  end

  defp reply_execute_count_result(result, state) do
    state = maybe_update_server_session(state, result.server_side_session_id)
    state = %{state | last_execution_metrics: result.execution_metrics}
    SparkEx.Observation.store_observed_metrics(result.observed_metrics, state.session_id)

    case extract_count(result.rows) do
      {:ok, count} -> {:reply, {:ok, count}, state}
      {:error, _} = error -> reply_error(error, state)
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
  def terminate(_reason, %{released: true}) do
    SparkEx.Internal.PlanIds.unregister_session(self())
    :ok
  end

  def terminate(_reason, %{channel: nil}) do
    SparkEx.Internal.PlanIds.unregister_session(self())
    :ok
  end

  def terminate(_reason, %{channel: channel} = state) do
    # Best-effort release before disconnect with timeout to prevent blocking
    task = Task.async(fn -> Client.release_session(state) end)
    Task.yield(task, 5_000) || Task.shutdown(task)
    safe_disconnect(channel)
    SparkEx.Internal.PlanIds.unregister_session(self())
    :ok
  end

  @doc false
  @spec safe_disconnect(term()) :: :ok
  def safe_disconnect(channel) do
    do_safe_disconnect(channel)
  end

  defp do_safe_disconnect(nil), do: :ok

  defp do_safe_disconnect(channel) do
    require Logger

    try do
      case Channel.disconnect(channel) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning("spark_ex session channel disconnect failed: #{inspect(reason)}")

          :telemetry.execute(
            [:spark_ex, :session, :disconnect, :error],
            %{system_time: System.system_time()},
            %{reason: reason}
          )

          :ok
      end
    rescue
      exception ->
        stacktrace = __STACKTRACE__

        Logger.warning(
          "spark_ex session channel disconnect raised #{inspect(exception.__struct__)}: #{Exception.message(exception)}\n" <>
            Exception.format_stacktrace(stacktrace)
        )

        :telemetry.execute(
          [:spark_ex, :session, :disconnect, :exception],
          %{system_time: System.system_time()},
          %{kind: :error, reason: exception, stacktrace: stacktrace}
        )

        :ok
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__

        Logger.warning(
          "spark_ex session channel disconnect #{kind}: #{inspect(reason)}\n" <>
            Exception.format_stacktrace(stacktrace)
        )

        :telemetry.execute(
          [:spark_ex, :session, :disconnect, :exception],
          %{system_time: System.system_time()},
          %{kind: kind, reason: reason, stacktrace: stacktrace}
        )

        :ok
    end
  end

  # --- Private ---

  defp operation_telemetry_span(operation, session_id, fun) do
    metadata = %{operation: operation, session_id: session_id}
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:spark_ex, :session, :operation, :start],
      %{system_time: System.system_time()},
      metadata
    )

    try do
      result = fun.()
      duration = System.monotonic_time() - start_time

      result_status =
        case result do
          {:reply, {:ok, _}, _} -> :ok
          {:reply, :ok, _} -> :ok
          {:reply, {:error, _}, _} -> :error
          _ -> :ok
        end

      :telemetry.execute(
        [:spark_ex, :session, :operation, :stop],
        %{duration: duration},
        Map.put(metadata, :result, result_status)
      )

      result
    rescue
      e ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:spark_ex, :session, :operation, :exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: :error, reason: e, stacktrace: __STACKTRACE__})
        )

        reraise e, __STACKTRACE__
    end
  end

  defp maybe_execute_collect_with_json_projection(state, plan, proto_plan, opts) do
    if maybe_preflight_collect_retry?(plan) do
      case Client.analyze_schema(state, proto_plan) do
        {:ok, schema, server_side_session_id} ->
          state = maybe_update_server_session(state, server_side_session_id)

          case unique_schema_column_names(schema) do
            unique_names when is_list(unique_names) ->
              execute_collect_with_unique_columns_pre(state, plan, unique_names, opts)

            _ ->
              maybe_execute_collect_with_json_projection_schema_retry(state, plan, schema, opts)
          end

        _ ->
          {:no_fallback, state}
      end
    else
      {:no_fallback, state}
    end
  end

  defp execute_collect_with_unique_columns_pre(state, plan, unique_names, opts) do
    :telemetry.execute(
      [:spark_ex, :session, :collect_retry],
      %{attempt: 1},
      %{session_id: state.session_id, strategy: :unique_columns_pre}
    )

    case execute_retry_plan(state, {:to_df, plan, unique_names}, opts) do
      {:ok, result, state} -> {:ok, result, state}
      {:error, state} -> {:no_fallback, state}
    end
  end

  defp maybe_execute_collect_with_json_projection_schema_retry(state, plan, schema, opts) do
    if schema_has_nested_map?(schema) or schema_has_struct_and_map?(schema) do
      case retry_collect_with_json_projection(state, plan, schema, opts) do
        {:ok, result, state} ->
          :telemetry.execute(
            [:spark_ex, :session, :collect_retry],
            %{attempt: 1},
            %{session_id: state.session_id, strategy: :json_projection_pre}
          )

          result = Map.update!(result, :rows, &decode_rows_from_json_projection(&1, schema))
          {:ok, result, state}

        {:error, state} ->
          {:no_fallback, state}
      end
    else
      {:no_fallback, state}
    end
  end

  defp maybe_preflight_collect_retry?(plan) do
    maybe_json_relation_plan?(plan) or maybe_sql_plan?(plan)
  end

  defp maybe_sql_plan?({:sql, query, _args}) when is_binary(query) do
    lowered =
      query
      |> String.trim_leading()
      |> String.downcase()

    String.starts_with?(lowered, "select") or
      String.starts_with?(lowered, "with") or
      String.starts_with?(lowered, "values")
  end

  defp maybe_sql_plan?(plan) when is_tuple(plan) do
    plan
    |> Tuple.to_list()
    |> Enum.any?(&maybe_sql_plan?/1)
  end

  defp maybe_sql_plan?(plan) when is_list(plan),
    do: Enum.any?(plan, &maybe_sql_plan?/1)

  defp maybe_sql_plan?(_), do: false

  defp schema_has_struct_and_map?(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    has_struct? = Enum.any?(struct.fields, &schema_contains_kind?(&1.data_type, :struct))
    has_map? = Enum.any?(struct.fields, &schema_contains_kind?(&1.data_type, :map))
    has_struct? and has_map?
  end

  defp schema_has_struct_and_map?(_), do: false

  defp schema_contains_kind?(%Spark.Connect.DataType{kind: {kind, _}}, kind), do: true

  defp schema_contains_kind?(%Spark.Connect.DataType{kind: {:array, array}}, kind) do
    schema_contains_kind?(array.element_type, kind)
  end

  defp schema_contains_kind?(
         %Spark.Connect.DataType{
           kind: {:map, %Spark.Connect.DataType.Map{key_type: key_type, value_type: value_type}}
         },
         kind
       ) do
    schema_contains_kind?(key_type, kind) or schema_contains_kind?(value_type, kind)
  end

  defp schema_contains_kind?(%Spark.Connect.DataType{kind: {:struct, struct}}, kind) do
    Enum.any?(struct.fields, &schema_contains_kind?(&1.data_type, kind))
  end

  defp schema_contains_kind?(_data_type, _kind), do: false

  defp retry_collect_with_unique_columns(state, plan, proto_plan, opts) do
    :telemetry.execute(
      [:spark_ex, :session, :collect_retry],
      %{attempt: 1},
      %{session_id: state.session_id, strategy: :unique_columns}
    )

    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
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

      _ ->
        {:error, state}
    end
  end

  defp maybe_retry_collect_with_json_projection(state, plan, proto_plan, opts, primary_result) do
    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)

        primary_result =
          Map.update!(primary_result, :rows, &decode_rows_from_json_projection(&1, schema))

        if schema_has_nested_map?(schema) and
             rows_show_nested_map_decode_loss?(primary_result.rows, schema) do
          case retry_collect_with_json_projection(state, plan, schema, opts) do
            {:ok, retry_result, state} ->
              :telemetry.execute(
                [:spark_ex, :session, :collect_retry],
                %{attempt: 1},
                %{session_id: state.session_id, strategy: :json_projection_post}
              )

              retry_result =
                Map.update!(retry_result, :rows, &decode_rows_from_json_projection(&1, schema))

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

  defp maybe_json_relation_plan?(plan) when is_tuple(plan) do
    plan
    |> Tuple.to_list()
    |> Enum.any?(&maybe_json_relation_plan?/1)
  end

  defp maybe_json_relation_plan?(plan) when is_list(plan),
    do: Enum.any?(plan, &maybe_json_relation_plan?/1)

  defp maybe_json_relation_plan?(_), do: false

  defp maybe_decode_retry_result_rows(state, _plan, proto_plan, result) do
    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        result = Map.update!(result, :rows, &decode_rows_from_json_projection(&1, schema))
        {result, state}

      _ ->
        {result, state}
    end
  end

  defp execute_retry_plan(state, retry_plan, opts) do
    case safe_encode(retry_plan, state.plan_id_counter) do
      {{retry_proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}

        case Client.execute_plan(state, retry_proto_plan, opts) do
          {:ok, result} ->
            {:ok, result, state}

          {:error, {:arrow_decode_failed, _reason}} ->
            retry_collect_with_unique_columns(state, retry_plan, retry_proto_plan, opts)

          {:error, _} ->
            {:error, state}
        end

      _ ->
        {:error, state}
    end
  end

  defp retry_collect_with_legacy_fallbacks(state, plan, opts, %SparkEx.Error.Remote{} = remote) do
    case classify_legacy_recovery_strategy(remote) do
      :legacy_grouping_sets ->
        :telemetry.execute(
          [:spark_ex, :session, :collect_retry],
          %{attempt: 1},
          %{session_id: state.session_id, strategy: :legacy_grouping_sets}
        )

        with {:ok, rewritten_plan} <- rewrite_grouping_sets_collect_plan(plan),
             {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
          {:ok, result, state}
        else
          _ -> :error
        end

      :legacy_unparsed ->
        :telemetry.execute(
          [:spark_ex, :session, :collect_retry],
          %{attempt: 1},
          %{session_id: state.session_id, strategy: :legacy_unparsed}
        )

        with {:ok, rewritten_plan} <- rewrite_parse_collect_plan(state, plan),
             {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
          {:ok, result, state}
        else
          _ -> :error
        end

      :empty_relation ->
        retry_collect_for_empty_relation_errors(state, plan, opts)

      nil ->
        :error
    end
  end

  # Each strategy is keyed first by the server's `errorClass` (set in
  # ErrorInfo.metadata, surfaced by `Connect.Errors`). A localised or
  # version-shifted server message no longer breaks the retry path
  # when the class is present. The message-substring tier remains as
  # a fallback for clusters that return errors without an errorClass.
  @legacy_recovery_error_classes %{
    "_LEGACY_ERROR_TEMP_GROUPING_SETS" => :legacy_grouping_sets,
    "_LEGACY_ERROR_TEMP_UNPARSED" => :legacy_unparsed,
    "_LEGACY_ERROR_TEMP_EMPTY_RELATION" => :empty_relation
  }

  @legacy_recovery_message_fragments [
    {"Unknown Group Type UNRECOGNIZED", :legacy_grouping_sets},
    {"Does not support convert UNPARSED to catalyst types.", :legacy_unparsed},
    {"Expected Relation to be set, but is empty.", :empty_relation}
  ]

  defp classify_legacy_recovery_strategy(%SparkEx.Error.Remote{
         error_class: error_class,
         message: message
       }) do
    case Map.get(@legacy_recovery_error_classes, error_class) do
      nil -> classify_legacy_recovery_by_message(message)
      strategy -> strategy
    end
  end

  defp classify_legacy_recovery_by_message(message) when is_binary(message) do
    Enum.find_value(@legacy_recovery_message_fragments, fn {fragment, strategy} ->
      if String.contains?(message, fragment), do: strategy
    end)
  end

  defp classify_legacy_recovery_by_message(_), do: nil

  defp retry_collect_for_empty_relation_errors(state, plan, opts) do
    rewriters = [
      &rewrite_transpose_collect_plan/1,
      &rewrite_table_function_collect_plan/1,
      &rewrite_as_of_join_collect_plan/1,
      &rewrite_subquery_collect_plan/1
    ]

    Enum.reduce_while(rewriters, :error, fn rewriter, _acc ->
      with {:ok, rewritten_plan} <- rewriter.(plan),
           {:ok, result, state} <- execute_retry_plan(state, rewritten_plan, opts) do
        :telemetry.execute(
          [:spark_ex, :session, :collect_retry],
          %{attempt: 1},
          %{session_id: state.session_id, strategy: :empty_relation_rewrite}
        )

        {:halt, {:ok, result, state}}
      else
        _ -> {:cont, :error}
      end
    end)
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
          join_type, _tolerance, _allow_exact_matches, _direction}
       ) do
    condition =
      case join_expr do
        {:lit, nil} -> nil
        other -> other
      end

    {:ok,
     {:join, left_plan, right_plan, condition, normalize_as_of_fallback_join_type(join_type),
      using_columns || []}}
  end

  defp rewrite_as_of_join_collect_plan(_plan), do: :error

  defp normalize_as_of_fallback_join_type(nil), do: :inner
  defp normalize_as_of_fallback_join_type("inner"), do: :inner
  defp normalize_as_of_fallback_join_type("left"), do: :left
  defp normalize_as_of_fallback_join_type("right"), do: :right
  defp normalize_as_of_fallback_join_type("full"), do: :full
  defp normalize_as_of_fallback_join_type(:inner), do: :inner
  defp normalize_as_of_fallback_join_type(:left), do: :left
  defp normalize_as_of_fallback_join_type(:right), do: :right
  defp normalize_as_of_fallback_join_type(:full), do: :full
  defp normalize_as_of_fallback_join_type(_), do: :inner

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
          build_grouping_set_plan(
            child_plan,
            set_exprs,
            set_names,
            grouping_names,
            agg_exprs,
            agg_aliases
          )
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

  defp build_grouping_set_plan(
         child_plan,
         set_exprs,
         set_names,
         grouping_names,
         agg_exprs,
         agg_aliases
       ) do
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
  end

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
          {:alias, {:unresolved_extract_value, {:col, parsed_alias}, {:lit, field_name}},
           field_name}
        end)

      {:ok, {:project, parsed_plan, projected_fields}}
    end
  end

  defp rewrite_parse_collect_plan(_state, _plan), do: :error

  defp first_schema_column_name(state, plan) do
    with {{proto_plan, _counter}, nil} <- safe_encode(plan, 0),
         {:ok, schema, _server_side_session_id} <- Client.analyze_schema(state, proto_plan),
         %Spark.Connect.DataType{
           kind: {:struct, %Spark.Connect.DataType.Struct{fields: [first | _]}}
         } <-
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
        %Spark.Connect.DataType{} = data_type ->
          {:lit, SparkEx.Types.data_type_to_json(data_type)}

        s when is_binary(s) ->
          {:lit, s}
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
        build_subquery_sql_expression(subquery_type, subquery_sql, opts)
      end
    end
  end

  defp build_subquery_sql_expression(:scalar, subquery_sql, _opts) do
    {:ok, {:expr, "(#{subquery_sql})"}}
  end

  defp build_subquery_sql_expression(:exists, subquery_sql, _opts) do
    {:ok, {:expr, "EXISTS (#{subquery_sql})"}}
  end

  defp build_subquery_sql_expression(:in, subquery_sql, opts) do
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
  end

  defp build_subquery_sql_expression(_subquery_type, _subquery_sql, _opts), do: :error

  defp explicit_subquery_reference_plan?(%{plan_id: plan_id, plan: _plan})
       when is_integer(plan_id),
       do: true

  defp explicit_subquery_reference_plan?({plan_id, _plan}) when is_integer(plan_id), do: true

  defp explicit_subquery_reference_plan?({:plan_id, plan_id, _plan}) when is_integer(plan_id),
    do: true

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
      fn_to_sql(name, arg_sql, is_distinct)
    end
  end

  defp expr_to_sql(_expr), do: :error

  defp fn_to_sql("==", [left, right], _), do: {:ok, "(#{left} = #{right})"}
  defp fn_to_sql("!=", [left, right], _), do: {:ok, "(#{left} <> #{right})"}
  defp fn_to_sql("<>", [left, right], _), do: {:ok, "(#{left} <> #{right})"}
  defp fn_to_sql(">", [left, right], _), do: {:ok, "(#{left} > #{right})"}
  defp fn_to_sql("<", [left, right], _), do: {:ok, "(#{left} < #{right})"}
  defp fn_to_sql(">=", [left, right], _), do: {:ok, "(#{left} >= #{right})"}
  defp fn_to_sql("<=", [left, right], _), do: {:ok, "(#{left} <= #{right})"}
  defp fn_to_sql("and", [left, right], _), do: {:ok, "(#{left} AND #{right})"}
  defp fn_to_sql("or", [left, right], _), do: {:ok, "(#{left} OR #{right})"}
  defp fn_to_sql("not", [arg], _), do: {:ok, "(NOT #{arg})"}
  defp fn_to_sql("+", [left, right], _), do: {:ok, "(#{left} + #{right})"}
  defp fn_to_sql("-", [left, right], _), do: {:ok, "(#{left} - #{right})"}
  defp fn_to_sql("*", [left, right], _), do: {:ok, "(#{left} * #{right})"}
  defp fn_to_sql("/", [left, right], _), do: {:ok, "(#{left} / #{right})"}

  defp fn_to_sql(name, arg_sql, is_distinct),
    do: {:ok, sql_function_call(name, arg_sql, is_distinct)}

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
    |> Enum.map_join(".", fn part ->
      escaped = String.replace(part, "`", "``")
      "`#{escaped}`"
    end)
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
           kind: {:map, %Spark.Connect.DataType.Map{key_type: key_type, value_type: value_type}}
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
         %Spark.Connect.DataType{
           kind: {:array, %Spark.Connect.DataType.Array{element_type: element_type}}
         }
       )
       when is_list(value) do
    Enum.map(value, &coerce_complex_decoded_value(&1, element_type))
  end

  defp coerce_complex_decoded_value(
         value,
         %Spark.Connect.DataType{
           kind: {:map, %Spark.Connect.DataType.Map{key_type: key_type, value_type: value_type}}
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
    safe_encode_with(&PlanEncoder.encode/2, plan, counter)
  end

  defp safe_encode_count(plan, counter) do
    safe_encode_with(&PlanEncoder.encode_count/2, plan, counter)
  end

  defp safe_encode_relation(plan, counter) do
    safe_encode_with(&PlanEncoder.encode_relation/2, plan, counter)
  end

  defp safe_encode_command(command, counter) do
    alias SparkEx.Connect.CommandEncoder
    safe_encode_with(&CommandEncoder.encode/2, command, counter)
  end

  defp safe_encode_with(encode_fn, plan, counter) do
    # Synthetic ids are reserved from the shared session-scoped atomic
    # inside the encoder (`PlanEncoder.next_id/1` calls
    # `SparkEx.Internal.PlanIds.next(self())`), so caller-side
    # `DataFrame.new/2` calls and encoder allocations race-safely on
    # the same counter. The threaded `counter` is vestigial as far as
    # uniqueness is concerned; pass it through for backward
    # compatibility.
    {encode_fn.(plan, counter), nil}
  rescue
    e ->
      formatted = Exception.format(:error, e, __STACKTRACE__)

      Logger.error("Plan encoding failed: #{formatted}")

      {nil, {:error, {:plan_encode_error, formatted}}}
  end

  defp maybe_update_server_session(state, nil), do: state
  defp maybe_update_server_session(state, ""), do: state

  defp maybe_update_server_session(state, id) do
    case SparkEx.Connect.SessionIntegrity.validate_server_session_id(
           id,
           state.server_side_session_id
         ) do
      {:ok, ^id} ->
        %{state | server_side_session_id: id}

      {:ok, current} ->
        %{state | server_side_session_id: current}

      {:error, {:server_session_changed, ctx}} ->
        Logger.warning(
          "spark_ex session #{state.session_id} closed: server-side session id changed " <>
            "(pinned=#{ctx.pinned}, got=#{ctx.got})"
        )

        %{state | closed: true}
    end
  end

  # Mirror the close-state path used by maybe_update_server_session/2 on
  # success: when an RPC reply (or streaming integrity violation) carries
  # an INVALID_HANDLE.SESSION_CHANGED signal, the server has rotated the
  # session and the client is no longer authorized to reuse it.
  defp maybe_close_on_error(%{closed: true} = state, _error), do: state

  defp maybe_close_on_error(state, error) do
    if SparkEx.Connect.SessionIntegrity.session_changed_error?(error) do
      Logger.warning(
        "spark_ex session #{state.session_id} closed: server returned " <>
          "INVALID_HANDLE.SESSION_CHANGED (#{summarize_session_changed(error)})"
      )

      %{state | closed: true}
    else
      state
    end
  end

  # Keep the warning compact and avoid leaking user-supplied details
  # (SQL fragments, stacktraces) that may be present on the error.
  defp summarize_session_changed({:error, inner}), do: summarize_session_changed(inner)

  defp summarize_session_changed({:server_session_changed, %{pinned: pinned, got: got}}),
    do: "pinned=#{pinned}, got=#{got}"

  defp summarize_session_changed(%SparkEx.Error.Remote{error_class: class})
       when is_binary(class),
       do: "error_class=#{class}"

  defp summarize_session_changed(%SparkEx.Error.Remote{}), do: "error_class=unknown"

  defp summarize_session_changed(_), do: "source=unknown"

  defp reply_error(error, state) do
    {:reply, error, maybe_close_on_error(state, error)}
  end

  @spark_ex_version Mix.Project.config()[:version]

  # Mirrors PySpark's user_agent shape: `<base> spark/<v> os/<system>`.
  # PySpark hard-codes the bundled pyspark version; we don't track a Spark
  # version (the proto we vendor targets Spark 4.x). Use the spark_ex
  # package version under `spark_ex/<v>` and add `spark/connect-1` as the
  # protocol marker plus `os/<system>` so server-side telemetry can
  # distinguish clients without relying on the free-form base.
  defp default_client_type do
    "elixir/#{System.version()}/otp#{otp_release()}/spark_ex/#{@spark_ex_version} " <>
      client_type_suffix()
  end

  defp client_type_suffix do
    "spark/connect-1 os/#{os_name()}"
  end

  defp otp_release do
    :erlang.system_info(:otp_release) |> List.to_string()
  end

  defp os_name do
    case :os.type() do
      {:unix, name} -> Atom.to_string(name)
      {:win32, _} -> "windows"
    end
  end

  defp extract_count([row]) when is_map(row) and map_size(row) == 1 do
    case Map.values(row) do
      [n] when is_integer(n) and n >= 0 -> {:ok, n}
      _ -> {:error, {:invalid_count_response, row}}
    end
  end

  defp extract_count(rows), do: {:error, {:invalid_count_response, rows}}

  defp extract_show_string([row]) when is_map(row) and map_size(row) == 1 do
    case Map.values(row) do
      [str] when is_binary(str) -> {:ok, str}
      _ -> {:error, {:invalid_show_response, row}}
    end
  end

  defp extract_show_string(rows), do: {:error, {:invalid_show_response, rows}}

  # Returns the GenServer call timeout for an operation. Adds a 5-second buffer
  # over the user-specified gRPC timeout so the gRPC layer times out first and
  # returns a proper error, rather than the GenServer call timing out with :exit.
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

  defp real_session_process?(session) do
    case :sys.get_state(session) do
      %__MODULE__{} -> true
      _ -> false
    end
  catch
    :exit, _ -> false
  end

  # --- Local data preparation ---

  defp prepare_local_data(data, opts) when is_struct(data, Explorer.DataFrame) do
    with {:ok, schema_ddl} <- normalize_create_dataframe_schema(opts) do
      schema_ddl = schema_ddl || explorer_to_ddl(data)

      if normalize_local_relation_arrow?(opts) and dataframe_contains_complex_dtype?(data) do
        prepare_sql_json_relation(data, schema_ddl)
      else
        case Explorer.DataFrame.dump_ipc_stream(data) do
          # Pass the source Explorer.DataFrame alongside the IPC bytes so the
          # chunked-cache path can re-slice it natively without a full IPC
          # decode round-trip on multi-hundred-MB payloads.
          {:ok, ipc_bytes} -> {:ok, {:local_relation, ipc_bytes, schema_ddl, data}}
          {:error, reason} -> {:error, {:arrow_encode_error, reason}}
        end
      end
    end
  end

  defp prepare_local_data(data, opts) when is_list(data) do
    with {:ok, schema} <- normalize_create_dataframe_schema(opts),
         {:ok, normalized_data, normalized_schema} <- normalize_list_data_and_schema(data, schema) do
      cond do
        is_binary(normalized_schema) ->
          prepare_list_data_with_schema(
            normalized_data,
            normalized_schema,
            Keyword.put(opts, :schema, normalized_schema)
          )

        match?({:json_schema, _, _}, normalized_schema) ->
          {:json_schema, json_str, ddl_str} = normalized_schema
          prepare_list_data_with_json_schema(normalized_data, json_str, ddl_str, opts)

        is_nil(normalized_schema) ->
          prepare_list_data_inferred(normalized_data, Keyword.delete(opts, :schema))
      end
    end
  end

  defp prepare_local_data(data, opts) when is_map(data) and not is_struct(data) do
    with {:ok, schema_ddl} <- normalize_create_dataframe_schema(opts),
         {:ok, ordered} <- order_column_map(data) do
      # Column-oriented data: %{"col1" => [1,2,3], "col2" => ["a","b","c"]}
      # Map iteration order is undefined for maps with >32 keys; sort by
      # column name so the encoded relation is deterministic regardless
      # of insertion order. Callers needing user-controlled order should
      # pass a list of `{name, values}` pairs instead.
      case safe_explorer_new(ordered) do
        {:ok, explorer_df} ->
          effective_schema = schema_ddl || explorer_to_ddl(explorer_df)
          prepare_local_data(explorer_df, Keyword.put(opts, :schema, effective_schema))

        {:error, reason} ->
          {:error, {:data_conversion_error, reason}}
      end
    end
  end

  defp prepare_local_data(_data, _opts) do
    {:error, {:invalid_data, "expected Explorer.DataFrame, list of maps, or column map"}}
  end

  defp struct_has_field_metadata?(fields) when is_list(fields) do
    Enum.any?(fields, fn
      %{metadata: meta} when is_map(meta) and map_size(meta) > 0 -> true
      _ -> false
    end)
  end

  defp normalize_create_dataframe_schema(opts) do
    case Keyword.get(opts, :schema, nil) do
      nil ->
        {:ok, nil}

      schema when is_binary(schema) ->
        {:ok, schema}

      {:struct, fields} = schema ->
        # PySpark sends `_schema.json()` for explicit StructType schemas so that
        # field nullability and metadata round-trip into the local relation.
        # Only emit JSON when metadata is non-empty for at least one field;
        # otherwise keep the DDL form so existing schema-string flows that
        # reparse fields (binary_top_level_fields, non_string_top_level_map_fields,
        # etc.) keep working.
        if struct_has_field_metadata?(fields) do
          {:ok, {:json_schema, SparkEx.Types.to_json(schema), SparkEx.Types.to_ddl(schema)}}
        else
          {:ok, SparkEx.Types.schema_to_string(schema)}
        end

      schema when is_list(schema) ->
        cond do
          schema == [] ->
            {:error, {:invalid_schema, "schema column-name list cannot be empty"}}

          Enum.all?(schema, &is_binary/1) ->
            {:ok, {:column_names, schema}}

          true ->
            {:error,
             {:invalid_schema,
              "expected list of column names as strings, got: #{inspect(schema)}"}}
        end

      other ->
        {:error,
         {:invalid_schema,
          "expected schema as DDL string, SparkEx.Types struct schema, or list of column names, got: #{inspect(other)}"}}
    end
  end

  # Bridges three input shapes (list of maps, list of tuples, list of
  # bare values) to the inferred / explicit-DDL paths. Returns the
  # rewritten data and the schema in canonical form: either a DDL
  # string or `nil` (inference). Tuples without column names are
  # rejected — the previous `is_list/1` arm silently fell through to
  # the inferred path which then failed inside Explorer.
  defp normalize_list_data_and_schema([], schema) do
    case schema do
      {:column_names, _} ->
        {:error,
         {:invalid_data, "cannot create DataFrame from empty list with column-name schema"}}

      _ ->
        {:ok, [], schema}
    end
  end

  defp normalize_list_data_and_schema(data, schema) when is_list(data) do
    cond do
      Enum.all?(data, &(is_map(&1) and not is_struct(&1))) ->
        case schema do
          {:column_names, names} ->
            # PySpark sorts dict items deterministically and renames the
            # resulting columns to the requested names via toDF(*_cols).
            apply_column_names_to_map_rows(data, names)

          _ ->
            {:ok, data, schema}
        end

      Enum.all?(data, &is_tuple/1) ->
        normalize_tuple_rows(data, schema)

      # PySpark treats list rows like tuple rows (multi-column); coerce to tuples.
      Enum.all?(data, &is_list/1) ->
        tupled = Enum.map(data, &List.to_tuple/1)
        normalize_list_data_and_schema(tupled, schema)

      # PySpark wraps non-row primitive iterables as 1-column rows before
      # schema inference (`[1, 2, 3]` → `[(1,), (2,), (3,)]`).
      # Maps, tuples, and lists are row-shaped and handled by the arms above.
      Enum.all?(data, &primitive_row?/1) ->
        wrapped = Enum.map(data, fn v -> {v} end)
        normalize_list_data_and_schema(wrapped, schema)

      true ->
        {:error,
         {:invalid_data,
          "expected list of maps or list of tuples for createDataFrame, got: #{inspect(data)}"}}
    end
  end

  defp normalize_tuple_rows(data, schema) do
    with {:ok, names} <- column_names_for_tuple_data(data, schema) do
      maps = Enum.map(data, fn tuple -> tuple_to_named_map(tuple, names) end)

      case schema do
        binary when is_binary(binary) -> {:ok, maps, binary}
        {:json_schema, _, _} = json_schema -> {:ok, maps, json_schema}
        {:column_names, _} -> {:ok, maps, nil}
        nil -> {:ok, maps, nil}
      end
    end
  end

  # Scalars that are not row-shaped (map / tuple / list). Struct values
  # also fall through here and are treated as single-column values.
  defp primitive_row?(v) when is_map(v) and not is_struct(v), do: false
  defp primitive_row?(v) when is_tuple(v), do: false
  defp primitive_row?(v) when is_list(v), do: false
  defp primitive_row?(_), do: true

  defp apply_column_names_to_map_rows(data, names) when is_list(names) do
    # Collect all unique keys across all rows (union), sorted alphabetically
    # for deterministic positional mapping — mirrors PySpark's
    # `dict(sorted(d.items()))` approach. Missing keys in individual rows
    # produce nil values (same as the existing list_of_maps_to_explorer path
    # which fills missing keys with nil).
    sorted_keys =
      data
      |> Enum.flat_map(fn row -> Enum.map(row, fn {k, _} -> to_string(k) end) end)
      |> Enum.uniq()
      |> Enum.sort()

    if length(sorted_keys) != length(names) do
      {:error,
       {:invalid_schema,
        "column-name list of length #{length(names)} does not match data with #{length(sorted_keys)} unique keys"}}
    else
      renamed =
        Enum.map(data, fn row ->
          stringified = Map.new(row, fn {k, v} -> {to_string(k), v} end)

          sorted_keys
          |> Enum.zip(names)
          |> Map.new(fn {orig, new} -> {new, Map.get(stringified, orig)} end)
        end)

      {:ok, renamed, nil}
    end
  end

  defp column_names_for_tuple_data(_data, {:column_names, names}), do: {:ok, names}

  defp column_names_for_tuple_data(_data, schema_ddl) when is_binary(schema_ddl) do
    names =
      schema_ddl
      |> split_top_level_schema_fields()
      |> Enum.flat_map(fn field ->
        case parse_schema_field(field) do
          {name, _type} -> [name]
          :error -> []
        end
      end)

    if names == [] do
      {:error, {:invalid_schema, "could not extract field names from DDL: #{schema_ddl}"}}
    else
      {:ok, names}
    end
  end

  defp column_names_for_tuple_data(data, {:json_schema, json, _ddl}) do
    # JSON schema (struct with metadata) — extract field names by decoding.
    case Jason.decode(json) do
      {:ok, %{"fields" => fields}} when is_list(fields) ->
        names =
          for %{"name" => name} <- fields, is_binary(name) do
            name
          end

        if names == [] do
          column_names_for_tuple_data(data, nil)
        else
          {:ok, names}
        end

      _ ->
        column_names_for_tuple_data(data, nil)
    end
  end

  defp column_names_for_tuple_data(data, nil) do
    # PySpark assigns "_1", "_2", ... to tuple/list rows when no names are
    # supplied (python/pyspark/sql/types.py:_infer_schema).
    arity = data |> Enum.map(&tuple_size/1) |> Enum.max(fn -> 0 end)

    if arity == 0 do
      {:error,
       {:invalid_data,
        "cannot infer schema from empty tuples; provide a schema or non-empty rows"}}
    else
      names = Enum.map(1..arity, fn i -> "_#{i}" end)
      {:ok, names}
    end
  end

  defp tuple_to_named_map(tuple, names) when is_tuple(tuple) and is_list(names) do
    values = Tuple.to_list(tuple)

    if length(values) == length(names) do
      names
      |> Enum.zip(values)
      |> Map.new()
    else
      raise ArgumentError,
            "tuple arity #{length(values)} does not match #{length(names)} column names"
    end
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
      case safe_list_of_maps_to_explorer(data) do
        {:ok, explorer_df} ->
          prepare_local_data(explorer_df, Keyword.put(opts, :schema, schema_ddl))

        {:error, reason} ->
          {:error, {:data_conversion_error, reason}}
      end
    end
  end

  defp prepare_list_data_with_schema_json_relation(
         data,
         schema_ddl,
         non_string_map_fields,
         binary_fields
       ) do
    with {:ok, validated_schema} <- validate_schema_ddl_for_sql_relation(schema_ddl),
         {:ok, normalized_rows} <-
           normalize_rows_for_schema(
             data,
             Enum.map(non_string_map_fields, & &1.name),
             binary_fields
           ),
         {:ok, row_json} <- encode_rows_as_json(normalized_rows) do
      if non_string_map_fields == [] do
        query = json_rows_to_sql_query(row_json, validated_schema)
        {:ok, {:sql_relation, query, nil}}
      else
        helper_schema =
          helper_schema_for_non_string_map_fields(validated_schema, non_string_map_fields)

        with {:ok, validated_helper_schema} <-
               validate_schema_ddl_for_sql_relation(helper_schema) do
          query =
            json_rows_to_sql_query_with_projection(
              row_json,
              validated_helper_schema,
              projected_select_list_for_non_string_map_fields(
                validated_schema,
                non_string_map_fields
              )
            )

          {:ok, {:sql_relation, query, nil}}
        end
      end
    end
  end

  # Handles metadata-bearing struct schemas (json_schema 3-tuple form).
  # When normalize_local_relation_arrow? is true AND the Explorer.DataFrame has
  # complex dtypes, the SQL-JSON path must be used — it cannot carry field
  # metadata, but at least complex-type Arrow incompatibilities are avoided.
  # For simple-column frames (no complex dtypes), or when normalization is
  # disabled, the Arrow local-relation path is used with the JSON schema string
  # so that LocalRelation.schema preserves field metadata.
  defp prepare_list_data_with_json_schema(data, json_str, ddl_str, opts) do
    case safe_list_of_maps_to_explorer(data) do
      {:ok, explorer_df} ->
        if normalize_local_relation_arrow?(opts) and
             dataframe_contains_complex_dtype?(explorer_df) do
          # Complex-type columns: fall back to SQL-JSON path with DDL schema.
          # Field metadata is not preserved on this path.
          prepare_sql_json_relation(explorer_df, ddl_str)
        else
          # Simple columns: Arrow path with JSON schema preserves metadata.
          # Disable JSON-relation normalization so the binary json_str schema
          # is not embedded in a SQL `from_json(val, ...)` template.
          prepare_local_data(
            explorer_df,
            opts
            |> Keyword.put(:schema, json_str)
            |> Keyword.put(:normalize_local_relation_arrow, false)
          )
        end

      {:error, reason} ->
        {:error, {:data_conversion_error, reason}}
    end
  end

  defp prepare_list_data_with_schema_arrow_fallback(data, schema_ddl, opts) do
    case safe_list_of_maps_to_explorer(data) do
      {:ok, explorer_df} ->
        prepare_local_data(
          explorer_df,
          opts
          |> Keyword.put(:schema, schema_ddl)
          |> Keyword.put(:normalize_local_relation_arrow, false)
        )

      {:error, _} ->
        {:error, :arrow_fallback_failed}
    end
  end

  defp prepare_list_data_inferred(data, opts) do
    if Enum.empty?(data) do
      {:error, {:invalid_data, "cannot infer schema from empty list"}}
    else
      case safe_list_of_maps_to_explorer(data) do
        {:ok, explorer_df} ->
          prepare_local_data(explorer_df, opts)

        {:error, reason} ->
          prepare_list_data_inferred_fallback(data, reason, opts)
      end
    end
  end

  defp prepare_list_data_inferred_fallback(data, reason, opts) do
    if normalize_local_relation_arrow?(opts) do
      with {:ok, normalized_rows} <- normalize_rows_for_schema(data),
           {:ok, inferred_schema_ddl} <- infer_schema_ddl_from_rows(normalized_rows),
           {:ok, validated_schema} <-
             validate_schema_ddl_for_sql_relation(inferred_schema_ddl),
           {:ok, row_json} <- encode_rows_as_json(normalized_rows) do
        query = json_rows_to_sql_query(row_json, validated_schema)
        {:ok, {:sql_relation, query, nil}}
      else
        {:error, _} = error -> error
      end
    else
      {:error, {:data_conversion_error, reason}}
    end
  end

  defp safe_list_of_maps_to_explorer(data) do
    {:ok, list_of_maps_to_explorer(data)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp safe_explorer_new(data) do
    {:ok, Explorer.DataFrame.new(data)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp order_column_map(data) do
    pairs =
      Enum.map(data, fn
        {key, value} when is_binary(key) ->
          {key, value}

        {key, value} when is_atom(key) and not is_nil(key) and not is_boolean(key) ->
          {Atom.to_string(key), value}

        {key, _value} ->
          throw({:invalid_column_key, key})
      end)

    {:ok, Enum.sort_by(pairs, fn {key, _} -> key end)}
  catch
    {:invalid_column_key, key} ->
      {:error,
       {:invalid_data, "column-oriented data keys must be strings or atoms, got: #{inspect(key)}"}}
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
              normalize_row_field_value(
                value,
                key_string,
                non_string_map_fields_map,
                binary_fields_map
              )

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

  defp normalize_row_field_value(value, key_string, non_string_map_fields_map, binary_fields_map) do
    cond do
      Map.has_key?(non_string_map_fields_map, key_string) -> normalize_non_string_map_value(value)
      Map.has_key?(binary_fields_map, key_string) -> normalize_binary_field_value(value)
      true -> normalize_json_value(value)
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

          not MapSet.member?(binary_fields_set, key_string) and
            value_contains_null_byte_text?(value)
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
        {name, type} -> non_string_map_field_entry(name, type)
        :error -> []
      end
    end)
  end

  defp non_string_map_field_entry(name, type) do
    case parse_map_type(type) do
      {:ok, key_type, value_type} ->
        if String.upcase(String.trim(key_type)) == "STRING" do
          []
        else
          [%{name: name, key_type: String.trim(key_type), value_type: String.trim(value_type)}]
        end

      :error ->
        []
    end
  end

  defp helper_schema_for_non_string_map_fields(schema_ddl, non_string_map_fields) do
    replacements =
      Map.new(non_string_map_fields, fn %{name: name, key_type: key_type, value_type: value_type} ->
        {name, "ARRAY<STRUCT<key: #{key_type}, value: #{value_type}>>"}
      end)

    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.map_join(", ", fn field ->
      case parse_schema_field(field) do
        {name, _type} ->
          replacement = Map.get(replacements, name)
          "#{name} #{replacement || schema_field_type(field)}"

        :error ->
          field
      end
    end)
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
    case String.trim_leading(field) do
      <<"`", rest::binary>> -> parse_backtick_quoted_field(rest)
      other -> parse_unquoted_field(other)
    end
  end

  defp parse_backtick_quoted_field(after_open_backtick) do
    case consume_backtick_identifier(after_open_backtick, []) do
      {:ok, name, after_close} ->
        type = String.trim(after_close)
        if type == "", do: :error, else: {name, type}

      :error ->
        :error
    end
  end

  # Consumes a backtick-quoted identifier body, treating `` `` `` as
  # an escaped literal backtick. Returns the decoded name and the
  # remainder of the input after the closing backtick.
  defp consume_backtick_identifier(<<"``", rest::binary>>, acc),
    do: consume_backtick_identifier(rest, ["`" | acc])

  defp consume_backtick_identifier(<<"`", rest::binary>>, acc),
    do: {:ok, IO.iodata_to_binary(Enum.reverse(acc)), rest}

  defp consume_backtick_identifier(<<ch::utf8, rest::binary>>, acc),
    do: consume_backtick_identifier(rest, [<<ch::utf8>> | acc])

  defp consume_backtick_identifier(<<>>, _acc), do: :error

  defp parse_unquoted_field(field) do
    case Regex.run(~r/^(\S+)\s+(.+)$/s, field) do
      [_, name, type] -> {name, String.trim(type)}
      _ -> :error
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
        "<", {k, v, a, p, s} ->
          {append_part(k, v, s, "<"), v_if_needed(v, s, "<"), a + 1, p, s}

        ">", {k, v, a, p, s} ->
          {append_part(k, v, s, ">"), v_if_needed(v, s, ">"), max(a - 1, 0), p, s}

        "(", {k, v, a, p, s} ->
          {append_part(k, v, s, "("), v_if_needed(v, s, "("), a, p + 1, s}

        ")", {k, v, a, p, s} ->
          {append_part(k, v, s, ")"), v_if_needed(v, s, ")"), a, max(p - 1, 0), s}

        ",", {k, v, 0, 0, false} ->
          {k, v, 0, 0, true}

        ch, {k, v, a, p, s} ->
          {append_part(k, v, s, ch), v_if_needed(v, s, ch), a, p, s}
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
     Enum.map_join(fields, ", ", fn {name, type} -> "#{name} #{type_to_inferred_ddl(type)}" end)}
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
  # PySpark infers Python int as LongType regardless of the value's bit-width
  # (python/pyspark/sql/types.py:_infer_type). Match that for parity even
  # though Elixir integers can fit in narrower types.
  defp infer_single_type(v) when is_integer(v), do: :long
  defp infer_single_type(v) when is_float(v), do: :double

  # PySpark infers decimal.Decimal as DecimalType(38, 18) regardless of the
  # individual value's precision/scale (python/pyspark/sql/types.py:_type_mappings
  # and `_infer_type`). Use the same fixed shape so server-side schema
  # inference matches.
  defp infer_single_type(%Decimal{}), do: {:decimal, 38, 18}

  defp infer_single_type(%Date{}), do: :date
  defp infer_single_type(%DateTime{}), do: :timestamp
  defp infer_single_type(%NaiveDateTime{}), do: :timestamp_ntz
  defp infer_single_type(%Time{}), do: :time
  defp infer_single_type({:binary, v}) when is_binary(v), do: :binary
  defp infer_single_type(v) when is_binary(v), do: :string

  defp infer_single_type(v) when is_list(v) do
    {:array, infer_value_type(v)}
  end

  defp infer_single_type(v) when is_map(v) and not is_struct(v) do
    fields =
      Enum.map(v, fn {k, value} -> {to_string(k), infer_single_type(value)} end)

    {:struct, fields}
  end

  defp infer_single_type(_), do: :string

  defp merge_inferred_types(:null, type), do: type
  defp merge_inferred_types(type, :null), do: type
  defp merge_inferred_types(type, type), do: type
  defp merge_inferred_types(:long, :double), do: :double
  defp merge_inferred_types(:double, :long), do: :double

  defp merge_inferred_types(:long, {:decimal, p, s}),
    do: widen_decimal_for_long(p, s)

  defp merge_inferred_types({:decimal, p, s}, :long),
    do: widen_decimal_for_long(p, s)

  defp merge_inferred_types({:decimal, p1, s1}, {:decimal, p2, s2}) do
    scale = max(s1, s2)
    precision = max(p1 - s1, p2 - s2) + scale
    {:decimal, precision, scale}
  end

  defp merge_inferred_types({:array, a}, {:array, b}), do: {:array, merge_inferred_types(a, b)}

  defp merge_inferred_types({:struct, a}, {:struct, b}) do
    a_map = Map.new(a)
    b_map = Map.new(b)
    a_keys = Enum.map(a, fn {k, _} -> k end)
    a_key_set = MapSet.new(a_keys)
    extra_keys = for {k, _} <- b, not MapSet.member?(a_key_set, k), do: k
    ordered_keys = a_keys ++ extra_keys

    merged =
      Enum.map(ordered_keys, fn key ->
        {key, merge_inferred_types(Map.get(a_map, key, :null), Map.get(b_map, key, :null))}
      end)

    {:struct, merged}
  end

  defp merge_inferred_types(a, b) do
    raise ArgumentError,
          "heterogeneous inferred types in createDataFrame column: " <>
            "#{inspect(a)} vs #{inspect(b)}. Provide an explicit schema, " <>
            "wrap mixed values in compatible types, or split into separate columns."
  end

  defp widen_decimal_for_long(p, s) do
    # 19 digits cover the full LongType range (-9.22e18..9.22e18).
    {:decimal, max(p, 19 + s), s}
  end

  defp type_to_inferred_ddl(:null), do: "STRING"
  defp type_to_inferred_ddl(:boolean), do: "BOOLEAN"
  defp type_to_inferred_ddl(:long), do: "BIGINT"
  defp type_to_inferred_ddl(:double), do: "DOUBLE"
  defp type_to_inferred_ddl({:decimal, p, s}), do: "DECIMAL(#{p}, #{s})"
  defp type_to_inferred_ddl(:date), do: "DATE"
  defp type_to_inferred_ddl(:timestamp), do: "TIMESTAMP"
  defp type_to_inferred_ddl(:timestamp_ntz), do: "TIMESTAMP_NTZ"
  defp type_to_inferred_ddl(:time), do: "TIME"
  defp type_to_inferred_ddl(:string), do: "STRING"
  defp type_to_inferred_ddl(:binary), do: "BINARY"

  defp type_to_inferred_ddl({:array, element_type}) do
    "ARRAY<#{type_to_inferred_ddl(element_type)}>"
  end

  defp type_to_inferred_ddl({:struct, fields}) do
    inner =
      Enum.map_join(fields, ", ", fn {name, type} -> "#{name}: #{type_to_inferred_ddl(type)}" end)

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

  # Date / Time / Timestamp values are not JSON-encodable through Jason without
  # an Encoder protocol implementation. Spark accepts ISO-8601 strings for the
  # corresponding DATE / TIME / TIMESTAMP / TIMESTAMP_NTZ schema types when the
  # JSON local-relation path is used (mirrors PySpark's typed local-data path
  # which sends string-formatted date/time literals).
  defp normalize_json_value(%Date{} = v), do: Date.to_iso8601(v)
  defp normalize_json_value(%Time{} = v), do: Time.to_iso8601(v)
  defp normalize_json_value(%DateTime{} = v), do: DateTime.to_iso8601(v)
  defp normalize_json_value(%NaiveDateTime{} = v), do: NaiveDateTime.to_iso8601(v)

  # {:binary, data} is the tagged-tuple form used by callers who want binary
  # values to survive the JSON-relation path. Encode as base64 to match the
  # schema-aware normalize_binary_field_value path.
  defp normalize_json_value({:binary, v}) when is_binary(v), do: Base.encode64(v)

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

    with {:ok, validated_schema} <- validate_schema_ddl_for_sql_relation(schema_ddl),
         {:ok, row_json} <- encode_rows_as_json(rows) do
      query = json_rows_to_sql_query(row_json, validated_schema)
      {:ok, {:sql_relation, query, nil}}
    end
  end

  # Reject schema DDL strings that are syntactically malformed before
  # they are interpolated into the `from_json('<ddl>', …)` literal.
  # `sql_escape_string/1` handles `'` and `\` so the literal stays
  # well-formed, but a schema with unterminated quoting or a stray
  # comment/statement-terminator outside any quoted context indicates
  # a malformed (or attacker-influenced) input and should be surfaced
  # as a clean error instead of being passed through to the server.
  #
  # Validation is token-aware: `;`, `--`, `/*`, and `*/` are only
  # rejected when they appear *outside* a backtick-quoted identifier
  # or single-quoted string literal. That keeps legitimate Spark DDL
  # like `` `weird;name` STRING COMMENT 'has -- dashes' `` working
  # while still catching ` id INT; DROP TABLE x ` and friends.
  # Backslash escapes (`\\`, `\'`) inside single-quoted strings are
  # consumed as a single token so they do not skew quote balance.
  defp validate_schema_ddl_for_sql_relation(schema_ddl) when is_binary(schema_ddl) do
    case scan_schema_ddl(schema_ddl, :outside) do
      :ok -> {:ok, schema_ddl}
      {:error, reason} -> {:error, {:invalid_schema_ddl, reason}}
    end
  end

  defp validate_schema_ddl_for_sql_relation(other) do
    {:error, {:invalid_schema_ddl, "expected DDL string, got: #{inspect(other)}"}}
  end

  defp scan_schema_ddl(<<>>, :outside), do: :ok
  defp scan_schema_ddl(<<>>, :backtick), do: {:error, "unterminated backtick-quoted identifier"}
  defp scan_schema_ddl(<<>>, :single_quote), do: {:error, "unterminated single-quoted string"}

  defp scan_schema_ddl(<<";", _::binary>>, :outside), do: {:error, "schema contains ';'"}

  defp scan_schema_ddl(<<"--", _::binary>>, :outside),
    do: {:error, "schema contains '--' comment marker"}

  defp scan_schema_ddl(<<"/*", _::binary>>, :outside),
    do: {:error, "schema contains '/*' block comment marker"}

  defp scan_schema_ddl(<<"*/", _::binary>>, :outside),
    do: {:error, "schema contains '*/' block comment marker"}

  defp scan_schema_ddl(<<"`", rest::binary>>, :outside), do: scan_schema_ddl(rest, :backtick)
  defp scan_schema_ddl(<<"'", rest::binary>>, :outside), do: scan_schema_ddl(rest, :single_quote)
  defp scan_schema_ddl(<<_::utf8, rest::binary>>, :outside), do: scan_schema_ddl(rest, :outside)

  # Inside a backtick-quoted identifier `…`. Spark treats a doubled
  # backtick as an escaped backtick character.
  defp scan_schema_ddl(<<"``", rest::binary>>, :backtick), do: scan_schema_ddl(rest, :backtick)
  defp scan_schema_ddl(<<"`", rest::binary>>, :backtick), do: scan_schema_ddl(rest, :outside)
  defp scan_schema_ddl(<<_::utf8, rest::binary>>, :backtick), do: scan_schema_ddl(rest, :backtick)

  # Inside a single-quoted string '…'. Both `''` and the
  # `escapedStringLiterals=true` `\\` / `\'` forms must be consumed
  # as single tokens so they don't terminate the literal early or
  # confuse balance accounting.
  defp scan_schema_ddl(<<"\\\\", rest::binary>>, :single_quote),
    do: scan_schema_ddl(rest, :single_quote)

  defp scan_schema_ddl(<<"\\'", rest::binary>>, :single_quote),
    do: scan_schema_ddl(rest, :single_quote)

  defp scan_schema_ddl(<<"''", rest::binary>>, :single_quote),
    do: scan_schema_ddl(rest, :single_quote)

  defp scan_schema_ddl(<<"'", rest::binary>>, :single_quote), do: scan_schema_ddl(rest, :outside)

  defp scan_schema_ddl(<<_::utf8, rest::binary>>, :single_quote),
    do: scan_schema_ddl(rest, :single_quote)

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

  defp json_rows_to_sql_query([], schema_ddl) do
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

  defp json_rows_to_sql_query(json_rows, schema_ddl) when is_list(json_rows) do
    escaped_schema = sql_escape_string(schema_ddl)
    values_clause = json_rows_to_values_clause(json_rows)

    """
    SELECT parsed.*
    FROM (
      SELECT from_json(_spark_ex_json, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
      FROM VALUES #{values_clause} AS _spark_ex_input(_spark_ex_json)
    ) _spark_ex_parsed
    """
    |> String.trim()
  end

  defp json_rows_to_sql_query_with_projection([], schema_ddl, select_list) do
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

  defp json_rows_to_sql_query_with_projection(json_rows, schema_ddl, select_list)
       when is_list(json_rows) do
    escaped_schema = sql_escape_string(schema_ddl)
    values_clause = json_rows_to_values_clause(json_rows)

    """
    SELECT #{select_list}
    FROM (
      SELECT from_json(_spark_ex_json, '#{escaped_schema}', map('mode', 'FAILFAST')) AS parsed
      FROM VALUES #{values_clause} AS _spark_ex_input(_spark_ex_json)
    ) _spark_ex_parsed
    """
    |> String.trim()
  end

  defp json_rows_to_values_clause(json_rows) do
    Enum.map_join(json_rows, ", ", fn json -> "('" <> sql_escape_string(json) <> "')" end)
  end

  defp sql_escape_string(value) when is_binary(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
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

  defp stat_local_file(local_path) do
    case File.stat(local_path) do
      {:ok, %File.Stat{size: size, type: :regular}} -> {:ok, size}
      {:ok, %File.Stat{type: type}} -> {:error, {:invalid_local_file, local_path, type}}
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

  # Splits an Explorer.DataFrame into a list of independently-decodable Arrow
  # IPC streams whose serialized size is bounded (best-effort) by
  # `chunk_size_bytes`. Each chunk carries its own schema header so the server
  # can decode them independently and concatenate the resulting rows.
  #
  # Mirrors PySpark's `_chunk_local_relation` (session.py). Driving this from
  # the source DataFrame avoids a full Arrow IPC decode + re-encode round-trip
  # on the hot path for multi-hundred-MB / GB payloads — only the per-chunk
  # encodes happen, plus one small head-sample to estimate bytes/row.
  @doc false
  @spec split_explorer_dataframe_for_cache(Explorer.DataFrame.t(), pos_integer()) ::
          {:ok, [binary(), ...]} | {:error, term()}
  def split_explorer_dataframe_for_cache(%Explorer.DataFrame{} = df, chunk_size_bytes)
      when is_integer(chunk_size_bytes) and chunk_size_bytes > 0 do
    total_rows = Explorer.DataFrame.n_rows(df)

    if total_rows <= 1 do
      case dump_ipc_stream_safe(df) do
        {:ok, ipc} -> {:ok, [ipc]}
        {:error, _} = error -> error
      end
    else
      with {:ok, rows_per_chunk} <-
             estimate_rows_per_chunk(df, total_rows, chunk_size_bytes) do
        do_split_dataframe(df, total_rows, rows_per_chunk, 0, [])
      end
    end
  end

  # Public bytes-based variant for callers that only have the encoded IPC
  # payload. Wraps `Explorer.DataFrame.load_ipc_stream/1` in try/rescue
  # because that function can raise on malformed IPC; in that case (and on a
  # documented `{:error, _}`) we fall back to returning the input unchanged
  # so the caller still produces a valid (single-hash) plan.
  @doc false
  @spec split_arrow_ipc_for_cache(binary(), pos_integer()) :: [binary(), ...]
  def split_arrow_ipc_for_cache(arrow_ipc, chunk_size_bytes)
      when is_binary(arrow_ipc) and is_integer(chunk_size_bytes) and chunk_size_bytes > 0 do
    if byte_size(arrow_ipc) <= chunk_size_bytes do
      [arrow_ipc]
    else
      case load_ipc_stream_safe(arrow_ipc) do
        {:ok, df} ->
          case split_explorer_dataframe_for_cache(df, chunk_size_bytes) do
            {:ok, chunks} -> chunks
            {:error, _} -> [arrow_ipc]
          end

        {:error, _} ->
          [arrow_ipc]
      end
    end
  end

  defp load_ipc_stream_safe(arrow_ipc) do
    Explorer.DataFrame.load_ipc_stream(arrow_ipc)
  rescue
    error -> {:error, {:arrow_decode_error, error}}
  end

  defp dump_ipc_stream_safe(df) do
    case Explorer.DataFrame.dump_ipc_stream(df) do
      {:ok, _} = ok -> ok
      {:error, reason} -> {:error, {:arrow_encode_error, reason}}
    end
  rescue
    error -> {:error, {:arrow_encode_error, error}}
  end

  defp estimate_rows_per_chunk(df, total_rows, chunk_size_bytes) do
    sample_rows = min(total_rows, 256)
    sample_df = Explorer.DataFrame.head(df, sample_rows)

    case dump_ipc_stream_safe(sample_df) do
      {:ok, sample_ipc} ->
        bytes_per_row = max(1, div(byte_size(sample_ipc), max(1, sample_rows)))
        {:ok, max(1, div(chunk_size_bytes, bytes_per_row))}

      {:error, _} = error ->
        error
    end
  end

  defp do_split_dataframe(_df, total_rows, _rows_per_chunk, offset, acc)
       when offset >= total_rows do
    {:ok, Enum.reverse(acc)}
  end

  defp do_split_dataframe(df, total_rows, rows_per_chunk, offset, acc) do
    length = min(rows_per_chunk, total_rows - offset)
    slice = Explorer.DataFrame.slice(df, offset, length)

    case dump_ipc_stream_safe(slice) do
      {:ok, bytes} ->
        do_split_dataframe(df, total_rows, rows_per_chunk, offset + length, [bytes | acc])

      {:error, _} = error ->
        error
    end
  end

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

  defp resolve_session_identity(opts, connect_opts) do
    user_id =
      Keyword.get(opts, :user_id) || Map.get(connect_opts, :user_id) || default_user_id()

    client_type =
      cond do
        ct = Keyword.get(opts, :client_type) ->
          ct

        ua = Map.get(connect_opts, :user_agent) ->
          # Mirror PySpark: user-supplied base is followed by spark/<v> os/<system>
          # so server-side telemetry can identify protocol version + OS regardless
          # of the free-form prefix the user picked.
          ua <> " " <> client_type_suffix()

        true ->
          default_client_type()
      end

    session_id =
      Keyword.get(opts, :session_id) || Map.get(connect_opts, :session_id) || UUID.generate_v4()

    if UUID.valid_uuid?(session_id) do
      {:ok, %{user_id: user_id, client_type: client_type, session_id: session_id}}
    else
      {:error, {:invalid_session_id, session_id}}
    end
  end

  # PySpark uses, in order: SPARK_USER → OS user → "anonymous". We follow the
  # same precedence but fall back to "spark_ex" to preserve the historical
  # behavior when no env hints are available.
  defp default_user_id() do
    case nonempty(System.get_env("SPARK_USER")) do
      {:ok, value} ->
        value

      :empty ->
        case nonempty(System.get_env("USER")) do
          {:ok, value} -> value
          :empty -> "spark_ex"
        end
    end
  end

  defp nonempty(value) when is_binary(value) and value != "", do: {:ok, value}
  defp nonempty(_), do: :empty

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
