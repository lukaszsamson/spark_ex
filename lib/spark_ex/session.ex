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
    retry_policies: nil,
    local_relation_configs: nil
  ]

  # Server configs that drive `create_dataframe/3` (T-64), mirroring PySpark's
  # `SparkSession.createDataFrame` which reads them via `get_config_dict`.
  # They are fetched lazily on the first local-relation build and cached in
  # `:local_relation_configs`. Servers that predate a key (Spark 3.5 knows
  # only the cache threshold) report `nil` for it and the default applies.
  @local_relation_config_keys [
    {"spark.sql.session.localRelationCacheThreshold", :cache_threshold},
    {"spark.sql.session.localRelationChunkSizeRows", :chunk_size_rows},
    {"spark.sql.session.localRelationChunkSizeBytes", :chunk_size_bytes},
    {"spark.sql.session.localRelationBatchOfChunksSizeBytes", :batch_of_chunks_size_bytes},
    {"spark.sql.session.localRelationSizeLimit", :size_limit}
  ]

  # Fallbacks match the pre-T-64 client default for the threshold (4 MiB) and
  # Spark's SQLConf defaults for the chunking knobs.
  @local_relation_config_timeout 5_000
  @local_relation_config_retry_ms 60_000
  @local_relation_config_defaults %{
    cache_threshold: 4 * 1024 * 1024,
    chunk_size_rows: 10_000,
    chunk_size_bytes: 16 * 1024 * 1024,
    batch_of_chunks_size_bytes: 1024 * 1024 * 1024,
    size_limit: nil
  }

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
          local_relation_configs:
            %{atom() => non_neg_integer() | nil} | {:unavailable, integer()} | nil,
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
    Tag.validate!(tag)
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
  def is_stopped(%__MODULE__{released: released}), do: released

  def is_stopped(session) do
    case GenServer.whereis(session) do
      nil ->
        true

      server ->
        try do
          GenServer.call(server, :is_stopped)
        catch
          :exit, {:noproc, _} -> true
          :exit, {{:nodedown, _}, _} -> true
        end
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
  Like `execute_plan_reattachable_stream/3`, but applies the same Arrow
  preflight `execute_collect/3` uses (duplicate-column renaming and the
  JSON/STRING projection fallback).

  Returns `{:ok, stream, json_schema}` where `json_schema` is non-nil when the
  plan was rewritten to a JSON projection: rows decoded from the stream must
  then be passed through `__decode_json_projection_rows__/2`.
  """
  @spec execute_plan_reattachable_stream_safe(GenServer.server(), term(), keyword()) ::
          {:ok, Enumerable.t(), term() | nil} | {:error, term()}
  def execute_plan_reattachable_stream_safe(session, plan, opts \\ []) do
    if real_session_process?(session) do
      case GenServer.call(
             session,
             {:prepare_safe_execute_plan_stream, plan, opts},
             call_timeout(opts)
           ) do
        {:ok, stream_state, proto_plan, stream_opts, json_schema} ->
          case Client.execute_plan_reattachable_response_stream(
                 stream_state,
                 proto_plan,
                 stream_opts
               ) do
            {:ok, stream} -> {:ok, stream, json_schema}
            {:error, _} = error -> error
          end

        {:error, _} = error ->
          error
      end
    else
      with {:ok, stream} <- execute_plan_reattachable_stream(session, plan, opts) do
        {:ok, stream, nil}
      end
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

  ## Options

    * `:timeout` — gRPC call timeout in ms (default: 60_000)
    * `:tags` — request tags merged with the session tags
  """
  @spec execute_count(GenServer.server(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def execute_count(session, plan, opts \\ []) do
    GenServer.call(session, {:execute_count, plan, opts}, call_timeout(opts))
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
    # Validate before entering the GenServer: protobuf encoding of a
    # non-list / non-binary `names` would raise inside the shared Session
    # process and take it down (T-01). Mirrors Client.add_artifacts.
    if is_list(names) and Enum.all?(names, &is_binary/1) do
      GenServer.call(session, {:artifact_status, names})
    else
      {:error, {:invalid_artifact_names, names}}
    end
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

  Accepted row shapes: an `Explorer.DataFrame`, a list of maps, a list of
  keyword lists (treated like map rows), a list of tuples or lists
  (positional; requires a schema or column names), or a list of scalars
  (single-column rows).

  For small data (under the cache threshold), the data is embedded directly
  in the plan as a `LocalRelation`. For larger data, the Arrow IPC bytes
  are split into one or more chunks, each uploaded to the server via
  `AddArtifacts` and referenced together via `ChunkedCachedLocalRelation`
  (mirroring PySpark's `_chunk_local_relation`).

  ## Options

  - `:schema` — DDL schema string (e.g. `"id INT, name STRING"`). If omitted,
    inferred from the Explorer.DataFrame or from the data.
  - `:cache_threshold` — byte size at or above which data is cached on the
    server instead of inlined. Defaults to the server's
    `spark.sql.session.localRelationCacheThreshold` (read once per session;
    4 MiB when the server does not expose it).
  - `:cache_chunk_size` — maximum byte size of each Arrow IPC chunk uploaded as
    a separate cache artifact when the payload reaches `:cache_threshold`.
    Defaults to the server's `spark.sql.session.localRelationChunkSizeBytes`
    (16 MiB fallback), capped by
    `spark.sql.session.localRelationBatchOfChunksSizeBytes`.
  - `:cache_chunk_rows` — maximum number of rows per chunk. Defaults to the
    server's `spark.sql.session.localRelationChunkSizeRows` (10 000
    fallback). Chunks are bounded by rows *and* bytes, and uploaded in
    batches no larger than the batch-of-chunks config, mirroring PySpark's
    `_cache_local_relation`.

  ## Schema inference

  Without an explicit `:schema`, map / keyword-list rows infer their columns
  from the **sorted** union of the row keys (keys are stringified first),
  matching PySpark's `sorted(row.items())` dict inference. An explicit schema
  — DDL string, `SparkEx.Types` struct schema, or column-name list — always
  keeps its own order.

  A column-name list shorter than the row width is padded with `"_N"`
  (1-based, continuing after the supplied names), so
  `create_dataframe(s, [{1, 2}], schema: ["a"])` yields columns `a` and `_2`.
  A list longer than the row width is an error.

  If a column is `nil` in every row, inference fails with
  `{:error, {:cannot_determine_type, column_name}}` (PySpark's
  `CANNOT_DETERMINE_TYPE`). Pass an explicit `:schema` for such columns.

  `Decimal` values infer `DECIMAL(38,18)` like PySpark; values with more than
  20 integer or 18 fractional digits are rejected with
  `{:error, {:invalid_data, message}}` rather than rounded or nulled.

  ## Nullability

  `nullable: false` (and array `contains_null` / map `value_contains_null`
  set to `false`) in an explicit struct schema is enforced locally on list
  rows: a `nil` returns `{:error, {:invalid_data, message}}`. Arrow payloads
  are always nullable on the wire, so for non-empty data the server-side
  schema reports `nullable: true`; only the empty-list relation carries the
  exact flags. For `Explorer.DataFrame` input only top-level columns are
  checked.

  Duplicate column names are supported with a column-name list schema
  (`schema: ["x", "x"]`); with a DDL or struct schema they return
  `{:error, {:invalid_schema, message}}`.
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

  ## Options

    * `:timeout` — gRPC call timeout in ms (default: 60_000)
    * `:tags` — request tags merged with the session tags
  """
  @spec execute_show(GenServer.server(), term(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def execute_show(session, plan, opts \\ []) do
    GenServer.call(session, {:execute_show, plan, opts}, call_timeout(opts))
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
    interrupt(session, :all)
  end

  @doc """
  Interrupts operations matching the given tag.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_tag(GenServer.server(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def interrupt_tag(session, tag) when is_binary(tag) do
    interrupt(session, {:tag, tag})
  end

  @doc """
  Interrupts a specific operation by its ID.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_operation(GenServer.server(), String.t()) ::
          {:ok, [String.t()]} | {:error, term()}
  def interrupt_operation(session, operation_id) when is_binary(operation_id) do
    interrupt(session, {:operation_id, operation_id})
  end

  # Interrupt must not queue behind a running execute on the Session
  # GenServer (its whole purpose is to cancel one), so it runs from the
  # caller's process against the published connection snapshot. Sessions
  # without a snapshot (fake test sessions, released/closed sessions) fall
  # back to the GenServer call and get its lifecycle error replies.
  defp interrupt(session, type) do
    case SparkEx.Internal.SessionSnapshot.fetch(session) do
      {:ok, snapshot} ->
        case Client.interrupt(snapshot, type) do
          {:ok, interrupted_ids, server_side_session_id} ->
            observe_server_session_id(session, server_side_session_id)
            {:ok, interrupted_ids}

          {:error, _} = error ->
            # The snapshot path has no GenServer reply to route through
            # reply_error/2, so hand the error to the Session: a
            # session-changed signal closes it and drops the snapshot exactly
            # like an in-band RPC failure would.
            GenServer.cast(session, {:observe_rpc_error, error})
            error
        end

      :error ->
        GenServer.call(session, {:interrupt, type})
    end
  end

  defp observe_server_session_id(_session, nil), do: :ok
  defp observe_server_session_id(_session, ""), do: :ok

  # Routed through maybe_update_server_session/2 (not the test-only raw
  # setter) so the first id learned via an out-of-band interrupt republishes
  # the ETS snapshot and a rotated id closes the session.
  defp observe_server_session_id(session, id),
    do: GenServer.cast(session, {:observe_server_session_id, id})

  @doc """
  Stops the session process. Calls `ReleaseSession` if not already released,
  then disconnects the gRPC channel.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(session) do
    GenServer.stop(session)
  catch
    :exit, reason ->
      # The session may already be terminating: with trap_exit set (init/1),
      # OTP's parent-exit protocol gracefully stops the session when the
      # process that started it exits, so stop/1 can race that shutdown.
      # A graceful concurrent termination means the stop goal is met.
      if graceful_stop_exit?(reason), do: :ok, else: exit(reason)
  end

  defp graceful_stop_exit?({reason, {GenServer, :stop, _args}}), do: graceful_stop_exit?(reason)
  defp graceful_stop_exit?({reason, {:sys, :terminate, _args}}), do: graceful_stop_exit?(reason)
  defp graceful_stop_exit?(:noproc), do: true
  defp graceful_stop_exit?(:normal), do: true
  defp graceful_stop_exit?(:shutdown), do: true
  defp graceful_stop_exit?({:shutdown, _}), do: true
  defp graceful_stop_exit?(_), do: false

  # --- GenServer Callbacks ---

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 runs (server-side release_session + PlanIds
    # cleanup) even when a linked caller crashes, rather than the GenServer
    # dying silently and leaking the server-side session and atomics ref.
    Process.flag(:trap_exit, true)

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

      publish_connection_snapshot(state)

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

  # A closed session holds a stale server-side handle; cloning with it would
  # hit the server with an INVALID_HANDLE. Fail fast like the closed guard.
  def handle_call({:clone_session, _new_session_id}, _from, %{closed: true} = state) do
    {:reply, {:error, :session_closed}, state}
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
          preferred_arrow_chunk_size: state.preferred_arrow_chunk_size,
          retry_policies: state.retry_policies
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
      SparkEx.Internal.SessionSnapshot.delete(self())

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
              reply_collect_result(result, state, opts, proto_plan)

            {:no_fallback, state} ->
              execute_collect_no_fallback(state, plan, proto_plan, opts)
          end

        {nil, error} ->
          reply_error(error, state)
      end
    end)
  end

  def handle_call({:prepare_safe_execute_plan_stream, plan, opts}, _from, state) do
    {safe_plan, json_schema, state} = prepare_safe_collect_plan(state, plan)

    case safe_encode(safe_plan, state.plan_id_counter) do
      {{proto_plan, counter}, nil} ->
        state = %{state | plan_id_counter: counter}
        opts = merge_session_tags(opts, state.tags)
        {:reply, {:ok, state, proto_plan, opts, json_schema}, state}

      {nil, error} ->
        reply_error(error, state)
    end
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

      # Apply the same Arrow preflight collect uses: duplicate column names and
      # nested/unsupported types otherwise panic the Explorer decoder (T-33).
      {plan, _json_schema, state} = prepare_safe_collect_plan(state, plan, :explorer)

      # `:infinity` is a decoder-only option (DataFrame.to_explorer docs): it
      # must not be injected into the int32 LIMIT field. Any other
      # non-integer/negative value is rejected instead of crashing on encode.
      effective_plan =
        cond do
          max_rows != :infinity and not (is_integer(max_rows) and max_rows >= 0) ->
            {:error, {:invalid_option, {:max_rows, max_rows}}}

          unsafe or max_rows == :infinity ->
            {:ok, plan}

          true ->
            {:ok, {:limit, plan, max_rows}}
        end

      decoder_opts = opts

      case execute_explorer_encode(effective_plan, state.plan_id_counter) do
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

  def handle_call({:execute_count, plan}, from, state) do
    handle_call({:execute_count, plan, []}, from, state)
  end

  def handle_call({:execute_count, plan, opts}, _from, state) do
    operation_telemetry_span(:execute_count, state.session_id, fn ->
      case safe_encode_count(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}
          opts = merge_session_tags(opts, state.tags)

          case Client.execute_plan(state, proto_plan, opts) do
            {:ok, result} ->
              reply_execute_count_result(result, state)

            {:error, %SparkEx.Error.Remote{} = remote} = error ->
              reply_count_with_legacy_fallback(state, plan, opts, remote, error)

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

          {:error, %SparkEx.Error.Remote{} = remote} = error ->
            reply_analyze_schema_with_legacy_fallback(state, plan, remote, error)

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
        state =
          state
          |> maybe_update_server_session(server_side_session_id)
          |> invalidate_local_relation_configs()

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
        state =
          state
          |> maybe_update_server_session(server_side_session_id)
          |> invalidate_local_relation_configs()

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
    case safe_prepare_local_data(data, opts) do
      # Positional rows whose requested column names repeat were built under
      # unique internal names; restore the requested names with a final
      # `toDF` projection (PySpark: `_dedup_names` + `toDF(*names)`).
      {:ok, {:rename_columns, prepared, names}} ->
        case create_dataframe_from_prepared(prepared, opts, from, state) do
          {:reply, {:ok, df}, state} -> {:reply, {:ok, SparkEx.DataFrame.to_df(df, names)}, state}
          other -> other
        end

      {:ok, prepared} ->
        create_dataframe_from_prepared(prepared, opts, from, state)

      {:error, _} = error ->
        reply_error(error, state)
    end
  end

  def handle_call(
        {:create_dataframe_chunked_cache, source_df, schema_ddl, chunk_params},
        _from,
        state
      ) do
    %{
      chunk_size_bytes: chunk_size_bytes,
      chunk_size_rows: chunk_size_rows,
      batch_of_chunks_size_bytes: batch_bytes
    } = chunk_params

    with :ok <- validate_cache_configuration(state),
         {:ok, data_chunks} <-
           split_explorer_dataframe_for_cache(source_df, chunk_size_bytes, chunk_size_rows),
         :ok <-
           validate_local_relation_size(
             data_chunks,
             schema_ddl,
             Map.get(chunk_params, :size_limit)
           ) do
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

      case upload_missing_cache_artifacts(state, artifacts, batch_bytes) do
        {:ok, state} ->
          plan = {:chunked_cached_local_relation, data_hashes, schema_hash}
          df = SparkEx.DataFrame.new(self(), plan)
          {:reply, {:ok, df}, state}

        {:error, _reason} = error ->
          {:reply, error, state}
      end
    else
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

              SparkEx.Observation.store_observed_metrics(
                result.observed_metrics,
                state.session_id
              )

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

              SparkEx.Observation.store_observed_metrics(
                result.observed_metrics,
                state.session_id
              )

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

  def handle_call({:execute_show, plan}, from, state) do
    handle_call({:execute_show, plan, []}, from, state)
  end

  def handle_call({:execute_show, plan, opts}, _from, state) do
    operation_telemetry_span(:execute_show, state.session_id, fn ->
      case safe_encode(plan, state.plan_id_counter) do
        {{proto_plan, counter}, nil} ->
          state = %{state | plan_id_counter: counter}

          case Client.execute_plan(state, proto_plan, merge_session_tags(opts, state.tags)) do
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
    SparkEx.Observation.store_observed_metrics(result.observed_metrics, state.session_id)

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

        reply_collect_result(result, state, opts, proto_plan)

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
        # The retry renamed duplicate columns, so rows are keyed by the
        # deduped names: convert map columns against the renamed schema.
        reply_collect_result(result, state, opts, {:unique_columns, proto_plan})

      {:error, state} ->
        reply_error(error, state)
    end
  end

  defp execute_collect_retry_legacy(state, plan, opts, remote, error) do
    case retry_collect_with_legacy_fallbacks(state, plan, opts, remote) do
      {:ok, result, state} ->
        state = maybe_update_server_session(state, result.server_side_session_id)
        # The original plan is what the server rejected, so analyze the
        # rewritten plan that actually ran (tagged by execute_retry_plan/3).
        reply_collect_result(result, state, opts, Map.get(result, :executed_proto_plan))

      :error ->
        reply_error(error, state)

      {:error, {:unsupported_on_server, _, _}} = unsupported ->
        reply_error(unsupported, state)
    end
  end

  # `schema_source` tells the map_format: :map conversion which plan
  # describes the returned rows: a proto plan to analyze (the original plan
  # on the primary path; the rewritten plan on the legacy retry path),
  # `{:unique_columns, proto_plan}` when the retry renamed duplicate columns,
  # or nil to fall back to the response schema.
  defp reply_collect_result(result, state, opts, schema_source) do
    state = %{state | last_execution_metrics: result.execution_metrics}
    SparkEx.Observation.store_observed_metrics(result.observed_metrics, state.session_id)

    rows =
      case Keyword.get(opts, :map_format, :key_value_pairs) do
        :map ->
          # Analyze the plan for the logical schema (only when the caller
          # opted in): ExecutePlanResponse.schema is optional AND may
          # describe a JSON-projection fallback (map columns typed STRING)
          # rather than the logical types.
          schema = schema_for_map_format(state, schema_source) || result.schema
          SparkEx.Connect.ResultDecoder.convert_map_columns(result.rows, schema)

        _ ->
          result.rows
      end

    {:reply, {:ok, rows}, state}
  end

  defp schema_for_map_format(_state, nil), do: nil

  defp schema_for_map_format(state, {:unique_columns, proto_plan}) do
    state
    |> schema_for_map_format(proto_plan)
    |> __unique_columns_map_format_schema__()
  end

  defp schema_for_map_format(state, proto_plan) do
    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, _server_side_session_id} -> schema
      _ -> nil
    end
  end

  @doc false
  # Schema used to convert MAP columns after the unique-column-name retry:
  # converters are keyed by column name and the retried rows carry the
  # deduped names, so rename the fields positionally. Only the renamed
  # fields are returned: keeping the originals would bind a converter to a
  # duplicate name whose row value belongs to a differently-typed sibling.
  # Exposed for unit tests.
  @spec __unique_columns_map_format_schema__(term()) :: term()
  def __unique_columns_map_format_schema__(
        %Spark.Connect.DataType{kind: {:struct, struct}} = schema
      ) do
    case unique_schema_column_names(schema) do
      unique_names when is_list(unique_names) ->
        renamed =
          Enum.zip_with(struct.fields, unique_names, fn field, name -> %{field | name: name} end)

        %{schema | kind: {:struct, %{struct | fields: renamed}}}

      :no_duplicates ->
        schema
    end
  end

  def __unique_columns_map_format_schema__(other), do: other

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
    {:noreply, %{state | tags: Enum.uniq(state.tags ++ [tag])}}
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

  # Late casts from an in-flight out-of-band interrupt must not resurrect
  # the ETS snapshot of a closed/released session (the snapshot was deleted
  # on close; after release it would carry channel: nil).
  @impl true
  def handle_cast({:observe_server_session_id, _id}, %{closed: true} = state),
    do: {:noreply, state}

  def handle_cast({:observe_server_session_id, _id}, %{released: true} = state),
    do: {:noreply, state}

  def handle_cast({:observe_server_session_id, id}, state) do
    {:noreply, maybe_update_server_session(state, id)}
  end

  @impl true
  def handle_cast({:observe_rpc_error, _error}, %{released: true} = state),
    do: {:noreply, state}

  def handle_cast({:observe_rpc_error, error}, state) do
    {:noreply, maybe_close_on_error(state, error)}
  end

  # Silently discard gun messages that arrive after session release
  @impl true
  def handle_info({:gun_data, _, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_trailers, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_error, _, _, _}, state), do: {:noreply, state}
  def handle_info({:gun_down, _, _, _, _}, state), do: {:noreply, state}

  # With trap_exit set (see init/1), exit signals from linked processes arrive
  # as messages. A graceful exit (:normal/:shutdown) of a linked caller must
  # not tear the session down. An abnormal crash propagates: stop the session
  # so terminate/2 runs the server-side release + PlanIds cleanup.
  #
  # Note: exits of the session's PARENT (the process that called start_link)
  # never reach these clauses — gen_server's parent-exit protocol intercepts
  # them and terminates the session with the parent's reason (running
  # terminate/2), even for :normal/:shutdown. stop/1 tolerates racing that.
  def handle_info({:EXIT, _pid, reason}, state) when reason in [:normal, :shutdown] do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, {:shutdown, _}}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(msg, state) do
    require Logger

    Logger.error(
      "#{inspect(__MODULE__)} #{inspect(self())} received unexpected message in handle_info/2: #{inspect(msg)}"
    )

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{released: true} = state) do
    cleanup_session_resources(state)
    :ok
  end

  def terminate(_reason, %{channel: nil} = state) do
    cleanup_session_resources(state)
    :ok
  end

  def terminate(_reason, %{channel: channel} = state) do
    # Best-effort release before disconnect with timeout to prevent blocking
    task = Task.async(fn -> Client.release_session(state) end)
    Task.yield(task, 5_000) || Task.shutdown(task)
    safe_disconnect(channel)
    cleanup_session_resources(state)
    :ok
  end

  # Reclaim process/session-scoped resources on shutdown: the plan-id
  # allocator row (EtsTableOwner monitors as a backstop for abnormal exits)
  # and any observation rows this session accumulated (FABLE-29).
  defp cleanup_session_resources(state) do
    SparkEx.Internal.PlanIds.unregister_session(self())
    SparkEx.Internal.SessionSnapshot.delete(self())
    SparkEx.Observation.clear_session(Map.get(state, :session_id))
    :ok
  end

  @doc false
  # Test seam for the DDL top-level field splitter (T-10).
  def __split_top_level_schema_fields__(schema_ddl), do: split_top_level_schema_fields(schema_ddl)

  @doc false
  # Test seam for the parse-relation sibling walk (T-12): `rewriter` receives
  # each `{:parse, ...}` node and must return {rewritten, changed?}.
  def __rewrite_parse_walk__(term, rewriter) when is_function(rewriter, 1),
    do: rewrite_parse_deep({:test_rewriter, rewriter}, term)

  @doc false
  # Test seam for local-relation type inference (T-04).
  def __infer_value_type__(values), do: infer_value_type(values)

  @doc false
  # Test seam for local-relation schema preparation (T-47/T-48/T-49).
  def __prepare_local_data__(data, opts \\ []), do: prepare_local_data(data, opts)

  @doc false
  # Test seam for the inferred column order (T-47).
  def __collect_ordered_keys__(rows), do: collect_ordered_keys(rows)

  @doc false
  # Test seam for the all-null column check (T-48).
  def __check_determinable_columns__(rows), do: check_determinable_columns(rows)

  @doc false
  # Test seam for JSON local-relation row normalization (T-04).
  def __normalize_rows_for_schema__(rows), do: normalize_rows_for_schema(rows)

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

  # Shared Arrow preflight for the result paths that do not go through
  # `execute_collect`'s retry cascade (`to_explorer` / `to_local_iterator`).
  # Analyzes the plan schema and rewrites the plan the same way the collect
  # preflight does — dedupe duplicate column names, cast complex/unsupported
  # columns to JSON/STRING — so the Explorer decoder never sees a payload it
  # panics on (T-33).
  #
  # Returns `{plan_to_execute, json_schema_or_nil, state}`; `json_schema` is the
  # logical schema to decode a JSON-projected payload against (nil when no JSON
  # projection was applied).
  #
  # `mode` is `:rows` (collect/to_local_iterator: any schema the row decoder
  # cannot round-trip is JSON-projected and decoded back) or `:explorer`
  # (to_explorer: only the shapes that make the Explorer decoder panic —
  # nested maps and unsupported scalars — are projected; top-level struct/map/
  # array columns stay native containers).
  defp prepare_safe_collect_plan(state, plan, mode \\ :rows) do
    with true <- maybe_preflight_collect_retry?(plan),
         {{proto_plan, counter}, nil} <- safe_encode(plan, state.plan_id_counter),
         state = %{state | plan_id_counter: counter},
         {:ok, schema, server_side_session_id} <- Client.analyze_schema(state, proto_plan) do
      state = maybe_update_server_session(state, server_side_session_id)
      safe_collect_plan_for_schema(state, plan, schema, mode)
    else
      _ -> {plan, nil, state}
    end
  end

  defp safe_collect_plan_for_schema(state, plan, schema, mode) do
    case unique_schema_column_names(schema) do
      unique_names when is_list(unique_names) ->
        renamed_schema = __unique_columns_map_format_schema__(schema)

        {safe_plan, json_schema} =
          maybe_json_projection_plan({:to_df, plan, unique_names}, renamed_schema, mode)

        {safe_plan, json_schema, state}

      _ ->
        {safe_plan, json_schema} = maybe_json_projection_plan(plan, schema, mode)
        {safe_plan, json_schema, state}
    end
  end

  defp maybe_json_projection_plan(plan, schema, mode) do
    if json_projection_needed?(schema, mode) do
      case json_fallback_projection_plan(plan, schema) do
        nil -> {plan, nil}
        projected -> {projected, schema}
      end
    else
      {plan, nil}
    end
  end

  defp json_projection_needed?(schema, :rows) do
    schema_has_nested_map?(schema) or schema_has_struct_and_map?(schema) or
      schema_has_unsupported_scalar?(schema)
  end

  defp json_projection_needed?(schema, :explorer) do
    schema_has_nested_map?(schema) or schema_has_unsupported_scalar?(schema)
  end

  @doc false
  # Decodes rows produced by the JSON-projection fallback against the logical
  # schema. Exposed so streaming consumers (`DataFrame.to_local_iterator/2`)
  # can decode batches as they arrive.
  @spec __decode_json_projection_rows__([map()], term()) :: [map()]
  def __decode_json_projection_rows__(rows, schema),
    do: decode_rows_from_json_projection(rows, schema)

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
    if schema_has_nested_map?(schema) or schema_has_struct_and_map?(schema) or
         schema_has_unsupported_scalar?(schema) do
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
    maybe_json_relation_plan?(plan) or maybe_sql_plan?(plan) or maybe_read_plan?(plan)
  end

  # Reads (parquet/orc/datasource, named tables) can surface nested struct/map/
  # array or interval columns that make the Explorer/polars decoder panic in
  # native code (e.g. the "maximum length reached" / bigidx panic on a nested
  # parquet roundtrip — V02_BLOCKERS M2). Allowing these plans through the
  # preflight schema analysis lets the JSON/string projection fallback apply
  # before the panicking Arrow decode is attempted.
  defp maybe_read_plan?({:read_data_source, _format, _paths, _schema, _options}), do: true

  defp maybe_read_plan?({:read_data_source, _format, _paths, _schema, _options, _predicates}),
    do: true

  defp maybe_read_plan?({:read_named_table, _table, _options}), do: true

  defp maybe_read_plan?(plan) when is_tuple(plan) do
    plan
    |> Tuple.to_list()
    |> Enum.any?(&maybe_read_plan?/1)
  end

  defp maybe_read_plan?(plan) when is_list(plan), do: Enum.any?(plan, &maybe_read_plan?/1)

  defp maybe_read_plan?(_), do: false

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

        {retry_plan, retry_proto_plan, state} =
          preflight_retry_plan(state, retry_plan, retry_proto_plan)

        case Client.execute_plan(state, retry_proto_plan, opts) do
          {:ok, result} ->
            {:ok, Map.put(result, :executed_proto_plan, retry_proto_plan), state}

          {:error, {:arrow_decode_failed, _reason}} ->
            # Outermost rewrite wins: nested fallbacks (unique columns /
            # JSON projection) may not describe the logical row types.
            with {:ok, result, state} <-
                   retry_collect_with_unique_columns(state, retry_plan, retry_proto_plan, opts) do
              {:ok, Map.put(result, :executed_proto_plan, {:unique_columns, retry_proto_plan}),
               state}
            end

          {:error, _} ->
            {:error, state}
        end

      _ ->
        {:error, state}
    end
  end

  # A rewritten fallback plan can legitimately produce duplicate column names
  # (e.g. a Spark 3.5 lateral join downgraded to a cross join between two
  # relations that both expose `id`). Decoding such a payload panics inside
  # the Explorer/polars NIF before the unique-columns retry can recover, so
  # rename duplicates up front, exactly like the primary collect preflight.
  # Any analysis failure leaves the plan untouched; the retry then surfaces
  # the server's own error.
  defp preflight_retry_plan(state, retry_plan, retry_proto_plan) do
    case safe_analyze_schema(state, retry_proto_plan) do
      {:ok, schema, state} ->
        case unique_schema_column_names(schema) do
          unique_names when is_list(unique_names) ->
            renamed = {:to_df, retry_plan, unique_names}

            case safe_encode(renamed, state.plan_id_counter) do
              {{renamed_proto, counter}, nil} ->
                {renamed, renamed_proto, %{state | plan_id_counter: counter}}

              _ ->
                {retry_plan, retry_proto_plan, state}
            end

          _ ->
            {retry_plan, retry_proto_plan, state}
        end

      :error ->
        {retry_plan, retry_proto_plan, state}
    end
  end

  defp safe_analyze_schema(state, proto_plan) do
    case Client.analyze_schema(state, proto_plan) do
      {:ok, schema, server_side_session_id} ->
        {:ok, schema, maybe_update_server_session(state, server_side_session_id)}

      _ ->
        :error
    end
  end

  # analyze_schema shares the UNPARSED legacy fallback with collect/count so
  # DataFrame.schema/dtypes work on {:parse, ...} plans with string schemas on
  # Spark 4.x (which rejects the Unparsed DataType), and the empty-relation
  # fallback so Spark 3.5 can describe plans containing 4.x-only relations
  # (T-34).
  defp reply_analyze_schema_with_legacy_fallback(state, plan, remote, error) do
    with {:ok, rewritten} <- rewrite_plan_for_legacy_strategy(state, plan, remote),
         {{proto_plan, counter}, nil} <- safe_encode(rewritten, state.plan_id_counter),
         state = %{state | plan_id_counter: counter},
         {:ok, schema, server_side_session_id} <- Client.analyze_schema(state, proto_plan) do
      {:reply, {:ok, schema}, maybe_update_server_session(state, server_side_session_id)}
    else
      {:error, {:unsupported_on_server, _, _}} = unsupported -> reply_error(unsupported, state)
      _ -> reply_error(error, state)
    end
  end

  defp reply_count_with_legacy_fallback(state, plan, opts, remote, error) do
    case retry_count_with_legacy_parse_rewrite(state, plan, opts, remote) do
      {:ok, result, state} -> reply_execute_count_result(result, state)
      {:error, {:unsupported_on_server, _, _}} = unsupported -> reply_error(unsupported, state)
      :error -> reply_error(error, state)
    end
  end

  # count/1 shares the UNPARSED legacy fallback with collect: Spark 4.x
  # rejects the Unparsed schema on {:parse, ...} plans, so rewrite the parse
  # node into from_csv/from_json projections and retry the count. The same
  # path handles Spark 3.5's empty-relation rejection of 4.x-only relation
  # types anywhere in the tree (T-34).
  defp retry_count_with_legacy_parse_rewrite(state, plan, opts, remote) do
    with {:ok, rewritten} <- rewrite_plan_for_legacy_strategy(state, plan, remote),
         {{proto_plan, counter}, nil} <- safe_encode_count(rewritten, state.plan_id_counter),
         {:ok, result} <-
           Client.execute_plan(%{state | plan_id_counter: counter}, proto_plan, opts) do
      {:ok, result, %{state | plan_id_counter: counter}}
    else
      {:error, {:unsupported_on_server, _, _}} = unsupported -> unsupported
      _ -> :error
    end
  end

  # Shared by the count and analyze_schema fallbacks: pick the plan rewrite
  # matching the server's rejection. Collect has its own dispatcher because it
  # also handles grouping sets and subquery rewrites.
  defp rewrite_plan_for_legacy_strategy(state, plan, remote) do
    case classify_legacy_recovery_strategy(remote) do
      :legacy_unparsed -> rewrite_parse_collect_plan(state, plan)
      :empty_relation -> rewrite_empty_relation_collect_plan(plan)
      _ -> :error
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
      &rewrite_empty_relation_collect_plan/1,
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
        {:error, {:unsupported_on_server, _, _}} = unsupported -> {:halt, unsupported}
        _ -> {:cont, :error}
      end
    end)
  end

  # Spark 3.5 servers reject relation types added in 4.x (Transpose,
  # LateralJoin, AsOfJoin, TableValuedFunction with certain args) with
  # "Expected Relation to be set, but is empty." — the unknown proto field is
  # dropped and the parent sees an empty Relation. That can happen at any
  # depth (`df |> lateral_join(...) |> select(...)`), so walk the whole plan
  # tree (T-34) — modelled on rewrite_parse_deep/2 — and rewrite every such
  # node. DataFrame plans carry a {:plan_id, n, inner} envelope which is kept
  # in place; the node rewriters below match the bare plan shapes. Children
  # are rewritten before the node itself so nested unsupported relations
  # (a transpose feeding a lateral join) are handled in one pass. Returns
  # :error when nothing in the tree needed rewriting, so the caller can move
  # on to the next strategy.
  defp rewrite_empty_relation_collect_plan(plan) do
    if plan_contains_node?(plan, :as_of_join, 11) do
      # An as-of join cannot be expressed as a plain join without losing its
      # time-matching semantics (as-of columns, tolerance, direction, exact
      # matches), so refuse instead of silently returning different rows.
      {:error,
       {:unsupported_on_server, :as_of_join,
        "as-of joins require a Spark 4.0+ Connect server; this server rejected the " <>
          "AsOfJoin relation and no semantics-preserving fallback exists"}}
    else
      case rewrite_empty_relation_deep(plan) do
        {rewritten, true} -> {:ok, rewritten}
        {_plan, false} -> :error
      end
    end
  end

  defp plan_contains_node?(term, tag, size) when is_tuple(term) do
    (tuple_size(term) == size and elem(term, 0) == tag) or
      Enum.any?(Tuple.to_list(term), &plan_contains_node?(&1, tag, size))
  end

  defp plan_contains_node?(term, tag, size) when is_list(term),
    do: Enum.any?(term, &plan_contains_node?(&1, tag, size))

  defp plan_contains_node?(_term, _tag, _size), do: false

  @doc false
  def __rewrite_empty_relation_deep__(plan), do: rewrite_empty_relation_deep(plan)

  @doc false
  def __rewrite_empty_relation_collect_plan__(plan), do: rewrite_empty_relation_collect_plan(plan)

  defp rewrite_empty_relation_deep(term) when is_tuple(term) do
    {elements, children_changed?} = rewrite_empty_relation_walk_list(Tuple.to_list(term))
    node = List.to_tuple(elements)

    case rewrite_empty_relation_node(node) do
      {:ok, rewritten} -> {rewritten, true}
      :error -> {node, children_changed?}
    end
  end

  defp rewrite_empty_relation_deep(term) when is_list(term),
    do: rewrite_empty_relation_walk_list(term)

  defp rewrite_empty_relation_deep(term), do: {term, false}

  defp rewrite_empty_relation_walk_list(elements) do
    Enum.map_reduce(elements, false, fn element, changed? ->
      {rewritten, element_changed?} = rewrite_empty_relation_deep(element)
      {rewritten, changed? or element_changed?}
    end)
  end

  defp rewrite_empty_relation_node({:transpose, _, _} = node),
    do: rewrite_transpose_node(node)

  defp rewrite_empty_relation_node({:table_valued_function, _, _} = node),
    do: rewrite_table_function_node(node)

  defp rewrite_empty_relation_node({:lateral_join, _, _, _, _} = node),
    do: rewrite_lateral_join_node(node)

  defp rewrite_empty_relation_node(_node), do: :error

  defp rewrite_transpose_node({:transpose, child_plan, index_columns}),
    do: transpose_emulation_plan(child_plan, index_columns)

  defp rewrite_table_function_node({:table_valued_function, function_name, arg_exprs})
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

  defp rewrite_table_function_node(_plan), do: :error

  # Spark 3.5 servers don't know the LateralJoin relation ("Expected Relation
  # to be set, but is empty."). Downgrade to a regular join — correct only for
  # correlation-free right sides, which is all a 3.5 server can express anyway.
  defp rewrite_lateral_join_node({:lateral_join, left_plan, right_plan, cond_expr, type}) do
    {:ok, {:join, left_plan, right_plan, cond_expr, type, []}}
  end

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

  defp rewrite_grouping_sets_collect_plan({:plan_id, _id, inner}),
    do: rewrite_grouping_sets_collect_plan(inner)

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

  # Rewrites {:parse, ...} nodes anywhere in the plan tree (the parse
  # relation may sit under filters/aggregates/joins, and every node is
  # wrapped in a {:plan_id, n, inner} envelope). Returns :error when no
  # parse node could be rewritten.
  defp rewrite_parse_collect_plan(state, plan) do
    case rewrite_parse_deep(state, plan) do
      {rewritten, true} -> {:ok, rewritten}
      {_plan, false} -> :error
    end
  end

  defp rewrite_parse_deep({:test_rewriter, rewriter}, {:parse, _, _, _, _} = node),
    do: rewriter.(node)

  defp rewrite_parse_deep(state, {:parse, child_plan, format, schema, options} = node)
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

      {{:project, parsed_plan, projected_fields}, true}
    else
      _ -> rewrite_parse_walk(state, node)
    end
  end

  defp rewrite_parse_deep(state, term) when is_tuple(term) or is_list(term),
    do: rewrite_parse_walk(state, term)

  defp rewrite_parse_deep(_state, term), do: {term, false}

  defp rewrite_parse_walk(state, term) when is_tuple(term) do
    {elements, changed?} = rewrite_parse_walk_list(state, Tuple.to_list(term))
    {List.to_tuple(elements), changed?}
  end

  defp rewrite_parse_walk(state, term) when is_list(term),
    do: rewrite_parse_walk_list(state, term)

  # Visit every sibling and OR the changed flags (T-12): a union/join of two
  # parse relations must have both branches rewritten, not just the first.
  defp rewrite_parse_walk_list(state, elements) do
    Enum.map_reduce(elements, false, fn element, changed? ->
      {rewritten, element_changed?} = rewrite_parse_deep(state, element)
      {rewritten, changed? or element_changed?}
    end)
  end

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

  # True when any top-level result column is an Arrow type the Explorer/polars
  # decoder cannot build a series for (year-month / day-time / calendar
  # intervals). Such columns make `Explorer.DataFrame.load_ipc_stream/1` panic in
  # native code; detecting them up front lets the collect path pre-emptively cast
  # them to STRING via the JSON/string projection fallback, so the panicking
  # decode is never attempted (V02_BLOCKERS M1).
  defp schema_has_unsupported_scalar?(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    Enum.any?(struct.fields, fn field -> unsupported_arrow_scalar_type?(field.data_type) end)
  end

  defp schema_has_unsupported_scalar?(_), do: false

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
    # `floats: :decimals` keeps DECIMAL digits exact (Spark's to_json emits
    # decimals as unquoted numbers); DOUBLE/FLOAT leaves are converted back.
    case Jason.decode(value, floats: :decimals) do
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

  # Scalar leaves inside a `to_json` fallback payload arrive as JSON scalars
  # (strings/numbers). The Arrow path decodes the same values as Decimal /
  # DateTime / NaiveDateTime / Date / raw binary, so mirror that here, guarded
  # by the schema type so plain STRING columns are never mangled (T-32).
  defp coerce_complex_decoded_value(%Decimal{} = value, %Spark.Connect.DataType{kind: {kind, _}})
       when kind in [:double, :float],
       do: Decimal.to_float(value)

  # Spark's `to_json` writes non-finite doubles as the strings "NaN",
  # "Infinity" and "-Infinity"; the Arrow path decodes the same values as
  # Explorer's sentinel atoms, so mirror that (STRING columns are untouched).
  defp coerce_complex_decoded_value("NaN", %Spark.Connect.DataType{kind: {kind, _}})
       when kind in [:double, :float],
       do: :nan

  defp coerce_complex_decoded_value("Infinity", %Spark.Connect.DataType{kind: {kind, _}})
       when kind in [:double, :float],
       do: :infinity

  defp coerce_complex_decoded_value("-Infinity", %Spark.Connect.DataType{kind: {kind, _}})
       when kind in [:double, :float],
       do: :neg_infinity

  defp coerce_complex_decoded_value(value, %Spark.Connect.DataType{kind: {:decimal, _}}) do
    coerce_json_decimal(value)
  end

  defp coerce_complex_decoded_value(value, %Spark.Connect.DataType{kind: {:timestamp, _}})
       when is_binary(value) do
    coerce_json_timestamp(value)
  end

  defp coerce_complex_decoded_value(value, %Spark.Connect.DataType{kind: {:timestamp_ntz, _}})
       when is_binary(value) do
    coerce_json_timestamp_ntz(value)
  end

  defp coerce_complex_decoded_value(value, %Spark.Connect.DataType{kind: {:date, _}})
       when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> value
    end
  end

  defp coerce_complex_decoded_value(value, %Spark.Connect.DataType{kind: {:binary, _}})
       when is_binary(value) do
    case Base.decode64(value) do
      {:ok, decoded} -> decoded
      :error -> value
    end
  end

  defp coerce_complex_decoded_value(value, _data_type), do: value

  # Spark's `to_json` renders DECIMAL as an unquoted JSON number; Jason decodes
  # that as integer/float. Strings are also accepted (some Spark configs quote
  # decimals).
  defp coerce_json_decimal(%Decimal{} = value), do: value
  defp coerce_json_decimal(value) when is_integer(value), do: Decimal.new(value)
  defp coerce_json_decimal(value) when is_float(value), do: Decimal.from_float(value)

  defp coerce_json_decimal(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> value
    end
  end

  defp coerce_json_decimal(value), do: value

  # TIMESTAMP renders as an ISO-8601 instant (millisecond precision, with the
  # session time zone offset). Spark's to_json truncates to milliseconds, so
  # JSON-projected timestamps lose sub-millisecond digits the Arrow path keeps.
  defp coerce_json_timestamp(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        datetime

      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive} -> DateTime.from_naive!(naive, "Etc/UTC")
          _ -> value
        end
    end
  end

  # TIMESTAMP_NTZ renders without an offset; the Arrow path yields a
  # NaiveDateTime.
  defp coerce_json_timestamp_ntz(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, naive} ->
        naive

      _ ->
        case DateTime.from_iso8601(value) do
          {:ok, datetime, _offset} -> DateTime.to_naive(datetime)
          _ -> value
        end
    end
  end

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

  # Scalar Arrow types the Explorer/polars decoder cannot build a series for, so
  # collect must cast them to STRING via the fallback projection. Day-time
  # intervals are intentionally NOT listed: Explorer maps them to a native
  # {:duration, :microsecond} series (SparkEx.Connect.TypeMapper.to_explorer_dtype/1),
  # so casting them to STRING would needlessly turn durations into strings. Only
  # year-month and calendar (month-day-nano) intervals lack native support and
  # actually panic the NIF.
  defp unsupported_arrow_scalar_type?(%Spark.Connect.DataType{kind: {tag, _}}),
    do: tag in [:year_month_interval, :calendar_interval]

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

  # T-02: the explorer path validates `:max_rows` before encoding; a
  # validation error skips encoding and is reported like an encode failure.
  defp execute_explorer_encode({:ok, plan}, counter), do: safe_encode(plan, counter)
  defp execute_explorer_encode({:error, _} = error, _counter), do: {nil, error}

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

  # Publishes the connection fields Interrupt needs so it can run from the
  # caller's process while this GenServer is busy (see
  # SparkEx.Internal.SessionSnapshot). Removed on release/close/terminate so
  # the fast path falls back to the GenServer's lifecycle error replies.
  defp publish_connection_snapshot(state) do
    SparkEx.Internal.SessionSnapshot.put(self(), %{
      channel: state.channel,
      session_id: state.session_id,
      server_side_session_id: state.server_side_session_id,
      user_id: state.user_id,
      client_type: state.client_type,
      retry_policies: state.retry_policies
    })

    state
  end

  defp maybe_update_server_session(state, nil), do: state
  defp maybe_update_server_session(state, ""), do: state

  defp maybe_update_server_session(state, id) do
    case SparkEx.Connect.SessionIntegrity.validate_server_session_id(
           id,
           state.server_side_session_id
         ) do
      {:ok, ^id} when state.server_side_session_id == id ->
        state

      {:ok, ^id} ->
        publish_connection_snapshot(%{state | server_side_session_id: id})

      {:ok, current} ->
        %{state | server_side_session_id: current}

      {:error, {:server_session_changed, ctx}} ->
        Logger.warning(
          "spark_ex session #{state.session_id} closed: server-side session id changed " <>
            "(pinned=#{ctx.pinned}, got=#{ctx.got})"
        )

        SparkEx.Internal.SessionSnapshot.delete(self())
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

      SparkEx.Internal.SessionSnapshot.delete(self())
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

  # Non-blocking probe: a real Session publishes a connection snapshot, and
  # its process dictionary carries `$initial_call` = {__MODULE__, :init, 1}.
  # Never touches the GenServer mailbox (a `:sys.get_state/1` here would
  # queue behind a running execute and defeat the out-of-band stream setup).
  defp real_session_process?(session), do: __real_session_process__?(session)

  @doc false
  @spec __real_session_process__?(GenServer.server()) :: boolean()
  def __real_session_process__?(session) do
    case SparkEx.Internal.SessionSnapshot.fetch(session) do
      {:ok, _snapshot} ->
        true

      :error ->
        case GenServer.whereis(session) do
          pid when is_pid(pid) and node(pid) == node() ->
            case Process.info(pid, :dictionary) do
              {:dictionary, dict} -> Keyword.get(dict, :"$initial_call") == {__MODULE__, :init, 1}
              nil -> false
            end

          _ ->
            false
        end
    end
  end

  # --- Local data preparation ---

  # Wraps prepare_local_data so malformed local input (e.g. mismatched tuple
  # arities or heterogeneous inferred column types) returns {:error, ...}
  # rather than raising inside the GenServer and tearing the session down.
  # Mirrors the safe_encode/2 rescue convention used elsewhere in this module.
  defp safe_prepare_local_data(data, opts) do
    prepare_local_data(data, opts)
  rescue
    e in [ArgumentError, FunctionClauseError, ArithmeticError] ->
      {:error, {:invalid_local_data, Exception.message(e)}}
  end

  # Empty typed relation: no Arrow payload, the schema alone defines the
  # columns (PySpark's `LocalRelation(table=None, schema=schema.json())`).
  defp create_dataframe_from_prepared(
         {:local_relation, nil, schema, _source_df},
         _opts,
         _from,
         state
       ) do
    df = SparkEx.DataFrame.new(self(), {:local_relation, nil, schema})
    {:reply, {:ok, df}, state}
  end

  defp create_dataframe_from_prepared(
         {:local_relation, arrow_ipc, schema_ddl, source_df},
         opts,
         from,
         state
       ) do
    {configs, state} = ensure_local_relation_configs(state, opts)

    case local_relation_chunk_params(opts, configs) do
      {:error, _} = error ->
        {:reply, error, state}

      # PySpark: `if cache_threshold <= table.nbytes: cache`, so a payload
      # exactly at the threshold is cached, not inlined.
      {:ok, %{cache_threshold: cache_threshold}} when byte_size(arrow_ipc) < cache_threshold ->
        plan = {:local_relation, arrow_ipc, schema_ddl}
        df = SparkEx.DataFrame.new(self(), plan)
        {:reply, {:ok, df}, state}

      {:ok, chunk_params} ->
        # Drop arrow_ipc from this scope before re-encoding per chunk so
        # peak memory stays close to the size of the source DataFrame
        # rather than 2x that for very large payloads.
        handle_call(
          {:create_dataframe_chunked_cache, source_df, schema_ddl, chunk_params},
          from,
          state
        )
    end
  end

  defp create_dataframe_from_prepared({:sql_relation, query, args}, _opts, _from, state) do
    df = SparkEx.DataFrame.new(self(), {:sql, query, args})
    {:reply, {:ok, df}, state}
  end

  defp prepare_local_data(data, opts) when is_struct(data, Explorer.DataFrame) do
    with {:ok, normalized_schema} <- normalize_create_dataframe_schema(opts),
         :ok <- validate_non_null_constraints(data, Keyword.get(opts, :schema)),
         {:ok, data, schema} <- apply_schema_to_explorer(data, normalized_schema) do
      # Metadata-bearing struct schemas keep their JSON form on the Arrow
      # path so field metadata round-trips into the local relation (mirrors
      # prepare_list_data_with_json_schema); the SQL-JSON complex-dtype
      # fallback needs the DDL form (metadata not preserved there, same
      # documented limitation as the list path). Nullability is validated
      # locally and relaxed in the transmitted schema: every Arrow field
      # Explorer writes is nullable, and the server rejects a nullable column
      # for a non-nullable field (NULLABLE_COLUMN_OR_FIELD).
      {arrow_schema, sql_schema} =
        case schema do
          {:json_schema, json, ddl} ->
            {relax_json_schema_nullability(json), ddl}

          nil ->
            ddl = explorer_to_ddl(data)
            {ddl, ddl}

          ddl when is_binary(ddl) ->
            {ddl, ddl}
        end

      if normalize_local_relation_arrow?(opts) and dataframe_contains_complex_dtype?(data) do
        prepare_sql_json_relation(data, sql_schema)
      else
        case Explorer.DataFrame.dump_ipc_stream(data) do
          # Pass the source Explorer.DataFrame alongside the IPC bytes so the
          # chunked-cache path can re-slice it natively without a full IPC
          # decode round-trip on multi-hundred-MB payloads.
          {:ok, ipc_bytes} -> {:ok, {:local_relation, ipc_bytes, arrow_schema, data}}
          {:error, reason} -> {:error, {:arrow_encode_error, reason}}
        end
      end
    end
  end

  defp prepare_local_data(data, opts) when is_list(data) do
    struct_schema = Keyword.get(opts, :schema)

    with {:ok, schema} <- normalize_create_dataframe_schema(opts),
         :ok <- validate_non_null_constraints(data, struct_schema),
         {:ok, normalized_data, normalized_schema, restore_names} <-
           normalize_list_data_and_schema(data, schema) do
      result =
        cond do
          # An empty list with an explicit struct schema is an empty typed
          # relation (PySpark: `LocalRelation(table=None, schema=schema.json())`)
          # so nullability, container null flags and field metadata all
          # round-trip without inferring anything from the (absent) rows.
          normalized_data == [] and match?({:struct, _}, struct_schema) ->
            {:ok, {:local_relation, nil, SparkEx.Types.to_json(struct_schema), nil}}

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

          match?({:column_order, _}, normalized_schema) ->
            {:column_order, names} = normalized_schema

            prepare_list_data_inferred(
              normalized_data,
              opts |> Keyword.delete(:schema) |> Keyword.put(:column_order, names)
            )
        end

      with_restored_column_names(result, restore_names)
    end
  end

  defp prepare_local_data(data, opts) when is_map(data) and not is_struct(data) do
    with {:ok, normalized_schema} <- normalize_create_dataframe_schema(opts),
         {:ok, ordered} <- order_column_map(data) do
      # Column-oriented data: %{"col1" => [1,2,3], "col2" => ["a","b","c"]}
      # Map iteration order is undefined for maps with >32 keys; sort by
      # column name so the encoded relation is deterministic regardless
      # of insertion order. Callers needing user-controlled order should
      # pass a list of `{name, values}` pairs instead.
      with {:ok, explorer_df} <- safe_explorer_new(ordered),
           :ok <- validate_non_null_constraints(explorer_df, Keyword.get(opts, :schema)) do
        # Apply the (possibly column-name / json) schema against the built
        # frame here so a {:column_names, ...} tuple is resolved into a
        # rename/DDL rather than being re-normalized as a raw :schema opt.
        case apply_schema_to_explorer(explorer_df, normalized_schema) do
          {:ok, explorer_df, {:json_schema, _json, _ddl}} ->
            # Keep the original struct schema in opts; the Explorer clause
            # re-normalizes it to the metadata-preserving JSON form.
            prepare_local_data(explorer_df, opts)

          {:ok, explorer_df, schema_ddl} ->
            effective_schema = schema_ddl || explorer_to_ddl(explorer_df)
            prepare_local_data(explorer_df, Keyword.put(opts, :schema, effective_schema))

          {:error, _} = error ->
            error
        end
      else
        {:error, {:invalid_data, _}} = error -> error
        {:error, reason} -> {:error, {:data_conversion_error, reason}}
      end
    end
  end

  defp prepare_local_data(_data, _opts) do
    {:error, {:invalid_data, "expected Explorer.DataFrame, list of maps, or column map"}}
  end

  defp with_restored_column_names(result, nil), do: result

  defp with_restored_column_names({:ok, prepared}, names),
    do: {:ok, {:rename_columns, prepared, names}}

  defp with_restored_column_names({:error, _} = error, _names), do: error

  # Resolves the normalized schema against an Explorer.DataFrame and returns
  # `{:ok, frame, schema}` where schema is a DDL string, nil (inference /
  # column-name rename), or a `{:json_schema, json, ddl}` tuple (the caller
  # picks the JSON form on the Arrow path to preserve field metadata and the
  # DDL form on the SQL-JSON fallback). A `{:column_names, names}` schema
  # renames the frame's columns positionally (PySpark's `df.toDF(*_cols)`),
  # so the tuple never leaks into the local_relation schema field.
  defp apply_schema_to_explorer(data, nil), do: {:ok, data, nil}

  defp apply_schema_to_explorer(data, schema_ddl) when is_binary(schema_ddl) do
    {:ok, data, schema_ddl}
  end

  defp apply_schema_to_explorer(data, {:json_schema, _json, _ddl} = schema) do
    {:ok, data, schema}
  end

  defp apply_schema_to_explorer(data, {:column_names, names}) do
    existing = Explorer.DataFrame.names(data)

    if length(existing) == length(names) do
      renamed = Explorer.DataFrame.rename(data, Enum.zip(existing, names))
      # Names changed; let the column inference re-derive the DDL.
      {:ok, renamed, nil}
    else
      {:error,
       {:invalid_schema,
        "column-name list of length #{length(names)} does not match data with #{length(existing)} columns"}}
    end
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
        # reparse fields (parse_schema_field_types etc.) keep working.
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
  # Returns `{:ok, rows, schema, restore_names}`; `restore_names` is the list
  # of requested column names to re-apply with a final `toDF` projection when
  # positional rows had to be built under unique internal names (duplicate
  # requested names), otherwise nil.
  defp normalize_list_data_and_schema([], schema) do
    case schema do
      {:column_names, _} ->
        {:error,
         {:invalid_data, "cannot create DataFrame from empty list with column-name schema"}}

      _ ->
        {:ok, [], schema, nil}
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
            {:ok, data, schema, nil}
        end

      Enum.all?(data, &is_tuple/1) ->
        normalize_tuple_rows(data, schema)

      # Keyword-list rows are the idiomatic Elixir analogue of PySpark dict
      # rows; treat them as map rows (must come before the generic list arm,
      # which would coerce them to tuples of `{key, value}` pairs).
      Enum.all?(data, &keyword_row?/1) ->
        normalize_list_data_and_schema(Enum.map(data, &Map.new/1), schema)

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
      # Duplicate requested names (`schema: ["x", "x"]`) would collapse when
      # the positional rows become maps, so the relation is built under unique
      # internal names and the requested names are restored afterwards with a
      # `toDF` projection (PySpark: `_dedup_names` + `toDF(*names)`).
      {internal_names, restore_names} = dedup_positional_names(names, schema)

      # Tuple rows are positional: PySpark keeps their column order (only dict
      # rows are key-sorted), so carry the resolved names into inference.
      case schema do
        _ when internal_names != names and is_nil(restore_names) ->
          {:error,
           {:invalid_schema,
            "duplicate column names #{inspect(names)} are only supported with a " <>
              "column-name list schema; DDL and struct schemas would silently drop values"}}

        binary when is_binary(binary) ->
          {:ok, tuple_rows_to_maps(data, internal_names), binary, nil}

        {:json_schema, _, _} = json_schema ->
          {:ok, tuple_rows_to_maps(data, internal_names), json_schema, nil}

        {:column_names, _} ->
          {:ok, tuple_rows_to_maps(data, internal_names), {:column_order, internal_names},
           restore_names}

        nil ->
          {:ok, tuple_rows_to_maps(data, internal_names), {:column_order, internal_names}, nil}
      end
    end
  end

  defp tuple_rows_to_maps(data, names),
    do: Enum.map(data, fn tuple -> tuple_to_named_map(tuple, names) end)

  defp dedup_positional_names(names, {:column_names, _}) do
    if Enum.uniq(names) == names do
      {names, nil}
    else
      {Enum.with_index(names, fn _name, i -> "_spark_ex_col_#{i}" end), names}
    end
  end

  # DDL / struct schemas: duplicates are detected (internal names differ) but
  # cannot be restored positionally, so normalize_tuple_rows/2 rejects them.
  defp dedup_positional_names(names, _schema) do
    if Enum.uniq(names) == names do
      {names, nil}
    else
      {Enum.with_index(names, fn _name, i -> "_spark_ex_col_#{i}" end), nil}
    end
  end

  defp keyword_row?(row), do: row != [] and Keyword.keyword?(row)

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
      # Turn the key-sorted maps into positional rows so the requested names
      # keep the order they were given in (PySpark applies them with
      # `toDF(*names)`); the tuple path also copes with duplicate names.
      tuples =
        Enum.map(data, fn row ->
          stringified = Map.new(row, fn {k, v} -> {to_string(k), v} end)

          sorted_keys
          |> Enum.map(&Map.get(stringified, &1))
          |> List.to_tuple()
        end)

      normalize_tuple_rows(tuples, {:column_names, names})
    end
  end

  # PySpark pads a too-short column-name list for tuple/list rows with
  # `_N` (1-based, continuing after the supplied names) —
  # pyspark/sql/types.py:_infer_schema:
  #   `names.extend("_%d" % i for i in range(len(names) + 1, len(row) + 1))`
  # so `createDataFrame([(1, 2)], ["a"])` yields columns `a, _2`. A list
  # longer than the row width stays an error (PySpark raises
  # AXIS_LENGTH_MISMATCH from the `_num_cols != _table.shape[1]` check).
  defp column_names_for_tuple_data(data, {:column_names, names}) do
    arity = data |> Enum.map(&tuple_size/1) |> Enum.max(fn -> 0 end)
    given = length(names)

    cond do
      given > arity ->
        {:error,
         {:invalid_schema,
          "column-name list of length #{given} does not match data with #{arity} columns"}}

      given < arity ->
        {:ok, names ++ Enum.map((given + 1)..arity, fn i -> "_#{i}" end)}

      true ->
        {:ok, names}
    end
  end

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
      binary_fields = top_level_binary_fields(schema_ddl)

      if rows_contain_null_byte_text?(data, binary_fields) do
        case prepare_list_data_with_schema_arrow_fallback(data, schema_ddl, opts) do
          {:ok, _} = ok -> ok
          {:error, _} -> json_relation_from_rows(data, schema_ddl)
        end
      else
        json_relation_from_rows(data, schema_ddl)
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

  defp top_level_binary_fields(schema_ddl) do
    for {name, :binary} <- parse_schema_field_types(schema_ddl), do: name
  end

  # Shared JSON local-data path: list rows with a DDL schema, and the Explorer
  # complex-dtype fallback (`prepare_sql_json_relation/2`). Row values are
  # normalised by the schema's type tree so the encoding is schema-directed
  # at every nesting level: BINARY leaves are base64-encoded (Spark decodes
  # JSON binaries as base64), non-string-keyed MAPs become entry arrays
  # (from_json only accepts STRING map keys) and are rebuilt with
  # map_from_entries in a projection, and Explorer's non-finite float
  # sentinels become the spellings Spark's JSON parser accepts.
  defp json_relation_from_rows(rows, schema_ddl) do
    with {:ok, validated_schema} <- validate_schema_ddl_for_sql_relation(schema_ddl),
         fields = parse_schema_field_types(validated_schema),
         {:ok, normalized_rows} <- normalize_rows_for_schema(rows, fields),
         {:ok, row_json} <- encode_rows_as_json(normalized_rows) do
      if Enum.any?(fields, fn {_name, type} -> needs_json_projection?(type) end) do
        helper_schema = json_helper_schema_ddl(fields)

        with {:ok, validated_helper_schema} <-
               validate_schema_ddl_for_sql_relation(helper_schema) do
          query =
            json_rows_to_sql_query_with_projection(
              row_json,
              validated_helper_schema,
              json_projection_select_list(fields)
            )

          {:ok, {:sql_relation, query, nil}}
        end
      else
        {:ok, {:sql_relation, json_rows_to_sql_query(row_json, validated_schema), nil}}
      end
    end
  end

  # Handles metadata-bearing struct schemas (json_schema 3-tuple form).
  # Rows are projected onto the schema's fields, in field order (extra keys
  # are dropped, missing fields become null): the server applies a
  # LocalRelation schema positionally, so the Arrow columns must line up with
  # the fields (PySpark's LocalDataToArrowConversion indexes dict rows by
  # `field_names`). When normalize_local_relation_arrow? is true AND the
  # Explorer.DataFrame has complex dtypes, the SQL-JSON path must be used —
  # it cannot carry field metadata, but complex-type Arrow incompatibilities
  # are avoided. Otherwise the Arrow path carries the JSON schema so field
  # metadata is preserved.
  defp prepare_list_data_with_json_schema(data, json_str, ddl_str, opts) do
    field_names = json_schema_field_names(json_str)

    case safe_list_of_maps_to_explorer(data, field_names) do
      {:ok, explorer_df} ->
        if normalize_local_relation_arrow?(opts) and
             dataframe_contains_complex_dtype?(explorer_df) do
          # Complex-type columns: fall back to SQL-JSON path with DDL schema.
          # Field metadata is not preserved on this path.
          prepare_sql_json_relation(explorer_df, ddl_str)
        else
          # Simple columns: Arrow path with JSON schema preserves metadata.
          # Disable JSON-relation normalization so the binary json_str schema
          # is not embedded in a SQL `from_json(val, ...)` template. Nullability
          # is relaxed in the transmitted schema (the constraints were
          # validated locally against the rows).
          prepare_local_data(
            explorer_df,
            opts
            |> Keyword.put(:schema, relax_json_schema_nullability(json_str))
            |> Keyword.put(:normalize_local_relation_arrow, false)
          )
        end

      {:error, reason} ->
        {:error, {:data_conversion_error, reason}}
    end
  end

  defp json_schema_field_names(json_str) do
    case Jason.decode(json_str) do
      {:ok, %{"fields" => fields}} when is_list(fields) ->
        names = for %{"name" => name} <- fields, is_binary(name), do: name
        if names == [], do: nil, else: names

      _ ->
        nil
    end
  end

  # Polars marks every Arrow field it writes as nullable, and the server
  # reconciles the Arrow batch against the requested schema with
  # `Dataset.to`, which rejects a nullable column for a non-nullable field
  # (NULLABLE_COLUMN_OR_FIELD). Non-null constraints are validated locally
  # (validate_non_null_constraints/2) before the data is encoded, so the
  # transmitted schema relaxes them while keeping field metadata intact.
  defp relax_json_schema_nullability(json_str) do
    case Jason.decode(json_str) do
      {:ok, decoded} -> Jason.encode!(relax_nullability(decoded))
      _ -> json_str
    end
  end

  defp relax_nullability(%{"type" => "struct", "fields" => fields} = type) when is_list(fields) do
    relaxed =
      Enum.map(fields, fn field ->
        field
        |> Map.put("nullable", true)
        |> update_if_present("type", &relax_nullability/1)
      end)

    %{type | "fields" => relaxed}
  end

  defp relax_nullability(%{"type" => "array"} = type) do
    type
    |> Map.put("containsNull", true)
    |> update_if_present("elementType", &relax_nullability/1)
  end

  defp relax_nullability(%{"type" => "map"} = type) do
    type
    |> Map.put("valueContainsNull", true)
    |> update_if_present("valueType", &relax_nullability/1)
  end

  defp relax_nullability(other), do: other

  defp update_if_present(map, key, fun) do
    case map do
      %{^key => value} -> Map.put(map, key, fun.(value))
      _ -> map
    end
  end

  # Local enforcement of `nullable: false` (and array `contains_null` / map
  # `value_contains_null`) for explicit struct schemas, recursively, before
  # anything is encoded — mirrors PySpark's `_create_converter` /
  # `_check_type` ("input for LongType() must not be None"). The encodings
  # used on the wire cannot enforce the constraint themselves (from_json
  # yields nullable fields; Arrow fields written by Explorer are nullable).
  defp validate_non_null_constraints(rows, {:struct, fields} = schema) when is_list(rows) do
    if struct_has_non_null_constraint?(schema) do
      Enum.reduce_while(rows, :ok, fn row, :ok ->
        case check_non_null_row(row, fields) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    else
      :ok
    end
  end

  defp validate_non_null_constraints(data, {:struct, fields})
       when is_struct(data, Explorer.DataFrame) do
    names = Explorer.DataFrame.names(data)

    Enum.reduce_while(fields, :ok, fn field, :ok ->
      if Map.get(field, :nullable, true) == false and field.name in names and
           Explorer.Series.nil_count(data[field.name]) > 0 do
        {:halt, non_null_violation(field.name, field.type)}
      else
        {:cont, :ok}
      end
    end)
  end

  defp validate_non_null_constraints(_data, _schema), do: :ok

  defp struct_has_non_null_constraint?({:struct, fields}) do
    Enum.any?(fields, fn field ->
      Map.get(field, :nullable, true) == false or type_has_non_null_constraint?(field.type)
    end)
  end

  defp type_has_non_null_constraint?({:struct, _} = type),
    do: struct_has_non_null_constraint?(type)

  defp type_has_non_null_constraint?({:array, _elem, false}), do: true
  defp type_has_non_null_constraint?({:array, elem, _}), do: type_has_non_null_constraint?(elem)
  defp type_has_non_null_constraint?({:array, elem}), do: type_has_non_null_constraint?(elem)
  defp type_has_non_null_constraint?({:map, _k, _v, false}), do: true
  defp type_has_non_null_constraint?({:map, _k, v, _}), do: type_has_non_null_constraint?(v)
  defp type_has_non_null_constraint?({:map, _k, v}), do: type_has_non_null_constraint?(v)
  defp type_has_non_null_constraint?(_), do: false

  defp check_non_null_row(row, fields) when is_map(row) and not is_struct(row) do
    stringified = Map.new(row, fn {k, v} -> {to_string(k), v} end)
    check_non_null_fields(fields, fn field, _index -> Map.get(stringified, field.name) end)
  end

  defp check_non_null_row(row, fields) when is_tuple(row) do
    values = Tuple.to_list(row)

    # Arity mismatches are reported by the row normalisation step.
    if length(values) == length(fields) do
      check_non_null_fields(fields, fn _field, index -> Enum.at(values, index) end)
    else
      :ok
    end
  end

  defp check_non_null_row(row, fields) when is_list(row) do
    if keyword_row?(row),
      do: check_non_null_row(Map.new(row), fields),
      else: check_non_null_row(List.to_tuple(row), fields)
  end

  defp check_non_null_row(value, fields), do: check_non_null_row({value}, fields)

  defp check_non_null_fields(fields, lookup) do
    fields
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {field, index}, :ok ->
      value = lookup.(field, index)

      case check_non_null_value(value, field.type, Map.get(field, :nullable, true), field.name) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp check_non_null_value(nil, type, false, path), do: non_null_violation(path, type)
  defp check_non_null_value(nil, _type, _nullable, _path), do: :ok

  defp check_non_null_value(value, {:struct, fields}, _nullable, path)
       when is_map(value) and not is_struct(value) do
    stringified = Map.new(value, fn {k, v} -> {to_string(k), v} end)

    Enum.reduce_while(fields, :ok, fn field, :ok ->
      nested_path = "#{path}.#{field.name}"

      case check_non_null_value(
             Map.get(stringified, field.name),
             field.type,
             Map.get(field, :nullable, true),
             nested_path
           ) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp check_non_null_value(value, {:array, elem}, nullable, path),
    do: check_non_null_value(value, {:array, elem, true}, nullable, path)

  defp check_non_null_value(value, {:array, elem, contains_null}, _nullable, path)
       when is_list(value) do
    Enum.reduce_while(value, :ok, fn item, :ok ->
      case check_non_null_value(item, elem, contains_null, path <> "[]") do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp check_non_null_value(value, {:map, key, val}, nullable, path),
    do: check_non_null_value(value, {:map, key, val, true}, nullable, path)

  defp check_non_null_value(value, {:map, _key, val, value_contains_null}, _nullable, path)
       when is_map(value) and not is_struct(value) do
    Enum.reduce_while(value, :ok, fn {_k, item}, :ok ->
      case check_non_null_value(item, val, value_contains_null, path <> "[]") do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp check_non_null_value(_value, _type, _nullable, _path), do: :ok

  defp non_null_violation(path, type) do
    {:error,
     {:invalid_data,
      "input for field #{inspect(path)} (#{SparkEx.Types.data_type_to_ddl(type)}) must not be nil"}}
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
    {column_order, opts} = Keyword.pop(opts, :column_order)

    if Enum.empty?(data) do
      {:error, {:invalid_data, "cannot infer schema from empty list"}}
    else
      case check_determinable_columns(data, column_order) do
        {:error, _} = error ->
          error

        :ok ->
          with :ok <- validate_inferred_decimals(data),
               {:ok, explorer_df} <- safe_list_of_maps_to_explorer(data, column_order) do
            prepare_local_data(widen_inferred_decimal_columns(explorer_df), opts)
          else
            {:error, {:invalid_data, _}} = error ->
              error

            {:error, reason} ->
              prepare_list_data_inferred_fallback(data, reason, opts, column_order)
          end
      end
    end
  end

  # PySpark infers `decimal.Decimal` as DecimalType(38, 18) whatever the
  # values look like (same shape infer_single_type/1 declares); Explorer
  # derives the scale from the values instead, so inferred Elixir rows are
  # cast to the fixed shape. User-supplied Explorer frames keep their dtypes.
  defp widen_inferred_decimal_columns(explorer_df) do
    explorer_df
    |> Explorer.DataFrame.dtypes()
    |> Enum.reduce(explorer_df, fn
      {name, {:decimal, _precision, _scale}}, acc ->
        Explorer.DataFrame.put(acc, name, Explorer.Series.cast(acc[name], {:decimal, 38, 18}))

      _other, acc ->
        acc
    end)
  end

  # The decimal(38,18) shape holds at most 20 integer digits and 18 fractional
  # digits. Explorer's cast is non-strict (it rounds excess scale and turns
  # overflow into nulls), so values that do not fit are rejected up front the
  # way PyArrow rejects a lossy rescale in PySpark.
  @inferred_decimal_scale 18
  @inferred_decimal_integer_digits 20

  defp validate_inferred_decimals(rows) do
    rows
    |> Enum.flat_map(fn row -> Enum.map(row, fn {key, value} -> {to_string(key), value} end) end)
    |> Enum.find_value(:ok, fn
      {key, %Decimal{} = value} ->
        if decimal_fits_inferred_shape?(value) do
          nil
        else
          {:error,
           {:invalid_data,
            "decimal value #{Decimal.to_string(value, :normal)} in column \"#{key}\" does not " <>
              "fit the inferred DECIMAL(38,18) (at most 20 integer and 18 fractional " <>
              "digits); pass an explicit :schema"}}
        end

      _ ->
        nil
    end)
  end

  defp decimal_fits_inferred_shape?(%Decimal{coef: coef, exp: exp}) when is_integer(coef) do
    digits = coef |> Integer.digits() |> length()
    scale = max(-exp, 0)
    integer_digits = if coef == 0, do: 0, else: max(digits + exp, 0)
    scale <= @inferred_decimal_scale and integer_digits <= @inferred_decimal_integer_digits
  end

  # NaN / infinity decimals cannot be represented in a fixed-shape column.
  defp decimal_fits_inferred_shape?(_), do: false

  # Column order for inference: positional names for tuple rows, the sorted
  # key union (T-47) for map/keyword rows.
  defp ordered_keys(rows, nil), do: collect_ordered_keys(rows)
  defp ordered_keys(_rows, names) when is_list(names), do: names

  # PySpark refuses to infer a schema when any column is entirely null
  # (`_has_nulltype(_schema)` -> CANNOT_DETERMINE_TYPE in
  # pyspark/sql/connect/session.py:createDataFrame). Mirror that instead of
  # silently defaulting such columns to STRING (T-48): the user must pass an
  # explicit `:schema`. The check is recursive, matching `_has_nulltype`, so
  # `[%{"a" => []}]` (ARRAY<NULL>) is rejected too.
  defp check_determinable_columns(rows, column_order \\ nil) do
    stringified = Enum.map(rows, fn row -> Map.new(row, fn {k, v} -> {to_string(k), v} end) end)

    Enum.reduce_while(ordered_keys(stringified, column_order), :ok, fn key, :ok ->
      values = Enum.map(stringified, &Map.get(&1, key))

      if has_null_type?(infer_value_type(values)) do
        {:halt, {:error, {:cannot_determine_type, key}}}
      else
        {:cont, :ok}
      end
    end)
  rescue
    # Heterogeneous inferred types raise here; leave that error to the
    # existing inference paths so its message stays unchanged.
    ArgumentError -> :ok
  end

  defp has_null_type?(:null), do: true
  defp has_null_type?({:array, inner}), do: has_null_type?(inner)

  defp has_null_type?({:struct, fields}),
    do: Enum.any?(fields, fn {_n, t} -> has_null_type?(t) end)

  defp has_null_type?(_), do: false

  defp prepare_list_data_inferred_fallback(data, reason, opts, column_order) do
    if normalize_local_relation_arrow?(opts) do
      with {:ok, normalized_rows} <- normalize_rows_for_schema(data),
           {:ok, inferred_schema_ddl} <-
             infer_schema_ddl_from_rows(normalized_rows, column_order),
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

  defp safe_list_of_maps_to_explorer(data, column_order \\ nil) do
    {:ok, list_of_maps_to_explorer(data, column_order)}
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

  defp list_of_maps_to_explorer([], _column_order) do
    Explorer.DataFrame.new(%{})
  end

  defp list_of_maps_to_explorer([first | _] = data, column_order) when is_map(first) do
    normalized_rows =
      Enum.map(data, fn row ->
        Map.new(row, fn {key, value} -> {to_string(key), value} end)
      end)

    # Sorted key union (T-47): mirrors PySpark's `sorted(row.items())` dict
    # inference and keeps the column order independent of Erlang's map
    # iteration order (unspecified above 32 keys).
    columns =
      normalized_rows
      |> ordered_keys(column_order)
      |> Enum.map(fn key ->
        values = Enum.map(normalized_rows, fn row -> Map.get(row, key) end)
        {key, values}
      end)

    Explorer.DataFrame.new(columns)
  end

  # Normalises map rows for the JSON local-data path. `fields` is the
  # `[{name, type_tree}]` list from parse_schema_field_types/1; values whose
  # column is not in the schema (or when no schema is known) go through the
  # plain normalize_json_value/1 pass.
  defp normalize_rows_for_schema(rows, fields \\ []) when is_list(rows) do
    field_types = Map.new(fields)

    Enum.reduce_while(rows, {:ok, []}, fn
      row, {:ok, acc} when is_map(row) and not is_struct(row) ->
        normalized =
          Map.new(row, fn {key, value} ->
            key_string = to_string(key)
            {key_string, normalize_value_for_type(value, Map.get(field_types, key_string))}
          end)

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

  # Schema-directed JSON value normalisation (see json_relation_from_rows/2).
  defp normalize_value_for_type(nil, _type), do: nil
  defp normalize_value_for_type(value, nil), do: normalize_json_value(value)
  defp normalize_value_for_type(value, :binary) when is_binary(value), do: Base.encode64(value)

  defp normalize_value_for_type({:binary, value}, :binary) when is_binary(value),
    do: Base.encode64(value)

  defp normalize_value_for_type(value, {:array, elem}) when is_list(value),
    do: Enum.map(value, &normalize_value_for_type(&1, elem))

  defp normalize_value_for_type(value, {:map, key_type, value_type})
       when is_map(value) and not is_struct(value) do
    if string_key_type?(key_type) do
      Map.new(value, fn {k, v} -> {to_string(k), normalize_value_for_type(v, value_type)} end)
    else
      # from_json only accepts STRING map keys: ship the map as an array of
      # `{key, value}` entries and rebuild it with map_from_entries.
      Enum.map(value, fn {k, v} ->
        %{
          "key" => normalize_value_for_type(k, key_type),
          "value" => normalize_value_for_type(v, value_type)
        }
      end)
    end
  end

  defp normalize_value_for_type(value, {:struct, fields})
       when is_map(value) and not is_struct(value) do
    stringified = Map.new(value, fn {k, v} -> {to_string(k), v} end)

    Map.new(fields, fn {name, type} ->
      {name, normalize_value_for_type(Map.get(stringified, name), type)}
    end)
  end

  defp normalize_value_for_type(value, _type), do: normalize_json_value(value)

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

  # Parses the top-level fields of a DDL schema into `[{name, type_tree}]`.
  # The type tree only distinguishes what the JSON local-data path needs:
  # `:binary`, `{:array, t}`, `{:map, k, v}`, `{:struct, [{name, t}]}` and
  # `{:opaque, raw}` for every other type (kept verbatim). Trailing
  # `NOT NULL` / `COMMENT '...'` qualifiers are stripped at every level.
  defp parse_schema_field_types(schema_ddl) do
    schema_ddl
    |> split_top_level_schema_fields()
    |> Enum.flat_map(fn field ->
      case parse_schema_field(field) do
        {name, type} -> [{name, parse_type_tree(type)}]
        :error -> []
      end
    end)
  end

  defp parse_type_tree(type) do
    trimmed = type |> String.trim() |> strip_type_qualifiers()
    upper = String.upcase(trimmed)

    cond do
      upper == "BINARY" ->
        :binary

      String.starts_with?(upper, "ARRAY<") and String.ends_with?(trimmed, ">") ->
        {:array, parse_type_tree(inner_type_argument(trimmed, 6))}

      String.starts_with?(upper, "MAP<") and String.ends_with?(trimmed, ">") ->
        case parse_map_type(trimmed) do
          {:ok, key_type, value_type} ->
            {:map, parse_type_tree(key_type), parse_type_tree(value_type)}

          :error ->
            {:opaque, trimmed}
        end

      String.starts_with?(upper, "STRUCT<") and String.ends_with?(trimmed, ">") ->
        parse_struct_type_tree(inner_type_argument(trimmed, 7), trimmed)

      true ->
        {:opaque, trimmed}
    end
  end

  defp inner_type_argument(type, prefix_length),
    do: String.slice(type, prefix_length, String.length(type) - prefix_length - 1)

  defp strip_type_qualifiers(type) do
    type
    |> String.replace(~r/\s+COMMENT\s+'(?:[^']|'')*'\s*\z/i, "")
    |> String.replace(~r/\s+NOT\s+NULL\s*\z/i, "")
    |> String.trim()
  end

  defp parse_struct_type_tree(inner, raw) do
    fields =
      inner
      |> split_top_level_schema_fields()
      |> Enum.map(&parse_struct_field_type/1)

    if Enum.any?(fields, &(&1 == :error)), do: {:opaque, raw}, else: {:struct, fields}
  end

  # STRUCT fields accept `name: TYPE`, `name:TYPE` and `name TYPE`, with an
  # optionally backtick-quoted name.
  defp parse_struct_field_type(field) do
    case String.trim(field) do
      <<"`", rest::binary>> ->
        case consume_backtick_identifier(rest, []) do
          {:ok, name, after_close} ->
            type =
              after_close
              |> String.trim_leading()
              |> String.replace_prefix(":", "")
              |> String.trim()

            if type == "", do: :error, else: {name, parse_type_tree(type)}

          :error ->
            :error
        end

      other ->
        case Regex.run(~r/^([^\s:`]+)\s*:?\s*(\S.*)$/s, other) do
          [_, name, type] -> {name, parse_type_tree(type)}
          _ -> :error
        end
    end
  end

  defp string_key_type?({:opaque, raw}), do: String.upcase(raw) == "STRING"
  defp string_key_type?(_type), do: false

  # True when the type (at any depth) holds a non-string-keyed MAP, which
  # from_json cannot parse directly and must be rebuilt in a projection.
  defp needs_json_projection?({:map, key_type, value_type}),
    do: not string_key_type?(key_type) or needs_json_projection?(value_type)

  defp needs_json_projection?({:array, elem}), do: needs_json_projection?(elem)

  defp needs_json_projection?({:struct, fields}),
    do: Enum.any?(fields, fn {_name, type} -> needs_json_projection?(type) end)

  defp needs_json_projection?(_type), do: false

  # Rebuilds the DDL for the from_json helper schema with non-string-keyed
  # maps replaced by entry arrays. Field names are re-quoted (the parser
  # decoded them), so names with spaces or backticks survive the round-trip.
  defp json_helper_schema_ddl(fields) do
    Enum.map_join(fields, ", ", fn {name, type} ->
      "#{SparkEx.Types.quote_identifier(name)} #{render_json_helper_type(type)}"
    end)
  end

  defp render_json_helper_type(:binary), do: "BINARY"
  defp render_json_helper_type({:opaque, raw}), do: raw
  defp render_json_helper_type({:array, elem}), do: "ARRAY<#{render_json_helper_type(elem)}>"

  defp render_json_helper_type({:map, key_type, value_type}) do
    key = render_json_helper_type(key_type)
    value = render_json_helper_type(value_type)

    if string_key_type?(key_type),
      do: "MAP<#{key}, #{value}>",
      else: "ARRAY<STRUCT<key: #{key}, value: #{value}>>"
  end

  defp render_json_helper_type({:struct, fields}) do
    inner =
      Enum.map_join(fields, ", ", fn {name, type} ->
        "#{SparkEx.Types.quote_identifier(name)}: #{render_json_helper_type(type)}"
      end)

    "STRUCT<#{inner}>"
  end

  # Projection that turns the helper-schema columns back into the requested
  # types: `map_from_entries` for entry arrays, `transform` through arrays,
  # `transform_values` through string-keyed maps and `named_struct` through
  # structs (null-preserving). Every identifier is backtick-quoted with
  # embedded backticks escaped.
  defp json_projection_select_list(fields) do
    Enum.map_join(fields, ", ", fn {name, type} ->
      quoted = backtick_quote(name)
      "#{json_projection_expr(type, "parsed.#{quoted}", 0)} AS #{quoted}"
    end)
  end

  defp backtick_quote(name), do: "`" <> String.replace(name, "`", "``") <> "`"

  defp json_projection_expr(type, expr, depth) do
    if needs_json_projection?(type),
      do: json_projection_transform(type, expr, depth),
      else: expr
  end

  defp json_projection_transform({:map, key_type, value_type}, expr, depth) do
    var = "_spark_ex_e#{depth}"

    if string_key_type?(key_type) do
      "transform_values(#{expr}, (_spark_ex_k#{depth}, #{var}) -> " <>
        "#{json_projection_expr(value_type, var, depth + 1)})"
    else
      entries =
        if needs_json_projection?(key_type) or needs_json_projection?(value_type) do
          "transform(#{expr}, #{var} -> named_struct(" <>
            "'key', #{json_projection_expr(key_type, var <> ".key", depth + 1)}, " <>
            "'value', #{json_projection_expr(value_type, var <> ".value", depth + 1)}))"
        else
          expr
        end

      "map_from_entries(#{entries})"
    end
  end

  defp json_projection_transform({:array, elem}, expr, depth) do
    var = "_spark_ex_e#{depth}"
    "transform(#{expr}, #{var} -> #{json_projection_expr(elem, var, depth + 1)})"
  end

  defp json_projection_transform({:struct, fields}, expr, depth) do
    inner =
      Enum.map_join(fields, ", ", fn {name, type} ->
        "'#{sql_escape_string(name)}', " <>
          json_projection_expr(type, "#{expr}.#{backtick_quote(name)}", depth)
      end)

    "CASE WHEN #{expr} IS NULL THEN NULL ELSE named_struct(#{inner}) END"
  end

  # Splits a DDL field list on top-level commas only. Tracks `<>` and `()`
  # nesting (STRUCT<…>, DECIMAL(10, 2)) and never splits inside a
  # backtick-quoted identifier or a single/double-quoted string literal
  # (doubled-quote escapes are honoured) (T-10).
  defp split_top_level_schema_fields(schema_ddl) do
    {parts, current} = split_top_level_fields(schema_ddl, [], [], 0, nil)
    parts = [String.trim(current) | parts]

    parts
    |> Enum.reverse()
    |> Enum.reject(&(&1 == ""))
  end

  # split_top_level_fields(rest, parts_rev, current_rev_iodata, depth, quote)
  defp split_top_level_fields(<<>>, parts, current, _depth, _quote),
    do: {parts, IO.iodata_to_binary(Enum.reverse(current))}

  # Inside a quoted run: a backslash escapes the next char (Spark string
  # literals accept \' and \"), a doubled quote char is an escape, a single
  # one closes.
  defp split_top_level_fields(<<"\\", ch::utf8, rest::binary>>, parts, current, depth, q)
       when not is_nil(q),
       do: split_top_level_fields(rest, parts, [<<"\\", ch::utf8>> | current], depth, q)

  defp split_top_level_fields(<<q::utf8, q::utf8, rest::binary>>, parts, current, depth, q),
    do: split_top_level_fields(rest, parts, [<<q::utf8, q::utf8>> | current], depth, q)

  defp split_top_level_fields(<<q::utf8, rest::binary>>, parts, current, depth, q),
    do: split_top_level_fields(rest, parts, [<<q::utf8>> | current], depth, nil)

  defp split_top_level_fields(<<ch::utf8, rest::binary>>, parts, current, depth, q)
       when not is_nil(q),
       do: split_top_level_fields(rest, parts, [<<ch::utf8>> | current], depth, q)

  defp split_top_level_fields(<<q::utf8, rest::binary>>, parts, current, depth, nil)
       when q in [?`, ?', ?"],
       do: split_top_level_fields(rest, parts, [<<q::utf8>> | current], depth, q)

  defp split_top_level_fields(<<open::utf8, rest::binary>>, parts, current, depth, nil)
       when open in [?<, ?(],
       do: split_top_level_fields(rest, parts, [<<open::utf8>> | current], depth + 1, nil)

  defp split_top_level_fields(<<close::utf8, rest::binary>>, parts, current, depth, nil)
       when close in [?>, ?)] and depth > 0,
       do: split_top_level_fields(rest, parts, [<<close::utf8>> | current], depth - 1, nil)

  defp split_top_level_fields(<<",", rest::binary>>, parts, current, 0, nil) do
    field = current |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim()
    split_top_level_fields(rest, [field | parts], [], 0, nil)
  end

  defp split_top_level_fields(<<ch::utf8, rest::binary>>, parts, current, depth, nil),
    do: split_top_level_fields(rest, parts, [<<ch::utf8>> | current], depth, nil)

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

  defp infer_schema_ddl_from_rows(rows, column_order) when is_list(rows) do
    ordered_keys = ordered_keys(rows, column_order)

    fields =
      Enum.map(ordered_keys, fn key ->
        values = Enum.map(rows, &Map.get(&1, key))
        {key, infer_value_type(values)}
      end)

    {:ok,
     Enum.map_join(fields, ", ", fn {name, type} -> "#{name} #{type_to_inferred_ddl(type)}" end)}
  end

  # Column order for map/keyword rows without an explicit schema (T-47).
  #
  # Erlang map iteration order is only guaranteed for maps with <= 32 keys
  # (and even then it is term order, which mixes atoms and binaries), so
  # first-seen key order made the inferred column order depend on the number
  # of keys. PySpark sorts dict keys when inferring
  # (`items = sorted(row.items())` in pyspark/sql/types.py:_infer_schema and
  # `dict(sorted(d.items()))` in connect/session.py:createDataFrame), so keys
  # are stringified and sorted alphabetically here for the same deterministic
  # result. An explicit schema (DDL string, struct schema, or column-name
  # list) always keeps its own order.
  defp collect_ordered_keys(rows) do
    rows
    |> Enum.flat_map(fn row -> Enum.map(row, fn {key, _value} -> to_string(key) end) end)
    |> Enum.uniq()
    |> Enum.sort()
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
  # though Elixir integers can fit in narrower types. Values outside the
  # 64-bit range would otherwise be shipped as-is and die server-side with an
  # opaque MALFORMED_RECORD_IN_PARSING error.
  defp infer_single_type(v)
       when is_integer(v) and
              (v < -9_223_372_036_854_775_808 or v > 9_223_372_036_854_775_807) do
    raise ArgumentError,
          "integer #{v} is out of range for Spark BIGINT (64-bit); " <>
            "pass it as a Decimal (DECIMAL supports up to 38 digits) or a string"
  end

  defp infer_single_type(v) when is_integer(v), do: :long
  defp infer_single_type(v) when is_float(v), do: :double
  # Explorer represents non-finite floats as these atoms (T-04).
  defp infer_single_type(v) when v in [:nan, :infinity, :neg_infinity], do: :double

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

  # Explorer's non-finite float sentinels. Jason would encode the atoms as the
  # strings "nan"/"infinity"/"neg_infinity"; Spark's from_json accepts exactly
  # "NaN" / "Infinity" / "-Infinity" for DOUBLE/FLOAT (JacksonParser, with the
  # default allowNonNumericNumbers=true) (T-04).
  defp normalize_json_value(:nan), do: "NaN"
  defp normalize_json_value(:infinity), do: "Infinity"
  defp normalize_json_value(:neg_infinity), do: "-Infinity"

  defp normalize_json_value(value), do: value

  defp explorer_to_ddl(explorer_df) do
    dtypes = Explorer.DataFrame.dtypes(explorer_df)

    ordered_dtypes =
      explorer_df
      |> Explorer.DataFrame.names()
      |> Enum.map(fn name -> {name, Map.fetch!(dtypes, name)} end)

    TypeMapper.explorer_schema_to_ddl(ordered_dtypes)
  end

  # Explorer frames with complex dtypes take the JSON path; the rows go
  # through the same schema-directed normalisation as list rows (binary
  # columns, non-finite floats, non-string-keyed maps).
  defp prepare_sql_json_relation(explorer_df, schema_ddl) do
    explorer_df
    |> Explorer.DataFrame.to_rows()
    |> json_relation_from_rows(schema_ddl)
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
  #
  # `max_chunk_rows` additionally caps every chunk by row count (T-64), like
  # PySpark's `_serialize_table_chunks(max_chunk_size_rows, max_chunk_size_bytes)`.
  @doc false
  @spec split_explorer_dataframe_for_cache(
          Explorer.DataFrame.t(),
          pos_integer(),
          pos_integer() | nil
        ) ::
          {:ok, [binary(), ...]} | {:error, term()}
  def split_explorer_dataframe_for_cache(
        %Explorer.DataFrame{} = df,
        chunk_size_bytes,
        max_chunk_rows \\ nil
      )
      when is_integer(chunk_size_bytes) and chunk_size_bytes > 0 and
             (is_nil(max_chunk_rows) or (is_integer(max_chunk_rows) and max_chunk_rows > 0)) do
    total_rows = Explorer.DataFrame.n_rows(df)

    if total_rows <= 1 do
      case dump_ipc_stream_safe(df) do
        {:ok, ipc} -> {:ok, [ipc]}
        {:error, _} = error -> error
      end
    else
      with {:ok, rows_per_chunk} <-
             estimate_rows_per_chunk(df, total_rows, chunk_size_bytes, max_chunk_rows) do
        do_split_dataframe(df, total_rows, rows_per_chunk, 0, [])
      end
    end
  end

  # Fetches the local-relation server configs once per session (T-64). A
  # failed config RPC keeps default inline behavior, but cached uploads fail
  # closed until the configuration can be read. A missing key on a successful
  # old-server response is different from an unknown limit after an RPC error.
  defp ensure_local_relation_configs(%{local_relation_configs: %{} = configs} = state, _opts),
    do: {configs, state}

  # A failed probe is remembered for @local_relation_config_retry_ms so a
  # flaky server does not cost a bounded RPC timeout on every create_dataframe,
  # yet the session recovers once the server answers again.
  defp ensure_local_relation_configs(
         %{local_relation_configs: {:unavailable, failed_at}} = state,
         opts
       ) do
    if System.monotonic_time(:millisecond) - failed_at < @local_relation_config_retry_ms do
      {@local_relation_config_defaults, state}
    else
      ensure_local_relation_configs(%{state | local_relation_configs: nil}, opts)
    end
  end

  # Upload strategy overrides must not bypass the server's total size limit.
  defp ensure_local_relation_configs(state, _opts), do: fetch_local_relation_configs(state)

  # Unit stubs drive `handle_call/3` with a bare map; only a connected session
  # can issue the Config RPC.
  defp fetch_local_relation_configs(%{channel: channel, session_id: id} = state)
       when not is_nil(channel) and is_binary(id) do
    keys = Enum.map(@local_relation_config_keys, fn {key, _atom} -> key end)

    # Bounded timeout: this runs inside the Session GenServer on the first
    # create_dataframe of a session. Failures are retried after the bounded
    # cooldown; cached uploads must not bypass a potentially unknown limit.
    case Client.config_get_option(state, keys, timeout: @local_relation_config_timeout) do
      {:ok, pairs, server_side_session_id} ->
        configs = parse_local_relation_configs(pairs)
        state = maybe_update_server_session(state, server_side_session_id)
        {configs, Map.put(state, :local_relation_configs, configs)}

      {:error, reason} ->
        Logger.debug(fn ->
          "create_dataframe: could not read localRelation configs, using defaults: " <>
            inspect(reason)
        end)

        failed_at = System.monotonic_time(:millisecond)

        {@local_relation_config_defaults,
         Map.put(state, :local_relation_configs, {:unavailable, failed_at})}
    end
  end

  defp fetch_local_relation_configs(state), do: {@local_relation_config_defaults, state}

  # `config_set`/`config_unset` may change the localRelation* settings, so the
  # cached snapshot is dropped and re-read on the next create_dataframe.
  defp invalidate_local_relation_configs(state),
    do: Map.put(state, :local_relation_configs, nil)

  @doc false
  def __parse_local_relation_configs__(pairs), do: parse_local_relation_configs(pairs)

  # `pairs` is the ConfigResponse key/value list; a value is `nil` when the
  # server does not know the key (Spark 3.5 predates the chunking configs) and
  # is a decimal string otherwise. Anything unparsable keeps the default.
  defp parse_local_relation_configs(pairs) when is_list(pairs) do
    parsed =
      Enum.reduce(@local_relation_config_keys, %{}, fn {key, atom}, acc ->
        case List.keyfind(pairs, key, 0) do
          {_key, value} when is_binary(value) ->
            case Integer.parse(String.trim(value)) do
              {int, ""} when int >= 0 -> Map.put(acc, atom, int)
              _ -> acc
            end

          _ ->
            acc
        end
      end)

    # `localRelationCacheThreshold` exists since Spark 3.5, but the chunked
    # cached relation the cache path emits is 4.1+. Only a server that also
    # reports the 4.1 chunking configs can accept it, so on older servers the
    # legacy client-side threshold is kept and payloads stay inlined.
    if Map.has_key?(parsed, :chunk_size_rows) do
      Map.merge(@local_relation_config_defaults, parsed)
    else
      Map.merge(@local_relation_config_defaults, Map.delete(parsed, :cache_threshold))
    end
  end

  defp parse_local_relation_configs(_pairs), do: @local_relation_config_defaults

  @doc false
  def __local_relation_chunk_params__(opts, configs),
    do: local_relation_chunk_params(opts, configs)

  # Resolves the effective threshold / chunk limits for one create_dataframe
  # call: explicit options win, then the server configs (or their defaults).
  # The byte cap is `min(chunkSizeBytes, batchOfChunksSizeBytes)` like PySpark,
  # so a single chunk always fits in one upload batch.
  defp local_relation_chunk_params(opts, configs) do
    cache_threshold = Keyword.get(opts, :cache_threshold, configs.cache_threshold)

    chunk_size_bytes =
      Keyword.get(
        opts,
        :cache_chunk_size,
        min(configs.chunk_size_bytes, configs.batch_of_chunks_size_bytes)
      )

    chunk_size_rows = Keyword.get(opts, :cache_chunk_rows, configs.chunk_size_rows)

    cond do
      not (is_integer(cache_threshold) and cache_threshold >= 0) ->
        {:error, {:invalid_option, {:cache_threshold, cache_threshold}}}

      not (is_integer(chunk_size_bytes) and chunk_size_bytes > 0) ->
        {:error, {:invalid_option, {:cache_chunk_size, chunk_size_bytes}}}

      not (is_integer(chunk_size_rows) and chunk_size_rows > 0) ->
        {:error, {:invalid_option, {:cache_chunk_rows, chunk_size_rows}}}

      true ->
        {:ok,
         %{
           cache_threshold: cache_threshold,
           chunk_size_bytes: chunk_size_bytes,
           chunk_size_rows: chunk_size_rows,
           batch_of_chunks_size_bytes: max(configs.batch_of_chunks_size_bytes, chunk_size_bytes),
           size_limit: Map.get(configs, :size_limit)
         }}
    end
  end

  defp validate_cache_configuration(%{local_relation_configs: {:unavailable, _}}),
    do: {:error, :local_relation_config_unavailable}

  defp validate_cache_configuration(_state), do: :ok

  @doc false
  def __validate_local_relation_size__(chunks, schema, limit),
    do: validate_local_relation_size(chunks, schema, limit)

  defp validate_local_relation_size(_chunks, _schema, nil), do: :ok

  defp validate_local_relation_size(chunks, schema, limit) do
    # Count the logical serialized relation, before artifact deduplication.
    # Repeated hashes still represent repeated rows, and schema bytes count too.
    size = Enum.reduce(chunks, byte_size(schema || ""), &(byte_size(&1) + &2))

    if size > limit do
      {:error,
       %SparkEx.Error.Remote{
         error_class: "LOCAL_RELATION_SIZE_LIMIT_EXCEEDED",
         message: "Local relation size #{size} exceeds limit #{limit}",
         message_parameters: %{
           "actualSize" => Integer.to_string(size),
           "sizeLimit" => Integer.to_string(limit)
         }
       }}
    else
      :ok
    end
  end

  @doc false
  def __local_relation_rows_per_chunk__(sample_bytes, sample_rows, chunk_size_bytes, max_rows),
    do: local_relation_rows_per_chunk(sample_bytes, sample_rows, chunk_size_bytes, max_rows)

  # Rows per chunk from a head-sample's IPC size: bounded by bytes (at least
  # one row so a single oversized row still ships) and, when given, by rows.
  defp local_relation_rows_per_chunk(sample_bytes, sample_rows, chunk_size_bytes, max_rows) do
    bytes_per_row = max(1, div(sample_bytes, max(1, sample_rows)))
    by_bytes = max(1, div(chunk_size_bytes, bytes_per_row))

    case max_rows do
      n when is_integer(n) and n > 0 -> min(by_bytes, n)
      _ -> by_bytes
    end
  end

  @doc false
  def __batch_cache_artifacts__(artifacts, batch_bytes),
    do: batch_cache_artifacts(artifacts, batch_bytes)

  # Groups {name, bytes} artifacts into upload batches whose summed size stays
  # within `batch_bytes` (PySpark's max_batch_of_chunks_size_bytes). A single
  # artifact larger than the limit still forms its own batch.
  defp batch_cache_artifacts(artifacts, batch_bytes)
       when is_integer(batch_bytes) and batch_bytes > 0 do
    {batches, current, _size} =
      Enum.reduce(artifacts, {[], [], 0}, fn {_name, data} = artifact, {batches, current, size} ->
        artifact_size = byte_size(data)

        if current != [] and size + artifact_size > batch_bytes do
          {[Enum.reverse(current) | batches], [artifact], artifact_size}
        else
          {batches, [artifact | current], size + artifact_size}
        end
      end)

    batches = if current == [], do: batches, else: [Enum.reverse(current) | batches]
    Enum.reverse(batches)
  end

  defp batch_cache_artifacts([], _batch_bytes), do: []
  defp batch_cache_artifacts(artifacts, _batch_bytes), do: [artifacts]

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

  defp estimate_rows_per_chunk(df, total_rows, chunk_size_bytes, max_chunk_rows) do
    sample_rows = min(total_rows, 256)
    sample_df = Explorer.DataFrame.head(df, sample_rows)

    case dump_ipc_stream_safe(sample_df) do
      {:ok, sample_ipc} ->
        {:ok,
         local_relation_rows_per_chunk(
           byte_size(sample_ipc),
           sample_rows,
           chunk_size_bytes,
           max_chunk_rows
         )}

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

  # Uploads the artifacts the server does not already hold, in batches of at
  # most `batch_bytes` (PySpark's max_batch_of_chunks_size_bytes): one RPC per
  # chunk is too chatty, one RPC for everything materialises the whole payload.
  defp upload_missing_cache_artifacts(state, artifacts, batch_bytes) do
    names = Enum.map(artifacts, fn {name, _data} -> name end)

    case Client.artifact_status(state, names) do
      {:ok, statuses, server_side_session_id} ->
        state = maybe_update_server_session(state, server_side_session_id)
        missing = Enum.reject(artifacts, fn {name, _data} -> Map.get(statuses, name, false) end)

        missing
        |> batch_cache_artifacts(batch_bytes)
        |> Enum.reduce_while({:ok, state}, fn batch, {:ok, state} ->
          case maybe_upload_cache_artifacts(state, batch) do
            {:ok, state} -> {:cont, {:ok, state}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)

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

  defp session_id_for(%__MODULE__{session_id: session_id}), do: session_id

  defp session_id_for(session) do
    GenServer.call(resolve_server!(session), :get_session_id)
  end

  # Resolves any GenServer.server() reference (pid, registered name, or
  # via-tuple) to a pid so name/via-tuple sessions work like raw pids.
  # Raises if the name does not resolve to a live process (consistent with
  # GenServer.call's behavior on a dead pid).
  defp resolve_server!(session) when is_pid(session), do: session

  defp resolve_server!(session) do
    case GenServer.whereis(session) do
      pid when is_pid(pid) -> pid
      {name, node} -> {name, node}
      nil -> raise ArgumentError, "no process associated with #{inspect(session)}"
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
    combined = Enum.uniq(request_tags ++ session_tags)

    if combined == [] do
      opts
    else
      Keyword.put(opts, :tags, combined)
    end
  end
end
