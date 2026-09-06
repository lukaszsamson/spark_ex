defmodule SparkEx.Connect.Client do
  @moduledoc false

  alias Spark.Connect.SparkConnectService.Stub

  alias Spark.Connect.{
    AddArtifactsRequest,
    AddArtifactsResponse,
    AnalyzePlanRequest,
    AnalyzePlanResponse,
    ArtifactStatusesRequest,
    ArtifactStatusesResponse,
    ConfigRequest,
    ConfigResponse,
    ExecutePlanRequest,
    ExecutePlanResponse,
    GetStatusRequest,
    GetStatusResponse,
    CloneSessionRequest,
    CloneSessionResponse,
    InterruptRequest,
    InterruptResponse,
    ReattachExecuteRequest,
    ReattachOptions,
    ReleaseExecuteRequest,
    ReleaseExecuteResponse,
    ReleaseSessionRequest,
    ReleaseSessionResponse,
    ResultChunkingOptions,
    StorageLevel,
    KeyValue,
    Plan
  }

  alias SparkEx.Connect.{Errors, ResultDecoder, SessionIntegrity}
  alias SparkEx.Internal.UUID
  alias SparkEx.{ManagedStream, RetryPolicyRegistry, UserContextExtensions}

  require Logger

  # gRPC status codes considered transient (eligible for retry)
  @status_unavailable 14
  @status_internal 13
  @status_not_found 5

  @artifact_chunk_size 32 * 1024
  @release_execute_timeout 5_000

  # Grace period for fire-and-forget release tasks after the primary
  # `release_execute_timeout` elapses. We previously used `:brutal_kill`,
  # which can leave an in-flight gRPC call half-open. Switching to a short
  # graceful shutdown gives the gRPC client a chance to cancel/unwind the
  # call cleanly before the local task is killed.
  @shutdown_grace_ms 1_000

  # Max concurrent in-flight `release_until(response_id)` checkpoint tasks
  # per reattachable stream. PySpark's reattach iterator fires one
  # checkpoint per consumed response; under bursty consumption this can
  # pile supervised tasks up faster than the server processes them. We cap
  # the inflight count and coalesce intermediate ids — `until_response_id`
  # is monotonic, so the newest checkpoint subsumes any older ones we
  # dropped.
  @max_inflight_release_checkpoints 1

  # Retry budget for non-idempotent release RPCs (`ReleaseExecute`,
  # `ReleaseSession`). The default unary policy (15 retries × 60s max
  # backoff) can keep a release call alive for ~10 minutes; server-side
  # state is GC'd eventually so we don't need to be that persistent — a
  # handful of retries covers transient blips without masking outages.
  @release_max_retries 3

  # --- AnalyzePlan RPCs ---

  @doc """
  Calls `AnalyzePlan` with `SparkVersion` to retrieve the Spark version string.
  """
  @spec analyze_spark_version(SparkEx.Session.t()) ::
          {:ok, String.t(), String.t() | nil} | {:error, term()}
  def analyze_spark_version(session) do
    request =
      build_analyze_request(session,
        analyze: {:spark_version, %AnalyzePlanRequest.SparkVersion{}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :spark_version}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:spark_version, %{version: version}}} = resp} ->
        {:ok, version, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `Schema` to retrieve the schema of a plan.
  """
  @spec analyze_schema(SparkEx.Session.t(), Plan.t()) ::
          {:ok, term(), String.t() | nil} | {:error, term()}
  def analyze_schema(session, plan) do
    request =
      build_analyze_request(session,
        analyze: {:schema, %AnalyzePlanRequest.Schema{plan: plan}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request, extra_metadata: %{analyze: :schema}) do
      {:ok, %AnalyzePlanResponse{result: {:schema, %{schema: schema}}} = resp} ->
        {:ok, schema, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `Explain` to get a plan explanation string.
  """
  @spec analyze_explain(SparkEx.Session.t(), Plan.t(), atom()) ::
          {:ok, String.t(), String.t() | nil} | {:error, term()}
  def analyze_explain(session, plan, mode) do
    with {:ok, explain_mode} <- explain_mode_to_proto(mode) do
      request =
        build_analyze_request(session,
          analyze: {:explain, %AnalyzePlanRequest.Explain{plan: plan, explain_mode: explain_mode}}
        )

      case dispatch_unary_rpc(:analyze_plan, session, request,
             extra_metadata: %{analyze: :explain}
           ) do
        {:ok, %AnalyzePlanResponse{result: {:explain, %{explain_string: str}}} = resp} ->
          {:ok, str, resp.server_side_session_id}

        {:ok, %AnalyzePlanResponse{result: other}} ->
          {:error, {:unexpected_response, other}}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Calls `AnalyzePlan` with `TreeString` to get a tree-string representation of a plan.

  ## Options

  - `:level` — tree depth level (optional, default: server decides)
  """
  @spec analyze_tree_string(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, String.t(), String.t() | nil} | {:error, term()}
  def analyze_tree_string(session, plan, opts \\ []) do
    level = Keyword.get(opts, :level, nil)

    tree_string_msg =
      if level do
        %AnalyzePlanRequest.TreeString{plan: plan, level: level}
      else
        %AnalyzePlanRequest.TreeString{plan: plan}
      end

    request =
      build_analyze_request(session,
        analyze: {:tree_string, tree_string_msg}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :tree_string}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:tree_string, %{tree_string: str}}} = resp} ->
        {:ok, str, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `IsLocal` to check if a plan is local.
  """
  @spec analyze_is_local(SparkEx.Session.t(), Plan.t()) ::
          {:ok, boolean(), String.t() | nil} | {:error, term()}
  def analyze_is_local(session, plan) do
    request =
      build_analyze_request(session,
        analyze: {:is_local, %AnalyzePlanRequest.IsLocal{plan: plan}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :is_local}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:is_local, %{is_local: is_local}}} = resp} ->
        {:ok, is_local, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `IsStreaming` to check if a plan is streaming.
  """
  @spec analyze_is_streaming(SparkEx.Session.t(), Plan.t()) ::
          {:ok, boolean(), String.t() | nil} | {:error, term()}
  def analyze_is_streaming(session, plan) do
    request =
      build_analyze_request(session,
        analyze: {:is_streaming, %AnalyzePlanRequest.IsStreaming{plan: plan}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :is_streaming}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:is_streaming, %{is_streaming: is_streaming}}} = resp} ->
        {:ok, is_streaming, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `InputFiles` to get the input files of a plan.
  """
  @spec analyze_input_files(SparkEx.Session.t(), Plan.t()) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term()}
  def analyze_input_files(session, plan) do
    request =
      build_analyze_request(session,
        analyze: {:input_files, %AnalyzePlanRequest.InputFiles{plan: plan}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :input_files}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:input_files, %{files: files}}} = resp} ->
        {:ok, files, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `DDLParse` to parse a DDL string into a DataType.
  """
  @spec analyze_ddl_parse(SparkEx.Session.t(), String.t()) ::
          {:ok, term(), String.t() | nil} | {:error, term()}
  def analyze_ddl_parse(session, ddl_string) do
    request =
      build_analyze_request(session,
        analyze: {:ddl_parse, %AnalyzePlanRequest.DDLParse{ddl_string: ddl_string}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :ddl_parse}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:ddl_parse, %{parsed: parsed}}} = resp} ->
        {:ok, parsed, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `JsonToDDL` to convert a JSON schema string to DDL.
  """
  @spec analyze_json_to_ddl(SparkEx.Session.t(), String.t()) ::
          {:ok, String.t(), String.t() | nil} | {:error, term()}
  def analyze_json_to_ddl(session, json_string) do
    request =
      build_analyze_request(session,
        analyze: {:json_to_ddl, %AnalyzePlanRequest.JsonToDDL{json_string: json_string}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :json_to_ddl}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:json_to_ddl, %{ddl_string: ddl}}} = resp} ->
        {:ok, ddl, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `SameSemantics` to check if two plans have the same semantics.
  """
  @spec analyze_same_semantics(SparkEx.Session.t(), Plan.t(), Plan.t()) ::
          {:ok, boolean(), String.t() | nil} | {:error, term()}
  def analyze_same_semantics(session, target_plan, other_plan) do
    request =
      build_analyze_request(session,
        analyze:
          {:same_semantics,
           %AnalyzePlanRequest.SameSemantics{
             target_plan: target_plan,
             other_plan: other_plan
           }}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :same_semantics}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:same_semantics, %{result: result}}} = resp} ->
        {:ok, result, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `SemanticHash` to get the semantic hash of a plan.
  """
  @spec analyze_semantic_hash(SparkEx.Session.t(), Plan.t()) ::
          {:ok, integer(), String.t() | nil} | {:error, term()}
  def analyze_semantic_hash(session, plan) do
    request =
      build_analyze_request(session,
        analyze: {:semantic_hash, %AnalyzePlanRequest.SemanticHash{plan: plan}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :semantic_hash}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:semantic_hash, %{result: hash}}} = resp} ->
        {:ok, hash, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `Persist` to persist a relation with optional storage level.

  ## Options

  - `:storage_level` — a `Spark.Connect.StorageLevel` struct (optional)
  """
  @spec analyze_persist(SparkEx.Session.t(), Spark.Connect.Relation.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  # PySpark default: StorageLevel(True, True, False, True, 1) = MEMORY_AND_DISK_DESER
  @default_storage_level %Spark.Connect.StorageLevel{
    use_disk: true,
    use_memory: true,
    use_off_heap: false,
    deserialized: true,
    replication: 1
  }

  def analyze_persist(session, relation, opts \\ []) do
    storage_level = Keyword.get(opts, :storage_level, @default_storage_level)

    request =
      build_analyze_request(session,
        analyze:
          {:persist,
           %AnalyzePlanRequest.Persist{
             relation: relation,
             storage_level: storage_level
           }}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request, extra_metadata: %{analyze: :persist}) do
      {:ok, %AnalyzePlanResponse{result: {:persist, _}} = resp} ->
        {:ok, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `Unpersist` to unpersist a relation.

  ## Options

  - `:blocking` — whether to block until unpersisted (default: false)
  """
  @spec analyze_unpersist(SparkEx.Session.t(), Spark.Connect.Relation.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def analyze_unpersist(session, relation, opts \\ []) do
    blocking = Keyword.get(opts, :blocking, nil)

    request =
      build_analyze_request(session,
        analyze:
          {:unpersist,
           %AnalyzePlanRequest.Unpersist{
             relation: relation,
             blocking: blocking
           }}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :unpersist}
         ) do
      {:ok, %AnalyzePlanResponse{result: {:unpersist, _}} = resp} ->
        {:ok, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AnalyzePlan` with `GetStorageLevel` to get the storage level of a persisted relation.
  """
  @spec analyze_get_storage_level(SparkEx.Session.t(), Spark.Connect.Relation.t()) ::
          {:ok, StorageLevel.t(), String.t() | nil} | {:error, term()}
  def analyze_get_storage_level(session, relation) do
    request =
      build_analyze_request(session,
        analyze: {:get_storage_level, %AnalyzePlanRequest.GetStorageLevel{relation: relation}}
      )

    case dispatch_unary_rpc(:analyze_plan, session, request,
           extra_metadata: %{analyze: :get_storage_level}
         ) do
      {:ok,
       %AnalyzePlanResponse{result: {:get_storage_level, %{storage_level: storage_level}}} =
           resp} ->
        {:ok, storage_level, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, _} = error ->
        error
    end
  end

  # --- ExecutePlan RPC ---

  @doc """
  Calls `ExecutePlan` (server-streaming) and decodes the response stream.

  Returns `{:ok, result}` where result contains `:rows`, `:schema`, and
  `:server_side_session_id`.

  By default, uses reattachable execution which can recover from mid-stream
  disconnects. Pass `reattachable: false` to disable.
  """
  @spec execute_plan(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.decode_result()} | {:error, term()}
  def execute_plan(session, plan, opts \\ []) do
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    with :ok <- validate_tags(tags) do
      run_execute_plan(
        :execute_plan,
        session,
        plan,
        tags,
        operation_id,
        reattachable,
        opts,
        &ResultDecoder.decode_stream(&1, session)
      )
    end
  end

  @doc """
  Calls `ExecutePlan` and decodes the response as an `Explorer.DataFrame`.

  Enforces row and byte limits. See `ResultDecoder.decode_stream_explorer/3`.

  By default, uses reattachable execution which can recover from mid-stream
  disconnects. Pass `reattachable: false` to disable.
  """
  @spec execute_plan_explorer(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.explorer_result()} | {:error, term()}
  def execute_plan_explorer(session, plan, opts \\ []) do
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    with :ok <- validate_tags(tags) do
      run_execute_plan(
        :execute_plan_explorer,
        session,
        plan,
        tags,
        operation_id,
        reattachable,
        opts,
        &ResultDecoder.decode_stream_explorer(&1, session, opts)
      )
    end
  end

  @doc """
  Calls `ExecutePlan` and decodes the response as raw Arrow IPC payloads.
  """
  @spec execute_plan_arrow(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.arrow_result()} | {:error, term()}
  def execute_plan_arrow(session, plan, opts \\ []) do
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    with :ok <- validate_tags(tags) do
      run_execute_plan(
        :execute_plan_arrow,
        session,
        plan,
        tags,
        operation_id,
        reattachable,
        opts,
        &ResultDecoder.decode_stream_arrow(
          &1,
          session,
          Keyword.take(opts, [:max_rows, :max_bytes])
        )
      )
    end
  end

  defp run_execute_plan(
         rpc_name,
         session,
         plan,
         tags,
         operation_id,
         reattachable,
         opts,
         decode_fn
       ) do
    timeout = normalize_grpc_timeout(Keyword.get(opts, :timeout, 60_000))
    request = build_execute_request(session, plan, tags, operation_id, reattachable, opts)

    metadata = %{
      rpc: rpc_name,
      session_id: session.session_id,
      operation_id: operation_id
    }

    rpc_telemetry_span(metadata, fn ->
      if reattachable do
        execute_reattachable(session, request, operation_id, timeout, opts, decode_fn)
      else
        execute_plan_non_reattachable(session, request, timeout, opts, decode_fn)
      end
    end)
  end

  @doc """
  Executes a command plan and returns the raw gRPC response stream.

  Used for long-lived streaming operations like the listener bus where
  the caller needs to iterate over responses as they arrive.

  Returns `{:ok, stream}` where stream is an enumerable of
  `{:ok, ExecutePlanResponse.t()} | {:error, term()}` tuples.
  """
  @spec execute_plan_raw_stream(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def execute_plan_raw_stream(session, plan, opts \\ []) do
    timeout = normalize_grpc_timeout(Keyword.get(opts, :timeout, :infinity))
    request = build_raw_stream_request(session, plan, opts)

    case Stub.execute_plan(session.channel, request, timeout: timeout) do
      {:ok, stream} -> {:ok, stream}
      {:error, %GRPC.RPCError{} = error} -> {:error, Errors.from_grpc_error(error, session)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Executes a plan and returns a lazy reattachable response stream.

  The returned enumerable yields `{:ok, ExecutePlanResponse.t()} | {:error, term()}`
  tuples and transparently handles graceful-EOF reattach and retryable
  error reattach. The stream registers an asynchronous `release_until`
  checkpoint after each consumed response and `release_all` on
  completion / terminal error, mirroring PySpark's
  `ExecutePlanResponseReattachableIterator`.
  """
  @spec execute_plan_reattachable_response_stream(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def execute_plan_reattachable_response_stream(session, plan, opts \\ []) do
    timeout = normalize_grpc_timeout(Keyword.get(opts, :timeout, :infinity))
    tags = Keyword.get(opts, :tags, [])

    with :ok <- validate_tags(tags) do
      operation_id = generate_operation_id()
      request = build_execute_request(session, plan, tags, operation_id, true, opts)

      execute_reattachable(session, request, operation_id, timeout, opts, fn responses ->
        {:ok, responses}
      end)
    end
  end

  @doc """
  Executes a plan as a reattachable stream and returns a managed stream handle.

  The handle auto-releases server-side execute state when:
  - stream consumption finishes/halts
  - owner process exits
  - optional idle timeout elapses
  """
  @spec execute_plan_managed_stream(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, SparkEx.ManagedStream.t()} | {:error, term()}
  def execute_plan_managed_stream(session, plan, opts \\ []) do
    timeout = normalize_grpc_timeout(Keyword.get(opts, :timeout, :infinity))
    owner = Keyword.get(opts, :stream_owner, self())
    idle_timeout = Keyword.get(opts, :idle_timeout, nil)
    release_timeout = Keyword.get(opts, :release_execute_timeout, @release_execute_timeout)
    operation_id = generate_operation_id()
    request = build_managed_stream_request(session, plan, operation_id, opts)

    release_fun =
      Keyword.get(opts, :release_execute_fun, fn release_opts ->
        release_execute(session, operation_id, release_opts)
      end)

    # Shared closed flag: set by the controller on any close (explicit, owner
    # down, idle timeout, stream finished) and consulted by the reattach
    # machinery so a consumer blocked in the stream halts instead of
    # reattaching to / re-issuing the operation the owner just released.
    closed_flag = ManagedStream.new_closed_flag()

    # The controller owns the terminal release_all (exactly once, async, also
    # on owner-down). The inner reattach stream therefore only issues the
    # per-response `release_until` checkpoints; its release_all calls (which
    # carry no `until_response_id`) are no-ops so an early halt neither
    # blocks the consumer on a synchronous release nor double-releases.
    inner_release_fun = fn
      [] -> {:ok, nil}
      release_opts -> release_fun.(release_opts)
    end

    reattach_opts =
      opts
      |> Keyword.put(:release_execute_fun, inner_release_fun)
      |> Keyword.put(:stream_closed_fun, fn -> ManagedStream.closed?(closed_flag) end)

    # T-09: the request advertises `reattachable: true`, so the consumed stream
    # must actually reattach on graceful EOF / transient transport loss instead
    # of handing the raw initial gRPC stream to the caller. Route through the
    # same reattach machinery as execute_plan/3; the identity decode_fn keeps
    # the lazy `{:ok, resp} | {:error, term}` shape the managed-stream
    # consumers expect.
    case execute_reattachable(session, request, operation_id, timeout, reattach_opts, &{:ok, &1}) do
      {:ok, stream} ->
        case ManagedStream.new(stream,
               owner: owner,
               idle_timeout: idle_timeout,
               release_fun: release_fun,
               release_timeout: release_timeout,
               closed_flag: closed_flag
             ) do
          {:ok, managed_stream} ->
            {:ok, managed_stream}

          {:error, _} = error ->
            _ = release_fun.([])
            error
        end

      {:error, _} = error ->
        error
    end
  end

  # --- Config RPCs ---

  @typedoc """
  A config key/value type that can be coerced to a string before send.
  Strings, booleans, numbers, and non-nil atoms are accepted; anything
  else raises `ArgumentError` from `coerce_config_string/1`.
  """
  @type config_value :: String.t() | boolean() | integer() | float() | atom()

  @doc """
  Sets Spark configuration key-value pairs.

  Keys/values may be strings, booleans, numbers, or non-nil atoms; non-string
  values are coerced to strings before send. Raises `ArgumentError` for
  unsupported value types.
  """
  @spec config_set(SparkEx.Session.t(), [{config_value(), config_value()}]) ::
          {:ok, String.t() | nil} | {:error, term()}
  def config_set(session, pairs) do
    kv_pairs =
      Enum.map(pairs, fn {k, v} ->
        %KeyValue{key: coerce_config_string(k), value: coerce_config_string(v)}
      end)

    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:set, %ConfigRequest.Set{pairs: kv_pairs}}
        }
      )

    case dispatch_unary_rpc(:config, session, request, extra_metadata: %{operation: :set}) do
      {:ok, %ConfigResponse{} = resp} ->
        log_config_warnings(resp)
        {:ok, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets Spark configuration values for the given keys.

  Returns a list of `{key, value}` pairs.
  """
  @spec config_get(SparkEx.Session.t(), [config_value()]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get, %ConfigRequest.Get{keys: Enum.map(keys, &coerce_config_string/1)}}
        }
      )

    case dispatch_unary_rpc(:config, session, request, extra_metadata: %{operation: :get}) do
      {:ok, %ConfigResponse{pairs: pairs} = resp} ->
        log_config_warnings(resp)
        result = Enum.map(pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets Spark configuration values with defaults for the given key-value pairs.

  When the config key has no value set, the provided default is returned.
  Returns a list of `{key, value}` pairs.
  """
  @spec config_get_with_default(SparkEx.Session.t(), [{config_value(), config_value()}]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get_with_default(session, pairs) do
    kv_pairs =
      Enum.map(pairs, fn {k, v} ->
        # `value` is `optional string`; leaving it unset (nil) means
        # "use the server's built-in default" — distinct from the empty
        # string. PySpark passes None through unchanged.
        encoded_value = if is_nil(v), do: nil, else: coerce_config_string(v)
        %KeyValue{key: coerce_config_string(k), value: encoded_value}
      end)

    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get_with_default, %ConfigRequest.GetWithDefault{pairs: kv_pairs}}
        }
      )

    case dispatch_unary_rpc(:config, session, request,
           extra_metadata: %{operation: :get_with_default}
         ) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        log_config_warnings(resp)
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets optional Spark configuration values for the given keys.

  Returns a list of `{key, value}` pairs. When a key is not set, the value
  is nil (unlike `config_get/2` which may raise on the server).
  """
  @spec config_get_option(SparkEx.Session.t(), [config_value()]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get_option(session, keys, opts \\ []) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type:
            {:get_option, %ConfigRequest.GetOption{keys: Enum.map(keys, &coerce_config_string/1)}}
        }
      )

    rpc_opts = Keyword.take(opts, [:timeout]) ++ [extra_metadata: %{operation: :get_option}]

    case dispatch_unary_rpc(:config, session, request, rpc_opts) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        log_config_warnings(resp)
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets all Spark configuration values, optionally filtered by prefix.

  Returns a list of `{key, value}` pairs.
  """
  @spec config_get_all(SparkEx.Session.t(), String.t() | nil) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get_all(session, prefix \\ nil) do
    get_all_msg =
      if prefix do
        %ConfigRequest.GetAll{prefix: prefix}
      else
        %ConfigRequest.GetAll{}
      end

    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get_all, get_all_msg}
        }
      )

    case dispatch_unary_rpc(:config, session, request, extra_metadata: %{operation: :get_all}) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        log_config_warnings(resp)
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Unsets Spark configuration values for the given keys.
  """
  @spec config_unset(SparkEx.Session.t(), [config_value()]) ::
          {:ok, String.t() | nil} | {:error, term()}
  def config_unset(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:unset, %ConfigRequest.Unset{keys: Enum.map(keys, &coerce_config_string/1)}}
        }
      )

    case dispatch_unary_rpc(:config, session, request, extra_metadata: %{operation: :unset}) do
      {:ok, %ConfigResponse{} = resp} ->
        log_config_warnings(resp)
        {:ok, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Checks whether the given configuration keys are modifiable at runtime.

  Returns a list of `{key, boolean_or_nil}` pairs: `true` if modifiable, `false` if not,
  `nil` if the server returned an empty or unrecognised value for that key.
  """
  @spec config_is_modifiable(SparkEx.Session.t(), [config_value()]) ::
          {:ok, [{String.t(), boolean() | nil}], String.t() | nil} | {:error, term()}
  def config_is_modifiable(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type:
            {:is_modifiable,
             %ConfigRequest.IsModifiable{keys: Enum.map(keys, &coerce_config_string/1)}}
        }
      )

    case dispatch_unary_rpc(:config, session, request,
           extra_metadata: %{operation: :is_modifiable}
         ) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        log_config_warnings(resp)
        result = parse_is_modifiable_pairs(resp_pairs)
        {:ok, result, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  defp parse_is_modifiable_pairs(pairs) do
    Enum.map(pairs, fn %KeyValue{key: k, value: v} ->
      {k, parse_is_modifiable_value(k, v)}
    end)
  end

  @doc false
  def parse_is_modifiable_value(_key, nil), do: nil

  @doc false
  def parse_is_modifiable_value(key, value) when is_binary(value) do
    case value do
      "" ->
        nil

      "true" ->
        true

      "false" ->
        false

      _ ->
        Logger.warning(
          "Spark Connect config is_modifiable returned unexpected value for #{inspect(key)}: #{inspect(value)}"
        )

        nil
    end
  end

  defp coerce_config_string(value) when is_binary(value), do: value
  defp coerce_config_string(value) when is_boolean(value), do: to_string(value)
  defp coerce_config_string(value) when is_integer(value), do: Integer.to_string(value)
  defp coerce_config_string(value) when is_float(value), do: Float.to_string(value)

  defp coerce_config_string(nil) do
    raise ArgumentError,
          "config key/value cannot be nil; use config_unset/2 to remove a configuration key"
  end

  defp coerce_config_string(value) when is_atom(value), do: Atom.to_string(value)

  defp coerce_config_string(value) do
    raise ArgumentError,
          "expected config key/value to be a string, boolean, integer, float, or atom, got: #{inspect(value)}"
  end

  defp log_config_warnings(%ConfigResponse{warnings: warnings}) when is_list(warnings) do
    Enum.each(warnings, fn warning ->
      Logger.warning("Spark Connect config warning: #{warning}")
    end)
  end

  defp log_config_warnings(_), do: :ok

  # --- Session Lifecycle RPCs ---

  @doc """
  Calls `CloneSession` and returns cloned session identifiers.
  """
  @spec clone_session(SparkEx.Session.t(), String.t() | nil) ::
          {:ok,
           %{
             new_session_id: String.t(),
             new_server_side_session_id: String.t() | nil,
             source_server_side_session_id: String.t() | nil
           }}
          | {:error, term()}
  def clone_session(session, new_session_id \\ nil)
  def clone_session(session, nil), do: do_clone_session(session, nil)

  def clone_session(session, new_session_id) when is_binary(new_session_id) do
    if SparkEx.Internal.UUID.valid_uuid?(new_session_id) do
      do_clone_session(session, new_session_id)
    else
      {:error, {:invalid_new_session_id, new_session_id}}
    end
  end

  def clone_session(_session, new_session_id),
    do: {:error, {:invalid_new_session_id, new_session_id}}

  defp do_clone_session(session, new_session_id) do
    request = %CloneSessionRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      new_session_id: new_session_id
    }

    case dispatch_unary_rpc(:clone_session, session, request) do
      {:ok, %CloneSessionResponse{} = resp} ->
        cond do
          not is_binary(resp.new_session_id) or resp.new_session_id == "" ->
            {:error, {:invalid_clone_response, :missing_new_session_id}}

          not SparkEx.Internal.UUID.valid_uuid?(resp.new_session_id) ->
            {:error, {:invalid_clone_response, {:not_a_uuid, resp.new_session_id}}}

          not is_nil(new_session_id) and resp.new_session_id != new_session_id ->
            {:error,
             {:invalid_clone_response,
              {:new_session_id_mismatch, requested: new_session_id, got: resp.new_session_id}}}

          true ->
            {:ok,
             %{
               new_session_id: resp.new_session_id,
               new_server_side_session_id: blank_to_nil(resp.new_server_side_session_id),
               source_server_side_session_id: blank_to_nil(resp.server_side_session_id)
             }}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `ReleaseSession` to release the server-side session.

  ## Options

    * `:allow_reconnect` — when `true`, signals that the server should
      keep the session reachable for a brief reconnect window after
      release. Mirrors the `allow_reconnect` field on the proto request.
      Defaults to `false`.

  A successful release whose response was lost mid-flight, followed by a
  retry, will hit `INVALID_HANDLE.SESSION_NOT_FOUND` on the second attempt
  because the server already disposed of the session. We treat that
  variant as a successful release (the desired terminal state is reached)
  so callers don't surface a spurious error.
  """
  @spec release_session(SparkEx.Session.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def release_session(session, opts \\ []) do
    allow_reconnect = Keyword.get(opts, :allow_reconnect, false)

    request = %ReleaseSessionRequest{
      session_id: session.session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      allow_reconnect: allow_reconnect
    }

    case dispatch_unary_rpc(:release_session, session, request) do
      {:ok, %ReleaseSessionResponse{} = resp} ->
        {:ok, resp.server_side_session_id}

      {:error, %SparkEx.Error.Remote{} = remote} = error ->
        if benign_release_session_error?(remote) do
          {:ok, session.server_side_session_id}
        else
          error
        end

      {:error, _} = error ->
        error
    end
  end

  defp benign_release_session_error?(%SparkEx.Error.Remote{error_class: error_class})
       when error_class in [
              "INVALID_HANDLE.SESSION_NOT_FOUND",
              "INVALID_HANDLE.SESSION_CLOSED"
            ],
       do: true

  defp benign_release_session_error?(%SparkEx.Error.Remote{grpc_status: @status_not_found}),
    do: true

  defp benign_release_session_error?(_), do: false

  # --- Interrupt RPC ---

  @doc """
  Calls `Interrupt` to cancel running operations.

  ## Interrupt types

  - `:all` — interrupt all running operations
  - `{:tag, tag}` — interrupt operations matching the given tag
  - `{:operation_id, id}` — interrupt a specific operation by ID
  """
  @spec interrupt(
          SparkEx.Session.t() | SparkEx.Internal.SessionSnapshot.snapshot(),
          :all | {:tag, String.t()} | {:operation_id, String.t()}
        ) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term()}
  def interrupt(session, type) do
    with {:ok, type} <- validate_interrupt_type(type) do
      request = build_interrupt_request(session, type)

      case dispatch_unary_rpc(:interrupt, session, request,
             extra_metadata: %{interrupt_type: type}
           ) do
        {:ok, %InterruptResponse{} = resp} ->
          {:ok, resp.interrupted_ids, resp.server_side_session_id}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc false
  @spec get_operation_statuses(
          SparkEx.Session.t() | SparkEx.Internal.SessionSnapshot.snapshot(),
          [String.t()],
          keyword()
        ) ::
          {:ok, GetStatusResponse.t(), String.t()} | {:error, term()}
  def get_operation_statuses(session, operation_ids \\ [], opts \\ []) do
    if is_list(operation_ids) and Enum.all?(operation_ids, &is_binary/1) do
      request = %GetStatusRequest{
        session_id: session.session_id,
        user_context: UserContextExtensions.build_user_context(session.user_id),
        client_type: session.client_type,
        client_observed_server_side_session_id: session.server_side_session_id,
        operation_status: %GetStatusRequest.OperationStatusRequest{
          operation_ids: operation_ids,
          extensions: Keyword.get(opts, :operation_extensions, [])
        },
        extensions: Keyword.get(opts, :extensions, [])
      }

      case dispatch_unary_rpc(:get_status, session, request, opts) do
        {:ok, %GetStatusResponse{} = response} ->
          {:ok, response, response.server_side_session_id}

        {:error, _} = error ->
          error
      end
    else
      {:error, {:invalid_operation_ids, operation_ids}}
    end
  end

  defp validate_interrupt_type(:all), do: {:ok, :all}

  defp validate_interrupt_type({:tag, tag}) when is_binary(tag) do
    case validate_tag(tag) do
      :ok -> {:ok, {:tag, tag}}
      {:error, reason} -> {:error, {:invalid_tag, reason, tag}}
    end
  end

  defp validate_interrupt_type({:operation_id, id}) when is_binary(id) and id != "" do
    {:ok, {:operation_id, id}}
  end

  defp validate_interrupt_type(other), do: {:error, {:invalid_interrupt_type, other}}

  @doc false
  @spec validate_tag(term()) :: :ok | {:error, atom()}
  def validate_tag(tag) when is_binary(tag) do
    cond do
      tag == "" -> {:error, :empty}
      String.contains?(tag, ",") -> {:error, :contains_comma}
      true -> :ok
    end
  end

  def validate_tag(_), do: {:error, :not_a_string}

  @doc false
  @spec validate_tags(term()) :: :ok | {:error, term()}
  def validate_tags(tags) when is_list(tags) do
    Enum.reduce_while(tags, :ok, fn tag, :ok ->
      case validate_tag(tag) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_tag, reason, tag}}}
      end
    end)
  end

  def validate_tags(other), do: {:error, {:invalid_tags, other}}

  defp build_interrupt_request(session, :all) do
    %InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_ALL
    }
  end

  defp build_interrupt_request(session, {:tag, tag}) when is_binary(tag) do
    %InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_TAG,
      interrupt: {:operation_tag, tag}
    }
  end

  defp build_interrupt_request(session, {:operation_id, id}) when is_binary(id) do
    %InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_OPERATION_ID,
      interrupt: {:operation_id, id}
    }
  end

  # --- Artifact RPCs ---

  @doc """
  Calls `ArtifactStatus` to check existence of artifacts on the server.

  Returns a map of artifact name to boolean (exists or not). Use
  `artifact_status_full/2` to retrieve the full `ArtifactStatus` struct
  for forward-compatibility with future proto fields.
  """
  @spec artifact_status(SparkEx.Session.t(), [String.t()]) ::
          {:ok, %{String.t() => boolean()}, String.t() | nil} | {:error, term()}
  def artifact_status(session, names) do
    case artifact_status_full(session, names) do
      {:ok, statuses, session_id} ->
        bool_map = Map.new(statuses, fn {name, status} -> {name, status.exists} end)
        {:ok, bool_map, session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Like `artifact_status/2` but returns the full `ArtifactStatus` proto
  struct for each artifact, preserving any fields the server might add
  in future protocol revisions.
  """
  @spec artifact_status_full(SparkEx.Session.t(), [String.t()]) ::
          {:ok, %{String.t() => ArtifactStatusesResponse.ArtifactStatus.t()}, String.t() | nil}
          | {:error, term()}
  def artifact_status_full(session, names) do
    request = %ArtifactStatusesRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      names: names
    }

    case dispatch_unary_rpc(:artifact_status, session, request) do
      {:ok, %ArtifactStatusesResponse{} = resp} ->
        {:ok, resp.statuses || %{}, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Calls `AddArtifacts` (client-streaming) to upload artifacts to the server.

  Artifacts are provided as a list of `{name, data}` tuples where `data`
  is either:

    * a `binary()` — the artifact contents in memory; or
    * `{:file, path, size}` — the path is opened and streamed in
      chunks at upload time, so peak memory stays at chunk size
      regardless of file size.

  Small artifacts are batched; large artifacts are streamed in chunks
  (`begin_chunk` + `chunk` payloads).

  Returns a list of `{name, crc_successful?}` tuples.
  """
  @spec add_artifacts(SparkEx.Session.t(), [
          {String.t(), binary() | {:file, Path.t(), non_neg_integer()}}
        ]) ::
          {:ok, [{String.t(), boolean()}], String.t() | nil} | {:error, term()}
  def add_artifacts(session, artifacts) when is_list(artifacts) do
    # Validate without raising so malformed input is returned as an error tuple
    # instead of crashing the calling SparkEx.Session GenServer (V02_BLOCKERS H1).
    with {:ok, artifacts} <- validate_artifacts(artifacts) do
      do_add_artifacts(session, artifacts)
    end
  end

  def add_artifacts(_session, other) do
    {:error,
     {:invalid_artifacts,
      "expected a list of {name, binary} or {name, {:file, path, size}} tuples, got: #{inspect(other)}"}}
  end

  defp do_add_artifacts(session, artifacts) do
    metadata = %{rpc: :add_artifacts, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
      case artifacts do
        [] ->
          {:ok, [], session.server_side_session_id}

        _ ->
          # Each retry attempt rebuilds the gRPC stream from scratch by
          # re-evaluating the lazy `stream_artifact_requests/3` enum:
          # reusing a gRPC stream after a transient failure is not
          # supported by the library, and the request enumerable is
          # pure data + lazy file IO so reconstruction is cheap.
          retry_with_backoff(
            fn ->
              stream =
                session.channel
                |> Stub.add_artifacts()
                |> send_artifact_request_stream(
                  stream_artifact_requests(session, artifacts, @artifact_chunk_size)
                )

              handle_add_artifacts_response(GRPC.Stub.recv(stream), session)
            end,
            session: session
          )
      end
    end)
  end

  @doc false
  @spec build_add_artifacts_requests(
          SparkEx.Session.t(),
          [{String.t(), binary() | {:file, Path.t(), non_neg_integer()}}],
          pos_integer()
        ) :: [AddArtifactsRequest.t()]
  def build_add_artifacts_requests(session, artifacts, chunk_size \\ @artifact_chunk_size)
      when is_list(artifacts) and is_integer(chunk_size) and chunk_size > 0 do
    session
    |> stream_artifact_requests(validate_artifacts!(artifacts), chunk_size)
    |> Enum.to_list()
  end

  defp stream_artifact_requests(session, artifacts, chunk_size) do
    artifacts
    |> producer_stream(chunk_size)
    |> Stream.flat_map(fn
      {:batch, entries} ->
        [build_batch_request(session, entries)]

      {:binary_chunks, name, data} ->
        build_chunked_requests(session, name, data, chunk_size)

      {:file_chunks, name, path, size} ->
        file_chunk_request_stream(session, name, path, size, chunk_size)
    end)
  end

  defp producer_stream(artifacts, chunk_size) do
    Stream.transform(
      Stream.concat(artifacts, [:__flush__]),
      {[], 0},
      fn
        :__flush__, {batch, _bs} ->
          if batch == [] do
            {[], {[], 0}}
          else
            {[{:batch, batch}], {[], 0}}
          end

        entry, {batch, batch_size} ->
          size = artifact_size(entry)

          cond do
            size > chunk_size ->
              flush_emit = if batch == [], do: [], else: [{:batch, batch}]
              {flush_emit ++ [large_producer(entry)], {[], 0}}

            batch_size + size > chunk_size and batch != [] ->
              {[{:batch, batch}], {[materialize_for_batch(entry)], size}}

            true ->
              {[], {[materialize_for_batch(entry) | batch], batch_size + size}}
          end
      end
    )
  end

  defp artifact_size({_name, data}) when is_binary(data), do: byte_size(data)
  defp artifact_size({_name, {:file, _path, size}}), do: size

  defp large_producer({name, data}) when is_binary(data), do: {:binary_chunks, name, data}

  defp large_producer({name, {:file, path, size}}), do: {:file_chunks, name, path, size}

  defp materialize_for_batch({name, data}) when is_binary(data), do: {name, data}

  defp materialize_for_batch({name, {:file, path, _size}}), do: {name, File.read!(path)}

  defp send_artifact_request_stream(stream, request_stream) do
    {final_stream, prev} =
      Enum.reduce(request_stream, {stream, nil}, fn request, {acc_stream, prev_req} ->
        case prev_req do
          nil ->
            {acc_stream, request}

          earlier ->
            {GRPC.Stub.send_request(acc_stream, earlier, end_stream: false), request}
        end
      end)

    case prev do
      nil -> final_stream
      last -> GRPC.Stub.send_request(final_stream, last, end_stream: true)
    end
  end

  # --- ReattachExecute / ReleaseExecute RPCs ---

  @doc """
  Calls `ReattachExecute` (server-streaming) to resume a reattachable operation.

  Returns `{:ok, stream}` with the response stream starting after `last_response_id`.
  """
  @spec reattach_execute(SparkEx.Session.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def reattach_execute(session, operation_id, last_response_id, opts \\ []) do
    timeout = normalize_grpc_timeout(Keyword.get(opts, :timeout, 60_000))

    request = %ReattachExecuteRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      operation_id: operation_id,
      last_response_id: last_response_id
    }

    # Retry only the initial RPC handshake; once the server-streaming
    # response begins, the outer `collect_with_reattach` is responsible
    # for mid-stream recovery and must not be double-wrapped.
    retry_with_backoff(
      fn ->
        case Stub.reattach_execute(session.channel, request, timeout: timeout) do
          {:ok, stream} ->
            {:ok, stream}

          {:error, %GRPC.RPCError{} = error} ->
            {:error, Errors.decode_grpc_error(error)}

          {:error, reason} ->
            {:error, reason}
        end
      end,
      session: session,
      policy_type: :reattach
    )
  end

  @doc """
  Calls `ReleaseExecute` to release server-side cached results for an operation.

  Uses `release_all` by default. Pass `until_response_id: id` to release
  only up to a specific response.

  Returns `{:ok, %{server_side_session_id: id, operation_id: op_id}}` on
  success. Per `base.proto`, `operation_id` is `""` if the server could
  not find the operation to release (e.g. it was concurrently released);
  callers that care about that distinction should branch on the empty
  string.
  """
  @spec release_execute(SparkEx.Session.t(), String.t(), keyword()) ::
          {:ok, %{server_side_session_id: String.t() | nil, operation_id: String.t()}}
          | {:error, term()}
  def release_execute(session, operation_id, opts \\ []) do
    until_response_id = Keyword.get(opts, :until_response_id, nil)
    timeout = Keyword.get(opts, :timeout, @release_execute_timeout)

    release =
      if until_response_id do
        {:release_until, %ReleaseExecuteRequest.ReleaseUntil{response_id: until_response_id}}
      else
        {:release_all, %ReleaseExecuteRequest.ReleaseAll{}}
      end

    request = %ReleaseExecuteRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      operation_id: operation_id,
      release: release
    }

    case dispatch_unary_rpc(:release_execute, session, request,
           timeout: timeout,
           extra_metadata: %{operation_id: operation_id}
         ) do
      {:ok, %ReleaseExecuteResponse{} = resp} ->
        {:ok,
         %{
           server_side_session_id: resp.server_side_session_id,
           operation_id: resp.operation_id || ""
         }}

      {:error, _} = error ->
        error
    end
  end

  # --- Reattachable execution helpers ---

  defp execute_reattachable(session, request, operation_id, timeout, opts, decode_fn) do
    policy_type = Keyword.get(opts, :reattach_policy, :reattach)
    policy = RetryPolicyRegistry.policy_for(session, policy_type)

    policy =
      case Keyword.fetch(opts, :reattach_retries) do
        {:ok, retries} -> %{policy | max_retries: retries}
        :error -> policy
      end

    release_execute_timeout =
      Keyword.get(opts, :release_execute_timeout, @release_execute_timeout)

    execute_stream_fun =
      Keyword.get(opts, :execute_stream_fun, fn req, req_timeout ->
        execute_plan_stream(session, req, req_timeout)
      end)

    reattach_stream_fun =
      Keyword.get(opts, :reattach_stream_fun, fn last_response_id ->
        reattach_execute(session, operation_id, last_response_id, timeout: timeout)
      end)

    release_execute_fun =
      Keyword.get(opts, :release_execute_fun, fn release_opts ->
        release_execute(session, operation_id, release_opts)
      end)

    stream_closed_fun = Keyword.get(opts, :stream_closed_fun, fn -> false end)

    # T-08: a retryable failure of the initial ExecutePlan handshake must NOT
    # be retried by re-sending the identical request: grpc-elixir's do_call is
    # send_request |> recv, so the server may already have registered the
    # operation and Spark 3.5 answers a re-sent operation_id with the
    # non-retryable INVALID_HANDLE.OPERATION_ALREADY_EXISTS (leaking the
    # execution). PySpark (reattach.py _call_iter) re-issues ReattachExecute
    # instead and only falls back to ExecutePlan on OPERATION_NOT_FOUND. We
    # get the same behaviour by feeding the error into the reattach state
    # machine as a one-element initial stream: handle_inner_error ->
    # perform_reattach(nil) -> handle_invalid_handle nil-branch (fresh
    # ExecutePlan). Non-retryable failures are returned immediately.
    initial_result =
      case execute_stream_fun.(request, timeout) do
        {:ok, _} = ok ->
          ok

        {:error, %GRPC.RPCError{} = grpc_error} ->
          # T-35: decode only (no FetchErrorDetails RPC) so a retryable
          # handshake failure costs nothing extra; enrich lazily on the
          # terminal error we hand back to the caller.
          remote = Errors.decode_grpc_error(grpc_error)

          if retryable_error?(remote),
            do: {:ok, [{:error, grpc_error}]},
            else: {:error, Errors.enrich(remote, session)}

        {:error, _} = error ->
          error
      end

    case initial_result do
      {:ok, initial_stream} ->
        ctx = %{
          # Consulted before every reattach / fresh ExecutePlan so a stream
          # whose owner released it (ManagedStream.close/1) halts instead of
          # resurrecting the cancelled operation (T-09 review).
          stream_closed_fun: stream_closed_fun,
          session: session,
          request: request,
          execute_stream_fun: execute_stream_fun,
          reattach_stream_fun: reattach_stream_fun,
          release_execute_fun: release_execute_fun,
          operation_id: operation_id,
          timeout: timeout,
          policy: policy,
          release_execute_timeout: release_execute_timeout,
          # Per-stream atomic counter that bounds the number of concurrent
          # `release_until(...)` checkpoint tasks. Checkpoints are monotonic
          # — dropping intermediate ids is safe because a later checkpoint
          # subsumes earlier ones, and terminal release_all on completion
          # covers anything missed.
          release_checkpoint_counter: :counters.new(1, [:atomics])
        }

        response_stream = build_reattachable_response_stream(ctx, initial_stream)
        decode_fn.(response_stream)

      {:error, _} = error ->
        error
    end
  end

  # Builds a lazy stream of `{:ok, ExecutePlanResponse.t()} | {:error, term()}`
  # that transparently handles reattach/retry semantics. Responses are yielded
  # one at a time so decoders can short-circuit (e.g. on max_bytes) without
  # buffering the entire upstream into memory first.
  defp build_reattachable_response_stream(ctx, initial_stream) do
    Stream.resource(
      fn ->
        %{
          ctx: ctx,
          iter: start_iter(initial_stream),
          attempt: 0,
          last_response_id: nil,
          result_complete?: false,
          emitted_count: 0,
          # Tracks consecutive graceful-EOF reattaches that did not yield a
          # new response. Used purely as an anti-spin backoff signal: it never
          # consumes the retry budget (PySpark reattaches indefinitely on
          # graceful EOF, reattach.py:175-188) — see do_perform_reattach.
          empty_eof_streak: 0
        }
      end,
      &reattach_stream_step/1,
      &reattach_stream_finalize/1
    )
  end

  # Upper bound on how far the zero-progress graceful-EOF streak escalates the
  # anti-spin backoff. The streak itself never charges the retry budget
  # (FABLE-38); this only caps the backoff exponent so a long quiet stream
  # settles at the policy's max backoff instead of growing the exponent
  # unboundedly.
  @empty_eof_streak_backoff_cap 3

  # T-42: ResultComplete (or a terminal error) is the end of the operation.
  # PySpark's reattach iterator stops there too; anything the server might
  # still have buffered after it is not part of the result and must not be
  # yielded, so halt without pulling from the underlying stream again.
  defp reattach_stream_step(%{result_complete?: true} = state), do: {:halt, state}

  defp reattach_stream_step(state) do
    case pull_iter(state.iter) do
      {:value, {:ok, %ExecutePlanResponse{} = resp}, new_iter} ->
        new_id = response_id_or_nil(resp.response_id) || state.last_response_id
        complete? = match?({:result_complete, _}, resp.response_type)

        new_state = %{
          state
          | iter: new_iter,
            last_response_id: new_id,
            result_complete?: complete?,
            emitted_count: state.emitted_count + 1,
            empty_eof_streak: 0,
            # PySpark constructs a fresh Retrying per consumed response
            # (reattach.py:159), so the retry budget applies per fetch, not for
            # the whole stream lifetime. Reset the attempt counter on progress
            # so a long-lived stream surviving many *spaced-out* transient blips
            # never exhausts a lifetime budget, and the next blip's backoff
            # restarts from initial_backoff_ms rather than the pinned cap
            # (FABLE-11).
            attempt: 0
        }

        # Per-response release: PySpark's reattach iterator fires
        # release_until(last_response_id) after each consumed response (and
        # release_all on result_complete). Mirror that here so the server
        # can drop its retry buffer as the client makes progress.
        new_state =
          if complete? do
            fire_release_all(
              new_state.ctx.release_execute_fun,
              new_state.ctx.release_execute_timeout
            )

            new_state
          else
            fire_release_checkpoint(
              new_state.ctx.release_execute_fun,
              new_id,
              new_state.ctx.release_execute_timeout,
              new_state.ctx.release_checkpoint_counter
            )

            new_state
          end

        {[{:ok, resp}], new_state}

      {:value, {:error, error}, _new_iter} ->
        handle_inner_error(state, error)

      :done ->
        handle_graceful_eof(state)
    end
  end

  defp reattach_stream_finalize(state) do
    # PySpark's reattach iterator calls _release_all() in close() even after
    # an inline release on result_complete / terminal error — the server
    # is idempotent and benign-not-found is treated as success. Always run
    # release_execute_best_effort on finalize so timeout / error telemetry
    # remains observable for callers that rely on it.
    %{ctx: ctx} = state
    release_execute_best_effort(ctx.release_execute_fun, [], ctx.release_execute_timeout)
  end

  defp handle_graceful_eof(state) do
    # PySpark does NOT release-until on graceful EOF; reattach drives the
    # next batch. Server holds the retry buffer until the next consumed
    # response or release_all.
    perform_reattach(state, {:graceful_eof, nil})
  end

  defp handle_inner_error(state, %GRPC.RPCError{} = grpc_error) do
    remote = Errors.decode_grpc_error(grpc_error)

    if retryable_error?(remote) do
      # No pre-reattach release: reattach replays from last_response_id;
      # releasing first would tell the server to drop the very buffer the
      # reattach is about to read. PySpark only releases on consumed
      # responses or terminal completion.
      perform_reattach(state, {:transient_error, grpc_error})
    else
      emit_terminal_error(state, Errors.enrich(remote, state.ctx.session))
    end
  end

  defp handle_inner_error(state, other) do
    emit_terminal_error(state, other)
  end

  defp emit_terminal_error(state, error) do
    # On terminal error PySpark calls _release_all() to drop the server-side
    # buffer. We do the same and mark result_complete? so downstream halts.
    fire_release_all(state.ctx.release_execute_fun, state.ctx.release_execute_timeout)
    {[{:error, error}], %{state | result_complete?: true}}
  end

  # T-19: `Task.Supervisor.start_child/2` and `async_nolink/2` exit with
  # `{:noproc, _}` once SparkEx.TaskSupervisor has stopped (app shutdown or a
  # caller outliving the application). Release RPCs are best-effort, so a
  # missing supervisor must degrade to a reported failure, not crash the
  # stream consumer mid-teardown.
  @doc false
  @spec start_supervised_task((-> term())) :: {:ok, pid()} | {:error, term()}
  def start_supervised_task(fun) when is_function(fun, 0) do
    Task.Supervisor.start_child(SparkEx.TaskSupervisor, fun)
  catch
    :exit, {:noproc, _} -> {:error, :noproc}
  end

  @doc false
  @spec async_nolink_supervised((-> term())) :: {:ok, Task.t()} | {:error, :noproc}
  def async_nolink_supervised(fun) when is_function(fun, 0) do
    {:ok, Task.Supervisor.async_nolink(SparkEx.TaskSupervisor, fun)}
  catch
    :exit, {:noproc, _} -> {:error, :noproc}
  end

  defp fire_release_all(release_execute_fun, timeout_ms) do
    start_supervised_task(fn ->
      task =
        Task.async(fn ->
          try do
            release_execute_fun.([])
          catch
            _, _ -> :ok
          end
        end)

      _ = Task.yield(task, timeout_ms) || Task.shutdown(task, @shutdown_grace_ms)
      :ok
    end)

    :ok
  end

  # Graceful EOF (server ended the stream without ResponseComplete) is NOT a
  # failure: PySpark reattaches immediately and indefinitely until either a new
  # response or ResponseComplete arrives (reattach.py:175-188) — no sleep, no
  # budget. We keep a small bounded anti-spin sleep (see do_perform_reattach)
  # but never charge the retry budget nor surface :reattach_incomplete_result
  # for graceful EOFs (FABLE-38). The retry budget applies only to transient
  # errors, which raise the max-retries error as before.
  defp perform_reattach(state, reason) do
    cond do
      state.ctx.stream_closed_fun.() ->
        # The owner released the operation; do not reattach to (or re-issue)
        # something the user just cancelled.
        {:halt, state}

      match?({:graceful_eof, _}, reason) ->
        do_perform_reattach(state, reason)

      state.attempt >= state.ctx.policy.max_retries ->
        reattach_max_retries_error(state)

      true ->
        do_perform_reattach(state, reason)
    end
  end

  defp reattach_max_retries_error(state) do
    error =
      {:reattach_incomplete_result,
       %{
         retries_attempted: state.attempt,
         last_response_id: state.last_response_id,
         responses_received: state.emitted_count
       }}

    {[{:error, error}], %{state | result_complete?: true}}
  end

  defp do_perform_reattach(state, reason) do
    %{ctx: ctx, attempt: attempt} = state
    %{policy: policy, operation_id: operation_id, session: session} = ctx

    {telemetry_metadata, server_retry_delay_ms} =
      reattach_telemetry_info(reason, operation_id, state)

    :telemetry.execute(
      [:spark_ex, :reattach, :attempt],
      %{attempt: attempt + 1},
      telemetry_metadata
    )

    # Backoff escalates with the larger of `attempt` and the (capped)
    # graceful-EOF streak. Without folding the streak in, repeated graceful-EOF
    # reattaches would all sleep for `initial_backoff_ms` (since `attempt` stays
    # at 0 — graceful EOF never charges the budget) and hammer the server at a
    # constant rate. The streak is capped at @empty_eof_streak_backoff_cap so a
    # long quiet stream settles at the policy max backoff and reattaches forever
    # without ever failing (FABLE-38).
    eof_backoff = min(state.empty_eof_streak, @empty_eof_streak_backoff_cap)
    backoff_attempt = max(attempt, eof_backoff)
    sleep_ms = backoff_with_retry_info(backoff_attempt, policy, server_retry_delay_ms)
    policy.sleep_fun.(sleep_ms)

    {next_attempt, next_streak} =
      case reason do
        {:graceful_eof, _} ->
          # PySpark reattaches indefinitely on graceful EOF (reattach.py:175-188):
          # never charge the retry budget. The growing streak only feeds the
          # anti-spin backoff above (FABLE-38); attempt stays put.
          {attempt, state.empty_eof_streak + 1}

        _ ->
          {attempt + 1, 0}
      end

    case ctx.reattach_stream_fun.(state.last_response_id) do
      {:ok, new_stream} ->
        new_state = %{
          state
          | iter: start_iter(new_stream),
            attempt: next_attempt,
            empty_eof_streak: next_streak
        }

        reattach_stream_step(new_state)

      {:error,
       %SparkEx.Error.Remote{error_class: "INVALID_CURSOR.RESPONSE_ALREADY_RECEIVED"} = remote} ->
        {[{:error, remote}], %{state | result_complete?: true}}

      {:error, %SparkEx.Error.Remote{} = remote}
      when remote.error_class in [
             "INVALID_HANDLE.OPERATION_NOT_FOUND",
             "INVALID_HANDLE.SESSION_NOT_FOUND"
           ] ->
        handle_invalid_handle(state, remote, session, operation_id, next_attempt, ctx)

      {:error, _} = err ->
        {[err], %{state | result_complete?: true}}
    end
  end

  defp reattach_telemetry_info(
         {:transient_error, %GRPC.RPCError{status: status} = error},
         operation_id,
         state
       ) do
    delay = extract_server_retry_delay(error)

    metadata = %{
      operation_id: operation_id,
      last_response_id: state.last_response_id,
      grpc_status: status,
      error: error,
      reason: :transient_error
    }

    {metadata, delay}
  end

  defp reattach_telemetry_info({:graceful_eof, _}, operation_id, state) do
    metadata = %{
      operation_id: operation_id,
      last_response_id: state.last_response_id,
      reason: :graceful_eof
    }

    {metadata, nil}
  end

  defp handle_invalid_handle(state, _remote, session, _operation_id, next_attempt, ctx)
       when is_nil(state.last_response_id) do
    # T-15: re-issuing a fresh ExecutePlan after the server lost the operation
    # consumes the retry budget like any other retry. Without this check a
    # graceful-EOF -> OPERATION_NOT_FOUND cycle (graceful EOF never charges the
    # budget) would reissue the plan forever.
    # `next_attempt` already includes the reattach that just failed; the
    # fresh ExecutePlan is itself a retry and is charged (+1) once issued, so
    # it is only allowed while the configured budget still has room
    # (`next_attempt < max_retries`). `reattach_retries: N` therefore never
    # issues more than N retries in total (reattaches plus fresh executes).
    cond do
      ctx.stream_closed_fun.() ->
        {:halt, state}

      next_attempt >= ctx.policy.max_retries ->
        reattach_max_retries_error(%{state | attempt: next_attempt})

      true ->
        reissue_execute_plan(state, session, next_attempt, ctx)
    end
  end

  defp handle_invalid_handle(state, remote, _session, operation_id, _next_attempt, _ctx) do
    error = %SparkEx.Error.ResponseAlreadyReceived{
      operation_id: operation_id,
      last_response_id: state.last_response_id,
      buffered_count: state.emitted_count,
      cause: remote
    }

    {[{:error, error}], %{state | result_complete?: true}}
  end

  defp reissue_execute_plan(state, session, next_attempt, ctx) do
    case ctx.execute_stream_fun.(ctx.request, ctx.timeout) do
      {:ok, fresh_stream} ->
        new_state = %{state | iter: start_iter(fresh_stream), attempt: next_attempt + 1}
        reattach_stream_step(new_state)

      {:error, %GRPC.RPCError{} = grpc_error} ->
        remote = Errors.decode_grpc_error(grpc_error)

        if retryable_error?(remote) do
          # Same reasoning as the initial handshake (T-08): the re-issued plan
          # may have been registered before the transport failed, so route the
          # error through the state machine (reattach first) rather than
          # failing terminally or re-sending the identical request.
          new_state = %{
            state
            | iter: start_iter([{:error, grpc_error}]),
              attempt: next_attempt + 1
          }

          reattach_stream_step(new_state)
        else
          {[{:error, Errors.enrich(remote, session)}], %{state | result_complete?: true}}
        end

      {:error, err} ->
        {[{:error, err}], %{state | result_complete?: true}}
    end
  end

  # Suspends an enumerable so elements can be pulled one at a time via pull_iter/1.
  defp start_iter(enum) do
    Enumerable.reduce(enum, {:suspend, nil}, fn elem, _acc -> {:suspend, elem} end)
  end

  defp pull_iter({:suspended, _prev, cont}) do
    case cont.({:cont, nil}) do
      {:suspended, elem, next_cont} -> {:value, elem, {:suspended, elem, next_cont}}
      {:done, _} -> :done
      {:halted, _} -> :done
    end
  end

  defp pull_iter(_), do: :done

  defp fire_release_checkpoint(_release_execute_fun, nil, _timeout_ms, _counter), do: :ok

  defp fire_release_checkpoint(release_execute_fun, response_id, timeout_ms, counter) do
    # Cap concurrent in-flight checkpoint tasks. `release_until(...)` is
    # monotonic on the server side, so dropping an intermediate id is safe:
    # the next response's checkpoint subsumes it, and terminal completion
    # always fires `release_all` which subsumes everything. Without this
    # cap, a fast producer can outpace the server and pile up supervised
    # tasks (CLAUDE-29).
    current = :counters.get(counter, 1)

    if current >= @max_inflight_release_checkpoints do
      :telemetry.execute(
        [:spark_ex, :release_execute, :checkpoint, :coalesced],
        %{inflight: current},
        %{response_id: response_id, cap: @max_inflight_release_checkpoints}
      )

      :ok
    else
      :counters.add(counter, 1, 1)

      case start_supervised_task(fn ->
             task =
               Task.async(fn ->
                 try do
                   release_execute_fun.(until_response_id: response_id)
                 catch
                   _, _ -> :ok
                 end
               end)

             _ = Task.yield(task, timeout_ms) || Task.shutdown(task, @shutdown_grace_ms)
             :counters.sub(counter, 1, 1)
             :ok
           end) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          # start_child failed — the task will never run so the counter
          # must be decremented manually to avoid wedging future checkpoints.
          :counters.sub(counter, 1, 1)

          :telemetry.execute(
            [:spark_ex, :release_execute, :checkpoint, :coalesced],
            %{inflight: :counters.get(counter, 1)},
            %{response_id: response_id, reason: {:start_child_failed, reason}}
          )

          :ok
      end
    end
  end

  @doc false
  @spec build_execute_request(
          SparkEx.Session.t(),
          Plan.t(),
          [String.t()],
          String.t() | nil,
          boolean(),
          keyword()
        ) :: ExecutePlanRequest.t()
  def build_execute_request(session, plan, tags, operation_id, reattachable, opts \\ []) do
    allow_arrow_batch_chunking =
      Keyword.get(opts, :allow_arrow_batch_chunking, session.allow_arrow_batch_chunking)

    preferred_arrow_chunk_size =
      opts
      |> Keyword.get(:preferred_arrow_chunk_size, session.preferred_arrow_chunk_size)
      |> normalize_preferred_arrow_chunk_size()

    request_options =
      [
        %ExecutePlanRequest.RequestOption{
          request_option:
            {:result_chunking_options,
             %ResultChunkingOptions{
               allow_arrow_batch_chunking: allow_arrow_batch_chunking,
               preferred_arrow_chunk_size: preferred_arrow_chunk_size
             }}
        }
      ] ++
        if reattachable do
          [
            %ExecutePlanRequest.RequestOption{
              request_option: {:reattach_options, %ReattachOptions{reattachable: true}}
            }
          ]
        else
          []
        end

    %ExecutePlanRequest{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_observed_server_side_session_id: session.server_side_session_id,
      plan: plan,
      tags: tags,
      operation_id: operation_id,
      request_options: request_options
    }
  end

  @doc false
  @spec build_raw_stream_request(SparkEx.Session.t(), Plan.t(), keyword()) ::
          ExecutePlanRequest.t()
  def build_raw_stream_request(session, plan, opts \\ []) do
    tags = Keyword.get(opts, :tags, [])
    build_execute_request(session, plan, tags, nil, false, opts)
  end

  @doc false
  @spec build_managed_stream_request(SparkEx.Session.t(), Plan.t(), String.t(), keyword()) ::
          ExecutePlanRequest.t()
  def build_managed_stream_request(session, plan, operation_id, opts \\ []) do
    tags = Keyword.get(opts, :tags, [])
    build_execute_request(session, plan, tags, operation_id, true, opts)
  end

  defp normalize_preferred_arrow_chunk_size(nil), do: nil

  defp normalize_preferred_arrow_chunk_size(size) when is_integer(size) and size > 0, do: size

  defp normalize_preferred_arrow_chunk_size(size) do
    raise ArgumentError,
          "expected :preferred_arrow_chunk_size to be a positive integer or nil, got: #{inspect(size)}"
  end

  # Raising variant for the pure request-builder path (runs in the caller, not
  # the Session GenServer, so raising on programmer error is fine there).
  defp validate_artifacts!(artifacts) do
    case validate_artifacts(artifacts) do
      {:ok, validated} -> validated
      {:error, {:invalid_artifacts, message}} -> raise ArgumentError, message
    end
  end

  # Non-raising validator: returns {:ok, validated} | {:error, {:invalid_artifacts, msg}}.
  defp validate_artifacts(artifacts) when is_list(artifacts) do
    artifacts
    |> Enum.reduce_while({:ok, []}, fn artifact, {:ok, acc} ->
      case artifact do
        {name, data} when is_binary(name) and is_binary(data) ->
          {:cont, {:ok, [{name, data} | acc]}}

        {name, {:file, path, size}}
        when is_binary(name) and is_binary(path) and is_integer(size) and size >= 0 ->
          {:cont, {:ok, [{name, {:file, path, size}} | acc]}}

        other ->
          {:halt,
           {:error,
            {:invalid_artifacts,
             "expected artifacts as list of {name, binary} or {name, {:file, path, size}} tuples, got: #{inspect(other)}"}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      {:error, _} = error -> error
    end
  end

  defp validate_artifacts(other) do
    {:error,
     {:invalid_artifacts,
      "expected a list of {name, binary} or {name, {:file, path, size}} tuples, got: #{inspect(other)}"}}
  end

  defp build_batch_request(session, artifacts) do
    single_chunks =
      artifacts
      |> Enum.reverse()
      |> Enum.map(fn {name, data} ->
        %AddArtifactsRequest.SingleChunkArtifact{
          name: name,
          data: %AddArtifactsRequest.ArtifactChunk{
            data: data,
            crc: :erlang.crc32(data)
          }
        }
      end)

    %AddArtifactsRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      payload: {:batch, %AddArtifactsRequest.Batch{artifacts: single_chunks}}
    }
  end

  defp build_chunked_requests(session, name, data, chunk_size) do
    chunks = chunk_binary(data, chunk_size)

    [first_chunk | rest] =
      case chunks do
        [] -> [<<>>]
        list -> list
      end

    begin_request = %AddArtifactsRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      payload:
        {:begin_chunk,
         %AddArtifactsRequest.BeginChunkedArtifact{
           name: name,
           total_bytes: byte_size(data),
           num_chunks: length(chunks),
           initial_chunk: %AddArtifactsRequest.ArtifactChunk{
             data: first_chunk,
             crc: :erlang.crc32(first_chunk)
           }
         }}
    }

    chunk_requests =
      Enum.map(rest, fn chunk ->
        %AddArtifactsRequest{
          session_id: session.session_id,
          client_observed_server_side_session_id: session.server_side_session_id,
          user_context: UserContextExtensions.build_user_context(session.user_id),
          client_type: session.client_type,
          payload:
            {:chunk, %AddArtifactsRequest.ArtifactChunk{data: chunk, crc: :erlang.crc32(chunk)}}
        }
      end)

    [begin_request | chunk_requests]
  end

  # Only called when `total_bytes > chunk_size` (see `producer_stream/2`).
  # Empty and small-but-non-empty artifacts are routed to the batch path,
  # matching PySpark's `_add_artifacts` (artifact.py: `if size > CHUNK_SIZE`
  # → chunked, else → batched). We do not need (and previously had a dead)
  # `total_bytes == 0` branch here.
  defp file_chunk_request_stream(session, name, path, total_bytes, chunk_size)
       when total_bytes > 0 do
    num_chunks = div(total_bytes + chunk_size - 1, chunk_size)

    path
    |> file_byte_stream(chunk_size)
    |> Stream.transform(:first, fn chunk, acc ->
      case acc do
        :first ->
          begin = %AddArtifactsRequest{
            session_id: session.session_id,
            client_observed_server_side_session_id: session.server_side_session_id,
            user_context: UserContextExtensions.build_user_context(session.user_id),
            client_type: session.client_type,
            payload:
              {:begin_chunk,
               %AddArtifactsRequest.BeginChunkedArtifact{
                 name: name,
                 total_bytes: total_bytes,
                 num_chunks: num_chunks,
                 initial_chunk: %AddArtifactsRequest.ArtifactChunk{
                   data: chunk,
                   crc: :erlang.crc32(chunk)
                 }
               }}
          }

          {[begin], :rest}

        :rest ->
          req = %AddArtifactsRequest{
            session_id: session.session_id,
            client_observed_server_side_session_id: session.server_side_session_id,
            user_context: UserContextExtensions.build_user_context(session.user_id),
            client_type: session.client_type,
            payload:
              {:chunk, %AddArtifactsRequest.ArtifactChunk{data: chunk, crc: :erlang.crc32(chunk)}}
          }

          {[req], :rest}
      end
    end)
  end

  # Lazy byte-chunk stream over a file. Avoids `File.stream!/2`/`/3`
  # because the argument order changed between Elixir 1.15 and 1.19,
  # which makes a single call site impossible to satisfy across the
  # supported version range.
  defp file_byte_stream(path, chunk_size) do
    Stream.resource(
      fn -> File.open!(path, [:read, :binary, :raw]) end,
      fn io ->
        case IO.binread(io, chunk_size) do
          :eof ->
            {:halt, io}

          {:error, reason} ->
            raise File.Error,
              reason: reason,
              action: "read from",
              path: IO.chardata_to_string(path)

          data when is_binary(data) ->
            {[data], io}
        end
      end,
      fn io -> File.close(io) end
    )
  end

  defp chunk_binary(data, chunk_size), do: do_chunk_binary(data, chunk_size, [])

  defp do_chunk_binary(<<>>, _chunk_size, acc), do: Enum.reverse(acc)

  defp do_chunk_binary(data, chunk_size, acc) when byte_size(data) <= chunk_size do
    Enum.reverse([data | acc])
  end

  defp do_chunk_binary(data, chunk_size, acc) do
    <<chunk::binary-size(^chunk_size), rest::binary>> = data
    do_chunk_binary(rest, chunk_size, [chunk | acc])
  end

  defp release_execute_best_effort(release_execute_fun, opts, timeout_ms) do
    start_time = System.monotonic_time()

    outcome =
      case async_nolink_supervised(fn -> release_execute_fun.(opts) end) do
        {:ok, task} -> await_release_task(task, timeout_ms)
        {:error, :noproc} -> {:error, :noproc}
      end

    duration = System.monotonic_time() - start_time

    case outcome do
      :ok ->
        :ok

      :benign_not_found ->
        :ok

      :timeout ->
        :telemetry.execute(
          [:spark_ex, :release_execute, :best_effort],
          %{duration: duration},
          %{result: :timeout, timeout_ms: timeout_ms}
        )

        :ok

      {:error, reason} ->
        :telemetry.execute(
          [:spark_ex, :release_execute, :best_effort],
          %{duration: duration},
          %{result: :error, timeout_ms: timeout_ms, error: reason}
        )

        :ok
    end
  end

  defp await_release_task(task, timeout_ms) do
    case Task.yield(task, timeout_ms) || Task.shutdown(task, @shutdown_grace_ms) do
      {:ok, {:ok, _}} ->
        :ok

      {:ok, {:error, reason}} ->
        if benign_release_execute_error?(reason), do: :benign_not_found, else: {:error, reason}

      {:ok, other} ->
        {:error, {:unexpected_release_execute_result, other}}

      {:exit, reason} ->
        {:error, {:task_exit, reason}}

      nil ->
        :timeout
    end
  end

  defp benign_release_execute_error?(%SparkEx.Error.Remote{error_class: error_class})
       when error_class in [
              "INVALID_HANDLE.OPERATION_NOT_FOUND",
              "INVALID_HANDLE.SESSION_NOT_FOUND"
            ],
       do: true

  defp benign_release_execute_error?(%SparkEx.Error.Remote{grpc_status: @status_not_found}),
    do: true

  defp benign_release_execute_error?(_), do: false

  # `until_response_id` is a checkpoint: it implicitly releases everything
  # the server has produced up to and including that id. Only the last
  # consumed (non-result_complete) response_id needs to be released —
  # fanning out one ReleaseExecute per response is redundant work that the
  # server interprets identically to a single call against the highest id.
  defp execute_plan_stream(session, request, timeout) do
    Stub.execute_plan(session.channel, request, timeout: timeout)
  end

  defp response_id_or_nil(nil), do: nil
  defp response_id_or_nil(""), do: nil
  defp response_id_or_nil(value), do: value

  defp generate_operation_id do
    UUID.generate_v4()
  end

  # --- Helpers ---

  defp build_analyze_request(session, fields) do
    struct!(
      AnalyzePlanRequest,
      [
        session_id: session.session_id,
        client_type: session.client_type,
        user_context: UserContextExtensions.build_user_context(session.user_id),
        client_observed_server_side_session_id: session.server_side_session_id
      ] ++ fields
    )
  end

  defp build_config_request(session, fields) do
    struct!(
      ConfigRequest,
      [
        session_id: session.session_id,
        client_type: session.client_type,
        user_context: UserContextExtensions.build_user_context(session.user_id),
        client_observed_server_side_session_id: session.server_side_session_id
      ] ++ fields
    )
  end

  # --- Unary RPC dispatch wrapper (A3) ---
  #
  # Every unary (single-response) gRPC call funnels through this helper.
  # It guarantees three things on every dispatch:
  #
  #   1. Telemetry — start/stop spans with a consistent metadata shape.
  #   2. Session integrity — the response's `session_id` matches our
  #      client id, and the `server_side_session_id` has not rotated
  #      since being pinned (handled by `SessionIntegrity.validate_response/2`).
  #   3. Error normalization — `%GRPC.RPCError{}` is converted to
  #      `%SparkEx.Error.Remote{}` via `Errors.from_grpc_error/2`.
  #
  # Streaming RPCs do not pass through this helper. `ExecutePlan` and
  # `ReattachExecute` perform per-message integrity checks inside
  # `ResultDecoder`; `AddArtifacts` is client-streaming with a single
  # response and is validated inline in `add_artifacts/2`. All three
  # paths share the same `SessionIntegrity` module.
  @typep unary_rpc ::
           :analyze_plan
           | :config
           | :clone_session
           | :release_session
           | :interrupt
           | :get_status
           | :artifact_status
           | :release_execute

  @spec dispatch_unary_rpc(unary_rpc, SparkEx.Session.t(), struct(), keyword()) ::
          {:ok, struct()} | {:error, term()}
  defp dispatch_unary_rpc(rpc, session, request, opts \\ []) do
    {extra_metadata, opts} = Keyword.pop(opts, :extra_metadata, %{})
    {grpc_opts, _} = Keyword.split(opts, [:timeout])

    # Map a `timeout: nil` / `:infinity` opt to gun's infinite timeout so a
    # literal nil never reaches the Stub call and crashes (FABLE-03). Absent
    # key keeps the Stub's own default.
    grpc_opts =
      case Keyword.fetch(grpc_opts, :timeout) do
        {:ok, timeout} -> Keyword.put(grpc_opts, :timeout, normalize_grpc_timeout(timeout))
        :error -> grpc_opts
      end

    # Reserved keys (`:rpc`, `:session_id`) win over `extra_metadata` so
    # callers can't accidentally corrupt telemetry attribution by passing
    # a colliding key.
    metadata =
      Map.merge(
        extra_metadata,
        %{rpc: rpc, session_id: session.session_id}
      )

    # Every unary RPC funnels through the retry wrapper so transient
    # failures (UNAVAILABLE, server-supplied RetryInfo, INVALID_CURSOR.
    # DISCONNECTED) are recovered uniformly without each call site
    # needing its own retry loop. Idempotent operations: AnalyzePlan,
    # Config, ArtifactStatus, FetchErrorDetails. Non-idempotent but
    # safe-to-retry under transient failure: CloneSession (returns the
    # same new id), ReleaseSession / ReleaseExecute / Interrupt (the
    # server treats a missing handle as success).
    # Non-idempotent release RPCs get a much smaller retry budget than the
    # default (PySpark DefaultPolicy: 15 retries × up to 60s = ~10 minutes).
    # Server-side state for both ReleaseExecute and ReleaseSession is GC'd
    # by the server, so persisting that long after a transient failure adds
    # no value — a handful of retries with the same backoff curve covers
    # blips. The caller still bounds wall-clock via `release_execute_timeout`
    # / GenServer call timeouts, but capping retries avoids the inner loop
    # spinning until something else kills the task (CLAUDE-25).
    retry_opts =
      case rpc do
        :release_execute -> [session: session, max_retries: @release_max_retries]
        :release_session -> [session: session, max_retries: @release_max_retries]
        _ -> [session: session]
      end

    rpc_telemetry_span(metadata, fn ->
      retry_with_backoff(
        fn -> do_unary_rpc_call(rpc, session, request, grpc_opts) end,
        retry_opts
      )
    end)
  end

  # gRPC/gun interprets `timeout: nil` literally (`receive ... after nil`),
  # which raises :timeout_value and would crash the calling process. PySpark's
  # awaitTermination()-style "no timeout" means block indefinitely, so map both
  # `nil` and `:infinity` to gun's infinite timeout (FABLE-03). Integer values
  # pass through unchanged. The streaming modules rely on this contract: they
  # may put `timeout: nil` into opts to signal "no gRPC deadline".
  @doc false
  @spec normalize_grpc_timeout(nil | :infinity | non_neg_integer()) ::
          :infinity | non_neg_integer()
  def normalize_grpc_timeout(nil), do: :infinity
  def normalize_grpc_timeout(:infinity), do: :infinity
  def normalize_grpc_timeout(timeout) when is_integer(timeout), do: timeout

  defp do_unary_rpc_call(rpc, session, request, grpc_opts) do
    case do_unary_stub_call(rpc, session.channel, request, grpc_opts) do
      {:ok, response} ->
        with {:ok, _} <- SessionIntegrity.validate_response(response, session) do
          {:ok, response}
        end

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.decode_grpc_error(error)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_unary_stub_call(:analyze_plan, channel, request, opts),
    do: Stub.analyze_plan(channel, request, opts)

  defp do_unary_stub_call(:config, channel, request, opts),
    do: Stub.config(channel, request, opts)

  defp do_unary_stub_call(:clone_session, channel, request, opts),
    do: Stub.clone_session(channel, request, opts)

  defp do_unary_stub_call(:release_session, channel, request, opts),
    do: Stub.release_session(channel, request, opts)

  defp do_unary_stub_call(:interrupt, channel, request, opts),
    do: Stub.interrupt(channel, request, opts)

  defp do_unary_stub_call(:get_status, channel, request, opts),
    do: Stub.get_status(channel, request, opts)

  defp do_unary_stub_call(:artifact_status, channel, request, opts),
    do: Stub.artifact_status(channel, request, opts)

  defp do_unary_stub_call(:release_execute, channel, request, opts),
    do: Stub.release_execute(channel, request, opts)

  defp handle_add_artifacts_response({:ok, %AddArtifactsResponse{} = resp}, session) do
    case SessionIntegrity.validate_response(resp, session) do
      {:ok, _} ->
        summaries = Enum.map(resp.artifacts, fn s -> {s.name, s.is_crc_successful} end)
        {:ok, summaries, resp.server_side_session_id}

      {:error, _} = error ->
        error
    end
  end

  defp handle_add_artifacts_response({:error, %GRPC.RPCError{} = error}, _session),
    do: {:error, Errors.decode_grpc_error(error)}

  defp handle_add_artifacts_response({:error, reason}, _session), do: {:error, reason}

  defp stub_execute_plan(session, request, timeout) do
    case Stub.execute_plan(session.channel, request, timeout: timeout) do
      {:ok, stream} -> {:ok, stream}
      {:error, %GRPC.RPCError{} = error} -> {:error, Errors.decode_grpc_error(error)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_plan_non_reattachable(session, request, timeout, opts, decode_fn) do
    # Pass :session so per-session retry policies apply (matches add_artifacts/2
    # and dispatch_unary_rpc/4). Without it RetryPolicyRegistry.policy_for(nil, ...)
    # would ignore session.retry_policies (FABLE-37). Caller-supplied :session in
    # opts wins via Keyword.put_new.
    retry_with_backoff(
      fn ->
        with {:ok, stream} <- stub_execute_plan(session, request, timeout) do
          decode_fn.(stream)
        end
      end,
      Keyword.put_new(opts, :session, session)
    )
  end

  defp explain_mode_to_proto(:simple), do: {:ok, :EXPLAIN_MODE_SIMPLE}
  defp explain_mode_to_proto(:extended), do: {:ok, :EXPLAIN_MODE_EXTENDED}
  defp explain_mode_to_proto(:codegen), do: {:ok, :EXPLAIN_MODE_CODEGEN}
  defp explain_mode_to_proto(:cost), do: {:ok, :EXPLAIN_MODE_COST}
  defp explain_mode_to_proto(:formatted), do: {:ok, :EXPLAIN_MODE_FORMATTED}
  defp explain_mode_to_proto(other), do: {:error, {:invalid_explain_mode, other}}

  # --- Retry logic ---

  @doc false
  @spec retry_with_backoff((-> term()), keyword()) :: term()
  def retry_with_backoff(fun, opts \\ []) when is_function(fun, 0) do
    session = Keyword.get(opts, :session)
    policy_type = Keyword.get(opts, :policy_type, :retry)
    base_policy = RetryPolicyRegistry.policy_for(session, policy_type)

    overrides =
      opts
      |> Keyword.drop([:session, :policy_type])
      |> Map.new()
      |> Map.take(Map.keys(base_policy))

    policy = Map.merge(base_policy, overrides)

    # T-35: the terminal error is the only one worth the FetchErrorDetails
    # round-trip; retried attempts stay RPC-free.
    fun
    |> do_retry(0, policy)
    |> enrich_terminal_error(session)
  end

  defp enrich_terminal_error({:error, %SparkEx.Error.Remote{} = error}, %SparkEx.Session{} = s),
    do: {:error, Errors.enrich(error, s)}

  defp enrich_terminal_error(result, _session), do: result

  defp do_retry(fun, attempt, policy) do
    case fun.() do
      {:error, %SparkEx.Error.Remote{} = error} = result ->
        if attempt < policy.max_retries and retryable_error?(error) do
          sleep_ms =
            backoff_with_retry_info(
              attempt,
              policy,
              error.retry_delay_ms
            )

          :telemetry.execute(
            [:spark_ex, :retry, :attempt],
            %{attempt: attempt + 1, backoff_ms: sleep_ms},
            %{
              grpc_status: error.grpc_status,
              error: error,
              max_retries: policy.max_retries,
              retry_delay_ms: error.retry_delay_ms
            }
          )

          policy.sleep_fun.(sleep_ms)
          do_retry(fun, attempt + 1, policy)
        else
          result
        end

      result ->
        result
    end
  end

  # PySpark DefaultPolicy retryable predicate.
  # Retries:
  #   * gRPC UNAVAILABLE
  #   * gRPC INTERNAL with INVALID_CURSOR.DISCONNECTED errorClass
  #   * Any error whose status details carry a RetryInfo message, even when its
  #     retry_delay is unset (retries.py:367-369). Presence is tracked via
  #     has_retry_info; retry_delay_ms may be nil for such errors.
  # Explicitly does NOT retry DEADLINE_EXCEEDED — Spark client treats it as terminal.
  @doc false
  @spec retryable_error?(SparkEx.Error.Remote.t()) :: boolean()
  def retryable_error?(%SparkEx.Error.Remote{} = error) do
    cond do
      error.has_retry_info == true ->
        true

      is_integer(error.retry_delay_ms) and error.retry_delay_ms >= 0 ->
        true

      error.grpc_status == @status_unavailable ->
        true

      error.grpc_status == @status_internal and
          error.error_class == "INVALID_CURSOR.DISCONNECTED" ->
        true

      true ->
        false
    end
  end

  defp backoff_ms(attempt, policy) do
    with_jitter(exponential_backoff_ms(attempt, policy), policy)
  end

  # Exponential backoff without jitter: initial * multiplier^attempt, capped.
  defp exponential_backoff_ms(attempt, policy) do
    multiplier = Map.get(policy, :backoff_multiplier, 2.0)
    base = policy.initial_backoff_ms * :math.pow(multiplier, attempt)
    Kernel.min(round(base), policy.max_backoff_ms)
  end

  defp with_jitter(wait_ms, policy), do: wait_ms + jitter_amount(wait_ms, policy)

  defp jitter_amount(capped, policy) do
    jitter_fun = Map.get(policy, :jitter_fun)
    jitter_ms = Map.get(policy, :jitter_ms)
    threshold = Map.get(policy, :min_jitter_threshold_ms, 0)

    cond do
      is_integer(jitter_ms) and capped >= threshold ->
        :rand.uniform(jitter_ms + 1) - 1

      is_function(jitter_fun, 1) and capped >= threshold ->
        # Legacy callers may have configured jitter_fun directly; preserve
        # behavior but only apply once we cross the threshold.
        max(0, jitter_fun.(capped) - capped)

      true ->
        0
    end
  end

  @retry_info_type_url "type.googleapis.com/google.rpc.RetryInfo"

  # A transport-level error (or one built without status details) may carry
  # `details: nil` at runtime despite the struct type; List.wrap/1 treats it as
  # "no RetryInfo" so the caller falls back to the policy backoff.
  defp extract_server_retry_delay(%GRPC.RPCError{details: details}) do
    details
    |> List.wrap()
    |> Enum.find_value(nil, fn
      %Google.Protobuf.Any{type_url: @retry_info_type_url, value: value} ->
        case safe_decode_retry_info(value) do
          {:ok, %Google.Rpc.RetryInfo{retry_delay: nil}} ->
            # No hint — caller falls back to policy backoff.
            nil

          {:ok,
           %Google.Rpc.RetryInfo{
             retry_delay: %Google.Protobuf.Duration{seconds: seconds, nanos: nanos}
           }} ->
            seconds * 1000 + div(nanos, 1_000_000)

          :error ->
            nil
        end

      _ ->
        nil
    end)
  end

  defp safe_decode_retry_info(value) do
    {:ok, Protobuf.decode(value, Google.Rpc.RetryInfo)}
  rescue
    _ -> :error
  end

  # The server-supplied RetryInfo.retry_delay is a FLOOR on the normal
  # exponential backoff, never a replacement (pyspark client/retries.py:157-167):
  #   wait = max(exponential_backoff_with_jitter, min(retry_delay, max_server_retry_delay))
  # A small server hint (10-100 ms) must not suppress attempt-based backoff.
  #
  # T-16: jitter is applied AFTER the max(...) with the server floor (retries.py
  # nextAttempt: "Jitter current backoff, after the future backoff was
  # computed"). Adding jitter inside the exponential term and then taking the
  # max would discard it whenever the server floor dominates, so many clients
  # honouring the same RetryInfo would all wake at exactly the same instant.
  defp backoff_with_retry_info(attempt, policy, retry_delay_ms)
       when is_integer(retry_delay_ms) and retry_delay_ms > 0 do
    server_floor = Kernel.min(retry_delay_ms, policy.max_server_retry_delay)

    attempt
    |> exponential_backoff_ms(policy)
    |> Kernel.max(server_floor)
    |> with_jitter(policy)
  end

  defp backoff_with_retry_info(attempt, policy, _retry_delay_ms) do
    backoff_ms(attempt, policy)
  end

  # --- Telemetry ---

  @doc false
  @spec rpc_telemetry_span(map(), (-> term())) :: term()
  def rpc_telemetry_span(metadata, fun) when is_map(metadata) and is_function(fun, 0) do
    start_time = System.monotonic_time()
    :telemetry.execute([:spark_ex, :rpc, :start], %{system_time: System.system_time()}, metadata)

    try do
      result = fun.()

      duration = System.monotonic_time() - start_time

      result_metadata = Map.merge(metadata, row_count_metadata(result))

      :telemetry.execute(
        [:spark_ex, :rpc, :stop],
        %{duration: duration},
        Map.put(result_metadata, :result, result_status(result))
      )

      result
    rescue
      e ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:spark_ex, :rpc, :exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: :error, reason: e, stacktrace: __STACKTRACE__})
        )

        reraise e, __STACKTRACE__
    catch
      kind, reason when kind in [:exit, :throw] ->
        duration = System.monotonic_time() - start_time
        stacktrace = __STACKTRACE__

        :telemetry.execute(
          [:spark_ex, :rpc, :exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: stacktrace})
        )

        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp result_status({:ok, _}), do: :ok
  defp result_status({:ok, _, _}), do: :ok
  defp result_status({:error, _}), do: :error
  defp result_status(_), do: :error

  defp row_count_metadata({:ok, %{rows: rows}}) when is_list(rows) do
    %{row_count: length(rows)}
  end

  defp row_count_metadata({:ok, %{dataframe: dataframe}})
       when is_struct(dataframe, Explorer.DataFrame) do
    %{row_count: Explorer.DataFrame.n_rows(dataframe)}
  end

  defp row_count_metadata({:ok, _}), do: %{}

  defp row_count_metadata(_result), do: %{}

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value
end
