defmodule SparkEx.Connect.Client do
  @moduledoc """
  Low-level gRPC client for Spark Connect RPCs.

  Builds request messages from session state and calls the generated
  `Spark.Connect.SparkConnectService.Stub`.
  """

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

  @compile {:no_warn_undefined, Explorer.DataFrame}

  alias SparkEx.Connect.{Errors, ResultDecoder}
  alias SparkEx.Internal.UUID
  alias SparkEx.{ManagedStream, RetryPolicyRegistry, UserContextExtensions}

  require Logger

  # gRPC status codes considered transient (eligible for retry)
  @status_unavailable 14
  @status_deadline_exceeded 4
  @status_not_found 5

  @artifact_chunk_size 32 * 1024
  @release_checkpoint_max_concurrency 8
  @release_checkpoint_timeout 15_000
  @release_execute_timeout 5_000

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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:spark_version, %{version: version}}} = resp} ->
        {:ok, version, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:schema, %{schema: schema}}} = resp} ->
        {:ok, schema, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

      case Stub.analyze_plan(session.channel, request) do
        {:ok, %AnalyzePlanResponse{result: {:explain, %{explain_string: str}}} = resp} ->
          {:ok, str, resp.server_side_session_id}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:tree_string, %{tree_string: str}}} = resp} ->
        {:ok, str, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:is_local, %{is_local: is_local}}} = resp} ->
        {:ok, is_local, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:is_streaming, %{is_streaming: is_streaming}}} = resp} ->
        {:ok, is_streaming, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:input_files, %{files: files}}} = resp} ->
        {:ok, files, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:ddl_parse, %{parsed: parsed}}} = resp} ->
        {:ok, parsed, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:json_to_ddl, %{ddl_string: ddl}}} = resp} ->
        {:ok, ddl, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:same_semantics, %{result: result}}} = resp} ->
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:semantic_hash, %{result: hash}}} = resp} ->
        {:ok, hash, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Calls `AnalyzePlan` with `Persist` to persist a relation with optional storage level.

  ## Options

  - `:storage_level` — a `Spark.Connect.StorageLevel` struct (optional)
  """
  @spec analyze_persist(SparkEx.Session.t(), Spark.Connect.Relation.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  # PySpark default: StorageLevel(True, True, True, False, 1) = MEMORY_AND_DISK_DESER
  @default_storage_level %Spark.Connect.StorageLevel{
    use_disk: true,
    use_memory: true,
    use_off_heap: true,
    deserialized: false,
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:persist, _}} = resp} ->
        {:ok, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:unpersist, _}} = resp} ->
        {:ok, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.analyze_plan(session.channel, request) do
      {:ok,
       %AnalyzePlanResponse{result: {:get_storage_level, %{storage_level: storage_level}}} =
           resp} ->
        {:ok, storage_level, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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
    timeout = Keyword.get(opts, :timeout, 60_000)
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    request = build_execute_request(session, plan, tags, operation_id, reattachable, opts)
    metadata = %{rpc: :execute_plan, session_id: session.session_id, operation_id: operation_id}

    rpc_telemetry_span(metadata, fn ->
      if reattachable do
        execute_reattachable(session, request, operation_id, timeout, opts, fn responses ->
          ResultDecoder.decode_stream(responses, session)
        end)
      else
        retry_with_backoff(
          fn ->
            case Stub.execute_plan(session.channel, request, timeout: timeout) do
              {:ok, stream} ->
                ResultDecoder.decode_stream(stream, session)

              {:error, %GRPC.RPCError{} = error} ->
                {:error, Errors.from_grpc_error(error, session)}

              {:error, reason} ->
                {:error, reason}
            end
          end,
          opts
        )
      end
    end)
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
    timeout = Keyword.get(opts, :timeout, 60_000)
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    request = build_execute_request(session, plan, tags, operation_id, reattachable, opts)

    metadata = %{
      rpc: :execute_plan_explorer,
      session_id: session.session_id,
      operation_id: operation_id
    }

    rpc_telemetry_span(metadata, fn ->
      if reattachable do
        execute_reattachable(session, request, operation_id, timeout, opts, fn responses ->
          ResultDecoder.decode_stream_explorer(responses, session, opts)
        end)
      else
        retry_with_backoff(
          fn ->
            case Stub.execute_plan(session.channel, request, timeout: timeout) do
              {:ok, stream} ->
                ResultDecoder.decode_stream_explorer(stream, session, opts)

              {:error, %GRPC.RPCError{} = error} ->
                {:error, Errors.from_grpc_error(error, session)}

              {:error, reason} ->
                {:error, reason}
            end
          end,
          opts
        )
      end
    end)
  end

  @doc """
  Calls `ExecutePlan` and decodes the response as raw Arrow IPC bytes.
  """
  @spec execute_plan_arrow(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.arrow_result()} | {:error, term()}
  def execute_plan_arrow(session, plan, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    tags = Keyword.get(opts, :tags, [])
    reattachable = Keyword.get(opts, :reattachable, true)
    operation_id = if reattachable, do: generate_operation_id(), else: nil

    request = build_execute_request(session, plan, tags, operation_id, reattachable, opts)

    metadata = %{
      rpc: :execute_plan_arrow,
      session_id: session.session_id,
      operation_id: operation_id
    }

    rpc_telemetry_span(metadata, fn ->
      if reattachable do
        execute_reattachable(session, request, operation_id, timeout, opts, fn responses ->
          ResultDecoder.decode_stream_arrow(responses, session)
        end)
      else
        retry_with_backoff(
          fn ->
            case Stub.execute_plan(session.channel, request, timeout: timeout) do
              {:ok, stream} ->
                ResultDecoder.decode_stream_arrow(stream, session)

              {:error, %GRPC.RPCError{} = error} ->
                {:error, Errors.from_grpc_error(error, session)}

              {:error, reason} ->
                {:error, reason}
            end
          end,
          opts
        )
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
    timeout = Keyword.get(opts, :timeout, :infinity)
    request = build_execute_request(session, plan, [], nil, false, opts)

    case Stub.execute_plan(session.channel, request, timeout: timeout) do
      {:ok, stream} -> {:ok, stream}
      {:error, %GRPC.RPCError{} = error} -> {:error, Errors.from_grpc_error(error, session)}
      {:error, reason} -> {:error, reason}
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
    timeout = Keyword.get(opts, :timeout, :infinity)
    owner = Keyword.get(opts, :stream_owner, self())
    idle_timeout = Keyword.get(opts, :idle_timeout, nil)
    operation_id = generate_operation_id()
    request = build_execute_request(session, plan, [], operation_id, true, opts)

    case Stub.execute_plan(session.channel, request, timeout: timeout) do
      {:ok, stream} ->
        release_fun = fn release_opts -> release_execute(session, operation_id, release_opts) end

        case ManagedStream.new(stream,
               owner: owner,
               idle_timeout: idle_timeout,
               release_fun: release_fun
             ) do
          {:ok, managed_stream} ->
            {:ok, managed_stream}

          {:error, _} = error ->
            _ = release_execute(session, operation_id)
            error
        end

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Config RPCs ---

  @doc """
  Sets Spark configuration key-value pairs.
  """
  @spec config_set(SparkEx.Session.t(), [{String.t(), String.t()}]) ::
          {:ok, String.t() | nil} | {:error, term()}
  def config_set(session, pairs) do
    kv_pairs = Enum.map(pairs, fn {k, v} -> %KeyValue{key: k, value: v} end)

    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:set, %ConfigRequest.Set{pairs: kv_pairs}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{} = resp} ->
        {:ok, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets Spark configuration values for the given keys.

  Returns a list of `{key, value}` pairs.
  """
  @spec config_get(SparkEx.Session.t(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get, %ConfigRequest.Get{keys: keys}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{pairs: pairs} = resp} ->
        result = Enum.map(pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets Spark configuration values with defaults for the given key-value pairs.

  When the config key has no value set, the provided default is returned.
  Returns a list of `{key, value}` pairs.
  """
  @spec config_get_with_default(SparkEx.Session.t(), [{String.t(), String.t()}]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get_with_default(session, pairs) do
    kv_pairs = Enum.map(pairs, fn {k, v} -> %KeyValue{key: k, value: v} end)

    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get_with_default, %ConfigRequest.GetWithDefault{pairs: kv_pairs}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets optional Spark configuration values for the given keys.

  Returns a list of `{key, value}` pairs. When a key is not set, the value
  is nil (unlike `config_get/2` which may raise on the server).
  """
  @spec config_get_option(SparkEx.Session.t(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}], String.t() | nil} | {:error, term()}
  def config_get_option(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:get_option, %ConfigRequest.GetOption{keys: keys}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
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

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Unsets Spark configuration values for the given keys.
  """
  @spec config_unset(SparkEx.Session.t(), [String.t()]) ::
          {:ok, String.t() | nil} | {:error, term()}
  def config_unset(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:unset, %ConfigRequest.Unset{keys: keys}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{} = resp} ->
        {:ok, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Checks whether the given configuration keys are modifiable at runtime.

  Returns a list of `{key, value}` pairs where value is `"true"` or `"false"`.
  """
  @spec config_is_modifiable(SparkEx.Session.t(), [String.t()]) ::
          {:ok, [{String.t(), String.t()}], String.t() | nil} | {:error, term()}
  def config_is_modifiable(session, keys) do
    request =
      build_config_request(session,
        operation: %ConfigRequest.Operation{
          op_type: {:is_modifiable, %ConfigRequest.IsModifiable{keys: keys}}
        }
      )

    case Stub.config(session.channel, request) do
      {:ok, %ConfigResponse{pairs: resp_pairs} = resp} ->
        result = Enum.map(resp_pairs, fn %KeyValue{key: k, value: v} -> {k, v} end)
        {:ok, result, resp.server_side_session_id}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

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
  def clone_session(session, new_session_id \\ nil) do
    request = %CloneSessionRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      new_session_id: new_session_id
    }

    metadata = %{rpc: :clone_session, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
      case Stub.clone_session(session.channel, request) do
        {:ok, %CloneSessionResponse{} = resp} ->
          {:ok,
           %{
             new_session_id: resp.new_session_id,
             new_server_side_session_id: blank_to_nil(resp.new_server_side_session_id),
             source_server_side_session_id: blank_to_nil(resp.server_side_session_id)
           }}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @doc """
  Calls `ReleaseSession` to release the server-side session.
  """
  @spec release_session(SparkEx.Session.t()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def release_session(session) do
    request = %ReleaseSessionRequest{
      session_id: session.session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type
    }

    metadata = %{rpc: :release_session, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
      case Stub.release_session(session.channel, request) do
        {:ok, %ReleaseSessionResponse{} = resp} ->
          {:ok, resp.server_side_session_id}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  # --- Interrupt RPC ---

  @doc """
  Calls `Interrupt` to cancel running operations.

  ## Interrupt types

  - `:all` — interrupt all running operations
  - `{:tag, tag}` — interrupt operations matching the given tag
  - `{:operation_id, id}` — interrupt a specific operation by ID
  """
  @spec interrupt(SparkEx.Session.t(), :all | {:tag, String.t()} | {:operation_id, String.t()}) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term()}
  def interrupt(session, type) do
    request = build_interrupt_request(session, type)
    metadata = %{rpc: :interrupt, session_id: session.session_id, interrupt_type: type}

    rpc_telemetry_span(metadata, fn ->
      case Stub.interrupt(session.channel, request) do
        {:ok, %InterruptResponse{} = resp} ->
          {:ok, resp.interrupted_ids, resp.server_side_session_id}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

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

  Returns a map of artifact name to boolean (exists or not).
  """
  @spec artifact_status(SparkEx.Session.t(), [String.t()]) ::
          {:ok, %{String.t() => boolean()}, String.t() | nil} | {:error, term()}
  def artifact_status(session, names) do
    request = %ArtifactStatusesRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      names: names
    }

    metadata = %{rpc: :artifact_status, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
      case Stub.artifact_status(session.channel, request) do
        {:ok, %ArtifactStatusesResponse{} = resp} ->
          statuses =
            Map.new(resp.statuses, fn {name, %{exists: exists}} -> {name, exists} end)

          {:ok, statuses, resp.server_side_session_id}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @doc """
  Calls `AddArtifacts` (client-streaming) to upload artifacts to the server.

  Artifacts are provided as a list of `{name, data}` tuples where `data` is
  binary content. Small artifacts are batched; large artifacts are streamed
  in chunks (`begin_chunk` + `chunk` payloads).

  Returns a list of `{name, crc_successful?}` tuples.
  """
  @spec add_artifacts(SparkEx.Session.t(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}], String.t() | nil} | {:error, term()}
  def add_artifacts(session, artifacts) when is_list(artifacts) do
    requests = build_add_artifacts_requests(session, artifacts)

    metadata = %{rpc: :add_artifacts, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
      case requests do
        [] ->
          {:ok, [], session.server_side_session_id}

        _ ->
          stream =
            session.channel
            |> Stub.add_artifacts()
            |> send_add_artifacts_requests(requests)

          case GRPC.Stub.recv(stream) do
            {:ok, %AddArtifactsResponse{} = resp} ->
              summaries =
                Enum.map(resp.artifacts, fn summary ->
                  {summary.name, summary.is_crc_successful}
                end)

              {:ok, summaries, resp.server_side_session_id}

            {:error, %GRPC.RPCError{} = error} ->
              {:error, Errors.from_grpc_error(error, session)}

            {:error, reason} ->
              {:error, reason}
          end
      end
    end)
  end

  @doc false
  @spec build_add_artifacts_requests(SparkEx.Session.t(), [{String.t(), binary()}], pos_integer()) ::
          [AddArtifactsRequest.t()]
  def build_add_artifacts_requests(session, artifacts, chunk_size \\ @artifact_chunk_size)
      when is_list(artifacts) and is_integer(chunk_size) and chunk_size > 0 do
    artifact_entries = validate_artifacts!(artifacts)

    {requests, pending_batch, _pending_size} =
      Enum.reduce(artifact_entries, {[], [], 0}, fn {name, data}, {acc, batch, batch_size} ->
        data_size = byte_size(data)

        if data_size > chunk_size do
          acc =
            case batch do
              [] -> acc
              _ -> [build_batch_request(session, batch) | acc]
            end

          chunked = build_chunked_requests(session, name, data, chunk_size)

          acc =
            Enum.reduce(chunked, acc, fn req, req_acc ->
              [req | req_acc]
            end)

          {acc, [], 0}
        else
          if batch_size + data_size > chunk_size and batch != [] do
            acc = [build_batch_request(session, batch) | acc]
            {acc, [{name, data}], data_size}
          else
            {acc, [{name, data} | batch], batch_size + data_size}
          end
        end
      end)

    requests =
      case pending_batch do
        [] -> requests
        _ -> [build_batch_request(session, pending_batch) | requests]
      end

    Enum.reverse(requests)
  end

  # --- ReattachExecute / ReleaseExecute RPCs ---

  @doc """
  Calls `ReattachExecute` (server-streaming) to resume a reattachable operation.

  Returns `{:ok, stream}` with the response stream starting after `last_response_id`.
  """
  @spec reattach_execute(SparkEx.Session.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def reattach_execute(session, operation_id, last_response_id, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)

    request = %ReattachExecuteRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      operation_id: operation_id,
      last_response_id: last_response_id
    }

    case Stub.reattach_execute(session.channel, request, timeout: timeout) do
      {:ok, stream} ->
        {:ok, stream}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Calls `ReleaseExecute` to release server-side cached results for an operation.

  Uses `release_all` by default. Pass `until_response_id: id` to release
  only up to a specific response.
  """
  @spec release_execute(SparkEx.Session.t(), String.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def release_execute(session, operation_id, opts \\ []) do
    until_response_id = Keyword.get(opts, :until_response_id, nil)

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

    metadata = %{
      rpc: :release_execute,
      session_id: session.session_id,
      operation_id: operation_id
    }

    rpc_telemetry_span(metadata, fn ->
      case Stub.release_execute(session.channel, request) do
        {:ok, %ReleaseExecuteResponse{} = resp} ->
          {:ok, resp.server_side_session_id}

        {:error, %GRPC.RPCError{} = error} ->
          {:error, Errors.from_grpc_error(error, session)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  # --- Reattachable execution helpers ---

  defp execute_reattachable(session, request, operation_id, timeout, opts, decode_fn) do
    default_policy = RetryPolicyRegistry.policy(:reattach)
    max_retries = Keyword.get(opts, :reattach_retries, default_policy.max_retries)

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

    case execute_stream_fun.(request, timeout) do
      {:ok, initial_stream} ->
        case collect_with_reattach(
               initial_stream,
               session,
               request,
               execute_stream_fun,
               reattach_stream_fun,
               release_execute_fun,
               operation_id,
               timeout,
               max_retries,
               [],
               nil,
               0
             ) do
          {:ok, responses, _last_response_id} ->
            wrapped = Enum.map(responses, &{:ok, &1})
            result = decode_fn.(wrapped)

            release_execute_best_effort(release_execute_fun, [], release_execute_timeout)

            result

          {:error, _} = error ->
            release_execute_best_effort(release_execute_fun, [], release_execute_timeout)
            error
        end

      {:error, %GRPC.RPCError{} = error} ->
        {:error, Errors.from_grpc_error(error, session)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp collect_with_reattach(
         stream,
         session,
         request,
         execute_stream_fun,
         reattach_stream_fun,
         release_execute_fun,
         operation_id,
         timeout,
         max_retries,
         acc,
         last_response_id,
         attempt
       ) do
    case consume_until_error(stream) do
      {:ok, new_items, last_id, result_complete?} ->
        all_items = prepend_items(acc, new_items)
        final_last_id = last_id || last_response_id

        release_checkpoints_best_effort(release_execute_fun, new_items)

        if result_complete? do
          {:ok, Enum.reverse(all_items), final_last_id}
        else
          retry_reattach(
            session,
            request,
            execute_stream_fun,
            reattach_stream_fun,
            release_execute_fun,
            operation_id,
            timeout,
            max_retries,
            all_items,
            final_last_id,
            attempt,
            {:graceful_eof, nil}
          )
        end

      {:error, error, new_items, last_id} ->
        all_items = prepend_items(acc, new_items)
        final_last_id = last_id || last_response_id

        release_checkpoints_best_effort(release_execute_fun, new_items)

        case error do
          %GRPC.RPCError{status: status}
          when status in [@status_unavailable, @status_deadline_exceeded] ->
            retry_reattach(
              session,
              request,
              execute_stream_fun,
              reattach_stream_fun,
              release_execute_fun,
              operation_id,
              timeout,
              max_retries,
              all_items,
              final_last_id,
              attempt,
              {:transient_error, error}
            )

          %GRPC.RPCError{} = grpc_error ->
            {:error, Errors.from_grpc_error(grpc_error, session)}

          other ->
            {:error, other}
        end
    end
  end

  defp consume_until_error(stream) do
    result =
      Enum.reduce_while(stream, {[], nil, false}, fn
        {:ok, %ExecutePlanResponse{} = resp}, {items, _last_id, result_complete?} ->
          complete? =
            result_complete? or match?({:result_complete, _}, resp.response_type)

          {:cont, {[resp | items], response_id_or_nil(resp.response_id), complete?}}

        {:error, error}, {items, last_id, _result_complete?} ->
          {:halt, {:error, error, Enum.reverse(items), last_id}}
      end)

    case result do
      {items, last_id, result_complete?} ->
        {:ok, Enum.reverse(items), last_id, result_complete?}

      {:error, _, _, _} = error ->
        error
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

  defp normalize_preferred_arrow_chunk_size(nil), do: nil

  defp normalize_preferred_arrow_chunk_size(size) when is_integer(size) and size > 0, do: size

  defp normalize_preferred_arrow_chunk_size(size) do
    raise ArgumentError,
          "expected :preferred_arrow_chunk_size to be a positive integer or nil, got: #{inspect(size)}"
  end

  defp send_add_artifacts_requests(stream, requests) do
    total = length(requests)

    Enum.with_index(requests, 1)
    |> Enum.reduce(stream, fn {request, idx}, acc_stream ->
      GRPC.Stub.send_request(acc_stream, request, end_stream: idx == total)
    end)
  end

  defp validate_artifacts!(artifacts) do
    Enum.map(artifacts, fn
      {name, data} when is_binary(name) and is_binary(data) ->
        {name, data}

      other ->
        raise ArgumentError,
              "expected artifacts as list of {name, binary} tuples, got: #{inspect(other)}"
    end)
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

  defp chunk_binary(data, chunk_size), do: do_chunk_binary(data, chunk_size, [])

  defp do_chunk_binary(<<>>, _chunk_size, acc), do: Enum.reverse(acc)

  defp do_chunk_binary(data, chunk_size, acc) when byte_size(data) <= chunk_size do
    Enum.reverse([data | acc])
  end

  defp do_chunk_binary(data, chunk_size, acc) do
    <<chunk::binary-size(chunk_size), rest::binary>> = data
    do_chunk_binary(rest, chunk_size, [chunk | acc])
  end

  defp release_execute_best_effort(release_execute_fun, opts, timeout_ms) do
    start_time = System.monotonic_time()

    task =
      Task.Supervisor.async_nolink(SparkEx.TaskSupervisor, fn -> release_execute_fun.(opts) end)

    outcome =
      case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
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

  defp benign_release_execute_error?(%SparkEx.Error.Remote{error_class: error_class})
       when error_class in [
              "INVALID_HANDLE.OPERATION_NOT_FOUND",
              "INVALID_HANDLE.SESSION_NOT_FOUND"
            ],
       do: true

  defp benign_release_execute_error?(%SparkEx.Error.Remote{grpc_status: @status_not_found}),
    do: true

  defp benign_release_execute_error?(_), do: false

  defp release_checkpoints_best_effort(release_execute_fun, responses) do
    response_ids =
      responses
      |> Enum.reject(fn resp -> match?({:result_complete, _}, resp.response_type) end)
      |> Enum.map(&response_id_or_nil(&1.response_id))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    case response_ids do
      [] ->
        :ok

      ids ->
        Task.Supervisor.start_child(SparkEx.TaskSupervisor, fn ->
          ids
          |> Task.async_stream(
            fn response_id ->
              release_execute_fun.(until_response_id: response_id)
            end,
            max_concurrency: @release_checkpoint_max_concurrency,
            ordered: false,
            timeout: @release_checkpoint_timeout,
            on_timeout: :kill_task
          )
          |> Stream.run()
        end)

        :ok
    end
  end

  defp retry_reattach(
         _session,
         _request,
         _execute_stream_fun,
         _reattach_stream_fun,
         _release_execute_fun,
         _operation_id,
         _timeout,
         max_retries,
         all_items,
         final_last_id,
         attempt,
         _reason
       )
       when attempt >= max_retries do
    {:error,
     {:reattach_incomplete_result,
      %{
        retries_attempted: attempt,
        last_response_id: final_last_id,
        responses_received: length(all_items)
      }}}
  end

  defp retry_reattach(
         session,
         request,
         execute_stream_fun,
         reattach_stream_fun,
         release_execute_fun,
         operation_id,
         timeout,
         max_retries,
         all_items,
         final_last_id,
         attempt,
         reason
       ) do
    telemetry_metadata =
      case reason do
        {:transient_error, %GRPC.RPCError{status: status} = error} ->
          %{
            operation_id: operation_id,
            last_response_id: final_last_id,
            grpc_status: status,
            error: error,
            reason: :transient_error
          }

        {:graceful_eof, _} ->
          %{
            operation_id: operation_id,
            last_response_id: final_last_id,
            reason: :graceful_eof
          }
      end

    :telemetry.execute(
      [:spark_ex, :reattach, :attempt],
      %{attempt: attempt + 1},
      telemetry_metadata
    )

    default_policy = RetryPolicyRegistry.policy(:reattach)

    sleep_ms =
      backoff_ms(
        attempt,
        default_policy.initial_backoff_ms,
        default_policy.max_backoff_ms,
        default_policy.jitter_fun
      )

    default_policy.sleep_fun.(sleep_ms)

    case reattach_stream_fun.(final_last_id) do
      {:ok, new_stream} ->
        collect_with_reattach(
          new_stream,
          session,
          request,
          execute_stream_fun,
          reattach_stream_fun,
          release_execute_fun,
          operation_id,
          timeout,
          max_retries,
          all_items,
          final_last_id,
          attempt + 1
        )

      {:error, %SparkEx.Error.Remote{} = remote}
      when length(all_items) == 0 and
             remote.error_class in [
               "INVALID_HANDLE.OPERATION_NOT_FOUND",
               "INVALID_HANDLE.SESSION_NOT_FOUND"
             ] ->
        case execute_stream_fun.(request, timeout) do
          {:ok, fresh_stream} ->
            collect_with_reattach(
              fresh_stream,
              session,
              request,
              execute_stream_fun,
              reattach_stream_fun,
              release_execute_fun,
              operation_id,
              timeout,
              max_retries,
              all_items,
              final_last_id,
              attempt + 1
            )

          {:error, %GRPC.RPCError{} = grpc_error} ->
            {:error, Errors.from_grpc_error(grpc_error, session)}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

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
    default_policy = RetryPolicyRegistry.policy(:retry)

    max_retries = Keyword.get(opts, :max_retries, default_policy.max_retries)
    initial_backoff = Keyword.get(opts, :initial_backoff_ms, default_policy.initial_backoff_ms)
    max_backoff = Keyword.get(opts, :max_backoff_ms, default_policy.max_backoff_ms)
    sleep_fun = Keyword.get(opts, :sleep_fun, default_policy.sleep_fun)
    jitter_fun = Keyword.get(opts, :jitter_fun, default_policy.jitter_fun)

    max_server_retry_delay =
      Keyword.get(opts, :max_server_retry_delay, default_policy.max_server_retry_delay)

    do_retry(
      fun,
      0,
      max_retries,
      initial_backoff,
      max_backoff,
      sleep_fun,
      jitter_fun,
      max_server_retry_delay
    )
  end

  defp do_retry(
         fun,
         attempt,
         max_retries,
         initial_backoff,
         max_backoff,
         sleep_fun,
         jitter_fun,
         max_server_retry_delay
       ) do
    case fun.() do
      {:error, %SparkEx.Error.Remote{grpc_status: status} = error}
      when status in [@status_unavailable, @status_deadline_exceeded] and
             attempt < max_retries ->
        sleep_ms =
          backoff_with_retry_info(
            attempt,
            initial_backoff,
            max_backoff,
            max_server_retry_delay,
            jitter_fun,
            error.retry_delay_ms
          )

        :telemetry.execute(
          [:spark_ex, :retry, :attempt],
          %{attempt: attempt + 1, backoff_ms: sleep_ms},
          %{
            grpc_status: status,
            error: error,
            max_retries: max_retries,
            retry_delay_ms: error.retry_delay_ms
          }
        )

        sleep_fun.(sleep_ms)

        do_retry(
          fun,
          attempt + 1,
          max_retries,
          initial_backoff,
          max_backoff,
          sleep_fun,
          jitter_fun,
          max_server_retry_delay
        )

      result ->
        result
    end
  end

  defp backoff_ms(attempt, initial_backoff, max_backoff, jitter_fun) do
    base = initial_backoff * Integer.pow(2, attempt)
    capped = Kernel.min(base, max_backoff)
    jitter_fun.(capped)
  end

  defp prepend_items(acc, items) do
    Enum.reduce(items, acc, fn item, rev_acc ->
      [item | rev_acc]
    end)
  end

  defp backoff_with_retry_info(
         attempt,
         initial_backoff,
         max_backoff,
         max_server_retry_delay,
         jitter_fun,
         retry_delay_ms
       )

  defp backoff_with_retry_info(
         _attempt,
         _initial_backoff,
         _max_backoff,
         max_server_retry_delay,
         _jitter_fun,
         retry_delay_ms
       )
       when is_integer(retry_delay_ms) and retry_delay_ms > 0 do
    Kernel.min(retry_delay_ms, max_server_retry_delay)
  end

  defp backoff_with_retry_info(
         attempt,
         initial_backoff,
         max_backoff,
         _max_server_retry_delay,
         jitter_fun,
         _retry_delay_ms
       ) do
    backoff_ms(attempt, initial_backoff, max_backoff, jitter_fun)
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
    end
  end

  defp result_status({:ok, _}), do: :ok
  defp result_status({:ok, _, _}), do: :ok
  defp result_status({:error, _}), do: :error
  defp result_status(_), do: :error

  defp row_count_metadata({:ok, %{rows: rows}}) when is_list(rows) do
    %{row_count: length(rows)}
  end

  defp row_count_metadata({:ok, %{dataframe: dataframe}}) do
    if Code.ensure_loaded?(Explorer.DataFrame) and match?(%Explorer.DataFrame{}, dataframe) do
      %{row_count: Explorer.DataFrame.n_rows(dataframe)}
    else
      %{}
    end
  end

  defp row_count_metadata(_result), do: %{}

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value
end
