defmodule SparkEx.Connect.Client do
  @moduledoc """
  Low-level gRPC client for Spark Connect RPCs.

  Builds request messages from session state and calls the generated
  `Spark.Connect.SparkConnectService.Stub`.
  """

  alias Spark.Connect.SparkConnectService.Stub

  alias Spark.Connect.{
    AnalyzePlanRequest,
    AnalyzePlanResponse,
    ConfigRequest,
    ConfigResponse,
    ExecutePlanRequest,
    CloneSessionRequest,
    CloneSessionResponse,
    InterruptRequest,
    InterruptResponse,
    ReleaseSessionRequest,
    ReleaseSessionResponse,
    ResultChunkingOptions,
    KeyValue,
    Plan,
    UserContext
  }

  @compile {:no_warn_undefined, Explorer.DataFrame}

  alias SparkEx.Connect.{Errors, ResultDecoder}

  require Logger

  # gRPC status codes considered transient (eligible for retry)
  @status_unavailable 14
  @status_deadline_exceeded 4

  @default_max_retries 3
  @default_initial_backoff_ms 100
  @default_max_backoff_ms 5_000

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

  # --- ExecutePlan RPC ---

  @doc """
  Calls `ExecutePlan` (server-streaming) and decodes the response stream.

  Returns `{:ok, result}` where result contains `:rows`, `:schema`, and
  `:server_side_session_id`.
  """
  @spec execute_plan(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.decode_result()} | {:error, term()}
  def execute_plan(session, plan, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    tags = Keyword.get(opts, :tags, [])

    request = %ExecutePlanRequest{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id,
      plan: plan,
      tags: tags,
      request_options: [
        %ExecutePlanRequest.RequestOption{
          request_option:
            {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
        }
      ]
    }

    metadata = %{rpc: :execute_plan, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
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
    end)
  end

  @doc """
  Calls `ExecutePlan` and decodes the response as an `Explorer.DataFrame`.

  Enforces row and byte limits. See `ResultDecoder.decode_stream_explorer/3`.
  """
  @spec execute_plan_explorer(SparkEx.Session.t(), Plan.t(), keyword()) ::
          {:ok, ResultDecoder.explorer_result()} | {:error, term()}
  def execute_plan_explorer(session, plan, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    tags = Keyword.get(opts, :tags, [])

    request = %ExecutePlanRequest{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id,
      plan: plan,
      tags: tags,
      request_options: [
        %ExecutePlanRequest.RequestOption{
          request_option:
            {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
        }
      ]
    }

    metadata = %{rpc: :execute_plan_explorer, session_id: session.session_id}

    rpc_telemetry_span(metadata, fn ->
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
    end)
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
      user_context: %UserContext{user_id: session.user_id},
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
      user_context: %UserContext{user_id: session.user_id},
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
      user_context: %UserContext{user_id: session.user_id},
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_ALL
    }
  end

  defp build_interrupt_request(session, {:tag, tag}) when is_binary(tag) do
    %InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: %UserContext{user_id: session.user_id},
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_TAG,
      interrupt: {:operation_tag, tag}
    }
  end

  defp build_interrupt_request(session, {:operation_id, id}) when is_binary(id) do
    %InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_side_session_id,
      user_context: %UserContext{user_id: session.user_id},
      client_type: session.client_type,
      interrupt_type: :INTERRUPT_TYPE_OPERATION_ID,
      interrupt: {:operation_id, id}
    }
  end

  # --- Helpers ---

  defp build_analyze_request(session, fields) do
    struct!(
      AnalyzePlanRequest,
      [
        session_id: session.session_id,
        client_type: session.client_type,
        user_context: %UserContext{user_id: session.user_id},
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
        user_context: %UserContext{user_id: session.user_id},
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
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    initial_backoff = Keyword.get(opts, :initial_backoff_ms, @default_initial_backoff_ms)
    max_backoff = Keyword.get(opts, :max_backoff_ms, @default_max_backoff_ms)
    sleep_fun = Keyword.get(opts, :sleep_fun, &Process.sleep/1)
    jitter_fun = Keyword.get(opts, :jitter_fun, &default_jitter/1)

    do_retry(fun, 0, max_retries, initial_backoff, max_backoff, sleep_fun, jitter_fun)
  end

  defp do_retry(fun, attempt, max_retries, initial_backoff, max_backoff, sleep_fun, jitter_fun) do
    case fun.() do
      {:error, %SparkEx.Error.Remote{grpc_status: status} = error}
      when status in [@status_unavailable, @status_deadline_exceeded] and
             attempt < max_retries ->
        sleep_ms = backoff_ms(attempt, initial_backoff, max_backoff, jitter_fun)

        :telemetry.execute(
          [:spark_ex, :retry, :attempt],
          %{attempt: attempt + 1, backoff_ms: sleep_ms},
          %{grpc_status: status, error: error, max_retries: max_retries}
        )

        sleep_fun.(sleep_ms)

        do_retry(
          fun,
          attempt + 1,
          max_retries,
          initial_backoff,
          max_backoff,
          sleep_fun,
          jitter_fun
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

  defp default_jitter(capped) do
    # Add jitter: random value between 0 and capped
    :rand.uniform(capped + 1) - 1
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
