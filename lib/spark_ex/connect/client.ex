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
    ResultChunkingOptions,
    KeyValue,
    Plan,
    UserContext
  }

  alias SparkEx.Connect.{Errors, ResultDecoder}

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

    request = %ExecutePlanRequest{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id,
      plan: plan,
      request_options: [
        %ExecutePlanRequest.RequestOption{
          request_option:
            {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
        }
      ]
    }

    case Stub.execute_plan(session.channel, request, timeout: timeout) do
      {:ok, stream} ->
        ResultDecoder.decode_stream(stream, session)

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
end
