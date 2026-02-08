defmodule SparkEx.Connect.Client do
  @moduledoc """
  Low-level gRPC client for Spark Connect RPCs.

  Builds request messages from session state and calls the generated
  `Spark.Connect.SparkConnectService.Stub`.
  """

  alias Spark.Connect.SparkConnectService.Stub
  alias Spark.Connect.{AnalyzePlanRequest, AnalyzePlanResponse, UserContext}

  @doc """
  Calls `AnalyzePlan` with `SparkVersion` to retrieve the Spark version string.

  Returns `{:ok, version_string, server_side_session_id}` or `{:error, reason}`.
  """
  @spec analyze_spark_version(SparkEx.Session.t()) ::
          {:ok, String.t(), String.t() | nil} | {:error, term()}
  def analyze_spark_version(session) do
    request = %AnalyzePlanRequest{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id,
      analyze: {:spark_version, %AnalyzePlanRequest.SparkVersion{}}
    }

    case Stub.analyze_plan(session.channel, request) do
      {:ok, %AnalyzePlanResponse{result: {:spark_version, %{version: version}}} = resp} ->
        {:ok, version, resp.server_side_session_id}

      {:ok, %AnalyzePlanResponse{result: other}} ->
        {:error, {:unexpected_response, other}}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, {:grpc_error, error.status, error.message}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Builds the common request fields from session state.
  """
  @spec build_request_base(SparkEx.Session.t()) :: map()
  def build_request_base(session) do
    %{
      session_id: session.session_id,
      client_type: session.client_type,
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id
    }
  end
end
