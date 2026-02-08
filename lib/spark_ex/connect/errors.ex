defmodule SparkEx.Error do
  @moduledoc """
  Structured error types for SparkEx.
  """

  defmodule Remote do
    @moduledoc """
    A structured error from the Spark Connect server.
    """
    defexception [
      :error_class,
      :message,
      :sql_state,
      :message_parameters,
      :query_contexts,
      :server_message,
      :grpc_status
    ]

    @type t :: %__MODULE__{
            error_class: String.t() | nil,
            message: String.t(),
            sql_state: String.t() | nil,
            message_parameters: map() | nil,
            query_contexts: [map()] | nil,
            server_message: String.t() | nil,
            grpc_status: non_neg_integer() | nil
          }

    @impl true
    def message(%__MODULE__{} = e) do
      parts = [e.message || e.server_message || "Unknown Spark error"]

      parts =
        if e.error_class do
          parts ++ ["[#{e.error_class}]"]
        else
          parts
        end

      parts =
        if e.sql_state do
          parts ++ ["SQLSTATE: #{e.sql_state}"]
        else
          parts
        end

      Enum.join(parts, " ")
    end
  end
end

defmodule SparkEx.Connect.Errors do
  @moduledoc """
  Handles gRPC error enrichment via `FetchErrorDetails` RPC.

  When the Spark Connect server returns a gRPC error, it includes a
  `google.rpc.ErrorInfo` message in the error details containing an `errorId`.
  This module extracts that ID, calls `FetchErrorDetails`, and maps the
  response into a structured `SparkEx.Error.Remote`.
  """

  alias Spark.Connect.SparkConnectService.Stub
  alias Spark.Connect.{FetchErrorDetailsRequest, FetchErrorDetailsResponse, UserContext}

  @error_info_type_url "type.googleapis.com/google.rpc.ErrorInfo"

  @doc """
  Converts a gRPC error into a structured SparkEx error.

  If the error contains an `errorId` in `google.rpc.ErrorInfo` details,
  calls `FetchErrorDetails` to get the full `SparkThrowable`.
  """
  @spec from_grpc_error(GRPC.RPCError.t(), SparkEx.Session.t()) :: SparkEx.Error.Remote.t()
  def from_grpc_error(%GRPC.RPCError{} = error, session) do
    case extract_error_info(error) do
      {:ok, error_info} ->
        enriched = fetch_error_details(error_info, error, session)
        enriched

      :no_error_info ->
        %SparkEx.Error.Remote{
          message: error.message,
          grpc_status: error.status
        }
    end
  end

  # --- Private ---

  defp extract_error_info(%GRPC.RPCError{details: details}) when is_list(details) do
    Enum.find_value(details, :no_error_info, fn
      %Google.Protobuf.Any{type_url: @error_info_type_url, value: value} ->
        info = Protobuf.decode(value, Google.Rpc.ErrorInfo)
        {:ok, info}

      _ ->
        nil
    end)
  end

  defp extract_error_info(_), do: :no_error_info

  defp fetch_error_details(error_info, grpc_error, session) do
    error_id = Map.get(error_info.metadata, "errorId")

    # Build initial error from ErrorInfo metadata (available even without FetchErrorDetails)
    base_error = %SparkEx.Error.Remote{
      message: grpc_error.message,
      grpc_status: grpc_error.status,
      error_class: Map.get(error_info.metadata, "errorClass"),
      sql_state: Map.get(error_info.metadata, "sqlState"),
      message_parameters: parse_json(Map.get(error_info.metadata, "messageParameters")),
      server_message: Map.get(error_info.metadata, "message")
    }

    if error_id do
      case do_fetch_error_details(error_id, session) do
        {:ok, resp} ->
          enrich_from_response(base_error, resp)

        {:error, _} ->
          base_error
      end
    else
      base_error
    end
  end

  defp do_fetch_error_details(error_id, session) do
    request = %FetchErrorDetailsRequest{
      session_id: session.session_id,
      user_context: %UserContext{user_id: session.user_id},
      client_type: session.client_type,
      client_observed_server_side_session_id: session.server_side_session_id,
      error_id: error_id
    }

    try do
      case Stub.fetch_error_details(session.channel, request) do
        {:ok, %FetchErrorDetailsResponse{} = resp} ->
          {:ok, resp}

        {:error, reason} ->
          {:error, reason}
      end
    rescue
      e -> {:error, e}
    end
  end

  defp enrich_from_response(error, %FetchErrorDetailsResponse{} = resp) do
    case {resp.root_error_idx, resp.errors} do
      {idx, errors} when is_integer(idx) and is_list(errors) and length(errors) > idx ->
        root = Enum.at(errors, idx)

        throwable_fields =
          case root.spark_throwable do
            %FetchErrorDetailsResponse.SparkThrowable{} = t ->
              contexts =
                Enum.map(t.query_contexts || [], fn ctx ->
                  %{
                    context_type: ctx.context_type,
                    object_type: ctx.object_type,
                    object_name: ctx.object_name,
                    fragment: ctx.fragment,
                    summary: ctx.summary
                  }
                end)

              %{
                error_class: t.error_class || error.error_class,
                sql_state: t.sql_state || error.sql_state,
                message_parameters: t.message_parameters || error.message_parameters,
                query_contexts: contexts
              }

            _ ->
              %{}
          end

        %{error | server_message: root.message}
        |> Map.merge(throwable_fields, fn _k, existing, new ->
          new || existing
        end)

      _ ->
        error
    end
  end

  defp parse_json(nil), do: nil
  defp parse_json(""), do: nil

  defp parse_json(str) when is_binary(str) do
    case Jason.decode(str) do
      {:ok, map} when is_map(map) -> map
      _ -> nil
    end
  end
end
