defmodule SparkEx.StreamingQueryManager do
  @moduledoc """
  Manages all active streaming queries for a Spark session.

  Mirrors PySpark's `StreamingQueryManager`.

  ## Examples

      {:ok, queries} = SparkEx.StreamingQueryManager.active(session)
      {:ok, query} = SparkEx.StreamingQueryManager.get(session, "some-query-id")
  """

  alias SparkEx.StreamingQuery

  @doc """
  Returns a list of all active streaming queries.
  """
  @spec active(GenServer.server()) :: {:ok, [StreamingQuery.t()]} | {:error, term()}
  def active(session) do
    case execute_command(session, {:active}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:active, active_result} ->
            queries =
              Enum.map(active_result.active_queries, fn instance ->
                %StreamingQuery{
                  session: session,
                  query_id: instance.id.id,
                  run_id: instance.id.run_id,
                  name: if(instance.name == "", do: nil, else: instance.name)
                }
              end)

            {:ok, queries}

          _ ->
            {:ok, []}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the streaming query with the given ID, or nil if not found.
  """
  @spec get(GenServer.server(), String.t()) :: {:ok, StreamingQuery.t() | nil} | {:error, term()}
  def get(session, query_id) when is_binary(query_id) do
    case execute_command(session, {:get_query, query_id}) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:query, instance} ->
            query = %StreamingQuery{
              session: session,
              query_id: instance.id.id,
              run_id: instance.id.run_id,
              name: if(instance.name == "", do: nil, else: instance.name)
            }

            {:ok, query}

          _ ->
            {:ok, nil}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Waits until any of the active queries terminates.

  ## Options

    * `:timeout` — timeout in milliseconds (default: no timeout)
  """
  @spec await_any_termination(GenServer.server(), keyword()) ::
          {:ok, boolean() | nil} | {:error, term()}
  def await_any_termination(session, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, nil)

    case execute_command(session, {:await_any_termination, timeout_ms}, opts) do
      {:ok, {:streaming_query_manager, result}} ->
        case result.result_type do
          {:await_any_termination, at} -> {:ok, at.terminated}
          _ -> {:ok, nil}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Forgets about past terminated queries so that `active/1` only returns current queries.
  """
  @spec reset_terminated(GenServer.server()) :: :ok | {:error, term()}
  def reset_terminated(session) do
    case execute_command(session, {:reset_terminated}) do
      {:ok, _result} -> :ok
      {:error, _} = error -> error
    end
  end

  # --- Private ---

  defp execute_command(session, cmd_type, opts \\ []) do
    SparkEx.Session.execute_command_with_result(
      session,
      {:streaming_query_manager_command, cmd_type},
      opts
    )
  end
end
