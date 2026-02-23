defmodule SparkEx.StreamingQuery do
  @moduledoc """
  Controls a running structured streaming query.

  Mirrors PySpark's `StreamingQuery` with methods for monitoring
  and controlling the query lifecycle.

  ## Examples

      {:ok, query} = writer |> SparkEx.StreamWriter.start()
      {:ok, true} = SparkEx.StreamingQuery.is_active?(query)
      :ok = SparkEx.StreamingQuery.stop(query)
  """

  defstruct [:session, :query_id, :run_id, :name]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          query_id: String.t(),
          run_id: String.t(),
          name: String.t() | nil
        }

  @doc """
  Stops the streaming query.
  """
  @spec stop(t()) :: :ok | {:error, term()}
  def stop(%__MODULE__{} = query) do
    case execute_command(query, {:stop}) do
      {:ok, _result} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns the streaming query ID.
  """
  @spec id(t()) :: String.t()
  def id(%__MODULE__{} = query), do: query.query_id

  @doc """
  Returns the streaming query run ID.
  """
  @spec run_id(t()) :: String.t()
  def run_id(%__MODULE__{} = query), do: query.run_id

  @doc """
  Returns the streaming query name.
  """
  @spec name(t()) :: String.t() | nil
  def name(%__MODULE__{} = query), do: query.name

  @doc """
  Returns whether the streaming query is currently active.
  """
  @spec is_active?(t()) :: {:ok, boolean()} | {:error, term()}
  def is_active?(%__MODULE__{} = query) do
    case execute_command(query, {:status}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:status, status} -> {:ok, status.is_active}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the current status of the streaming query.
  """
  @spec status(t()) :: {:ok, map()} | {:error, term()}
  def status(%__MODULE__{} = query) do
    case execute_command(query, {:status}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:status, status} ->
            {:ok,
             %{
               message: status.status_message,
               is_data_available: status.is_data_available,
               is_trigger_active: status.is_trigger_active,
               is_active: status.is_active
             }}

          _ ->
            {:error, {:unexpected_result, result.result_type}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Waits for the termination of the query.

  ## Options

    * `:timeout` — timeout in milliseconds (default: no timeout)
  """
  @spec await_termination(t(), keyword()) :: {:ok, boolean() | nil} | {:error, term()}
  def await_termination(%__MODULE__{} = query, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, nil)

    case execute_command(query, {:await_termination, timeout_ms}, opts) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:await_termination, at} -> {:ok, at.terminated}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Blocks until all available data in the source has been processed.
  """
  @spec process_all_available(t()) :: :ok | {:error, term()}
  def process_all_available(%__MODULE__{} = query) do
    case execute_command(query, {:process_all_available}) do
      {:ok, _result} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns a list of recent progress reports as JSON strings.
  """
  @spec recent_progress(t()) :: {:ok, [String.t()]} | {:error, term()}
  def recent_progress(%__MODULE__{} = query) do
    case execute_command(query, {:recent_progress}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:recent_progress, rp} ->
            {:ok, rp.recent_progress_json}

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the most recent progress report as a JSON string, or nil.
  """
  @spec last_progress(t()) :: {:ok, String.t() | nil} | {:error, term()}
  def last_progress(%__MODULE__{} = query) do
    case execute_command(query, {:last_progress}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:recent_progress, rp} ->
            case List.last(rp.recent_progress_json) do
              nil -> {:ok, nil}
              json -> {:ok, json}
            end

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the query plan as a string.

  ## Options

    * `:extended` — whether to include extended details (default: false)
  """
  @spec explain(t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def explain(%__MODULE__{} = query, opts \\ []) do
    extended = Keyword.get(opts, :extended, false)

    unless is_boolean(extended) do
      raise ArgumentError, "expected :extended to be a boolean, got: #{inspect(extended)}"
    end

    case execute_command(query, {:explain, extended}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:explain, explain} -> {:ok, explain.result}
          other -> {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns exception information if the query has terminated with an error.
  """
  @spec exception(t()) :: {:ok, map() | nil} | {:error, term()}
  def exception(%__MODULE__{} = query) do
    case execute_command(query, {:exception}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:exception, ex} ->
            if ex.exception_message do
              message = strip_exception_type_prefix(ex.exception_message)

              message =
                if ex.stack_trace do
                  message <> "\n\n" <> "JVM stacktrace:\n" <> ex.stack_trace
                else
                  message
                end

              {:ok,
               %{
                 message: message,
                 error_class: ex.error_class,
                 stack_trace: ex.stack_trace
               }}
            else
              {:ok, nil}
            end

          # No exception set in response means no exception occurred
          nil ->
            {:ok, nil}

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  # --- Private ---

  defp execute_command(query, cmd_type, opts \\ []) do
    SparkEx.Session.execute_command_with_result(
      query.session,
      {:streaming_query_command, query.query_id, query.run_id, cmd_type},
      opts
    )
  end

  # PySpark strips the Java exception type prefix from the message.
  # e.g. "org.apache.spark.SparkException: some error" -> "some error"
  defp strip_exception_type_prefix(message) when is_binary(message) do
    case String.split(message, ": ", parts: 2) do
      [_type_prefix, rest] -> rest
      [msg] -> msg
    end
  end
end
