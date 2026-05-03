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

    * `:timeout` — timeout in seconds (default: no timeout). Must be a
      positive number when set; `nil` means wait forever. Mirrors
      PySpark's `awaitTermination(timeout=None)` semantics.

  Returns `{:ok, nil}` for the no-timeout form (wait forever, returns
  only after the query terminates). Returns `{:ok, terminated?}` for
  the timeout form, where `terminated?` is `true` if the query
  terminated within the timeout and `false` otherwise.
  """
  @spec await_termination(t(), keyword()) :: {:ok, boolean() | nil} | {:error, term()}
  def await_termination(%__MODULE__{} = query, opts \\ []) do
    {timeout_ms, opts} = SparkEx.Internal.StreamingTimeout.resolve(opts)
    opts = Keyword.put_new(opts, :reattach_policy, :streaming)

    case execute_command(query, {:await_termination, timeout_ms}, opts) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:await_termination, at} ->
            if timeout_ms == nil, do: {:ok, nil}, else: {:ok, at.terminated}

          other ->
            {:error, {:unexpected_result, other}}
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
    case execute_command(query, {:process_all_available}, reattach_policy: :streaming) do
      {:ok, _result} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns a list of recent progress reports as parsed maps.

  Each entry is the JSON-decoded `StreamingQueryProgress` reported by
  the server. If any entry fails to parse, the whole call returns
  `{:error, {:invalid_progress_json, reason, raw_payload}}` — matching
  the contract of `last_progress/1`.
  """
  @spec recent_progress(t()) :: {:ok, [map()]} | {:error, term()}
  def recent_progress(%__MODULE__{} = query) do
    case execute_command(query, {:recent_progress}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:recent_progress, rp} ->
            parse_progress_list(rp.recent_progress_json)

          other ->
            {:error, {:unexpected_result, other}}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns the most recent progress report as a parsed map, or `nil` if
  the server has not produced any progress yet.

  Returns `{:error, {:invalid_progress_json, reason, raw_payload}}` when
  the server-supplied JSON cannot be decoded — matching the contract of
  `recent_progress/1`.
  """
  @spec last_progress(t()) :: {:ok, map() | nil} | {:error, term()}
  def last_progress(%__MODULE__{} = query) do
    case execute_command(query, {:last_progress}) do
      {:ok, {:streaming_query, result}} ->
        case result.result_type do
          {:recent_progress, rp} ->
            case List.last(rp.recent_progress_json) do
              nil -> {:ok, nil}
              json -> parse_progress_ok(json)
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
            # `exception_message`, `error_class`, and `stack_trace` are
            # `proto3_optional` fields. Use HasField semantics (nil ==
            # unset) so that an explicitly empty exception message is not
            # treated as "no exception".
            if is_binary(ex.exception_message) do
              message = strip_exception_type_prefix(ex.exception_message)

              message =
                if is_binary(ex.stack_trace) do
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

  defp parse_progress_ok(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, reason} -> {:error, {:invalid_progress_json, reason, json}}
    end
  end

  defp parse_progress_ok(other), do: {:error, {:invalid_progress_payload, other}}

  defp parse_progress_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case parse_progress_ok(entry) do
        {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      {:error, _} = error -> error
    end
  end
end
