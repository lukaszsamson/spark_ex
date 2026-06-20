defmodule SparkEx.Error do
  @moduledoc """
  Structured error types for SparkEx.
  """

  defmodule LimitExceeded do
    @moduledoc """
    Raised when a materialization operation exceeds configured bounds.

    Provides `remediation` guidance to help users adjust their query or limits.
    """
    defexception [:limit_type, :limit_value, :actual_value, :remediation]

    @type t :: %__MODULE__{
            limit_type: :rows | :bytes,
            limit_value: non_neg_integer(),
            actual_value: non_neg_integer() | nil,
            remediation: String.t()
          }

    @impl true
    def message(%__MODULE__{} = e) do
      base =
        case e.limit_type do
          :rows ->
            "Collection limit exceeded: maximum #{e.limit_value} rows"

          :bytes ->
            "Collection limit exceeded: maximum #{format_bytes(e.limit_value)}"
        end

      base =
        if e.actual_value do
          base <> " (got #{e.actual_value})"
        else
          base
        end

      if e.remediation do
        base <> ". " <> e.remediation
      else
        base
      end
    end

    defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{div(bytes, 1_048_576)} MB"
    defp format_bytes(bytes) when bytes >= 1_024, do: "#{div(bytes, 1_024)} KB"
    defp format_bytes(bytes), do: "#{bytes} bytes"
  end

  defmodule ResponseAlreadyReceived do
    @moduledoc """
    Raised when reattach discovers the operation/session is gone server-side
    after the client has already buffered partial responses.

    Mirrors PySpark's `RESPONSE_ALREADY_RECEIVED`: a fresh `ExecutePlan` would
    duplicate already-consumed responses, and discarding the buffer would
    silently drop user-visible data, so neither recovery path is safe.
    """
    defexception [:operation_id, :last_response_id, :buffered_count, :cause]

    @type t :: %__MODULE__{
            operation_id: String.t() | nil,
            last_response_id: String.t() | nil,
            buffered_count: non_neg_integer(),
            cause: term()
          }

    @impl true
    def message(%__MODULE__{} = e) do
      "Reattach failed after #{e.buffered_count} response(s) were already buffered " <>
        "(operation_id=#{inspect(e.operation_id)}, last_response_id=#{inspect(e.last_response_id)}). " <>
        "Re-issuing ExecutePlan would duplicate consumed responses; the previous result is unrecoverable."
    end
  end

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
      :stacktrace,
      :server_message,
      :grpc_status,
      :retry_delay_ms,
      :has_retry_info,
      :classes,
      :error_type_hierarchy,
      :stack_trace_inline,
      :cause_chain,
      :breaking_change_info
    ]

    @type stack_frame :: %{
            declaring_class: String.t(),
            method_name: String.t(),
            file_name: String.t() | nil,
            line_number: integer()
          }

    @type cause :: %{
            message: String.t() | nil,
            error_type_hierarchy: [String.t()],
            stack_trace: [stack_frame()]
          }

    @type breaking_change :: %{
            optional(:migration_message) => [String.t()],
            optional(:needs_audit) => boolean() | nil,
            optional(:mitigation_config) => %{key: String.t(), value: String.t()}
          }

    @type t :: %__MODULE__{
            error_class: String.t() | nil,
            message: String.t(),
            sql_state: String.t() | nil,
            message_parameters: map() | String.t() | nil,
            query_contexts: [map()] | nil,
            stacktrace: [stack_frame()] | nil,
            server_message: String.t() | nil,
            grpc_status: non_neg_integer() | nil,
            retry_delay_ms: non_neg_integer() | nil,
            has_retry_info: boolean() | nil,
            classes: [String.t()] | nil,
            error_type_hierarchy: [String.t()] | nil,
            stack_trace_inline: String.t() | nil,
            cause_chain: [cause()] | nil,
            breaking_change_info: breaking_change() | nil
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
  alias Spark.Connect.{FetchErrorDetailsRequest, FetchErrorDetailsResponse}
  alias SparkEx.UserContextExtensions

  @error_info_type_url "type.googleapis.com/google.rpc.ErrorInfo"
  @retry_info_type_url "type.googleapis.com/google.rpc.RetryInfo"

  # Bound the FetchErrorDetails enrichment so a slow/hanging server cannot
  # stall the failed-RPC handle_call path indefinitely. On timeout the
  # caller falls back to the base error built from inline ErrorInfo
  # metadata.
  @fetch_error_details_timeout_ms 5_000

  @doc """
  Converts a gRPC error into a structured SparkEx error.

  If the error contains an `errorId` in `google.rpc.ErrorInfo` details,
  calls `FetchErrorDetails` to get the full `SparkThrowable`.
  """
  @spec from_grpc_error(GRPC.RPCError.t(), SparkEx.Session.t()) :: SparkEx.Error.Remote.t()
  def from_grpc_error(%GRPC.RPCError{} = error, session) do
    retry_delay_ms = extract_retry_delay_ms(error)
    # Track RetryInfo *presence* separately from its retry_delay: PySpark retries
    # any error whose status details carry a RetryInfo, even when retry_delay is
    # unset (retries.py:367-369). retry_delay_ms can be nil in that case.
    has_retry_info = has_retry_info?(error)

    case extract_error_info(error) do
      {:ok, error_info} ->
        enriched = fetch_error_details(error_info, error, session)

        enriched
        |> maybe_add_retry_delay(retry_delay_ms)
        |> Map.put(:has_retry_info, has_retry_info)

      :no_error_info ->
        %SparkEx.Error.Remote{
          message: error.message,
          grpc_status: error.status,
          retry_delay_ms: retry_delay_ms,
          has_retry_info: has_retry_info
        }
    end
  end

  # --- Private ---

  defp extract_error_info(%GRPC.RPCError{details: details}) when is_list(details) do
    Enum.find_value(details, :no_error_info, fn
      %Google.Protobuf.Any{type_url: @error_info_type_url, value: value} ->
        case safe_decode(value, Google.Rpc.ErrorInfo) do
          {:ok, info} -> {:ok, info}
          :error -> nil
        end

      _ ->
        nil
    end)
  end

  defp extract_error_info(_), do: :no_error_info

  defp extract_retry_delay_ms(%GRPC.RPCError{details: details}) when is_list(details) do
    details
    |> Enum.find_value(nil, fn
      %Google.Protobuf.Any{type_url: @retry_info_type_url, value: value} ->
        case safe_decode(value, Google.Rpc.RetryInfo) do
          {:ok, info} -> retry_info_to_ms(info)
          :error -> nil
        end

      _ ->
        nil
    end)
  end

  defp extract_retry_delay_ms(_), do: nil

  # True when the gRPC error status details contain a (decodable) RetryInfo,
  # regardless of whether retry_delay is set.
  defp has_retry_info?(%GRPC.RPCError{details: details}) when is_list(details) do
    Enum.any?(details, fn
      %Google.Protobuf.Any{type_url: @retry_info_type_url, value: value} ->
        match?({:ok, _}, safe_decode(value, Google.Rpc.RetryInfo))

      _ ->
        false
    end)
  end

  defp has_retry_info?(_), do: false

  defp safe_decode(value, module) do
    {:ok, Protobuf.decode(value, module)}
  rescue
    _ -> :error
  end

  # Returns nil ("no hint") when RetryInfo carries no retry_delay so the
  # retry loop falls back to its policy backoff instead of treating "no
  # hint" as "retry immediately".
  defp retry_info_to_ms(%Google.Rpc.RetryInfo{retry_delay: nil}), do: nil

  defp retry_info_to_ms(%Google.Rpc.RetryInfo{
         retry_delay: %Google.Protobuf.Duration{} = duration
       }) do
    duration_to_ms(duration)
  end

  defp duration_to_ms(%Google.Protobuf.Duration{seconds: seconds, nanos: nanos}) do
    seconds * 1000 + div(nanos, 1_000_000)
  end

  defp maybe_add_retry_delay(%SparkEx.Error.Remote{} = error, nil), do: error

  defp maybe_add_retry_delay(%SparkEx.Error.Remote{} = error, retry_delay_ms) do
    %{error | retry_delay_ms: retry_delay_ms}
  end

  defp fetch_error_details(error_info, grpc_error, session) do
    error_id = Map.get(error_info.metadata, "errorId")

    # Build initial error from ErrorInfo metadata (available even without FetchErrorDetails).
    # PySpark's _convert_exception (errors/exceptions/connect.py) reads `classes`
    # (JSON list of Java exception class names) and `stackTrace` (raw JVM stacktrace
    # string) directly from ErrorInfo metadata; mirror that so callers can pattern
    # match on Java exception class names without a separate FetchErrorDetails call.
    base_error = %SparkEx.Error.Remote{
      message: grpc_error.message,
      grpc_status: grpc_error.status,
      error_class: Map.get(error_info.metadata, "errorClass"),
      sql_state: Map.get(error_info.metadata, "sqlState"),
      message_parameters: parse_json(Map.get(error_info.metadata, "messageParameters")),
      server_message: Map.get(error_info.metadata, "message"),
      classes: parse_classes(Map.get(error_info.metadata, "classes")),
      stack_trace_inline: Map.get(error_info.metadata, "stackTrace")
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
    do_fetch_error_details(error_id, session, @fetch_error_details_timeout_ms)
  end

  defp do_fetch_error_details(error_id, session, timeout_ms) do
    request = %FetchErrorDetailsRequest{
      session_id: session.session_id,
      user_context: UserContextExtensions.build_user_context(session.user_id),
      client_type: session.client_type,
      client_observed_server_side_session_id: session.server_side_session_id,
      error_id: error_id
    }

    # Run on a supervised task so a slow/hanging server can't block the
    # caller's GenServer handle_call beyond `timeout_ms`. If the yield
    # times out we fire-and-forget a graceful shutdown in a separate task
    # (to avoid adding an extra 1 s to the caller's wall-clock wait) and
    # fall back to the base error immediately.
    task =
      Task.Supervisor.async_nolink(SparkEx.TaskSupervisor, fn ->
        try do
          # Pass timeout to gRPC too so the RPC itself is bounded even if
          # the task supervisor cannot interrupt it cleanly.
          Stub.fetch_error_details(session.channel, request, timeout: timeout_ms)
        rescue
          e -> {:error, e}
        catch
          kind, reason -> {:error, {kind, reason}}
        end
      end)

    case Task.yield(task, timeout_ms) do
      nil ->
        # Yield timed out — shut the task down asynchronously so the
        # caller is not blocked for an additional grace period.
        Task.Supervisor.start_child(SparkEx.TaskSupervisor, fn ->
          _ = Task.shutdown(task, 1_000)
        end)

        {:error, :timeout}

      result ->
        case result do
          {:ok, {:ok, %FetchErrorDetailsResponse{} = resp}} ->
            {:ok, resp}

          {:ok, {:error, reason}} ->
            {:error, reason}

          {:ok, other} ->
            {:error, {:unexpected_response, other}}

          {:exit, reason} ->
            {:error, {:task_exit, reason}}
        end
    end
  end

  defp enrich_from_response(error, %FetchErrorDetailsResponse{} = resp) do
    case {resp.root_error_idx, resp.errors} do
      {idx, errors} when is_integer(idx) and is_list(errors) and idx >= 0 ->
        errors_tuple = List.to_tuple(errors)

        if idx >= tuple_size(errors_tuple) do
          error
        else
          root = elem(errors_tuple, idx)
          throwable_fields = extract_throwable_fields(error, root.spark_throwable)
          cause_chain = walk_cause_chain(errors_tuple, idx)
          stack_trace_inline = format_jvm_stacktrace(cause_chain) || error.stack_trace_inline

          %{
            error
            | server_message: root.message,
              stacktrace: map_stack_trace(root.stack_trace),
              error_type_hierarchy: root.error_type_hierarchy || [],
              cause_chain: cause_chain,
              stack_trace_inline: stack_trace_inline
          }
          |> Map.merge(throwable_fields, fn _k, existing, new -> new || existing end)
        end

      _ ->
        error
    end
  end

  defp extract_throwable_fields(_error, %FetchErrorDetailsResponse.SparkThrowable{} = t) do
    %{
      error_class: t.error_class,
      sql_state: t.sql_state,
      message_parameters: t.message_parameters,
      query_contexts: map_query_contexts(t.query_contexts || []),
      breaking_change_info: map_breaking_change_info(t.breaking_change_info)
    }
  end

  defp extract_throwable_fields(_error, _other), do: %{}

  # Walk the cause_idx chain starting from `idx`, mirroring PySpark's
  # `_extract_jvm_stacktrace` recursion. Guards against cycles by tracking
  # visited indices.
  @doc false
  def walk_cause_chain(errors_tuple, idx) do
    walk_cause_chain(errors_tuple, idx, %{})
  end

  defp walk_cause_chain(errors_tuple, idx, seen) do
    cond do
      Map.has_key?(seen, idx) ->
        []

      idx < 0 or idx >= tuple_size(errors_tuple) ->
        []

      true ->
        err = elem(errors_tuple, idx)
        seen = Map.put(seen, idx, true)

        entry = %{
          message: err.message,
          error_type_hierarchy: err.error_type_hierarchy || [],
          stack_trace: map_stack_trace(err.stack_trace)
        }

        case err.cause_idx do
          nil -> [entry]
          cause when is_integer(cause) -> [entry | walk_cause_chain(errors_tuple, cause, seen)]
        end
    end
  end

  defp map_stack_trace(nil), do: []

  defp map_stack_trace(frames) when is_list(frames) do
    Enum.map(frames, fn f ->
      %{
        declaring_class: f.declaring_class,
        method_name: f.method_name,
        file_name: f.file_name,
        line_number: f.line_number
      }
    end)
  end

  defp map_breaking_change_info(nil), do: nil

  defp map_breaking_change_info(%FetchErrorDetailsResponse.BreakingChangeInfo{} = bci) do
    base = %{migration_message: bci.migration_message || []}

    base =
      case bci.needs_audit do
        nil -> base
        v -> Map.put(base, :needs_audit, v)
      end

    case bci.mitigation_config do
      nil ->
        base

      %FetchErrorDetailsResponse.MitigationConfig{key: k, value: v} ->
        Map.put(base, :mitigation_config, %{key: k, value: v})
    end
  end

  @doc false
  def format_jvm_stacktrace([]), do: nil

  @doc false
  def format_jvm_stacktrace([root | _] = chain) do
    case root.error_type_hierarchy do
      [top | _] ->
        header = "#{top}: #{root.message}"
        lines = format_chain(chain, [header], true)
        Enum.join(lines, "\n")

      _ ->
        nil
    end
  end

  defp format_chain([], acc, _root?), do: Enum.reverse(acc)

  defp format_chain([err | rest], acc, root?) do
    acc =
      if root? do
        acc
      else
        head =
          case err.error_type_hierarchy do
            [top | _] -> top
            _ -> ""
          end

        ["Caused by: #{head}: #{err.message}" | acc]
      end

    acc =
      Enum.reduce(err.stack_trace, acc, fn f, inner ->
        ["\tat #{f.declaring_class}.#{f.method_name}(#{f.file_name}:#{f.line_number})" | inner]
      end)

    format_chain(rest, acc, false)
  end

  defp map_query_contexts(contexts) do
    Enum.map(contexts, fn ctx ->
      %{
        context_type: ctx.context_type,
        object_type: ctx.object_type,
        object_name: ctx.object_name,
        start_index: ctx.start_index,
        stop_index: ctx.stop_index,
        fragment: ctx.fragment,
        call_site: ctx.call_site,
        summary: ctx.summary
      }
    end)
  end

  # PySpark hands `messageParameters` in as `dict(json.loads(raw))`, but the
  # spec only guarantees JSON. Surface non-map JSON (lists, primitives) as the
  # raw string so the user can see *something*; this is more useful than a
  # silent nil and avoids losing diagnostic data on a server-shape change.
  defp parse_json(nil), do: nil
  defp parse_json(""), do: nil

  defp parse_json(str) when is_binary(str) do
    case Jason.decode(str) do
      {:ok, map} when is_map(map) -> map
      {:ok, _other} -> str
      _ -> str
    end
  end

  # `classes` is documented as a JSON array of Java exception class names.
  defp parse_classes(nil), do: nil
  defp parse_classes(""), do: nil

  defp parse_classes(str) when is_binary(str) do
    case Jason.decode(str) do
      {:ok, list} when is_list(list) -> Enum.filter(list, &is_binary/1)
      _ -> nil
    end
  end
end
