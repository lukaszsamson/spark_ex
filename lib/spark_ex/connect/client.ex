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

    request = build_execute_request(session, plan, tags, operation_id, reattachable)
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

    request = build_execute_request(session, plan, tags, operation_id, reattachable)

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
      user_context: %UserContext{user_id: session.user_id},
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
      user_context: %UserContext{user_id: session.user_id},
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
    max_retries = Keyword.get(opts, :reattach_retries, @default_max_retries)

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

            release_execute_best_effort(release_execute_fun)

            result

          {:error, _} = error ->
            release_execute_best_effort(release_execute_fun)
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
        all_items = acc ++ new_items
        final_last_id = last_id || last_response_id

        release_checkpoints_best_effort(release_execute_fun, new_items)

        if result_complete? do
          {:ok, all_items, final_last_id}
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
        all_items = acc ++ new_items
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

  defp build_execute_request(session, plan, tags, operation_id, reattachable) do
    request_options =
      [
        %ExecutePlanRequest.RequestOption{
          request_option:
            {:result_chunking_options, %ResultChunkingOptions{allow_arrow_batch_chunking: true}}
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
      user_context: %UserContext{user_id: session.user_id},
      client_observed_server_side_session_id: session.server_side_session_id,
      plan: plan,
      tags: tags,
      operation_id: operation_id,
      request_options: request_options
    }
  end

  defp release_execute_best_effort(release_execute_fun, opts \\ []) do
    Task.start(fn ->
      release_execute_fun.(opts)
    end)
  end

  defp release_checkpoints_best_effort(release_execute_fun, responses) do
    responses
    |> Enum.reject(fn resp -> match?({:result_complete, _}, resp.response_type) end)
    |> Enum.map(&response_id_or_nil(&1.response_id))
    |> Enum.reject(&is_nil/1)
    |> Enum.each(fn response_id ->
      release_execute_best_effort(release_execute_fun, until_response_id: response_id)
    end)
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

    sleep_ms =
      backoff_ms(attempt, @default_initial_backoff_ms, @default_max_backoff_ms, &default_jitter/1)

    Process.sleep(sleep_ms)

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
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)

    <<a::48, 4::4, b::12, 2::2, c::62>>
    |> encode_uuid()
  end

  defp encode_uuid(<<a::32, b::16, c::16, d::16, e::48>>) do
    hex = &Base.encode16(&1, case: :lower)

    [
      hex.(<<a::32>>),
      "-",
      hex.(<<b::16>>),
      "-",
      hex.(<<c::16>>),
      "-",
      hex.(<<d::16>>),
      "-",
      hex.(<<e::48>>)
    ]
    |> IO.iodata_to_binary()
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
