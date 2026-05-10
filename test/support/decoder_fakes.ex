defmodule SparkEx.Test.DecoderFakes do
  @moduledoc """
  Fakes for `SparkEx.Connect.ResultDecoder` input streams.

  Builders return `{:ok, %ExecutePlanResponse{...}}` or `{:error, ...}`
  tuples in the exact shape that the decoder consumes from the gRPC
  client. They are designed for Stream C (result decoder + reattach
  lifecycle) tests where deterministic injection of:

    - ResultComplete (with optional trailing frames),
    - chunked Arrow batches,
    - observed/execution metrics,
    - gRPC errors carrying `Google.Rpc.RetryInfo` details,

  matters for forward-compat and retry-policy assertions.

  ## Quick example

      iter = DecoderFakes.iterator([
        DecoderFakes.arrow_batch(<<1, 2, 3>>, row_count: 1),
        DecoderFakes.result_complete(),
        DecoderFakes.metrics(execution: %{{"scan", 1} => %{"rows" => 10}})
      ])

      assert {:ok, _} = ResultDecoder.decode_stream(iter)
  """

  alias Spark.Connect.ExecutePlanResponse

  alias Spark.Connect.ExecutePlanResponse.{
    ArrowBatch,
    ExecutionProgress,
    Metrics,
    ObservedMetrics,
    ResultComplete,
    SqlCommandResult
  }

  @type frame :: {:ok, ExecutePlanResponse.t()} | {:error, term()}

  # ── Frame builders ──────────────────────────────────────────────────────

  @doc "Build an `:arrow_batch` frame."
  @spec arrow_batch(binary(), keyword()) :: frame()
  def arrow_batch(data, opts \\ []) when is_binary(data) do
    batch = %ArrowBatch{
      data: data,
      row_count: Keyword.get(opts, :row_count, 1),
      start_offset: Keyword.get(opts, :start_offset, 0),
      chunk_index: Keyword.get(opts, :chunk_index, 0),
      num_chunks_in_batch: Keyword.get(opts, :num_chunks_in_batch, 1)
    }

    response(opts, response_type: {:arrow_batch, batch})
  end

  @doc "Build a `:result_complete` frame."
  @spec result_complete(keyword()) :: frame()
  def result_complete(opts \\ []) do
    response(opts, response_type: {:result_complete, %ResultComplete{}})
  end

  @doc "Build a `:sql_command_result` frame."
  @spec sql_command_result(Spark.Connect.Relation.t() | nil, keyword()) :: frame()
  def sql_command_result(relation \\ nil, opts \\ []) do
    response(opts, response_type: {:sql_command_result, %SqlCommandResult{relation: relation}})
  end

  @doc "Build an `:execution_progress` frame."
  @spec execution_progress(keyword()) :: frame()
  def execution_progress(opts \\ []) do
    response(opts, response_type: {:execution_progress, %ExecutionProgress{}})
  end

  @doc """
  Build a frame that carries observed metrics + (optionally) a `result_complete` payload.

  Pass `observed:` as a list of `{name, [{key, %Literal{}}]}` pairs.
  """
  @spec observed(keyword()) :: frame()
  def observed(opts) do
    list =
      opts
      |> Keyword.fetch!(:observed)
      |> Enum.map(fn {name, kvs} ->
        {keys, vals} = Enum.unzip(kvs)
        %ObservedMetrics{name: name, keys: keys, values: vals}
      end)

    fields =
      [observed_metrics: list]
      |> maybe_put(:response_type, opts[:response_type])

    response(opts, fields)
  end

  @doc """
  Build a frame containing execution metrics. `execution:` is a map
  `%{{name, plan_id} => %{metric_name => integer_value}}`.
  """
  @spec metrics(keyword()) :: frame()
  def metrics(opts) do
    execution = Keyword.fetch!(opts, :execution)

    metric_objects =
      Enum.map(execution, fn {{name, plan_id}, kv} ->
        %Metrics.MetricObject{
          name: name,
          plan_id: plan_id,
          execution_metrics: Map.new(kv, fn {k, v} -> {k, %Metrics.MetricValue{value: v}} end)
        }
      end)

    response(opts,
      metrics: %Metrics{metrics: metric_objects},
      response_type: opts[:response_type]
    )
  end

  # ── Error frames ────────────────────────────────────────────────────────

  @doc "Build an `{:error, %GRPC.RPCError{}}` frame with optional details."
  @spec grpc_error(non_neg_integer(), String.t(), keyword()) :: frame()
  def grpc_error(status, message, opts \\ []) do
    {:error,
     %GRPC.RPCError{
       status: status,
       message: message,
       details: Keyword.get(opts, :details, [])
     }}
  end

  @doc """
  Build a `{:error, %GRPC.RPCError{}}` frame carrying a `Google.Rpc.RetryInfo`
  detail (used by reattach retry policy).
  """
  @spec retryable_error(non_neg_integer(), String.t(), non_neg_integer()) :: frame()
  def retryable_error(status, message, retry_delay_ms) when retry_delay_ms >= 0 do
    seconds = div(retry_delay_ms, 1000)
    nanos = rem(retry_delay_ms, 1000) * 1_000_000

    retry_info = %Google.Rpc.RetryInfo{
      retry_delay: %Google.Protobuf.Duration{seconds: seconds, nanos: nanos}
    }

    any = %Google.Protobuf.Any{
      type_url: "type.googleapis.com/google.rpc.RetryInfo",
      value: Google.Rpc.RetryInfo.encode(retry_info) |> IO.iodata_to_binary()
    }

    grpc_error(status, message, details: [any])
  end

  # ── Iterator helpers ────────────────────────────────────────────────────

  @doc """
  Wraps a list of frames as an Enumerable. Equivalent to passing the list
  directly to the decoder, but documents intent and is symmetric with
  `controllable/0`.
  """
  @spec iterator([frame()]) :: Enumerable.t()
  def iterator(frames) when is_list(frames), do: frames

  @doc """
  Returns `{push_fn, finalize_fn, stream}` for a controllable, lazy
  frame source.

  Use when a test needs to interleave decoder consumption with frame
  emission (e.g. assert that the decoder requested the next frame
  before halting).

      {push, finalize, stream} = DecoderFakes.controllable()
      push.(DecoderFakes.arrow_batch(<<1>>, row_count: 1))
      push.(DecoderFakes.result_complete())
      finalize.()

      assert {:ok, _} = ResultDecoder.decode_stream(stream)
  """
  @spec controllable() :: {(frame() -> :ok), (-> :ok), Enumerable.t()}
  def controllable do
    {:ok, queue} = Agent.start_link(fn -> {:queue.new(), false} end)

    push = fn frame ->
      Agent.update(queue, fn {q, finalized?} -> {:queue.in(frame, q), finalized?} end)
    end

    finalize = fn -> Agent.update(queue, fn {q, _} -> {q, true} end) end

    stream =
      Stream.unfold(:start, fn _ ->
        wait_for_frame(queue)
      end)

    {push, finalize, stream}
  end

  defp wait_for_frame(queue) do
    case Agent.get_and_update(queue, fn {q, finalized?} ->
           case :queue.out(q) do
             {{:value, frame}, q2} -> {{:frame, frame}, {q2, finalized?}}
             {:empty, _} -> {if(finalized?, do: :done, else: :wait), {q, finalized?}}
           end
         end) do
      {:frame, frame} ->
        {frame, :next}

      :done ->
        nil

      :wait ->
        Process.sleep(1)
        wait_for_frame(queue)
    end
  end

  # ── Internal ────────────────────────────────────────────────────────────

  defp response(opts, fields) do
    base =
      [
        server_side_session_id: Keyword.get(opts, :server_side_session_id, ""),
        operation_id: Keyword.get(opts, :operation_id, ""),
        response_id: Keyword.get(opts, :response_id, "")
      ]
      |> Enum.reject(fn {_, v} -> v == "" end)

    fields =
      (base ++ Enum.reject(fields, fn {_, v} -> is_nil(v) end))
      |> Enum.uniq_by(&elem(&1, 0))

    {:ok, struct(ExecutePlanResponse, fields)}
  end

  defp maybe_put(list, _k, nil), do: list
  defp maybe_put(list, k, v), do: Keyword.put(list, k, v)
end
