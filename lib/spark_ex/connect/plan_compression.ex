defmodule SparkEx.Connect.PlanCompression do
  @moduledoc false

  alias Spark.Connect.{AnalyzePlanRequest, Plan}
  alias SparkEx.Connect.{Client, SessionIntegrity}

  @threshold "spark.connect.session.planCompression.threshold"
  @algorithm "spark.connect.session.planCompression.defaultAlgorithm"

  @doc false
  def prepare_session(session, opts \\ []) do
    enabled =
      Keyword.get(opts, :enabled, Application.get_env(:spark_ex, :plan_compression, false))

    available = Keyword.get_lazy(opts, :codec_available, &codec_available?/0)

    cond do
      enabled == false or not available ->
        {:ok, Map.put(session, :plan_compression, :disabled)}

      Map.get(session, :plan_compression) != nil ->
        {:ok, session}

      true ->
        reader = Keyword.get(opts, :config_reader, &Client.config_get_option/3)
        negotiate(session, reader)
    end
  end

  defp negotiate(session, reader) do
    case reader.(session, [@threshold, @algorithm], timeout: 5_000) do
      {:ok, pairs, server_id} ->
        with {:ok, pinned} <-
               SessionIntegrity.validate_server_session_id(
                 server_id,
                 session.server_side_session_id
               ) do
          {:ok,
           session
           |> Map.put(:server_side_session_id, pinned)
           |> Map.put(:plan_compression, configuration(Map.new(pairs)))}
        end

      {:error, %SparkEx.Error.Remote{grpc_status: 12}} ->
        {:ok, Map.put(session, :plan_compression, :disabled)}

      {:error, %SparkEx.Error.Remote{error_class: "SQL_CONF_NOT_FOUND"}} ->
        {:ok, Map.put(session, :plan_compression, :disabled)}

      {:error, _} = error ->
        error
    end
  end

  defp configuration(%{@threshold => threshold, @algorithm => "ZSTD"})
       when is_binary(threshold) do
    case Integer.parse(threshold) do
      {bytes, ""} when bytes >= 0 -> {bytes, :zstd}
      _ -> :disabled
    end
  end

  defp configuration(_), do: :disabled

  @doc false
  def codec_available?, do: Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :compress, 1)

  @doc false
  def compress(plan, session) do
    case Map.get(session, :plan_compression) do
      {threshold, codec} when is_integer(threshold) and threshold >= 0 ->
        compress_plan(plan, threshold, codec)

      _ ->
        plan
    end
  end

  defp compress_plan(%Plan{op_type: {type, message}} = plan, threshold, codec)
       when type in [:root, :command] do
    serialized = message.__struct__.encode(message) |> IO.iodata_to_binary()

    if byte_size(serialized) > threshold do
      compress_message(plan, serialized, type, codec)
    else
      plan
    end
  end

  defp compress_plan(plan, _threshold, _codec), do: plan

  defp compress_message(plan, serialized, type, codec) do
    # OTP's streaming ZSTD implementation releases its context in an `after`
    # clause. One caller owns one context; no task pool or native worker threads.
    compressed = apply(codec, :compress, [serialized]) |> IO.iodata_to_binary()

    if byte_size(compressed) < byte_size(serialized) do
      operation = %Plan.CompressedOperation{
        data: compressed,
        op_type: if(type == :root, do: :OP_TYPE_RELATION, else: :OP_TYPE_COMMAND),
        compression_codec: :COMPRESSION_CODEC_ZSTD
      }

      %{plan | op_type: {:compressed_operation, operation}}
    else
      plan
    end
  rescue
    _ -> plan
  end

  @doc false
  def compress_analyze(%AnalyzePlanRequest{analyze: {type, message}} = request, session) do
    compressed =
      Enum.reduce([:plan, :target_plan, :other_plan], message, fn field, acc ->
        case Map.get(acc, field) do
          %Plan{} = plan -> Map.put(acc, field, compress(plan, session))
          _ -> acc
        end
      end)

    %{request | analyze: {type, compressed}}
  end

  def compress_analyze(request, _session), do: request
end
