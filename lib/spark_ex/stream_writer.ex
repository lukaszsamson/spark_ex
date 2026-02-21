defmodule SparkEx.StreamWriter do
  @moduledoc """
  Builder API for starting structured streaming queries.

  Mirrors PySpark's `DataStreamWriter` with a builder pattern.

  ## Examples

      df
      |> SparkEx.DataFrame.write_stream()
      |> SparkEx.StreamWriter.format("console")
      |> SparkEx.StreamWriter.output_mode("append")
      |> SparkEx.StreamWriter.start()
  """

  defstruct [
    :df,
    :source,
    :output_mode,
    :query_name,
    :trigger,
    :path,
    :table_name,
    :foreach_writer,
    :foreach_batch,
    options: %{},
    partition_by: [],
    cluster_by: []
  ]

  @type t :: %__MODULE__{
          df: SparkEx.DataFrame.t(),
          source: String.t() | nil,
          output_mode: String.t() | nil,
          query_name: String.t() | nil,
          trigger: term() | nil,
          path: String.t() | nil,
          table_name: String.t() | nil,
          foreach_writer: Spark.Connect.StreamingForeachFunction.t() | nil,
          foreach_batch: Spark.Connect.StreamingForeachFunction.t() | nil,
          options: %{String.t() => String.t()},
          partition_by: [String.t()],
          cluster_by: [String.t()]
        }

  @exec_opt_keys [
    :timeout,
    :tags,
    :reattachable,
    :allow_arrow_batch_chunking,
    :preferred_arrow_chunk_size,
    :reattach_retries,
    :execute_stream_fun,
    :reattach_stream_fun,
    :release_execute_fun
  ]

  @spec output_mode(t(), String.t()) :: t()
  def output_mode(%__MODULE__{} = writer, mode) when mode in ["append", "complete", "update"] do
    %{writer | output_mode: mode}
  end

  def output_mode(%__MODULE__{}, mode) do
    raise ArgumentError,
          "output_mode must be one of \"append\", \"complete\", \"update\", got: #{inspect(mode)}"
  end

  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = writer, source) when is_binary(source) do
    %{writer | source: source}
  end

  @doc """
  Sets the output path for the streaming sink.
  """
  @spec path(t(), String.t()) :: t()
  def path(%__MODULE__{} = writer, path) when is_binary(path) do
    %{writer | path: path}
  end

  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = writer, key, value) when is_binary(key) do
    if is_nil(value) do
      writer
    else
      %{writer | options: Map.put(writer.options, key, normalize_option_value(value))}
    end
  end

  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = writer, opts) when is_map(opts) do
    merged = Map.merge(writer.options, stringify_options(opts))
    %{writer | options: merged}
  end

  def options(%__MODULE__{} = writer, opts) when is_list(opts) do
    opts |> Enum.into(%{}) |> then(&options(writer, &1))
  end

  @spec query_name(t(), String.t()) :: t()
  def query_name(%__MODULE__{} = writer, name) when is_binary(name) do
    if String.trim(name) == "" do
      raise ArgumentError, "query name should not be empty or blank"
    end

    %{writer | query_name: name}
  end

  @doc """
  Sets the trigger for the streaming query.

  ## Trigger types

  - `processing_time: "5 seconds"` — micro-batch at the given interval
  - `available_now: true` — process all available data then stop
  - `once: true` — process one micro-batch then stop
  - `continuous: "1 second"` — continuous processing at the given checkpoint interval
  """
  @spec trigger(t(), keyword()) :: t()
  def trigger(%__MODULE__{} = writer, opts) when is_list(opts) do
    trigger_keys = [:processing_time, :available_now, :once, :continuous]
    set_keys = Enum.filter(trigger_keys, &Keyword.has_key?(opts, &1))

    case set_keys do
      [] ->
        raise ArgumentError,
              "expected one of :processing_time, :available_now, :once, :continuous"

      [_single] ->
        :ok

      multiple ->
        raise ArgumentError,
              "only one trigger type should be set, got: #{inspect(multiple)}"
    end

    trigger_value =
      cond do
        Keyword.has_key?(opts, :processing_time) ->
          value = Keyword.fetch!(opts, :processing_time)

          if not is_binary(value) or String.trim(value) == "" do
            raise ArgumentError,
                  "processing_time must be a non-empty string, got: #{inspect(value)}"
          end

          {:processing_time, String.trim(value)}

        Keyword.has_key?(opts, :available_now) ->
          value = Keyword.fetch!(opts, :available_now)

          if value != true do
            raise ArgumentError,
                  "available_now must be true, got: #{inspect(value)}"
          end

          :available_now

        Keyword.has_key?(opts, :once) ->
          value = Keyword.fetch!(opts, :once)

          if value != true do
            raise ArgumentError,
                  "once must be true, got: #{inspect(value)}"
          end

          :once

        Keyword.has_key?(opts, :continuous) ->
          value = Keyword.fetch!(opts, :continuous)

          if not is_binary(value) or String.trim(value) == "" do
            raise ArgumentError,
                  "continuous must be a non-empty string, got: #{inspect(value)}"
          end

          {:continuous, String.trim(value)}
      end

    %{writer | trigger: trigger_value}
  end

  @spec partition_by(t(), [String.t()]) :: t()
  def partition_by(%__MODULE__{} = writer, cols) when is_list(cols) do
    %{writer | partition_by: Enum.map(cols, &to_string/1)}
  end

  @spec cluster_by(t(), [String.t()]) :: t()
  def cluster_by(%__MODULE__{} = writer, cols) when is_list(cols) do
    %{writer | cluster_by: Enum.map(cols, &to_string/1)}
  end

  @doc """
  Sets a foreach writer function for row-level processing.

  Accepts a `Spark.Connect.StreamingForeachFunction` proto struct.
  Use this with Java/Scala UDF payloads.

  ## Example with Scala UDF

      foreach_fn = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{
          payload: serialized_scala_bytes
        }}
      }
      writer |> StreamWriter.foreach_writer(foreach_fn)
  """
  @spec foreach_writer(t(), Spark.Connect.StreamingForeachFunction.t()) :: t()
  def foreach_writer(%__MODULE__{} = writer, %Spark.Connect.StreamingForeachFunction{} = func) do
    %{writer | foreach_writer: func}
  end

  @doc """
  Sets a foreach batch function for micro-batch processing.

  Accepts a `Spark.Connect.StreamingForeachFunction` proto struct.
  Use this with Java/Scala UDF payloads.

  ## Example with Scala UDF

      foreach_fn = %Spark.Connect.StreamingForeachFunction{
        function: {:scala_function, %Spark.Connect.ScalarScalaUDF{
          payload: serialized_scala_bytes
        }}
      }
      writer |> StreamWriter.foreach_batch(foreach_fn)
  """
  @spec foreach_batch(t(), Spark.Connect.StreamingForeachFunction.t()) :: t()
  def foreach_batch(%__MODULE__{} = writer, %Spark.Connect.StreamingForeachFunction{} = func) do
    %{writer | foreach_batch: func}
  end

  @doc """
  Starts the streaming query, writing to the path set via `option("path", ...)`.

  Returns `{:ok, StreamingQuery.t()}` on success.
  """
  @spec start(t(), keyword()) :: {:ok, SparkEx.StreamingQuery.t()} | {:error, term()}
  def start(%__MODULE__{} = writer, opts \\ []) do
    {writer, exec_opts} = apply_call_time_options(writer, opts)
    write_opts = build_write_opts(writer)
    execute_stream_start(writer.df, write_opts, exec_opts)
  end

  @doc """
  Starts the streaming query writing XML to the given path.
  """
  @spec xml(t(), String.t()) :: t()
  def xml(%__MODULE__{} = writer, path) when is_binary(path) do
    writer
    |> format("xml")
    |> path(path)
  end

  @doc """
  Starts the streaming query, writing to the given table name.

  Returns `{:ok, StreamingQuery.t()}` on success.
  """
  @spec to_table(t(), String.t(), keyword()) ::
          {:ok, SparkEx.StreamingQuery.t()} | {:error, term()}
  def to_table(%__MODULE__{} = writer, table_name, opts \\ []) when is_binary(table_name) do
    {writer, exec_opts} = apply_call_time_options(writer, opts)
    writer = %{writer | table_name: table_name, path: nil}
    write_opts = build_write_opts(writer)
    execute_stream_start(writer.df, write_opts, exec_opts)
  end

  # --- Private ---

  defp build_write_opts(writer) do
    [
      format: writer.source,
      options: writer.options,
      output_mode: writer.output_mode,
      query_name: writer.query_name,
      trigger: writer.trigger,
      path: writer.path,
      table_name: writer.table_name,
      partition_by: writer.partition_by,
      cluster_by: writer.cluster_by,
      foreach_writer: writer.foreach_writer,
      foreach_batch: writer.foreach_batch
    ]
  end

  defp apply_call_time_options(writer, opts) do
    writer =
      writer
      |> maybe_override_writer_option(:format, Keyword.get(opts, :format))
      |> maybe_override_writer_option(
        :output_mode,
        Keyword.get(opts, :output_mode) || Keyword.get(opts, :outputMode)
      )
      |> maybe_override_writer_option(
        :partition_by,
        Keyword.get(opts, :partition_by) || Keyword.get(opts, :partitionBy)
      )
      |> maybe_override_writer_option(
        :cluster_by,
        Keyword.get(opts, :cluster_by) || Keyword.get(opts, :clusterBy)
      )
      |> maybe_override_writer_option(
        :query_name,
        Keyword.get(opts, :query_name) || Keyword.get(opts, :queryName)
      )
      |> maybe_override_writer_option(:trigger, Keyword.get(opts, :trigger))
      |> maybe_override_writer_option(:path, Keyword.get(opts, :path))

    sink_option_overrides = extract_sink_option_overrides(opts)
    writer = %{writer | options: Map.merge(writer.options, sink_option_overrides)}

    exec_opts = Keyword.take(opts, @exec_opt_keys)
    {writer, exec_opts}
  end

  defp maybe_override_writer_option(writer, _key, nil), do: writer
  defp maybe_override_writer_option(writer, :format, value), do: format(writer, value)
  defp maybe_override_writer_option(writer, :output_mode, value), do: output_mode(writer, value)

  defp maybe_override_writer_option(writer, :partition_by, value),
    do: partition_by(writer, normalize_columns(value))

  defp maybe_override_writer_option(writer, :cluster_by, value),
    do: cluster_by(writer, normalize_columns(value))

  defp maybe_override_writer_option(writer, :query_name, value), do: query_name(writer, value)
  defp maybe_override_writer_option(writer, :path, value), do: path(writer, value)

  defp maybe_override_writer_option(writer, :trigger, value) when is_list(value),
    do: trigger(writer, value)

  defp maybe_override_writer_option(_writer, :trigger, value) do
    raise ArgumentError, "trigger option must be a keyword list, got: #{inspect(value)}"
  end

  defp extract_sink_option_overrides(opts) do
    nested_options = opts |> Keyword.get(:options, %{}) |> stringify_options()

    top_level_options =
      opts
      |> Keyword.drop([
        :format,
        :output_mode,
        :outputMode,
        :partition_by,
        :partitionBy,
        :cluster_by,
        :clusterBy,
        :query_name,
        :queryName,
        :trigger,
        :path,
        :options
        | @exec_opt_keys
      ])
      |> stringify_options()

    Map.merge(top_level_options, nested_options)
  end

  defp execute_stream_start(df, write_opts, exec_opts) do
    case SparkEx.Session.execute_command_with_result(
           df.session,
           {:write_stream_operation_start, df.plan, write_opts},
           exec_opts
         ) do
      {:ok, {:write_stream_start, result}} ->
        query = %SparkEx.StreamingQuery{
          session: df.session,
          query_id: result.query_id.id,
          run_id: result.query_id.run_id,
          name: if(result.name == "", do: nil, else: result.name)
        }

        maybe_post_query_started(df.session, result)
        {:ok, query}

      {:ok, other} ->
        {:error, {:unexpected_result, other}}

      {:error, _} = error ->
        error
    end
  end

  defp maybe_post_query_started(session, result) do
    case extract_started_event_json(result) do
      json when is_binary(json) and json != "" ->
        SparkEx.StreamingQueryListenerBus.post_query_started(session, json)

      _ ->
        :ok
    end
  end

  defp extract_started_event_json(%{query_started_event_json: json}), do: json
  defp extract_started_event_json(_), do: nil

  defp stringify_options(opts) when is_map(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp stringify_options(opts) when is_list(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_columns(value) when is_binary(value), do: [value]
  defp normalize_columns(value) when is_list(value), do: value

  defp normalize_columns(value) do
    raise ArgumentError, "expected column name or list of column names, got: #{inspect(value)}"
  end

  defp normalize_option_value(value) do
    SparkEx.Internal.OptionUtils.normalize_option_value(value)
  end
end
