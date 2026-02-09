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
          options: %{String.t() => String.t()},
          partition_by: [String.t()],
          cluster_by: [String.t()]
        }

  @spec output_mode(t(), String.t()) :: t()
  def output_mode(%__MODULE__{} = writer, mode) when mode in ["append", "complete", "update"] do
    %{writer | output_mode: mode}
  end

  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = writer, source) when is_binary(source) do
    %{writer | source: source}
  end

  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = writer, key, value) when is_binary(key) do
    %{writer | options: Map.put(writer.options, key, normalize_option_value(value))}
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
    trigger_value =
      cond do
        Keyword.has_key?(opts, :processing_time) ->
          {:processing_time, Keyword.fetch!(opts, :processing_time)}

        Keyword.has_key?(opts, :available_now) ->
          :available_now

        Keyword.has_key?(opts, :once) ->
          :once

        Keyword.has_key?(opts, :continuous) ->
          {:continuous, Keyword.fetch!(opts, :continuous)}

        true ->
          raise ArgumentError,
                "expected one of :processing_time, :available_now, :once, :continuous"
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
  Starts the streaming query, writing to the path set via `option("path", ...)`.

  Returns `{:ok, StreamingQuery.t()}` on success.
  """
  @spec start(t(), keyword()) :: {:ok, SparkEx.StreamingQuery.t()} | {:error, term()}
  def start(%__MODULE__{} = writer, opts \\ []) do
    write_opts = build_write_opts(writer)
    execute_stream_start(writer.df, write_opts, opts)
  end

  @doc """
  Starts the streaming query, writing to the given table name.

  Returns `{:ok, StreamingQuery.t()}` on success.
  """
  @spec to_table(t(), String.t(), keyword()) :: {:ok, SparkEx.StreamingQuery.t()} | {:error, term()}
  def to_table(%__MODULE__{} = writer, table_name, opts \\ []) when is_binary(table_name) do
    writer = %{writer | table_name: table_name}
    write_opts = build_write_opts(writer)
    execute_stream_start(writer.df, write_opts, opts)
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
      cluster_by: writer.cluster_by
    ]
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

        {:ok, query}

      {:ok, other} ->
        {:error, {:unexpected_result, other}}

      {:error, _} = error ->
        error
    end
  end

  defp stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  defp normalize_option_value(value) when is_binary(value), do: value
  defp normalize_option_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_option_value(value) when is_float(value), do: Float.to_string(value)
  defp normalize_option_value(value) when is_boolean(value), do: to_string(value)

  defp normalize_option_value(value) do
    raise ArgumentError,
          "stream writer option value must be a primitive (string, integer, float, boolean), got: #{inspect(value)}"
  end
end
