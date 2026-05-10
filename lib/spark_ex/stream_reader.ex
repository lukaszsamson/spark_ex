defmodule SparkEx.StreamReader do
  @moduledoc """
  Data source reader APIs for creating streaming DataFrames.

  Mirrors PySpark's `DataStreamReader` with a builder pattern.

  ## Examples

      reader = SparkEx.StreamReader.new(session)
      df = reader |> SparkEx.StreamReader.format("rate") |> SparkEx.StreamReader.load()

      # Convenience: rate source for testing
      df = SparkEx.StreamReader.rate(session, rows_per_second: 10)
  """

  alias SparkEx.DataFrame

  defstruct [
    :session,
    :format,
    :schema,
    options: %{}
  ]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          format: String.t() | nil,
          schema: String.t() | nil,
          options: %{String.t() => String.t()}
        }

  @spec new(GenServer.server()) :: t()
  def new(session), do: %__MODULE__{session: session}

  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = reader, source) when is_binary(source) do
    %{reader | format: source}
  end

  @doc """
  Sets the schema for the streaming reader.

  Accepts either a DDL string or a struct type from `SparkEx.Types`.

  ## Examples

      reader |> StreamReader.schema("id LONG, name STRING")
      reader |> StreamReader.schema(SparkEx.Types.struct_type([
        SparkEx.Types.struct_field("id", :long),
        SparkEx.Types.struct_field("name", :string)
      ]))
  """
  @spec schema(t(), String.t() | SparkEx.Types.struct_type()) :: t()
  def schema(%__MODULE__{} = reader, schema_ddl) when is_binary(schema_ddl) do
    %{reader | schema: schema_ddl}
  end

  def schema(%__MODULE__{} = reader, {:struct, _} = struct_type) do
    %{reader | schema: SparkEx.Types.to_json(struct_type)}
  end

  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = reader, key, nil) when is_binary(key) do
    %{reader | options: Map.delete(reader.options, key)}
  end

  def option(%__MODULE__{} = reader, key, value) when is_binary(key) do
    %{reader | options: Map.put(reader.options, key, normalize_option_value(value))}
  end

  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = reader, opts) when is_map(opts) or is_list(opts) do
    {drops, sets} =
      Enum.reduce(opts, {[], []}, fn {k, v}, {drops, sets} ->
        key = to_string(k)
        if is_nil(v), do: {[key | drops], sets}, else: {drops, [{key, v} | sets]}
      end)

    new_options =
      reader.options
      |> Map.drop(drops)
      |> Map.merge(normalize_options(Map.new(sets)))

    %{reader | options: new_options}
  end

  @spec load(t()) :: DataFrame.t()
  def load(%__MODULE__{} = reader), do: load(reader, nil, [])

  @doc """
  Loads a streaming DataFrame using the configured builder. Mirrors
  PySpark's `DataStreamReader.load(path=None, format=None, schema=None,
  **options)` — call-time `:format`, `:schema`, and a top-level option
  map (or `:options` keyword) override the builder state.
  """
  @spec load(t(), String.t() | nil | keyword()) :: DataFrame.t()
  def load(%__MODULE__{} = reader, path_or_opts) when is_binary(path_or_opts) do
    load(reader, path_or_opts, [])
  end

  def load(%__MODULE__{} = reader, nil), do: load(reader, nil, [])

  def load(%__MODULE__{} = reader, opts) when is_list(opts) do
    if Keyword.keyword?(opts) and opts != [] do
      load(reader, nil, opts)
    else
      raise ArgumentError,
            "stream load only accepts a single path; got a list: #{inspect(opts)}. " <>
              "Streaming sources read from one location at a time — call load/2 once per path."
    end
  end

  def load(%__MODULE__{}, other) do
    raise ArgumentError, "stream load path must be a string, got: #{inspect(other)}"
  end

  @spec load(t(), String.t() | nil, keyword()) :: DataFrame.t()
  def load(%__MODULE__{} = reader, path, opts) when is_list(opts) do
    paths =
      case path do
        nil ->
          []

        p when is_binary(p) ->
          validate_path!(p)
          [p]

        other ->
          raise ArgumentError, "stream load path must be a string, got: #{inspect(other)}"
      end

    format = Keyword.get(opts, :format, reader.format)
    schema = opts |> Keyword.get(:schema, reader.schema) |> normalize_schema()
    call_time_options = merge_source_options(opts, [:format, :schema])
    merged_options = Map.merge(reader.options, call_time_options)

    %DataFrame{
      session: reader.session,
      plan: {:read_data_source_streaming, format, paths, schema, merged_options}
    }
  end

  @spec table(t(), String.t()) :: DataFrame.t()
  def table(%__MODULE__{} = reader, table_name) when is_binary(table_name) do
    %DataFrame{
      session: reader.session,
      plan: {:read_named_table_streaming, table_name, reader.options}
    }
  end

  # Format-specific convenience functions

  @spec rate(GenServer.server(), keyword()) :: DataFrame.t()
  def rate(session, opts \\ []) do
    options = Keyword.get(opts, :options, %{})

    extra =
      opts
      |> Keyword.drop([:options])
      |> Enum.reduce(options, fn
        {:rows_per_second, v}, acc -> Map.put(acc, "rowsPerSecond", to_string(v))
        {:num_partitions, v}, acc -> Map.put(acc, "numPartitions", to_string(v))
        {:ramp_up_time, v}, acc -> Map.put(acc, "rampUpTime", to_string(v))
        {k, v}, acc -> Map.put(acc, to_string(k), to_string(v))
      end)

    options = normalize_options(extra)

    %DataFrame{
      session: session,
      plan: {:read_data_source_streaming, "rate", [], nil, options}
    }
  end

  @spec text(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def text(session, path, opts \\ []) do
    streaming_data_source(session, "text", path, opts)
  end

  @spec json(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def json(session, path, opts \\ []) do
    streaming_data_source(session, "json", path, opts)
  end

  @spec csv(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def csv(session, path, opts \\ []) do
    streaming_data_source(session, "csv", path, opts)
  end

  @spec parquet(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def parquet(session, path, opts \\ []) do
    streaming_data_source(session, "parquet", path, opts)
  end

  @spec orc(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def orc(session, path, opts \\ []) do
    streaming_data_source(session, "orc", path, opts)
  end

  @spec xml(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def xml(session, path, opts \\ []) do
    streaming_data_source(session, "xml", path, opts)
  end

  # --- Private ---

  defp streaming_data_source(session, format, path, opts) do
    paths = normalize_stream_paths!(path)
    schema = opts |> Keyword.get(:schema, nil) |> normalize_schema()
    options = merge_source_options(opts, [:schema])

    %DataFrame{
      session: session,
      plan: {:read_data_source_streaming, format, paths, schema, options}
    }
  end

  defp normalize_stream_paths!(path) when is_binary(path) do
    validate_path!(path)
    [path]
  end

  defp normalize_stream_paths!(paths) when is_list(paths) do
    raise ArgumentError,
          "streaming sources accept a single path; got a list: #{inspect(paths)}"
  end

  defp normalize_stream_paths!(_path) do
    raise ArgumentError, "path must be a non-empty string"
  end

  defp merge_source_options(opts, reserved_keys) do
    nested_options = opts |> Keyword.get(:options, %{}) |> normalize_options()

    top_level_options =
      opts
      |> Keyword.drop([:options | reserved_keys])
      |> normalize_options()

    Map.merge(top_level_options, nested_options)
  end

  defp normalize_schema(nil), do: nil
  defp normalize_schema(schema) when is_binary(schema), do: schema
  defp normalize_schema({:struct, _} = schema), do: SparkEx.Types.to_json(schema)

  defp normalize_schema(schema) do
    raise ArgumentError,
          "schema must be a string or {:struct, fields} tuple, got: #{inspect(schema)}"
  end

  defp normalize_options(opts) when is_list(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_options(opts) when is_map(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_option_value(value) do
    SparkEx.Internal.OptionUtils.normalize_option_value(value)
  end

  defp validate_path!(path) when is_binary(path) do
    if String.trim(path) == "" do
      raise ArgumentError, "path must not be empty or blank"
    end
  end
end
