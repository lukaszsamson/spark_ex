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
    :source_name,
    options: %{}
  ]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          format: String.t() | nil,
          schema: String.t() | nil,
          source_name: String.t() | nil,
          options: %{String.t() => String.t()}
        }

  @spec new(GenServer.server()) :: t()
  def new(session), do: %__MODULE__{session: session}

  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = reader, source) when is_binary(source) do
    %{reader | format: source}
  end

  @doc """
  Sets the stable name used to identify this streaming source in checkpoints.

  Source names contain only ASCII letters, digits, and underscores. This is a
  Spark 4.2 additive wire field: callers must ensure the server and source
  provider support named sources before relying on its checkpoint semantics.
  Enable `spark.sql.streaming.queryEvolution.enableSourceEvolution` on the
  session before starting a query with named sources.
  """
  @spec name(t(), String.t()) :: t()
  def name(%__MODULE__{} = reader, source_name) when is_binary(source_name) do
    if Regex.match?(~r/\A[A-Za-z0-9_]+\z/, source_name) do
      %{reader | source_name: source_name}
    else
      raise ArgumentError,
            "source name must contain only ASCII letters, digits, and underscores, got: " <>
              inspect(source_name)
    end
  end

  def name(%__MODULE__{}, source_name) do
    raise ArgumentError, "source name must be a string, got: #{inspect(source_name)}"
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
  @spec schema(t(), String.t() | SparkEx.Types.struct_type() | SparkEx.Types.data_type_proto()) ::
          t()
  def schema(%__MODULE__{} = reader, schema_ddl) when is_binary(schema_ddl) do
    %{reader | schema: schema_ddl}
  end

  def schema(%__MODULE__{} = reader, {:struct, _} = struct_type) do
    %{reader | schema: SparkEx.Types.to_json(struct_type)}
  end

  def schema(%__MODULE__{} = reader, %Spark.Connect.DataType{} = schema) do
    %{reader | schema: SparkEx.Types.data_type_to_json(schema)}
  end

  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = reader, key, nil) when is_binary(key) or is_atom(key) do
    %{reader | options: Map.delete(reader.options, normalize_option_key(key))}
  end

  def option(%__MODULE__{} = reader, key, value) when is_binary(key) or is_atom(key) do
    %{
      reader
      | options: Map.put(reader.options, normalize_option_key(key), normalize_option_value(value))
    }
  end

  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = reader, opts) when is_map(opts) or is_list(opts) do
    # Collapse to a map first so duplicate keys resolve via last-wins semantics.
    pairs = if is_map(opts), do: opts, else: Enum.into(opts, %{})

    {drops, sets} =
      Enum.reduce(pairs, {[], []}, fn {k, v}, {drops, sets} ->
        key = normalize_option_key(k)
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

    # PySpark only overrides when the argument is non-None.
    format = Keyword.get(opts, :format) || reader.format
    schema = (Keyword.get(opts, :schema) || reader.schema) |> normalize_schema()
    call_time_options = merge_source_options(opts, [:format, :schema])
    merged_options = Map.merge(reader.options, call_time_options)

    DataFrame.new(
      reader.session,
      {:read_data_source_streaming, format, paths, schema, merged_options, reader.source_name}
    )
  end

  @spec table(t(), String.t()) :: DataFrame.t()
  def table(%__MODULE__{} = reader, table_name) when is_binary(table_name) do
    DataFrame.new(reader.session, {:read_named_table_streaming, table_name, reader.options})
  end

  @doc """
  Reads a streaming change feed from a Data Source V2 table.

  The table catalog must implement change-log loading (Spark 4.2+). Reader
  options select version or timestamp bounds and CDC behavior. User schemas are
  rejected because the provider defines the change-feed schema.
  """
  @spec changes(t(), String.t()) :: DataFrame.t()
  def changes(%__MODULE__{schema: schema}, _table_name) when not is_nil(schema) do
    raise ArgumentError, "user-specified schema is not supported with changes/2"
  end

  def changes(%__MODULE__{} = reader, table_name) when is_binary(table_name) do
    validate_table_name!(table_name)
    DataFrame.new(reader.session, {:relation_changes, table_name, reader.options, true})
  end

  # Format-specific convenience functions

  @spec rate(GenServer.server() | t(), keyword()) :: DataFrame.t()
  def rate(session, opts \\ []) do
    # :rows_per_second/:num_partitions/:ramp_up_time normalize to their camelCase
    # Spark names through the shared key normalizer.
    case session do
      %__MODULE__{} = reader ->
        load(%{reader | format: "rate"}, nil, opts)

      _ ->
        options = merge_source_options(opts, [])
        DataFrame.new(session, {:read_data_source_streaming, "rate", [], nil, options})
    end
  end

  @spec text(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def text(session, path, opts \\ []) do
    streaming_data_source(session, "text", path, opts)
  end

  @spec json(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def json(session, path, opts \\ []) do
    streaming_data_source(session, "json", path, opts)
  end

  @spec csv(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def csv(session, path, opts \\ []) do
    streaming_data_source(session, "csv", path, opts)
  end

  @spec parquet(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def parquet(session, path, opts \\ []) do
    streaming_data_source(session, "parquet", path, opts)
  end

  @spec orc(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def orc(session, path, opts \\ []) do
    streaming_data_source(session, "orc", path, opts)
  end

  @spec xml(GenServer.server() | t(), String.t(), keyword()) :: DataFrame.t()
  def xml(session, path, opts \\ []) do
    streaming_data_source(session, "xml", path, opts)
  end

  # --- Private ---

  defp streaming_data_source(%__MODULE__{} = reader, format, path, opts) do
    reader
    |> Map.put(:format, format)
    |> load(path, opts)
  end

  defp streaming_data_source(session, format, path, opts) do
    paths = normalize_stream_paths!(path)
    schema = opts |> Keyword.get(:schema, nil) |> normalize_schema()
    options = merge_source_options(opts, [:schema])

    DataFrame.new(session, {:read_data_source_streaming, format, paths, schema, options})
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
    SparkEx.Internal.OptionUtils.merge_source_options(opts, reserved_keys)
  end

  defp normalize_schema(nil), do: nil
  defp normalize_schema(schema) when is_binary(schema), do: schema
  defp normalize_schema({:struct, _} = schema), do: SparkEx.Types.to_json(schema)

  defp normalize_schema(%Spark.Connect.DataType{} = schema),
    do: SparkEx.Types.data_type_to_json(schema)

  defp normalize_schema(schema) do
    raise ArgumentError,
          "schema must be a string, {:struct, fields} tuple, or Spark.Connect.DataType, " <>
            "got: #{inspect(schema)}"
  end

  defp normalize_options(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_option_key(key) do
    SparkEx.Internal.OptionUtils.normalize_option_key(key)
  end

  defp normalize_option_value(value) do
    SparkEx.Internal.OptionUtils.normalize_option_value(value)
  end

  defp validate_path!(path) when is_binary(path) do
    if String.trim(path) == "" do
      raise ArgumentError, "path must not be empty or blank"
    end
  end

  defp validate_table_name!(table_name) do
    if String.trim(table_name) == "" do
      raise ArgumentError, "table name must not be empty or blank"
    end
  end
end
