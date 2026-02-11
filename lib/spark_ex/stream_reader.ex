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
    %{reader | schema: SparkEx.Types.schema_to_string(struct_type)}
  end

  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = reader, key, value) when is_binary(key) do
    if is_nil(value) do
      reader
    else
      %{reader | options: Map.put(reader.options, key, normalize_option_value(value))}
    end
  end

  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = reader, opts) when is_map(opts) or is_list(opts) do
    merged =
      opts
      |> normalize_options()
      |> then(&Map.merge(reader.options, &1))

    %{reader | options: merged}
  end

  @spec load(t()) :: DataFrame.t()
  def load(%__MODULE__{} = reader) do
    %DataFrame{
      session: reader.session,
      plan: {:read_data_source_streaming, reader.format, [], reader.schema, reader.options}
    }
  end

  @spec load(t(), String.t() | [String.t()]) :: DataFrame.t()
  def load(%__MODULE__{} = reader, path) when is_binary(path) do
    validate_path!(path)

    %DataFrame{
      session: reader.session,
      plan: {:read_data_source_streaming, reader.format, [path], reader.schema, reader.options}
    }
  end

  def load(%__MODULE__{} = reader, paths) when is_list(paths) do
    Enum.each(paths, &validate_path!/1)

    %DataFrame{
      session: reader.session,
      plan: {:read_data_source_streaming, reader.format, paths, reader.schema, reader.options}
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
    paths = List.wrap(path)
    schema = Keyword.get(opts, :schema, nil)
    options = opts |> Keyword.get(:options, %{}) |> normalize_options()

    %DataFrame{
      session: session,
      plan: {:read_data_source_streaming, format, paths, schema, options}
    }
  end

  defp normalize_options(opts) when is_list(opts) do
    opts |> Enum.into(%{}) |> normalize_options()
  end

  defp normalize_options(opts) when is_map(opts) do
    opts
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new(fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  defp normalize_option_value(value) when is_binary(value), do: value
  defp normalize_option_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_option_value(value) when is_float(value), do: Float.to_string(value)
  defp normalize_option_value(value) when is_boolean(value), do: to_string(value)

  defp normalize_option_value(value) do
    raise ArgumentError,
          "stream reader option value must be a primitive (string, integer, float, boolean), got: #{inspect(value)}"
  end

  defp validate_path!(path) when is_binary(path) do
    if String.trim(path) == "" do
      raise ArgumentError, "path must not be empty or blank"
    end
  end
end
