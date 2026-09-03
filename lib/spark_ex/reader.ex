defmodule SparkEx.Reader do
  @moduledoc """
  Data source reader APIs for creating DataFrames from external sources.

  ## Examples

      df = SparkEx.Reader.table(session, "catalog.db.my_table")
      df = SparkEx.Reader.parquet(session, "/data/events.parquet")
      df = SparkEx.Reader.csv(session, "/data/users.csv", header: true, infer_schema: true)
      df = SparkEx.Reader.json(session, "/data/logs.json")
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

  @doc """
  Creates a stateful reader builder.

  Mirrors PySpark's `spark.read` builder style.
  """
  @spec new(GenServer.server()) :: t()
  def new(session), do: %__MODULE__{session: session}

  @doc """
  Sets the source format for a reader builder.
  """
  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = reader, source) when is_binary(source) do
    %{reader | format: source}
  end

  @doc """
  Sets the schema for a reader builder.

  Accepts either a DDL string, a struct type from `SparkEx.Types`,
  or a Spark Connect DataType protobuf struct.

  ## Examples

      reader |> Reader.schema("id LONG, name STRING")
      reader |> Reader.schema(SparkEx.Types.struct_type([
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

  @doc """
  Sets a single option on a reader builder. A `nil` value removes the
  key (mirroring PySpark's `option(k, None)` behaviour).
  """
  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = reader, key, nil) when is_binary(key) do
    %{reader | options: Map.delete(reader.options, key)}
  end

  def option(%__MODULE__{} = reader, key, value) when is_binary(key) do
    %{reader | options: Map.put(reader.options, key, normalize_option_value(value))}
  end

  @doc """
  Merges options into a reader builder. Entries whose value is `nil`
  remove the key.
  """
  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = reader, opts) when is_map(opts) or is_list(opts) do
    # Collapse to a map first so duplicate keys resolve via last-wins semantics.
    pairs = if is_map(opts), do: opts, else: Enum.into(opts, %{})

    {drops, sets} =
      Enum.reduce(pairs, {[], []}, fn {k, v}, {drops, sets} ->
        key = to_string(k)
        if is_nil(v), do: {[key | drops], sets}, else: {drops, [{key, v} | sets]}
      end)

    new_options =
      reader.options
      |> Map.drop(drops)
      |> Map.merge(normalize_options(Map.new(sets)))

    %{reader | options: new_options}
  end

  @doc """
  Loads data from the configured source using reader builder state.
  """
  @spec load(t()) :: DataFrame.t()
  @spec load(t(), String.t() | [String.t()] | nil | keyword()) :: DataFrame.t()
  @spec load(t(), String.t() | [String.t()] | nil | keyword(), keyword()) :: DataFrame.t()
  def load(%__MODULE__{} = reader), do: load(reader, [], [])
  def load(%__MODULE__{} = reader, paths_or_opts), do: load(reader, paths_or_opts, [])

  @spec load(GenServer.server(), String.t()) :: DataFrame.t()
  @spec load(GenServer.server(), String.t(), String.t() | [String.t()] | keyword()) ::
          DataFrame.t()
  @spec load(GenServer.server(), String.t(), String.t() | [String.t()], keyword()) ::
          DataFrame.t()
  def load(session, format) when not is_struct(session, __MODULE__) and is_binary(format),
    do: load(session, format, [], [])

  def load(%__MODULE__{} = reader, paths_or_opts, opts) when is_binary(paths_or_opts) do
    load_from_builder(reader, [paths_or_opts], opts)
  end

  def load(%__MODULE__{} = reader, nil, opts) do
    load_from_builder(reader, [], opts)
  end

  def load(%__MODULE__{} = reader, paths_or_opts, opts) when is_list(paths_or_opts) do
    if Keyword.keyword?(paths_or_opts) and paths_or_opts != [] do
      load_from_builder(reader, [], paths_or_opts)
    else
      load_from_builder(reader, paths_or_opts, opts)
    end
  end

  def load(session, format, paths_or_opts)
      when not is_struct(session, __MODULE__) and is_binary(format) do
    load(session, format, paths_or_opts, [])
  end

  def load(session, format, paths_or_opts, opts)
      when not is_struct(session, __MODULE__) and is_binary(format) and is_binary(paths_or_opts) do
    data_source(session, format, [paths_or_opts], opts)
  end

  def load(session, format, paths_or_opts, opts)
      when not is_struct(session, __MODULE__) and is_binary(format) and is_list(paths_or_opts) do
    if Keyword.keyword?(paths_or_opts) and paths_or_opts != [] do
      # Called as load(session, format, opts) — keyword list is opts, not paths
      data_source(session, format, [], paths_or_opts)
    else
      data_source(session, format, paths_or_opts, opts)
    end
  end

  @doc """
  Reads a table from the catalog using reader builder options.
  """
  @spec table(t(), String.t()) :: DataFrame.t()
  def table(%__MODULE__{} = reader, table_name) when is_binary(table_name) do
    DataFrame.new(reader.session, {:read_named_table, table_name, reader.options})
  end

  @doc """
  Creates a DataFrame from a named table (catalog table).

  ## Options

  - `:options` — map of string options (default: `%{}`)

  ## Examples

      df = SparkEx.Reader.table(session, "my_database.my_table")
  """
  @spec table(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def table(session, table_name, opts \\ []) when is_binary(table_name) do
    options = merge_source_options(opts, [])
    DataFrame.new(session, {:read_named_table, table_name, options})
  end

  @doc """
  Creates a DataFrame by reading Parquet file(s).

  ## Options

  - `:schema` — optional schema string (e.g. `"id INT, name STRING"`)
  - `:options` — map of Parquet reader options (default: `%{}`)

  ## Examples

      df = SparkEx.Reader.parquet(session, "/data/events.parquet")
      df = SparkEx.Reader.parquet(session, ["/data/part1.parquet", "/data/part2.parquet"])
  """
  @spec parquet(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def parquet(session, paths, opts \\ []) do
    data_source(session, "parquet", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading CSV file(s).

  ## Options

  - `:schema` — optional schema string
  - `:header` — whether the CSV has a header row (maps to `"header"` option)
  - `:infer_schema` — whether to infer the schema (maps to `"inferSchema"` option)
  - `:separator` — field separator (maps to `"sep"` option)
  - `:options` — map of additional CSV reader options

  ## Examples

      df = SparkEx.Reader.csv(session, "/data/users.csv", header: true, infer_schema: true)
  """
  @spec csv(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def csv(session, paths, opts \\ []) do
    {csv_opts, rest} = Keyword.split(opts, [:header, :infer_schema, :separator, :sep])
    extra_options = Keyword.get(rest, :options, %{})

    options =
      csv_opts
      |> Enum.map(fn
        {:header, v} -> {"header", to_string(v)}
        {:infer_schema, v} -> {"inferSchema", to_string(v)}
        {:separator, v} -> {"sep", normalize_option_value(v)}
        {:sep, v} -> {"sep", normalize_option_value(v)}
      end)
      |> Enum.reduce(extra_options, fn {name, v}, acc ->
        if Map.has_key?(acc, name) do
          raise ArgumentError,
                "got multiple values for option #{inspect(name)} — pass it either as a " <>
                  "top-level keyword OR inside :options, not both"
        end

        Map.put(acc, name, v)
      end)

    data_source(session, "csv", paths, Keyword.put(rest, :options, options))
  end

  @doc """
  Creates a DataFrame by reading JSON file(s).

  ## Options

  - `:schema` — optional schema string
  - `:options` — map of JSON reader options

  ## Examples

      df = SparkEx.Reader.json(session, "/data/logs.json")
  """
  @spec json(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def json(session, paths, opts \\ []) do
    data_source(session, "json", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading text file(s).

  Each line becomes a row with a single `value` column.

  ## Options

  - `:options` — map of text reader options

  ## Examples

      df = SparkEx.Reader.text(session, "/data/lines.txt")
  """
  @spec text(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def text(session, paths, opts \\ []) do
    data_source(session, "text", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading ORC file(s).

  ## Options

  - `:schema` — optional schema string
  - `:options` — map of ORC reader options

  ## Examples

      df = SparkEx.Reader.orc(session, "/data/events.orc")
  """
  @spec orc(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def orc(session, paths, opts \\ []) do
    data_source(session, "orc", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading Avro file(s).

  ## Options

  - `:schema` — optional schema string
  - `:options` — map of Avro reader options
  """
  @spec avro(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def avro(session, paths, opts \\ []) do
    data_source(session, "avro", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading XML file(s).

  ## Options

  - `:schema` — optional schema string
  - `:options` — map of XML reader options
  """
  @spec xml(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def xml(session, paths, opts \\ []) do
    data_source(session, "xml", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading binary files.

  ## Options

  - `:options` — map of BinaryFile reader options
  """
  @spec binary_file(GenServer.server(), String.t() | [String.t()], keyword()) :: DataFrame.t()
  def binary_file(session, paths, opts \\ []) do
    data_source(session, "binaryFile", paths, opts)
  end

  @doc """
  Creates a DataFrame by reading from JDBC.

  ## Options

  - `:options` — map of JDBC reader options (e.g. `url`, `dbtable`)
  - `:predicates` — list of SQL predicate strings for JDBC predicate pushdown
  """
  @spec jdbc(GenServer.server(), String.t(), String.t(), keyword()) :: DataFrame.t()
  def jdbc(session, url, table, opts \\ []) when is_binary(url) and is_binary(table) do
    opts_options = opts |> Keyword.get(:options, %{}) |> normalize_options()
    merged = opts_options |> Map.put("url", url) |> Map.put("dbtable", table)

    data_source(
      session,
      "jdbc",
      [],
      Keyword.put(opts, :options, merged)
    )
  end

  @spec jdbc(GenServer.server(), keyword()) :: DataFrame.t()
  def jdbc(session, opts) when is_list(opts) do
    data_source(session, "jdbc", [], opts)
  end

  @doc """
  Reads from JDBC with partitioned parallel reads.

  Mirrors PySpark's `DataFrameReader.jdbc(url, table, column, lowerBound,
  upperBound, numPartitions, properties)` partitioned overload. The
  partitioning arguments are forwarded as JDBC data-source options
  (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`).

  `properties` is a map merged into the JDBC options after the partition
  options, matching PySpark's precedence.
  """
  @spec jdbc(
          GenServer.server(),
          String.t(),
          String.t(),
          String.t(),
          integer(),
          integer(),
          pos_integer(),
          map() | keyword() | nil
        ) :: DataFrame.t()
  def jdbc(session, url, table, column_name, lower_bound, upper_bound, num_partitions, properties)
      when is_binary(url) and is_binary(table) and is_binary(column_name) and
             is_integer(lower_bound) and is_integer(upper_bound) and
             is_integer(num_partitions) and num_partitions > 0 do
    validate_jdbc_partition_args(column_name, lower_bound, upper_bound, properties)

    partition_options = %{
      "partitionColumn" => column_name,
      "lowerBound" => Integer.to_string(lower_bound),
      "upperBound" => Integer.to_string(upper_bound),
      "numPartitions" => Integer.to_string(num_partitions)
    }

    extra_options = normalize_jdbc_properties(properties)

    merged =
      partition_options
      |> Map.merge(extra_options)
      |> Map.put("url", url)
      |> Map.put("dbtable", table)

    data_source(session, "jdbc", [], options: merged)
  end

  # --- Private ---

  defp validate_jdbc_partition_args(column_name, lower_bound, upper_bound, _properties) do
    if String.trim(column_name) == "" do
      raise ArgumentError, "column_name must be a non-empty string"
    end

    if lower_bound > upper_bound do
      raise ArgumentError,
            "lowerBound (#{lower_bound}) must not exceed upperBound (#{upper_bound})"
    end
  end

  defp normalize_jdbc_properties(nil), do: %{}

  defp normalize_jdbc_properties(props) when is_map(props) or is_list(props),
    do: normalize_options(props)

  defp normalize_jdbc_properties(other) do
    raise ArgumentError,
          "properties must be nil, a map, or a keyword list, got: #{inspect(other)}"
  end

  defp load_from_builder(reader, paths, opts) do
    format = Keyword.get(opts, :format, reader.format)
    schema = opts |> Keyword.get(:schema, reader.schema) |> normalize_schema()

    call_time_options = merge_source_options(opts, [:format, :schema, :predicates])
    merged_options = Map.merge(reader.options, call_time_options)
    predicates = opts |> Keyword.get(:predicates) |> normalize_predicates()

    plan =
      case predicates do
        [] -> {:read_data_source, format, List.wrap(paths), schema, merged_options}
        _ -> {:read_data_source, format, List.wrap(paths), schema, merged_options, predicates}
      end

    DataFrame.new(reader.session, plan)
  end

  defp data_source(session, format, paths, opts) do
    paths = List.wrap(paths)
    schema = opts |> Keyword.get(:schema, nil) |> normalize_schema()
    options = merge_source_options(opts, [:schema, :predicates])
    predicates = opts |> Keyword.get(:predicates) |> normalize_predicates()

    plan =
      case predicates do
        [] -> {:read_data_source, format, paths, schema, options}
        _ -> {:read_data_source, format, paths, schema, options, predicates}
      end

    DataFrame.new(session, plan)
  end

  defp merge_source_options(opts, reserved_keys) do
    nested_options = opts |> Keyword.get(:options, %{}) |> normalize_options_reject_nil()

    top_level_options =
      opts
      |> Keyword.drop([:options | reserved_keys])
      |> normalize_options_reject_nil()

    duplicates =
      top_level_options
      |> Map.keys()
      |> Enum.filter(&Map.has_key?(nested_options, &1))

    if duplicates != [] do
      raise ArgumentError,
            "got multiple values for keyword argument(s) " <>
              Enum.map_join(duplicates, ", ", &inspect/1) <>
              " — pass each option either as a top-level keyword OR inside :options, not both"
    end

    Map.merge(top_level_options, nested_options)
  end

  defp normalize_schema(nil), do: nil
  defp normalize_schema(schema) when is_binary(schema), do: schema
  defp normalize_schema({:struct, _} = schema), do: SparkEx.Types.to_json(schema)

  defp normalize_schema(%Spark.Connect.DataType{} = schema),
    do: SparkEx.Types.data_type_to_json(schema)

  defp normalize_schema(schema) do
    raise ArgumentError,
          "schema must be a string, {:struct, fields} tuple, or Spark.Connect.DataType, got: #{inspect(schema)}"
  end

  defp normalize_options(opts) when is_list(opts) do
    SparkEx.Internal.OptionUtils.stringify_options(opts)
  end

  defp normalize_options(opts) when is_map(opts) do
    SparkEx.Internal.OptionUtils.stringify_options(opts)
  end

  defp normalize_options_reject_nil(opts) when is_list(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_options_reject_nil(opts) when is_map(opts) do
    SparkEx.Internal.OptionUtils.stringify_options_reject_nil(opts)
  end

  defp normalize_option_value(value) do
    SparkEx.Internal.OptionUtils.normalize_option_value(value)
  end

  defp normalize_predicates(nil), do: []

  defp normalize_predicates(predicates) when is_list(predicates) do
    if Enum.all?(predicates, &is_binary/1) do
      predicates
    else
      raise ArgumentError,
            "reader predicates must be a list of strings, got: #{inspect(predicates)}"
    end
  end

  defp normalize_predicates(predicates) do
    raise ArgumentError,
          "reader predicates must be a list of strings, got: #{inspect(predicates)}"
  end
end
