defmodule SparkEx.Writer do
  @moduledoc """
  Builder API for writing DataFrames to external storage.

  Mirrors PySpark's `DataFrameWriter` with a builder pattern.

  ## Examples

      import SparkEx.Writer

      # Write to Parquet
      df
      |> SparkEx.DataFrame.write()
      |> format("parquet")
      |> mode(:overwrite)
      |> option("compression", "snappy")
      |> save("/data/output.parquet")

      # Save as table
      df
      |> SparkEx.DataFrame.write()
      |> format("parquet")
      |> mode(:append)
      |> save_as_table("my_database.my_table")

      # Insert into existing table
      df
      |> SparkEx.DataFrame.write()
      |> mode(:append)
      |> insert_into("my_table")

      # Shorthand: write Parquet
      SparkEx.Writer.parquet(df, "/data/output.parquet", mode: :overwrite)
  """

  defstruct [
    :df,
    :source,
    mode: :error_if_exists,
    options: %{},
    sort_by: [],
    partition_by: [],
    bucket_by: nil,
    cluster_by: []
  ]

  @type t :: %__MODULE__{
          df: SparkEx.DataFrame.t(),
          source: String.t() | nil,
          mode: atom(),
          options: %{String.t() => String.t()},
          sort_by: [String.t()],
          partition_by: [String.t()],
          bucket_by: {pos_integer(), [String.t()]} | nil,
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

  @doc """
  Sets the output data source format (e.g. `"parquet"`, `"csv"`, `"json"`, `"orc"`).
  """
  @spec format(t(), String.t()) :: t()
  def format(%__MODULE__{} = writer, fmt) when is_binary(fmt) do
    %{writer | source: fmt}
  end

  @doc """
  Sets the save mode.

  - `:append` — append to existing data
  - `:overwrite` — overwrite existing data
  - `:error_if_exists` — error if data already exists (default)
  - `:ignore` — silently ignore if data already exists
  """
  @spec mode(t(), atom() | String.t()) :: t()
  def mode(%__MODULE__{} = writer, save_mode) when is_binary(save_mode) do
    atom_mode =
      case String.downcase(save_mode) do
        "append" -> :append
        "overwrite" -> :overwrite
        "error" -> :error_if_exists
        "errorifexists" -> :error_if_exists
        "ignore" -> :ignore
        other -> raise ArgumentError, "unknown save mode: #{inspect(other)}"
      end

    %{writer | mode: atom_mode}
  end

  def mode(%__MODULE__{} = writer, save_mode)
      when save_mode in [:append, :overwrite, :error_if_exists, :ignore] do
    %{writer | mode: save_mode}
  end

  @doc """
  Sets a single writer option.
  """
  @spec option(t(), String.t(), term()) :: t()
  def option(%__MODULE__{} = writer, key, value)
      when is_binary(key) do
    %{writer | options: Map.put(writer.options, key, normalize_option_value(value))}
  end

  @doc """
  Merges a map of options into the writer.
  """
  @spec options(t(), map() | keyword()) :: t()
  def options(%__MODULE__{} = writer, opts) when is_map(opts) do
    merged = Map.merge(writer.options, stringify_options(opts))
    %{writer | options: merged}
  end

  def options(%__MODULE__{} = writer, opts) when is_list(opts) do
    opts
    |> Enum.into(%{})
    |> then(&options(writer, &1))
  end

  @doc """
  Sets the partitioning columns for the write.
  """
  @spec partition_by(t(), [String.t()]) :: t()
  def partition_by(%__MODULE__{} = writer, columns) when is_list(columns) do
    %{writer | partition_by: Enum.map(columns, &to_string/1)}
  end

  @doc """
  Sets the sort columns for the write.
  """
  @spec sort_by(t(), [String.t()]) :: t()
  def sort_by(%__MODULE__{} = writer, columns) when is_list(columns) do
    if columns == [] do
      raise ArgumentError, "sort_by columns should not be empty"
    end

    %{writer | sort_by: Enum.map(columns, &to_string/1)}
  end

  @doc """
  Sets bucketing for the write.
  """
  @spec bucket_by(t(), pos_integer(), [String.t()]) :: t()
  def bucket_by(%__MODULE__{} = writer, num_buckets, columns)
      when is_integer(num_buckets) and num_buckets > 0 and is_list(columns) do
    if columns == [] do
      raise ArgumentError, "bucket_by columns should not be empty"
    end

    %{writer | bucket_by: {num_buckets, Enum.map(columns, &to_string/1)}}
  end

  @doc """
  Sets clustering columns for the write.
  """
  @spec cluster_by(t(), [String.t()]) :: t()
  def cluster_by(%__MODULE__{} = writer, columns) when is_list(columns) do
    if columns == [] do
      raise ArgumentError, "cluster_by columns should not be empty"
    end

    %{writer | cluster_by: Enum.map(columns, &to_string/1)}
  end

  @doc """
  Saves the DataFrame to the given path.

  Executes the write operation on the Spark server.
  """
  @spec save(t(), String.t() | nil, keyword()) :: :ok | {:error, term()}
  def save(writer, path \\ nil, opts \\ [])

  def save(%__MODULE__{} = writer, nil, opts) do
    write_opts = build_write_opts(writer, [])
    execute_write(writer.df, write_opts, opts)
  end

  def save(%__MODULE__{} = writer, path, opts) when is_binary(path) do
    write_opts = build_write_opts(writer, path: path)
    execute_write(writer.df, write_opts, opts)
  end

  @doc """
  Saves the DataFrame as a named table.
  """
  @spec save_as_table(t(), String.t(), keyword()) :: :ok | {:error, term()}
  def save_as_table(%__MODULE__{} = writer, table_name, opts \\ []) when is_binary(table_name) do
    write_opts = build_write_opts(writer, table: table_name)
    execute_write(writer.df, write_opts, opts)
  end

  @doc """
  Inserts the DataFrame into an existing table.

  The save mode is automatically set to `:append` unless explicitly overridden.
  """
  @spec insert_into(t(), String.t(), boolean() | keyword()) :: :ok | {:error, term()}
  def insert_into(%__MODULE__{} = writer, table_name, overwrite_or_opts \\ [])
      when is_binary(table_name) do
    {effective_writer, exec_opts} = resolve_insert_into_opts(writer, overwrite_or_opts)
    write_opts = build_write_opts(effective_writer, insert_into: table_name)
    execute_write(effective_writer.df, write_opts, exec_opts)
  end

  # --- Format-specific convenience functions ---

  @doc """
  Writes the DataFrame as Parquet.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of Parquet writer options
  - `:partition_by` — partitioning columns
  """
  @spec parquet(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def parquet(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(opts, [:mode, :partition_by])

    writer =
      %__MODULE__{df: df, source: "parquet"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame as CSV.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:header` — whether to include a header row
  - `:separator` — field separator
  - `:options` — map of CSV writer options
  """
  @spec csv(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def csv(df, path, opts \\ []) do
    {csv_opts, rest} = Keyword.split(opts, [:header, :separator, :sep])

    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(rest, [:mode, :partition_by])

    extra_options =
      csv_opts
      |> Enum.reduce(%{}, fn
        {:header, v}, acc -> Map.put(acc, "header", to_string(v))
        {:separator, v}, acc -> Map.put(acc, "sep", v)
        {:sep, v}, acc -> Map.put(acc, "sep", v)
      end)

    writer =
      %__MODULE__{df: df, source: "csv"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{
      writer
      | options: extra_options |> Map.merge(option_overrides) |> Map.merge(writer.options)
    }

    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame as JSON.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of JSON writer options
  """
  @spec json(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def json(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(opts, [:mode, :partition_by])

    writer =
      %__MODULE__{df: df, source: "json"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame as ORC.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of ORC writer options
  """
  @spec orc(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def orc(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(opts, [:mode, :partition_by])

    writer =
      %__MODULE__{df: df, source: "orc"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame as Avro.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of Avro writer options
  - `:partition_by` — partitioning columns
  """
  @spec avro(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def avro(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(opts, [:mode, :partition_by])

    writer =
      %__MODULE__{df: df, source: "avro"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame as XML.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of XML writer options
  - `:partition_by` — partitioning columns
  """
  @spec xml(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def xml(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} =
      split_convenience_opts(opts, [:mode, :partition_by])

    writer =
      %__MODULE__{df: df, source: "xml"}
      |> maybe_set_mode(write_opts)
      |> maybe_set_partition_by(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  @doc """
  Writes the DataFrame via JDBC.

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of JDBC writer options (e.g. `url`, `dbtable`)
  """
  @spec jdbc(SparkEx.DataFrame.t(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def jdbc(df, url, table, opts \\ []) when is_binary(url) and is_binary(table) do
    {write_opts, option_overrides, exec_opts} = split_convenience_opts(opts, [:mode])
    merged = option_overrides |> Map.put("url", url) |> Map.put("dbtable", table)

    writer =
      %__MODULE__{df: df, source: "jdbc", options: merged}
      |> maybe_set_mode(write_opts)

    execute_write(writer.df, build_write_opts(writer, []), exec_opts)
  end

  @spec jdbc(SparkEx.DataFrame.t(), keyword()) :: :ok | {:error, term()}
  def jdbc(df, opts) when is_list(opts) do
    {write_opts, option_overrides, exec_opts} = split_convenience_opts(opts, [:mode])

    writer =
      %__MODULE__{df: df, source: "jdbc", options: option_overrides}
      |> maybe_set_mode(write_opts)

    execute_write(writer.df, build_write_opts(writer, []), exec_opts)
  end

  @doc """
  Writes the DataFrame as text (single column).

  ## Options

  - `:mode` — save mode (default: `:error_if_exists`)
  - `:options` — map of text writer options
  """
  @spec text(SparkEx.DataFrame.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def text(df, path, opts \\ []) do
    {write_opts, option_overrides, exec_opts} = split_convenience_opts(opts, [:mode])

    writer =
      %__MODULE__{df: df, source: "text"}
      |> maybe_set_mode(write_opts)

    writer = %{writer | options: Map.merge(writer.options, option_overrides)}
    save(writer, path, exec_opts)
  end

  # --- Private ---

  defp build_write_opts(writer, extra) do
    base = [
      format: writer.source,
      mode: writer.mode,
      options: writer.options,
      sort_by: writer.sort_by,
      partition_by: writer.partition_by,
      bucket_by: writer.bucket_by,
      cluster_by: writer.cluster_by
    ]

    Keyword.merge(base, extra)
  end

  defp execute_write(df, write_opts, exec_opts) do
    SparkEx.Session.execute_command(
      df.session,
      {:write_operation, df.plan, write_opts},
      exec_opts
    )
  end

  defp maybe_set_mode(writer, opts) do
    case Keyword.get(opts, :mode) do
      nil -> writer
      m -> mode(writer, m)
    end
  end

  defp maybe_set_partition_by(writer, opts) do
    case Keyword.get(opts, :partition_by) do
      nil -> writer
      cols -> partition_by(writer, cols)
    end
  end

  defp split_convenience_opts(opts, writer_keys) do
    write_opts = Keyword.take(opts, writer_keys)
    exec_opts = Keyword.take(opts, @exec_opt_keys)
    option_overrides = extract_option_overrides(opts, writer_keys)
    {write_opts, option_overrides, exec_opts}
  end

  defp extract_option_overrides(opts, reserved_keys) do
    nested_options = opts |> Keyword.get(:options, %{}) |> stringify_options()

    top_level_options =
      opts
      |> Keyword.drop([:options | reserved_keys ++ @exec_opt_keys])
      |> stringify_options()

    Map.merge(top_level_options, nested_options)
  end

  defp stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  defp stringify_options(opts) when is_list(opts) do
    opts
    |> Enum.into(%{})
    |> stringify_options()
  end

  defp normalize_option_value(value) when is_binary(value), do: value
  defp normalize_option_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_option_value(value) when is_float(value), do: Float.to_string(value)
  defp normalize_option_value(value) when is_boolean(value), do: to_string(value)

  defp normalize_option_value(value) do
    raise ArgumentError,
          "writer option value must be a primitive (string, integer, float, boolean), got: #{inspect(value)}"
  end

  defp resolve_insert_into_opts(writer, overwrite) when is_boolean(overwrite) do
    mode = if overwrite, do: :overwrite, else: :append
    {mode(writer, mode), []}
  end

  defp resolve_insert_into_opts(writer, opts) when is_list(opts) do
    overwrite = Keyword.get(opts, :overwrite)
    exec_opts = Keyword.delete(opts, :overwrite)

    normalized_writer =
      cond do
        is_boolean(overwrite) ->
          mode(writer, if(overwrite, do: :overwrite, else: :append))

        true ->
          writer
      end

    {normalized_writer, exec_opts}
  end
end
