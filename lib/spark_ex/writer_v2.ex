defmodule SparkEx.WriterV2 do
  @moduledoc """
  Builder API for writing DataFrames using the V2 DataSource API.

  Mirrors PySpark's `DataFrameWriterV2` with a builder pattern.

  ## Examples

      import SparkEx.WriterV2

      # Create a new table
      df
      |> SparkEx.DataFrame.write_v2("catalog.db.my_table")
      |> using("parquet")
      |> table_property("description", "My table")
      |> create()

      # Append to existing table
      df
      |> SparkEx.DataFrame.write_v2("my_table")
      |> append()

      # Overwrite with condition
      import SparkEx.Functions, only: [col: 1, lit: 1]

      df
      |> SparkEx.DataFrame.write_v2("my_table")
      |> overwrite(col("date") |> SparkEx.Column.eq(lit("2024-01-01")))
  """

  defstruct [
    :df,
    :table_name,
    :provider,
    :overwrite_condition,
    options: %{},
    table_properties: %{},
    partitioned_by: [],
    cluster_by: []
  ]

  @type t :: %__MODULE__{
          df: SparkEx.DataFrame.t(),
          table_name: String.t(),
          provider: String.t() | nil,
          options: %{String.t() => String.t()},
          table_properties: %{String.t() => String.t()},
          partitioned_by: [term()],
          cluster_by: [String.t()],
          overwrite_condition: term() | nil
        }

  @doc """
  Sets the provider (data source) for the table.
  """
  @spec using(t(), String.t()) :: t()
  def using(%__MODULE__{} = writer, provider) when is_binary(provider) do
    %{writer | provider: provider}
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
  Sets a single table property.
  """
  @spec table_property(t(), String.t(), term()) :: t()
  def table_property(%__MODULE__{} = writer, key, value)
      when is_binary(key) do
    %{
      writer
      | table_properties: Map.put(writer.table_properties, key, normalize_option_value(value))
    }
  end

  @doc """
  Merges a map of table properties.
  """
  @spec table_properties(t(), map() | keyword()) :: t()
  def table_properties(%__MODULE__{} = writer, props) when is_map(props) do
    merged = Map.merge(writer.table_properties, stringify_options(props))
    %{writer | table_properties: merged}
  end

  def table_properties(%__MODULE__{} = writer, props) when is_list(props) do
    props
    |> Enum.into(%{})
    |> then(&table_properties(writer, &1))
  end

  @doc """
  Sets the partitioning expressions.

  Accepts column expressions (e.g. from `SparkEx.Functions.col/1`).
  """
  @spec partitioned_by(t(), [term()]) :: t()
  def partitioned_by(%__MODULE__{} = writer, exprs) when is_list(exprs) do
    normalized = Enum.map(exprs, &normalize_partition_expr/1)
    %{writer | partitioned_by: normalized}
  end

  @doc """
  Sets the clustering columns.
  """
  @spec cluster_by(t(), [String.t()]) :: t()
  def cluster_by(%__MODULE__{} = writer, columns) when is_list(columns) do
    %{writer | cluster_by: Enum.map(columns, &to_string/1)}
  end

  # --- Write actions ---

  @doc """
  Creates a new table with the DataFrame's data.
  """
  @spec create(t(), keyword()) :: :ok | {:error, term()}
  def create(%__MODULE__{} = writer, opts \\ []) do
    execute_v2(writer, :create, opts)
  end

  @doc """
  Replaces an existing table with the DataFrame's data.
  """
  @spec replace(t(), keyword()) :: :ok | {:error, term()}
  def replace(%__MODULE__{} = writer, opts \\ []) do
    execute_v2(writer, :replace, opts)
  end

  @doc """
  Creates a table or replaces it if it already exists.
  """
  @spec create_or_replace(t(), keyword()) :: :ok | {:error, term()}
  def create_or_replace(%__MODULE__{} = writer, opts \\ []) do
    execute_v2(writer, :create_or_replace, opts)
  end

  @doc """
  Appends the DataFrame's data to the table.
  """
  @spec append(t(), keyword()) :: :ok | {:error, term()}
  def append(%__MODULE__{} = writer, opts \\ []) do
    execute_v2(writer, :append, opts)
  end

  @doc """
  Overwrites rows in the table.

  ## Examples

      # Overwrite all data
      overwrite(writer)

      # Overwrite with condition
      overwrite(writer, col("date") |> SparkEx.Column.eq(lit("2024-01-01")))
  """
  @spec overwrite(t()) :: :ok | {:error, term()}
  def overwrite(%__MODULE__{} = writer) do
    execute_v2(writer, :overwrite, [])
  end

  @spec overwrite(t(), keyword()) :: :ok | {:error, term()}
  def overwrite(%__MODULE__{} = writer, opts) when is_list(opts) do
    execute_v2(writer, :overwrite, opts)
  end

  @doc """
  Overwrites rows in the table matching the given condition.
  """
  @spec overwrite(t(), SparkEx.Column.t()) :: :ok | {:error, term()}
  def overwrite(%__MODULE__{} = writer, %SparkEx.Column{} = condition) do
    overwrite(writer, condition, [])
  end

  @spec overwrite(t(), SparkEx.Column.t(), keyword()) :: :ok | {:error, term()}
  def overwrite(%__MODULE__{} = writer, %SparkEx.Column{} = condition, opts) when is_list(opts) do
    writer = %{writer | overwrite_condition: condition.expr}
    execute_v2(writer, :overwrite, opts)
  end

  @doc """
  Overwrites all partitions that the DataFrame touches.
  """
  @spec overwrite_partitions(t(), keyword()) :: :ok | {:error, term()}
  def overwrite_partitions(%__MODULE__{} = writer, opts \\ []) do
    execute_v2(writer, :overwrite_partitions, opts)
  end

  # --- Private ---

  defp execute_v2(writer, mode, opts) do
    v2_opts = [
      mode: mode,
      provider: writer.provider,
      options: writer.options,
      table_properties: writer.table_properties,
      partitioned_by: writer.partitioned_by,
      cluster_by: writer.cluster_by,
      overwrite_condition: writer.overwrite_condition
    ]

    SparkEx.Session.execute_command(
      writer.df.session,
      {:write_operation_v2, writer.df.plan, writer.table_name, v2_opts},
      opts
    )
  end

  defp normalize_partition_expr(%SparkEx.Column{expr: expr}), do: expr
  defp normalize_partition_expr(name) when is_binary(name), do: {:col, name}
  defp normalize_partition_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}
  defp normalize_partition_expr(expr), do: expr

  defp stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  defp normalize_option_value(value) when is_binary(value), do: value
  defp normalize_option_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_option_value(value) when is_float(value), do: Float.to_string(value)
  defp normalize_option_value(value) when is_boolean(value), do: to_string(value)

  defp normalize_option_value(value) do
    raise ArgumentError,
          "writer_v2 option value must be a primitive (string, integer, float, boolean), got: #{inspect(value)}"
  end
end
