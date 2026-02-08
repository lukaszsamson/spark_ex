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

  @doc """
  Creates a DataFrame from a named table (catalog table).

  ## Options

  - `:options` — map of string options (default: `%{}`)

  ## Examples

      df = SparkEx.Reader.table(session, "my_database.my_table")
  """
  @spec table(GenServer.server(), String.t(), keyword()) :: DataFrame.t()
  def table(session, table_name, opts \\ []) when is_binary(table_name) do
    options = opts |> Keyword.get(:options, %{}) |> stringify_options()
    %DataFrame{session: session, plan: {:read_named_table, table_name, options}}
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
    {csv_opts, rest} = Keyword.split(opts, [:header, :infer_schema, :separator])
    extra_options = Keyword.get(rest, :options, %{})

    options =
      csv_opts
      |> Enum.reduce(extra_options, fn
        {:header, v}, acc -> Map.put(acc, "header", to_string(v))
        {:infer_schema, v}, acc -> Map.put(acc, "inferSchema", to_string(v))
        {:separator, v}, acc -> Map.put(acc, "sep", v)
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

  # --- Private ---

  defp data_source(session, format, paths, opts) do
    paths = List.wrap(paths)
    schema = Keyword.get(opts, :schema, nil)
    options = opts |> Keyword.get(:options, %{}) |> stringify_options()
    %DataFrame{session: session, plan: {:read_data_source, format, paths, schema, options}}
  end

  defp stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), to_string(v)} end)
  end
end
