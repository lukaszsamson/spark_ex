defmodule SparkEx.Catalog do
  @moduledoc """
  Catalog API for Spark Connect sessions.

  Provides introspection and management of catalogs, databases, tables,
  functions, columns, temp views, and table caching.

  All functions take a session (PID) as the first argument and execute
  eagerly against the Spark cluster.
  """

  alias SparkEx.DataFrame

  # ── Result Structs ──

  defmodule CatalogMetadata do
    @moduledoc "Metadata about a catalog."
    defstruct [:name, :description]
    @type t :: %__MODULE__{name: String.t(), description: String.t() | nil}
  end

  defmodule Database do
    @moduledoc "Metadata about a database."
    defstruct [:name, :catalog, :description, :location_uri]

    @type t :: %__MODULE__{
            name: String.t(),
            catalog: String.t() | nil,
            description: String.t() | nil,
            location_uri: String.t()
          }
  end

  defmodule Table do
    @moduledoc "Metadata about a table."
    defstruct [:name, :catalog, :namespace, :description, :table_type, :is_temporary]

    @type t :: %__MODULE__{
            name: String.t(),
            catalog: String.t() | nil,
            namespace: [String.t()] | nil,
            description: String.t() | nil,
            table_type: String.t(),
            is_temporary: boolean()
          }
  end

  defmodule Function do
    @moduledoc "Metadata about a function."
    defstruct [:name, :catalog, :namespace, :description, :class_name, :is_temporary]

    @type t :: %__MODULE__{
            name: String.t(),
            catalog: String.t() | nil,
            namespace: [String.t()] | nil,
            description: String.t() | nil,
            class_name: String.t(),
            is_temporary: boolean()
          }
  end

  defmodule ColumnInfo do
    @moduledoc "Metadata about a column."
    defstruct [:name, :description, :data_type, :nullable, :is_partition, :is_bucket, :is_cluster]

    @type t :: %__MODULE__{
            name: String.t(),
            description: String.t() | nil,
            data_type: String.t(),
            nullable: boolean(),
            is_partition: boolean(),
            is_bucket: boolean(),
            is_cluster: boolean()
          }
  end

  # ── Catalog Management ──

  @spec current_catalog(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def current_catalog(session) do
    execute_scalar(session, {:current_catalog})
  end

  @spec set_current_catalog(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def set_current_catalog(session, catalog_name) when is_binary(catalog_name) do
    execute_void(session, {:set_current_catalog, catalog_name})
  end

  @spec list_catalogs(GenServer.server(), String.t() | nil) ::
          {:ok, [CatalogMetadata.t()]} | {:error, term()}
  def list_catalogs(session, pattern \\ nil) do
    case execute_catalog(session, {:list_catalogs, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_catalog_metadata/1)}
      {:error, _} = err -> err
    end
  end

  # ── Database Management ──

  @spec current_database(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def current_database(session) do
    execute_scalar(session, {:current_database})
  end

  @spec set_current_database(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def set_current_database(session, db_name) when is_binary(db_name) do
    execute_void(session, {:set_current_database, db_name})
  end

  @spec list_databases(GenServer.server(), String.t() | nil) ::
          {:ok, [Database.t()]} | {:error, term()}
  def list_databases(session, pattern \\ nil) do
    case execute_catalog(session, {:list_databases, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_database/1)}
      {:error, _} = err -> err
    end
  end

  @spec get_database(GenServer.server(), String.t()) :: {:ok, Database.t()} | {:error, term()}
  def get_database(session, db_name) when is_binary(db_name) do
    case execute_catalog(session, {:get_database, db_name}) do
      {:ok, [row | _]} -> {:ok, parse_database(row)}
      {:ok, []} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @spec database_exists?(GenServer.server(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def database_exists?(session, db_name) when is_binary(db_name) do
    execute_scalar(session, {:database_exists, db_name})
  end

  # ── Table Management ──

  @spec list_tables(GenServer.server(), String.t() | nil, String.t() | nil) ::
          {:ok, [Table.t()]} | {:error, term()}
  def list_tables(session, db_name \\ nil, pattern \\ nil) do
    case execute_catalog(session, {:list_tables, db_name, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_table/1)}
      {:error, _} = err -> err
    end
  end

  @spec get_table(GenServer.server(), String.t()) :: {:ok, Table.t()} | {:error, term()}
  def get_table(session, table_name) when is_binary(table_name) do
    case execute_catalog(session, {:get_table, table_name, nil}) do
      {:ok, [row | _]} -> {:ok, parse_table(row)}
      {:ok, []} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @spec table_exists?(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, boolean()} | {:error, term()}
  def table_exists?(session, table_name, db_name \\ nil) when is_binary(table_name) do
    execute_scalar(session, {:table_exists, table_name, db_name})
  end

  @spec list_columns(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, [ColumnInfo.t()]} | {:error, term()}
  def list_columns(session, table_name, db_name \\ nil) when is_binary(table_name) do
    case execute_catalog(session, {:list_columns, table_name, db_name}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_column_info/1)}
      {:error, _} = err -> err
    end
  end

  # ── Function Management ──

  @spec list_functions(GenServer.server(), String.t() | nil, String.t() | nil) ::
          {:ok, [Function.t()]} | {:error, term()}
  def list_functions(session, db_name \\ nil, pattern \\ nil) do
    case execute_catalog(session, {:list_functions, db_name, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_function/1)}
      {:error, _} = err -> err
    end
  end

  @spec get_function(GenServer.server(), String.t()) :: {:ok, Function.t()} | {:error, term()}
  def get_function(session, function_name) when is_binary(function_name) do
    case execute_catalog(session, {:get_function, function_name, nil}) do
      {:ok, [row | _]} -> {:ok, parse_function(row)}
      {:ok, []} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @spec function_exists?(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, boolean()} | {:error, term()}
  def function_exists?(session, function_name, db_name \\ nil) when is_binary(function_name) do
    execute_scalar(session, {:function_exists, function_name, db_name})
  end

  # ── Temp Views ──

  @spec drop_temp_view(GenServer.server(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def drop_temp_view(session, view_name) when is_binary(view_name) do
    execute_scalar(session, {:drop_temp_view, view_name})
  end

  @spec drop_global_temp_view(GenServer.server(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def drop_global_temp_view(session, view_name) when is_binary(view_name) do
    execute_scalar(session, {:drop_global_temp_view, view_name})
  end

  # ── Caching ──

  @spec is_cached?(GenServer.server(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def is_cached?(session, table_name) when is_binary(table_name) do
    execute_scalar(session, {:is_cached, table_name})
  end

  @spec cache_table(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def cache_table(session, table_name, opts \\ []) when is_binary(table_name) do
    storage_level = Keyword.get(opts, :storage_level, nil)
    execute_void(session, {:cache_table, table_name, storage_level})
  end

  @spec uncache_table(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def uncache_table(session, table_name) when is_binary(table_name) do
    execute_void(session, {:uncache_table, table_name})
  end

  @spec clear_cache(GenServer.server()) :: :ok | {:error, term()}
  def clear_cache(session) do
    execute_void(session, {:clear_cache})
  end

  # ── Refresh / Recovery ──

  @spec refresh_table(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def refresh_table(session, table_name) when is_binary(table_name) do
    execute_void(session, {:refresh_table, table_name})
  end

  @spec refresh_by_path(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def refresh_by_path(session, path) when is_binary(path) do
    execute_void(session, {:refresh_by_path, path})
  end

  @spec recover_partitions(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def recover_partitions(session, table_name) when is_binary(table_name) do
    execute_void(session, {:recover_partitions, table_name})
  end

  # ── Table Creation ──

  @spec create_table(GenServer.server(), String.t(), keyword()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def create_table(session, table_name, opts \\ []) when is_binary(table_name) do
    path = Keyword.get(opts, :path, nil)
    source = Keyword.get(opts, :source, nil)
    description = Keyword.get(opts, :description, nil)
    schema = Keyword.get(opts, :schema, nil)
    options = Keyword.get(opts, :options, %{})

    plan = {:catalog, {:create_table, table_name, path, source, description, schema, options}}
    df = %DataFrame{session: session, plan: plan}

    case DataFrame.collect(df) do
      {:ok, _} -> {:ok, df}
      {:error, _} = err -> err
    end
  end

  # ── Private Helpers ──

  defp execute_catalog(session, cat_plan) do
    df = %DataFrame{session: session, plan: {:catalog, cat_plan}}
    DataFrame.collect(df)
  end

  defp execute_scalar(session, cat_plan) do
    case execute_catalog(session, cat_plan) do
      {:ok, [row]} when is_map(row) ->
        {:ok, row |> Map.values() |> hd()}

      {:ok, []} ->
        {:error, :no_result}

      {:error, _} = err ->
        err
    end
  end

  defp execute_void(session, cat_plan) do
    case execute_catalog(session, cat_plan) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  # ── Row Parsers ──

  defp parse_catalog_metadata(row) do
    %CatalogMetadata{
      name: row["name"],
      description: row["description"]
    }
  end

  defp parse_database(row) do
    %Database{
      name: row["name"],
      catalog: row["catalog"],
      description: row["description"],
      location_uri: row["locationUri"]
    }
  end

  defp parse_table(row) do
    %Table{
      name: row["name"],
      catalog: row["catalog"],
      namespace: parse_namespace(row["namespace"]),
      description: row["description"],
      table_type: row["tableType"],
      is_temporary: row["isTemporary"]
    }
  end

  defp parse_function(row) do
    %Function{
      name: row["name"],
      catalog: row["catalog"],
      namespace: parse_namespace(row["namespace"]),
      description: row["description"],
      class_name: row["className"],
      is_temporary: row["isTemporary"]
    }
  end

  defp parse_column_info(row) do
    %ColumnInfo{
      name: row["name"],
      description: row["description"],
      data_type: row["dataType"],
      nullable: row["nullable"],
      is_partition: row["isPartition"],
      is_bucket: row["isBucket"],
      is_cluster: row["isCluster"]
    }
  end

  defp parse_namespace(nil), do: nil
  defp parse_namespace(ns) when is_list(ns), do: ns

  defp parse_namespace(ns) when is_binary(ns) do
    case Jason.decode(ns) do
      {:ok, list} when is_list(list) -> list
      _ -> [ns]
    end
  end
end
