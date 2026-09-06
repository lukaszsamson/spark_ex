defmodule SparkEx.Catalog do
  @moduledoc """
  Catalog API for Spark Connect sessions.

  Provides introspection and management of catalogs, databases, tables,
  functions, columns, temp views, and table caching.

  All functions take a session (PID) as the first argument and execute
  eagerly against the Spark cluster.
  """

  alias SparkEx.DataFrame
  alias SparkEx.Internal.OptionUtils

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

  defmodule TablePartition do
    @moduledoc "A table partition specification returned by `list_partitions/3`."
    defstruct [:partition]
    @type t :: %__MODULE__{partition: String.t()}
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
  def list_catalogs(session, pattern \\ nil)

  def list_catalogs(session, pattern) when is_nil(pattern) or is_binary(pattern) do
    case execute_catalog(session, {:list_catalogs, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_catalog_metadata/1)}
      {:error, _} = err -> err
    end
  end

  def list_catalogs(_session, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      {:error,
       {:invalid_options, "list_catalogs/2 accepts only a pattern string, not keyword opts"}}
    else
      {:error, {:invalid_pattern, opts}}
    end
  end

  def list_catalogs(_session, pattern), do: {:error, {:invalid_pattern, pattern}}

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
  def list_databases(session, pattern \\ nil)

  def list_databases(session, pattern) when is_nil(pattern) or is_binary(pattern) do
    case execute_catalog(session, {:list_databases, pattern}) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_database/1)}
      {:error, _} = err -> err
    end
  end

  def list_databases(_session, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      {:error,
       {:invalid_options, "list_databases/2 accepts only a pattern string, not keyword opts"}}
    else
      {:error, {:invalid_pattern, opts}}
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

  @spec create_database(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_database(session, db_name, opts \\ []) when is_binary(db_name) and is_list(opts) do
    with :ok <- validate_create_database_opts(opts) do
      case catalog_backend(opts) do
        :catalog ->
          execute_void(session, {
            :create_database,
            db_name,
            Keyword.get(opts, :if_not_exists, false),
            normalize_properties(Keyword.get(opts, :properties, %{}))
          })

        :sql ->
          execute_sql_void(session, build_create_database_sql(db_name, opts))
      end
    end
  end

  @spec drop_database(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def drop_database(session, db_name, opts \\ []) when is_binary(db_name) do
    case catalog_backend(opts) do
      :catalog ->
        execute_void(session, {
          :drop_database,
          db_name,
          Keyword.get(opts, :if_exists, false),
          Keyword.get(opts, :cascade, false)
        })

      :sql ->
        execute_sql_void(session, build_drop_database_sql(db_name, opts))
    end
  end

  @spec alter_database(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def alter_database(session, db_name, opts) when is_binary(db_name) and is_list(opts) do
    sql = build_alter_database_sql(db_name, opts)
    execute_sql_void(session, sql)
  end

  # ── Table Management ──

  @spec list_tables(GenServer.server(), String.t() | nil, String.t() | nil) ::
          {:ok, [Table.t()]} | {:error, term()}
  def list_tables(session, db_name \\ nil, pattern \\ nil) do
    with {:ok, db_name} <- resolve_catalog_db_name(session, db_name),
         {:ok, pattern} <- normalize_catalog_pattern(pattern),
         {:ok, rows} <- execute_catalog(session, {:list_tables, db_name, pattern}) do
      {:ok, Enum.map(rows, &parse_table/1)}
    else
      {:error, _} = err -> err
    end
  end

  @spec get_table(GenServer.server(), String.t()) :: {:ok, Table.t()} | {:error, term()}
  def get_table(session, table_name) when is_binary(table_name) do
    get_table(session, table_name, nil)
  end

  @spec get_table(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, Table.t()} | {:error, term()}
  def get_table(session, table_name, db_name) when is_binary(table_name) do
    with {:ok, {table_name, db_name}} <- resolve_table_lookup(session, table_name, db_name),
         {:ok, rows} <- execute_catalog(session, {:get_table, table_name, db_name}) do
      case rows do
        [row | _] -> {:ok, parse_table(row)}
        [] -> {:error, :not_found}
      end
    else
      {:error, _} = err -> err
    end
  end

  @spec table_exists?(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, boolean()} | {:error, term()}
  def table_exists?(session, table_name, db_name \\ nil) when is_binary(table_name) do
    with {:ok, db_name} <- normalize_optional_db_name(db_name) do
      execute_scalar(session, {:table_exists, table_name, db_name})
    end
  end

  @spec list_columns(GenServer.server(), String.t(), String.t() | nil) ::
          {:ok, [ColumnInfo.t()]} | {:error, term()}
  def list_columns(session, table_name, db_name \\ nil) when is_binary(table_name) do
    with {:ok, {table_name, db_name}} <- resolve_table_lookup(session, table_name, db_name),
         {:ok, rows} <- execute_catalog(session, {:list_columns, table_name, db_name}) do
      {:ok, Enum.map(rows, &parse_column_info/1)}
    else
      {:error, _} = err -> err
    end
  end

  @spec drop_table(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def drop_table(session, table_name, opts \\ []) when is_binary(table_name) do
    case catalog_backend(opts) do
      :catalog ->
        execute_void(session, {
          :drop_table,
          table_name,
          Keyword.get(opts, :if_exists, false),
          Keyword.get(opts, :purge, false)
        })

      :sql ->
        execute_sql_void(session, build_drop_table_sql(table_name, opts))
    end
  end

  @doc "Drops a persistent view. Use `backend: :catalog` to require Spark 4.2's catalog relation."
  @spec drop_view(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def drop_view(session, view_name, opts \\ []) when is_binary(view_name) and is_list(opts) do
    if_exists = Keyword.get(opts, :if_exists, false)

    case catalog_backend(opts) do
      :catalog -> execute_void(session, {:drop_view, view_name, if_exists})
      :sql -> execute_sql_void(session, build_drop_view_sql(view_name, if_exists))
    end
  end

  @doc "Lists partition specifications for a table."
  @spec list_partitions(GenServer.server(), String.t(), keyword()) ::
          {:ok, [TablePartition.t()]} | {:error, term()}
  def list_partitions(session, table_name, opts \\ [])
      when is_binary(table_name) and is_list(opts) do
    result =
      case catalog_backend(opts) do
        :catalog -> execute_catalog(session, {:list_partitions, table_name})
        :sql -> execute_sql(session, "SHOW PARTITIONS " <> quote_qualified_name(table_name))
      end

    case result do
      {:ok, rows} -> {:ok, Enum.map(rows, &%TablePartition{partition: first_row_value(&1)})}
      {:error, _} = error -> error
    end
  end

  @doc "Lists views in a database. A pattern without a database uses the current database."
  @spec list_views(GenServer.server(), String.t() | nil, String.t() | nil, keyword()) ::
          {:ok, [Table.t()]} | {:error, term()}
  def list_views(session, db_name \\ nil, pattern \\ nil, opts \\ [])
      when (is_nil(db_name) or is_binary(db_name)) and (is_nil(pattern) or is_binary(pattern)) and
             is_list(opts) do
    backend = catalog_backend(opts)

    with {:ok, resolved_db_name} <- resolve_list_views_db_name(session, db_name, pattern, backend),
         {:ok, rows} <-
           (case backend do
              :catalog -> execute_catalog(session, {:list_views, resolved_db_name, pattern})
              :sql -> execute_sql(session, build_list_views_sql(resolved_db_name, pattern))
            end) do
      {:ok, Enum.map(rows, &parse_view/1)}
    end
  end

  @doc "Returns table properties as a string-keyed map."
  @spec get_table_properties(GenServer.server(), String.t(), keyword()) ::
          {:ok, %{String.t() => String.t()}} | {:error, term()}
  def get_table_properties(session, table_name, opts \\ [])
      when is_binary(table_name) and is_list(opts) do
    result =
      case catalog_backend(opts) do
        :catalog -> execute_catalog(session, {:get_table_properties, table_name})
        :sql -> execute_sql(session, "SHOW TBLPROPERTIES " <> quote_qualified_name(table_name))
      end

    case result do
      {:ok, rows} -> {:ok, Map.new(rows, &row_pair/1)}
      {:error, _} = error -> error
    end
  end

  @doc "Returns a table's DDL, or an empty string when the server returns no row."
  @spec get_create_table_string(GenServer.server(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def get_create_table_string(session, table_name, opts \\ [])
      when is_binary(table_name) and is_list(opts) do
    as_serde = Keyword.get(opts, :as_serde, false)

    result =
      case catalog_backend(opts) do
        :catalog -> execute_catalog(session, {:get_create_table_string, table_name, as_serde})
        :sql -> execute_sql(session, build_show_create_table_sql(table_name, as_serde))
      end

    case result do
      {:ok, []} -> {:ok, ""}
      {:ok, [row | _]} -> {:ok, first_row_value(row)}
      {:error, _} = error -> error
    end
  end

  @spec truncate_table(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def truncate_table(session, table_name, opts \\ [])
      when is_binary(table_name) and is_list(opts) do
    case catalog_backend(opts) do
      :catalog -> execute_void(session, {:truncate_table, table_name})
      :sql -> execute_sql_void(session, "TRUNCATE TABLE " <> quote_qualified_name(table_name))
    end
  end

  @spec analyze_table(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def analyze_table(session, table_name, opts \\ [])
      when is_binary(table_name) and is_list(opts) do
    no_scan = Keyword.get(opts, :no_scan, false)

    case catalog_backend(opts) do
      :catalog -> execute_void(session, {:analyze_table, table_name, no_scan})
      :sql -> execute_sql_void(session, build_analyze_table_sql(table_name, no_scan))
    end
  end

  @spec alter_table(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def alter_table(session, table_name, opts) when is_binary(table_name) and is_list(opts) do
    sql = build_alter_table_sql(table_name, opts)
    execute_sql_void(session, sql)
  end

  # ── Function Management ──

  @spec list_functions(GenServer.server(), String.t() | nil, String.t() | nil) ::
          {:ok, [Function.t()]} | {:error, term()}
  def list_functions(session, db_name \\ nil, pattern \\ nil) do
    with {:ok, db_name} <- resolve_catalog_db_name(session, db_name),
         {:ok, pattern} <- normalize_catalog_pattern(pattern),
         {:ok, rows} <- execute_catalog(session, {:list_functions, db_name, pattern}) do
      {:ok, Enum.map(rows, &parse_function/1)}
    else
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
    with {:ok, db_name} <- normalize_optional_db_name(db_name) do
      execute_scalar(session, {:function_exists, function_name, db_name})
    end
  end

  @spec create_function(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, term()}
  def create_function(session, function_name, class_name, opts \\ [])
      when is_binary(function_name) and is_binary(class_name) do
    sql = build_create_function_sql(function_name, class_name, opts)
    execute_sql_void(session, sql)
  end

  @spec drop_function(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def drop_function(session, function_name, opts \\ []) when is_binary(function_name) do
    sql = build_drop_function_sql(function_name, opts)
    execute_sql_void(session, sql)
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
  def cache_table(session, table_name, opts \\ []) when is_binary(table_name) and is_list(opts) do
    with {:ok, storage_level} <-
           normalize_cache_storage_level(Keyword.get(opts, :storage_level, nil)) do
      execute_void(session, {:cache_table, table_name, storage_level})
    end
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

    with {:ok, normalized_schema} <- normalize_table_schema(session, schema),
         {:ok, normalized_options} <- normalize_table_options(options) do
      plan =
        {:catalog,
         {:create_table, table_name, path, source, description, normalized_schema,
          normalized_options}}

      df = DataFrame.new(session, plan)

      case DataFrame.collect(df) do
        {:ok, _} -> {:ok, df}
        {:error, _} = err -> err
      end
    else
      {:error, _} = err -> err
    end
  end

  @doc """
  Creates an external table from the given path and returns a DataFrame.

  ## Options

    * `:source` — data source format (e.g., `"parquet"`, `"csv"`)
    * `:schema` — DDL schema string
    * `:options` — additional data source options (default: `%{}`)
  """
  @spec create_external_table(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def create_external_table(session, table_name, path, opts \\ [])
      when is_binary(table_name) and is_binary(path) do
    source = Keyword.get(opts, :source, nil)
    schema = Keyword.get(opts, :schema, nil)
    options = Keyword.get(opts, :options, %{})

    with {:ok, normalized_schema} <- normalize_table_schema(session, schema),
         {:ok, normalized_options} <- normalize_table_options(options) do
      plan =
        {:catalog,
         {:create_external_table, table_name, path, source, normalized_schema, normalized_options}}

      df = DataFrame.new(session, plan)

      case DataFrame.collect(df) do
        {:ok, _} -> {:ok, df}
        {:error, _} = err -> err
      end
    else
      {:error, _} = err -> err
    end
  end

  # ── Private Helpers ──

  defp execute_catalog(session, cat_plan) do
    df = DataFrame.new(session, {:catalog, cat_plan})
    DataFrame.collect(df)
  end

  defp execute_scalar(session, cat_plan) do
    case execute_catalog(session, cat_plan) do
      {:ok, [row]} when is_map(row) and map_size(row) == 1 ->
        [{_key, value}] = Map.to_list(row)
        {:ok, value}

      {:ok, [row]} when is_map(row) ->
        # Multi-column result — cannot safely pick a scalar
        {:error, {:unexpected_columns, Map.keys(row)}}

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

  defp execute_sql_void(session, sql) when is_binary(sql) do
    df = SparkEx.sql(session, sql)

    case DataFrame.collect(df) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  defp execute_sql(session, sql) when is_binary(sql),
    do: SparkEx.sql(session, sql) |> DataFrame.collect()

  defp catalog_backend(opts) do
    allowed_options = [
      :backend,
      :if_exists,
      :if_not_exists,
      :purge,
      :cascade,
      :properties,
      :comment,
      :location,
      :as_serde,
      :no_scan
    ]

    unknown = Keyword.keys(opts) -- allowed_options

    if unknown != [] do
      raise ArgumentError, "unsupported catalog options: #{inspect(unknown)}"
    end

    case Keyword.get(opts, :backend, :sql) do
      :sql ->
        :sql

      :catalog ->
        :catalog

      backend ->
        raise ArgumentError, "expected :backend to be :sql or :catalog, got: #{inspect(backend)}"
    end
  end

  defp resolve_list_views_db_name(session, nil, pattern, :catalog) when not is_nil(pattern),
    do: current_database(session)

  defp resolve_list_views_db_name(_session, db_name, _pattern, _backend), do: {:ok, db_name}

  defp resolve_catalog_db_name(_session, db_name) when is_binary(db_name), do: {:ok, db_name}
  defp resolve_catalog_db_name(_session, nil), do: {:ok, nil}
  defp resolve_catalog_db_name(_session, db_name), do: {:error, {:invalid_db_name, db_name}}

  defp normalize_catalog_pattern(pattern) when is_nil(pattern) or is_binary(pattern),
    do: {:ok, pattern}

  defp normalize_catalog_pattern(pattern) when is_list(pattern) do
    if Keyword.keyword?(pattern) do
      {:error, {:invalid_options, "expected pattern string, got keyword options"}}
    else
      {:error, {:invalid_pattern, pattern}}
    end
  end

  defp normalize_catalog_pattern(pattern), do: {:error, {:invalid_pattern, pattern}}

  defp normalize_optional_db_name(nil), do: {:ok, nil}
  defp normalize_optional_db_name(db_name) when is_binary(db_name), do: {:ok, db_name}

  defp normalize_optional_db_name(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case {Keyword.fetch(opts, :db_name), Keyword.fetch(opts, :db)} do
        {{:ok, db_name}, :error} when is_binary(db_name) -> {:ok, db_name}
        {:error, {:ok, db_name}} when is_binary(db_name) -> {:ok, db_name}
        {{:ok, db_name}, :error} -> {:error, {:invalid_db_name, db_name}}
        {:error, {:ok, db_name}} -> {:error, {:invalid_db_name, db_name}}
        {{:ok, _}, {:ok, _}} -> {:error, {:invalid_db_name, opts}}
        {:error, :error} -> {:error, {:invalid_db_name, opts}}
      end
    else
      {:error, {:invalid_db_name, opts}}
    end
  end

  defp normalize_optional_db_name(db_name), do: {:error, {:invalid_db_name, db_name}}

  defp validate_create_database_opts(opts) do
    allowed_keys = [:if_not_exists, :comment, :location, :properties, :backend]
    invalid_keys = Keyword.keys(opts) -- allowed_keys

    if invalid_keys != [] do
      {:error,
       {:invalid_options, "unsupported create_database options: #{inspect(invalid_keys)}"}}
    else
      with :ok <- validate_create_database_values(opts),
           :ok <- validate_create_database_backend(opts) do
        validate_create_database_properties(opts)
      end
    end
  end

  defp validate_create_database_values(opts) do
    if_not_exists = Keyword.get(opts, :if_not_exists, false)
    comment = Keyword.get(opts, :comment)
    location = Keyword.get(opts, :location)

    cond do
      not is_boolean(if_not_exists) -> {:error, {:invalid_if_not_exists, if_not_exists}}
      not optional_binary?(comment) -> {:error, {:invalid_comment, comment}}
      not optional_binary?(location) -> {:error, {:invalid_location, location}}
      true -> :ok
    end
  end

  defp validate_create_database_backend(opts) do
    backend = Keyword.get(opts, :backend, :sql)

    cond do
      backend not in [:sql, :catalog] ->
        {:error, {:invalid_backend, backend}}

      backend == :catalog and (not is_nil(opts[:comment]) or not is_nil(opts[:location])) ->
        {:error,
         {:invalid_options, "catalog backend supports :properties, not :comment or :location"}}

      true ->
        :ok
    end
  end

  defp validate_create_database_properties(opts) do
    if valid_properties?(Keyword.get(opts, :properties, %{})) do
      :ok
    else
      {:error, {:invalid_properties, Keyword.get(opts, :properties)}}
    end
  end

  defp optional_binary?(value), do: is_nil(value) or is_binary(value)

  defp resolve_table_lookup(_session, table_name, nil), do: {:ok, {table_name, nil}}

  defp resolve_table_lookup(_session, table_name, db_name)
       when is_binary(table_name) and is_binary(db_name) do
    {:ok, {table_name, db_name}}
  end

  defp normalize_table_schema(_session, nil), do: {:ok, nil}
  defp normalize_table_schema(_session, %Spark.Connect.DataType{} = schema), do: {:ok, schema}

  defp normalize_table_schema(session, schema) when is_binary(schema) do
    case SparkEx.Types.parse_ddl_type(schema) do
      {:ok, parsed} ->
        {:ok, parsed}

      :error ->
        case SparkEx.Session.analyze_ddl_parse(session, schema) do
          {:ok, %Spark.Connect.DataType{} = parsed} -> {:ok, parsed}
          {:ok, other} -> {:error, {:invalid_schema_type, other}}
          {:error, _} = err -> err
        end
    end
  end

  defp normalize_table_schema(_session, other),
    do: {:error, {:invalid_schema, "expected DDL string or DataType, got: #{inspect(other)}"}}

  defp normalize_table_options(nil), do: {:ok, %{}}

  defp normalize_table_options(options) when is_list(options) or is_map(options) do
    try do
      {:ok, OptionUtils.stringify_options_reject_nil(options)}
    rescue
      e in ArgumentError ->
        {:error, {:invalid_options, Exception.message(e)}}
    end
  end

  defp normalize_table_options(other) do
    {:error,
     {:invalid_options, "expected :options to be a map or keyword list, got: #{inspect(other)}"}}
  end

  defp normalize_properties(nil), do: %{}
  defp normalize_properties(properties), do: OptionUtils.stringify_options_reject_nil(properties)

  defp valid_properties?(nil), do: true

  defp valid_properties?(properties) when is_map(properties) or is_list(properties) do
    try do
      _ = normalize_properties(properties)
      true
    rescue
      ArgumentError -> false
    end
  end

  defp valid_properties?(_properties), do: false

  # Mirrors pyspark/storagelevel.py: each preset is
  # StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication).
  @storage_level_presets %{
    "NONE" => {false, false, false, false, 1},
    "DISK_ONLY" => {true, false, false, false, 1},
    "DISK_ONLY_2" => {true, false, false, false, 2},
    "DISK_ONLY_3" => {true, false, false, false, 3},
    "MEMORY_ONLY" => {false, true, false, false, 1},
    "MEMORY_ONLY_2" => {false, true, false, false, 2},
    "MEMORY_AND_DISK" => {true, true, false, false, 1},
    "MEMORY_AND_DISK_2" => {true, true, false, false, 2},
    "OFF_HEAP" => {true, true, true, false, 1},
    "MEMORY_AND_DISK_DESER" => {true, true, false, true, 1}
  }

  defp normalize_cache_storage_level(nil), do: {:ok, nil}

  defp normalize_cache_storage_level(%Spark.Connect.StorageLevel{} = storage_level),
    do: {:ok, storage_level}

  defp normalize_cache_storage_level(storage_level) when is_atom(storage_level) do
    normalize_cache_storage_level(storage_level |> Atom.to_string() |> String.upcase())
  end

  defp normalize_cache_storage_level(storage_level) when is_binary(storage_level) do
    case Map.fetch(@storage_level_presets, String.upcase(storage_level)) do
      {:ok, {use_disk, use_memory, use_off_heap, deserialized, replication}} ->
        {:ok,
         %Spark.Connect.StorageLevel{
           use_disk: use_disk,
           use_memory: use_memory,
           use_off_heap: use_off_heap,
           deserialized: deserialized,
           replication: replication
         }}

      :error ->
        {:error, {:invalid_storage_level, storage_level}}
    end
  end

  @doc false
  def build_create_database_sql(db_name, opts) do
    if_not_exists = Keyword.get(opts, :if_not_exists, false)
    comment = Keyword.get(opts, :comment)
    location = Keyword.get(opts, :location)
    properties = Keyword.get(opts, :properties)

    tail_clauses =
      []
      |> then(fn acc ->
        if comment do
          acc ++ ["COMMENT", sql_string(comment)]
        else
          acc
        end
      end)
      |> then(fn acc ->
        if location do
          acc ++ ["LOCATION", sql_string(location)]
        else
          acc
        end
      end)
      |> then(fn acc ->
        if is_nil(properties) do
          acc
        else
          acc ++ ["WITH", "DBPROPERTIES", format_properties(properties)]
        end
      end)

    join_sql(
      ["CREATE", "DATABASE"] ++
        maybe_add([], "IF NOT EXISTS", if_not_exists) ++
        [quote_qualified_name(db_name)] ++ tail_clauses
    )
  end

  @doc false
  def build_drop_database_sql(db_name, opts) do
    if_exists = Keyword.get(opts, :if_exists, false)
    cascade = Keyword.get(opts, :cascade, false)

    clauses =
      []
      |> maybe_add("IF EXISTS", if_exists)

    join_sql(
      ["DROP", "DATABASE"] ++
        clauses ++ [quote_qualified_name(db_name)] ++ maybe_add([], "CASCADE", cascade)
    )
  end

  @doc false
  def build_alter_database_sql(db_name, opts) do
    location = Keyword.get(opts, :set_location)
    properties = Keyword.get(opts, :set_properties)

    case {location, properties} do
      {nil, nil} ->
        raise ArgumentError, "alter_database requires :set_location or :set_properties"

      {loc, nil} ->
        join_sql([
          "ALTER",
          "DATABASE",
          quote_qualified_name(db_name),
          "SET",
          "LOCATION",
          sql_string(loc)
        ])

      {nil, props} when is_map(props) or is_list(props) ->
        props_sql = format_properties(props)

        join_sql([
          "ALTER",
          "DATABASE",
          quote_qualified_name(db_name),
          "SET",
          "DBPROPERTIES",
          props_sql
        ])

      {_loc, _props} ->
        raise ArgumentError,
              "alter_database supports only one of :set_location or :set_properties"
    end
  end

  @doc false
  def build_drop_table_sql(table_name, opts) do
    if_exists = Keyword.get(opts, :if_exists, false)
    purge = Keyword.get(opts, :purge, false)

    clauses =
      []
      |> maybe_add("IF EXISTS", if_exists)

    base = ["DROP", "TABLE"] ++ clauses ++ [quote_qualified_name(table_name)]

    if purge do
      join_sql(base ++ ["PURGE"])
    else
      join_sql(base)
    end
  end

  defp build_drop_view_sql(view_name, if_exists) do
    join_sql(
      ["DROP", "VIEW"] ++
        maybe_add([], "IF EXISTS", if_exists) ++ [quote_qualified_name(view_name)]
    )
  end

  defp build_list_views_sql(db_name, pattern) do
    clauses = ["SHOW", "VIEWS"]
    clauses = if db_name, do: clauses ++ ["IN", quote_qualified_name(db_name)], else: clauses
    clauses = if pattern, do: clauses ++ ["LIKE", sql_string(pattern)], else: clauses
    join_sql(clauses)
  end

  defp build_show_create_table_sql(table_name, as_serde) do
    join_sql(
      ["SHOW", "CREATE", "TABLE", quote_qualified_name(table_name)] ++
        maybe_add([], "AS SERDE", as_serde)
    )
  end

  defp build_analyze_table_sql(table_name, no_scan) do
    join_sql(
      ["ANALYZE", "TABLE", quote_qualified_name(table_name), "COMPUTE", "STATISTICS"] ++
        maybe_add([], "NOSCAN", no_scan)
    )
  end

  @doc false
  def build_alter_table_sql(table_name, opts) do
    rename_to = Keyword.get(opts, :rename_to)
    properties = Keyword.get(opts, :set_properties)

    case {rename_to, properties} do
      {nil, nil} ->
        raise ArgumentError, "alter_table requires :rename_to or :set_properties"

      {new_name, nil} when is_binary(new_name) ->
        join_sql([
          "ALTER",
          "TABLE",
          quote_qualified_name(table_name),
          "RENAME",
          "TO",
          quote_qualified_name(new_name)
        ])

      {nil, props} when is_map(props) or is_list(props) ->
        props_sql = format_properties(props)

        join_sql([
          "ALTER",
          "TABLE",
          quote_qualified_name(table_name),
          "SET",
          "TBLPROPERTIES",
          props_sql
        ])

      {_new_name, _props} ->
        raise ArgumentError, "alter_table supports only one of :rename_to or :set_properties"
    end
  end

  @doc false
  def build_create_function_sql(function_name, class_name, opts) do
    temporary = Keyword.get(opts, :temporary, false)
    if_not_exists = Keyword.get(opts, :if_not_exists, false)
    using_jar = Keyword.get(opts, :using_jar)
    using_jars = Keyword.get(opts, :using_jars)

    if using_jar && using_jars do
      raise ArgumentError, "provide only one of :using_jar or :using_jars"
    end

    using_clause =
      cond do
        is_binary(using_jar) ->
          ["USING", "JAR", sql_string(using_jar)]

        is_list(using_jars) and using_jars != [] ->
          jars = Enum.map(using_jars, &join_sql(["JAR", sql_string(&1)]))
          ["USING", Enum.join(jars, ", ")]

        true ->
          []
      end

    prefix_clauses =
      []
      |> maybe_add("TEMPORARY", temporary)

    suffix_clauses =
      []
      |> maybe_add("IF NOT EXISTS", if_not_exists)

    join_sql(
      ["CREATE"] ++
        prefix_clauses ++
        ["FUNCTION"] ++
        suffix_clauses ++
        [quote_qualified_name(function_name), "AS", sql_string(class_name)] ++
        using_clause
    )
  end

  @doc false
  def build_drop_function_sql(function_name, opts) do
    temporary = Keyword.get(opts, :temporary, false)
    if_exists = Keyword.get(opts, :if_exists, false)

    join_sql(
      ["DROP"] ++
        maybe_add([], "TEMPORARY", temporary) ++
        ["FUNCTION"] ++
        maybe_add([], "IF EXISTS", if_exists) ++ [quote_qualified_name(function_name)]
    )
  end

  defp format_properties(props) when is_list(props) do
    props
    |> Map.new(fn {k, v} -> {to_string(k), v} end)
    |> format_properties()
  end

  defp format_properties(props) when is_map(props) do
    pairs =
      Enum.map_join(props, ", ", fn {k, v} ->
        "#{sql_string(k)}=#{sql_string(v)}"
      end)

    "(" <> pairs <> ")"
  end

  defp sql_string(value) do
    value
    |> to_string()
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
    |> then(&("'" <> &1 <> "'"))
  end

  defp quote_identifier(parts) when is_list(parts) do
    Enum.map_join(parts, ".", &quote_identifier_part/1)
  end

  defp quote_identifier_part(name) when is_binary(name) do
    escaped = String.replace(name, "`", "``")
    "`#{escaped}`"
  end

  defp quote_qualified_name(name) when is_binary(name) do
    name
    |> split_qualified_name()
    |> quote_identifier()
  end

  # Splits a qualified name on `.` while respecting `` `…` `` quoting.
  # Inside backticks, `` `` `` is an escaped backtick (literal) and `.`
  # is part of the segment. Outside backticks, `.` is a separator. If
  # backticks are unbalanced (odd count), all backticks are treated as
  # literal characters and the name is split on bare `.` only — this
  # preserves the natural reading of names that happen to contain a
  # stray backtick, at the cost of disambiguating quoted segments.
  defp split_qualified_name(name) when is_binary(name) do
    if backticks_balanced?(name) do
      do_split_qualified(name, [], "", false)
    else
      String.split(name, ".")
    end
  end

  defp backticks_balanced?(name) do
    name
    |> :binary.matches("`")
    |> length()
    |> rem(2)
    |> Kernel.==(0)
  end

  defp do_split_qualified(<<>>, acc, current, _in_backtick) do
    Enum.reverse([current | acc])
  end

  defp do_split_qualified(<<"``", rest::binary>>, acc, current, true) do
    do_split_qualified(rest, acc, current <> "`", true)
  end

  defp do_split_qualified(<<"`", rest::binary>>, acc, current, in_backtick) do
    do_split_qualified(rest, acc, current, not in_backtick)
  end

  defp do_split_qualified(<<".", rest::binary>>, acc, current, false) do
    do_split_qualified(rest, [current | acc], "", false)
  end

  defp do_split_qualified(<<ch::utf8, rest::binary>>, acc, current, in_backtick) do
    do_split_qualified(rest, acc, current <> <<ch::utf8>>, in_backtick)
  end

  defp maybe_add(list, _value, false), do: list
  defp maybe_add(list, value, true), do: list ++ [value]

  defp join_sql(parts) do
    parts
    |> List.flatten()
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.join(" ")
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

  defp parse_view(%{"name" => _} = row), do: parse_table(row)

  defp parse_view(row) do
    %Table{
      name: row["viewName"] || row["view_name"] || first_row_value(row),
      catalog: row["catalog"],
      namespace: parse_namespace(row["namespace"]),
      description: row["description"],
      table_type: row["tableType"] || "VIEW",
      is_temporary: row["isTemporary"] || false
    }
  end

  defp first_row_value(row) when is_map(row) do
    row
    |> Map.values()
    |> List.first()
  end

  defp row_pair(row) when is_map(row) do
    case {row["key"] || row["Key"], row["value"] || row["Value"]} do
      {key, value} when not is_nil(key) and not is_nil(value) ->
        {to_string(key), to_string(value)}

      _ ->
        case Map.values(row) do
          [key, value] -> {to_string(key), to_string(value)}
          values -> raise ArgumentError, "expected a two-column result, got: #{inspect(values)}"
        end
    end
  end

  defp parse_namespace(nil), do: nil
  defp parse_namespace(ns) when is_list(ns), do: ns
  defp parse_namespace(ns) when is_binary(ns), do: [ns]
end
