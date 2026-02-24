defmodule SparkEx do
  @moduledoc """
  Elixir client for Apache Spark via the Spark Connect protocol.

  ## Quick Start

      {:ok, session} = SparkEx.connect(url: "sc://localhost:15002")
      {:ok, version} = SparkEx.spark_version(session)
  """

  alias SparkEx.Connect.Channel

  @doc """
  Connects to a Spark Connect endpoint and starts a session process.

  Validates the URI before starting the session. The underlying gRPC
  connection is established lazily — actual connectivity errors will
  surface on the first RPC call (e.g. `spark_version/1`).

  ## Options

  - `:url` — Spark Connect URI (required), e.g. `"sc://localhost:15002"`
  - `:user_id` — user identifier (default: `"spark_ex"`)
  - `:client_type` — client type string (default: auto-generated)
  - `:session_id` — custom session UUID (default: auto-generated)
  - `:allow_arrow_batch_chunking` — allow server-side Arrow chunk splitting (default: `true`)
  - `:preferred_arrow_chunk_size` — preferred chunk size in bytes (default: `nil`)

  URI parameters `use_ssl=true` and `token=<value>` are supported in the connection string
  (e.g. `"sc://host:port/;use_ssl=true;token=abc123"`). Providing a `token` automatically
  enables TLS, matching Spark Connect client behavior.
  """
  @spec connect(keyword()) :: {:ok, pid()} | {:error, term()}
  def connect(opts) do
    url = Keyword.fetch!(opts, :url)
    validate_connect_identity_opts!(opts)

    with {:ok, _connect_opts} <- Channel.parse_uri(url) do
      SparkEx.Session.start_link(opts)
    end
  end

  @doc """
  Clones a session on the server side and returns a new Session process.

  If `new_session_id` is nil, Spark generates a new one.
  """
  @spec clone_session(GenServer.server(), String.t() | nil) :: {:ok, pid()} | {:error, term()}
  def clone_session(session, new_session_id \\ nil) do
    SparkEx.Session.clone(session, new_session_id)
  end

  @doc """
  Returns the Spark version from the connected server.
  """
  @spec spark_version(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def spark_version(session) do
    SparkEx.Session.spark_version(session)
  end

  @doc """
  Creates a DataFrame from a SQL query.

  ## Examples

      df = SparkEx.sql(session, "SELECT 1 AS n")
      df = SparkEx.sql(session, "SELECT * FROM t WHERE id = ?", args: [42])
      df = SparkEx.sql(session, "SELECT * FROM t WHERE id = :id", args: %{id: 42})
  """
  @spec sql(GenServer.server(), String.t(), keyword()) :: SparkEx.DataFrame.t()
  def sql(session, query, opts \\ [])

  def sql(session, query, opts) when is_binary(query) do
    args = Keyword.get(opts, :args, nil)
    validate_sql_args!(args)
    %SparkEx.DataFrame{session: session, plan: {:sql, query, args}}
  end

  def sql(_session, query, _opts) do
    raise ArgumentError, "expected query to be a string, got: #{inspect(query)}"
  end

  @doc """
  Returns a stateful reader builder (PySpark-style `spark.read`).

  ## Examples

      reader = SparkEx.read(session)
      df = reader |> SparkEx.Reader.format("json") |> SparkEx.Reader.load("/data/events.json")
  """
  @spec read(GenServer.server()) :: SparkEx.Reader.t()
  def read(session) do
    SparkEx.Reader.new(session)
  end

  @doc """
  Returns a `SparkEx.StreamReader` builder for creating streaming DataFrames.

  ## Examples

      reader = SparkEx.read_stream(session)
      df = reader |> SparkEx.StreamReader.format("rate") |> SparkEx.StreamReader.load()
  """
  @spec read_stream(GenServer.server()) :: SparkEx.StreamReader.t()
  def read_stream(session) do
    SparkEx.StreamReader.new(session)
  end

  @doc """
  Returns a Table Valued Function accessor (PySpark-style `spark.tvf`).
  """
  @spec tvf(GenServer.server()) :: SparkEx.TableValuedFunction.t()
  def tvf(session) do
    SparkEx.TableValuedFunction.new(session)
  end

  @doc """
  Returns the `SparkEx.UDFRegistration` module for registering UDFs.

  Unlike PySpark's `spark.udf` which returns a session-bound accessor,
  this returns the module directly. The session must still be passed
  explicitly to registration functions:

      SparkEx.UDFRegistration.register_java(session, "fn_name", "com.example.Fn")
  """
  @spec udf(GenServer.server()) :: module()
  def udf(_session), do: SparkEx.UDFRegistration

  @doc """
  Returns the `SparkEx.UDFRegistration` module for registering UDTFs.

  The session must be passed explicitly to registration functions.
  See `udf/1` for usage pattern.
  """
  @spec udtf(GenServer.server()) :: module()
  def udtf(_session), do: SparkEx.UDFRegistration

  @doc """
  Returns the `SparkEx.UDFRegistration` module for registering data sources.

  The session must be passed explicitly to registration functions.
  See `udf/1` for usage pattern.
  """
  @spec data_source(GenServer.server()) :: module()
  def data_source(_session), do: SparkEx.UDFRegistration

  @doc """
  Registers a progress handler callback for the given session.
  """
  @spec register_progress_handler(GenServer.server(), (map() -> any())) :: :ok
  def register_progress_handler(session, handler) when is_function(handler, 1) do
    SparkEx.Session.register_progress_handler(session, handler)
  end

  @doc """
  Removes a previously registered progress handler for the given session.
  """
  @spec remove_progress_handler(GenServer.server(), (map() -> any())) :: :ok
  def remove_progress_handler(session, handler) when is_function(handler, 1) do
    SparkEx.Session.remove_progress_handler(session, handler)
  end

  @doc """
  Clears all progress handlers registered for the given session.
  """
  @spec clear_progress_handlers(GenServer.server()) :: :ok
  def clear_progress_handlers(session) do
    SparkEx.Session.clear_progress_handlers(session)
  end

  @doc """
  Returns whether the session has been released/stopped.
  """
  @spec is_stopped(GenServer.server()) :: boolean()
  def is_stopped(session) do
    SparkEx.Session.is_stopped(session)
  end

  @doc """
  Attaches a thread-local user context extension (protobuf Any).
  """
  @spec add_threadlocal_user_context_extension(Google.Protobuf.Any.t()) :: :ok
  def add_threadlocal_user_context_extension(extension) do
    SparkEx.UserContextExtensions.add_threadlocal_user_context_extension(extension)
  end

  @doc """
  Attaches a global user context extension (protobuf Any).
  """
  @spec add_global_user_context_extension(Google.Protobuf.Any.t()) :: :ok
  def add_global_user_context_extension(extension) do
    SparkEx.UserContextExtensions.add_global_user_context_extension(extension)
  end

  @doc """
  Removes a user context extension by type URL.
  """
  @spec remove_user_context_extension(String.t()) :: :ok
  def remove_user_context_extension(extension_id) when is_binary(extension_id) do
    SparkEx.UserContextExtensions.remove_user_context_extension(extension_id)
  end

  @doc """
  Clears all global user context extensions and thread-local extensions
  for the current process only. Thread-local extensions in other processes
  are not affected.
  """
  @spec clear_user_context_extensions() :: :ok
  def clear_user_context_extensions do
    SparkEx.UserContextExtensions.clear_user_context_extensions()
  end

  @doc """
  Sets retry policies for Spark Connect operations.
  """
  @spec set_retry_policies(map() | keyword()) :: :ok
  def set_retry_policies(policies) do
    SparkEx.RetryPolicyRegistry.set_policies(policies)
  end

  @doc """
  Returns the current retry policy configuration.
  """
  @spec get_retry_policies() :: map()
  def get_retry_policies do
    SparkEx.RetryPolicyRegistry.get_policies()
  end

  @doc """
  Formats a SQL string with positional or named parameters.
  """
  @spec format_sql(String.t(), list() | map() | nil) :: String.t()
  def format_sql(sql, args \\ nil) when is_binary(sql) do
    SparkEx.SqlFormatter.format(sql, args)
  end

  @doc """
  Creates a DataFrame from a range of integers.

  Supports both signatures:
  - `range(session, end)`
  - `range(session, start, end, step \\ 1, opts \\ [])`

  Backward-compatible options for the 2-arity form:
  - `:start` — range start (default: 0)
  - `:step` — step increment (default: 1)
  - `:num_partitions` — number of partitions (default: nil, server decides)

  ## Examples

      df = SparkEx.range(session, 10)
      df = SparkEx.range(session, 10, 100, 2)
      df = SparkEx.range(session, 100, start: 10, step: 2)
  """
  @spec range(GenServer.server(), integer()) :: SparkEx.DataFrame.t()
  def range(session, end_) when is_integer(end_) do
    build_range_df(session, 0, end_, 1, [])
  end

  @spec range(GenServer.server(), integer(), keyword()) :: SparkEx.DataFrame.t()
  def range(session, end_, opts) when is_integer(end_) and is_list(opts) do
    start = Keyword.get(opts, :start, 0)
    step = Keyword.get(opts, :step, 1)
    build_range_df(session, start, end_, step, opts)
  end

  @spec range(GenServer.server(), integer(), integer()) :: SparkEx.DataFrame.t()
  def range(session, start, end_) when is_integer(start) and is_integer(end_) do
    build_range_df(session, start, end_, 1, [])
  end

  @spec range(GenServer.server(), integer(), integer(), integer()) :: SparkEx.DataFrame.t()
  def range(session, start, end_, step)
      when is_integer(start) and is_integer(end_) and is_integer(step) do
    build_range_df(session, start, end_, step, [])
  end

  @spec range(GenServer.server(), integer(), integer(), integer(), keyword()) ::
          SparkEx.DataFrame.t()
  def range(session, start, end_, step, opts)
      when is_integer(start) and is_integer(end_) and is_integer(step) and is_list(opts) do
    build_range_df(session, start, end_, step, opts)
  end

  @doc """
  Sets Spark configuration key-value pairs.
  """
  @spec config_set(GenServer.server(), [{String.t(), String.t()}]) ::
          :ok | {:error, term()}
  def config_set(session, pairs) do
    SparkEx.Session.config_set(session, pairs)
  end

  @doc """
  Gets Spark configuration values for the given keys.
  """
  @spec config_get(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get(session, keys) do
    SparkEx.Session.config_get(session, keys)
  end

  @doc """
  Gets Spark configuration values with fallback defaults.
  """
  @spec config_get_with_default(GenServer.server(), [{String.t(), String.t()}]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_with_default(session, pairs) do
    SparkEx.Session.config_get_with_default(session, pairs)
  end

  @doc """
  Gets optional Spark configuration values (returns nil for unset keys).
  """
  @spec config_get_option(GenServer.server(), [String.t()]) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_option(session, keys) do
    SparkEx.Session.config_get_option(session, keys)
  end

  @doc """
  Gets all Spark configuration values, optionally filtered by prefix.
  """
  @spec config_get_all(GenServer.server(), String.t() | nil) ::
          {:ok, [{String.t(), String.t() | nil}]} | {:error, term()}
  def config_get_all(session, prefix \\ nil) do
    SparkEx.Session.config_get_all(session, prefix)
  end

  @doc """
  Unsets Spark configuration values.
  """
  @spec config_unset(GenServer.server(), [String.t()]) :: :ok | {:error, term()}
  def config_unset(session, keys) do
    SparkEx.Session.config_unset(session, keys)
  end

  @doc """
  Checks whether configuration keys are modifiable at runtime.
  """
  @spec config_is_modifiable(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  def config_is_modifiable(session, keys) do
    SparkEx.Session.config_is_modifiable(session, keys)
  end

  @doc """
  Checks existence of artifacts on the server.
  """
  @spec artifact_status(GenServer.server(), [String.t()]) ::
          {:ok, %{String.t() => boolean()}} | {:error, term()}
  def artifact_status(session, names) do
    SparkEx.Session.artifact_status(session, names)
  end

  @doc """
  Uploads artifacts to the server.

  Artifacts are provided as a list of `{name, data}` tuples.
  """
  @spec add_artifacts(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_artifacts(session, artifacts) do
    SparkEx.Session.add_artifacts(session, artifacts)
  end

  @doc """
  Uploads JAR artifacts to the server.

  Artifact names are automatically prefixed with `jars/`.
  """
  @spec add_jars(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_jars(session, artifacts) do
    SparkEx.Session.add_jars(session, artifacts)
  end

  @doc """
  Uploads JAR files from local paths.
  """
  @spec add_jars_from_paths(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_jars_from_paths(session, paths) do
    SparkEx.Artifacts.add_jars(session, paths)
  end

  @doc """
  Uploads file artifacts to the server.

  Artifact names are automatically prefixed with `files/`.
  """
  @spec add_files(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_files(session, artifacts) do
    SparkEx.Session.add_files(session, artifacts)
  end

  @doc """
  Uploads files from local paths.
  """
  @spec add_files_from_paths(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_files_from_paths(session, paths) do
    SparkEx.Artifacts.add_files(session, paths)
  end

  @doc """
  Uploads archive artifacts to the server.

  Artifact names are automatically prefixed with `archives/`.
  """
  @spec add_archives(GenServer.server(), [{String.t(), binary()}]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_archives(session, artifacts) do
    SparkEx.Session.add_archives(session, artifacts)
  end

  @doc """
  Uploads archives from local paths.
  """
  @spec add_archives_from_paths(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_archives_from_paths(session, paths) do
    SparkEx.Artifacts.add_archives(session, paths)
  end

  @doc """
  Uploads Python files from local paths.

  Artifact names are automatically prefixed with `pyfiles/`.
  """
  @spec add_pyfiles_from_paths(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_pyfiles_from_paths(session, paths) do
    SparkEx.Artifacts.add_pyfiles(session, paths)
  end

  @doc """
  Copies a local file to the Spark driver filesystem.

  Reads the file at `local_path` and uploads it to the server via `AddArtifacts`.
  """
  @spec copy_from_local_to_fs(GenServer.server(), String.t(), String.t()) ::
          :ok | {:error, term()}
  def copy_from_local_to_fs(session, local_path, dest_path) do
    SparkEx.Session.copy_from_local_to_fs(session, local_path, dest_path)
  end

  @doc """
  Creates a DataFrame from local Elixir data.

  Accepts `Explorer.DataFrame`, a list of maps, or a column-oriented map.
  Small payloads are embedded directly; larger data is uploaded to the server
  cache via `AddArtifacts`.

  ## Options

  - `:schema` — DDL schema string (e.g. `"id INT, name STRING"`)
  - `:cache_threshold` — byte size above which data is cached (default: 4 MB)
  - `:normalize_local_relation_arrow` — when `true` (default), list-heavy payloads are sent
    via SQL/JSON local relation to avoid Spark Connect `LargeList` Arrow incompatibilities

  ## Examples

      # From Explorer.DataFrame
      explorer_df = Explorer.DataFrame.new!(%{"id" => [1, 2], "name" => ["Alice", "Bob"]})
      {:ok, df} = SparkEx.create_dataframe(session, explorer_df)

      # From list of maps
      {:ok, df} = SparkEx.create_dataframe(session, [%{"id" => 1, "name" => "Alice"}])

      # With explicit schema
      {:ok, df} = SparkEx.create_dataframe(session, [%{"id" => 1}], schema: "id INT")
  """
  @spec create_dataframe(GenServer.server(), term(), keyword()) ::
          {:ok, SparkEx.DataFrame.t()} | {:error, term()}
  def create_dataframe(session, data, opts \\ []) do
    SparkEx.Session.create_dataframe(session, data, opts)
  end

  @doc """
  Interrupts all running operations on the session.

  Returns the list of interrupted operation IDs.
  """
  @spec interrupt_all(GenServer.server()) :: {:ok, [String.t()]} | {:error, term()}
  def interrupt_all(session) do
    SparkEx.Session.interrupt_all(session)
  end

  @doc """
  Interrupts operations matching the given tag.

  Tags are set on DataFrames via `SparkEx.DataFrame.tag/2` and propagated
  to the server when the DataFrame is executed.
  """
  @spec interrupt_tag(GenServer.server(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def interrupt_tag(session, tag) when is_binary(tag) do
    SparkEx.Session.interrupt_tag(session, tag)
  end

  @doc """
  Adds a tag to be applied to all subsequent operations in this session.
  """
  @spec add_tag(GenServer.server(), String.t()) :: :ok
  def add_tag(session, tag) when is_binary(tag) do
    SparkEx.Session.add_tag(session, tag)
  end

  @doc """
  Removes a tag from the session.
  """
  @spec remove_tag(GenServer.server(), String.t()) :: :ok
  def remove_tag(session, tag) when is_binary(tag) do
    SparkEx.Session.remove_tag(session, tag)
  end

  @doc """
  Returns all tags set on the session.
  """
  @spec get_tags(GenServer.server()) :: [String.t()]
  def get_tags(session) do
    SparkEx.Session.get_tags(session)
  end

  @doc """
  Clears all tags from the session.
  """
  @spec clear_tags(GenServer.server()) :: :ok
  def clear_tags(session) do
    SparkEx.Session.clear_tags(session)
  end

  @doc """
  Interrupts a specific operation by its server-assigned operation ID.
  """
  @spec interrupt_operation(GenServer.server(), String.t()) ::
          {:ok, [String.t()]} | {:error, term()}
  def interrupt_operation(session, operation_id) when is_binary(operation_id) do
    SparkEx.Session.interrupt_operation(session, operation_id)
  end

  defp build_range_df(session, start, stop, step, opts) do
    num_partitions = Keyword.get(opts, :num_partitions, nil)
    %SparkEx.DataFrame{session: session, plan: {:range, start, stop, step, num_partitions}}
  end

  defp validate_sql_args!(nil), do: :ok
  defp validate_sql_args!(args) when is_list(args), do: :ok
  defp validate_sql_args!(args) when is_map(args) and not is_struct(args), do: :ok

  defp validate_sql_args!(args) do
    raise ArgumentError,
          "expected :args to be a list, map, or nil, got: #{inspect(args)}"
  end

  defp validate_connect_identity_opts!(opts) do
    validate_optional_string_opt!(opts, :user_id)
    validate_optional_string_opt!(opts, :client_type)
    validate_optional_string_opt!(opts, :session_id)
    validate_optional_string_or_nil_opt!(opts, :server_side_session_id)
  end

  defp validate_optional_string_opt!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) -> :ok
      {:ok, value} -> raise ArgumentError, "#{key} must be a string, got: #{inspect(value)}"
      :error -> :ok
    end
  end

  defp validate_optional_string_or_nil_opt!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_nil(value) or is_binary(value) ->
        :ok

      {:ok, value} ->
        raise ArgumentError, "#{key} must be a string or nil, got: #{inspect(value)}"

      :error ->
        :ok
    end
  end
end
