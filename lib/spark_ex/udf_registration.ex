defmodule SparkEx.UDFRegistration do
  @moduledoc """
  Registers user-defined functions (UDFs), table functions (UDTFs),
  and data sources with a Spark session.

  Supports Java UDF registration via `register_java/4`, UDTF registration
  via `register_udtf/4`, and data source registration via `register_data_source/4`.
  Registered functions become available for SQL queries and DataFrame operations.
  """

  @doc """
  Registers a Java UDF class by fully qualified class name.

  ## Parameters

  - `session` — the SparkEx session (GenServer reference).
  - `name` — the SQL function name to register.
  - `class_name` — fully qualified Java class name (e.g. `"com.example.MyUDF"`).
  - `opts` — keyword options:
    - `:return_type` — optional Spark DataType protobuf struct. If omitted,
      the server uses reflection to infer the return type.
    - `:aggregate` — boolean, set `true` for aggregate UDFs (UDAFs). Default: `false`.

  ## Examples

      SparkEx.UDFRegistration.register_java(session, "my_upper", "com.example.UpperUDF")

      SparkEx.UDFRegistration.register_java(session, "my_sum", "com.example.SumUDAF",
        aggregate: true
      )
  """
  @spec register_java(
          GenServer.server(),
          String.t(),
          String.t(),
          SparkEx.Types.data_type_proto() | String.t() | keyword()
        ) ::
          :ok | {:error, term()}
  def register_java(session, name, class_name, opts \\ [])

  def register_java(session, name, class_name, %Spark.Connect.DataType{} = return_type)
      when is_binary(name) and is_binary(class_name) do
    register_java(session, name, class_name, return_type: return_type)
  end

  def register_java(session, name, class_name, return_type)
      when is_binary(name) and is_binary(class_name) and is_binary(return_type) do
    register_java(session, name, class_name, return_type: return_type)
  end

  def register_java(session, name, class_name, opts)
      when is_binary(name) and is_binary(class_name) and is_list(opts) do
    return_type = Keyword.get(opts, :return_type, nil)
    aggregate = Keyword.get(opts, :aggregate, false)

    with {:ok, normalized_return_type} <- normalize_return_type(session, return_type),
         {:ok, normalized_aggregate} <- normalize_aggregate(aggregate) do
      command =
        {:register_java_udf, name, class_name, normalized_return_type, normalized_aggregate}

      SparkEx.Session.execute_command(session, command)
    end
  end

  @doc """
  Registers a UDTF (User-Defined Table Function).

  UDTFs return tables instead of scalar values. The UDTF is registered using
  the `register_table_function` command with the `CommonInlineUserDefinedTableFunction`
  protocol.

  Both `:python_ver` and `:eval_type` must match the runtime that
  produced the bytes in `python_command` — there is no portable way to
  derive them from Elixir, so the caller is responsible for supplying
  them. There is no default.

  ## Parameters

  - `session` — the SparkEx session (GenServer reference).
  - `name` — the SQL function name to register.
  - `python_command` — serialized Python UDTF command bytes.
  - `opts` — keyword options:
    - `:return_type` — optional Spark DataType protobuf struct for the UDTF output schema.
    - `:eval_type` — integer `PythonEvalType` constant matching the
      worker that produced `python_command` (e.g. `300` /
      `SQL_TABLE_UDF`). Required.
    - `:python_ver` — Python version string of the worker that produced
      `python_command` (e.g. `"3.11"`). Required.
    - `:deterministic` — boolean (default: `true`).

  ## Examples

      SparkEx.UDFRegistration.register_udtf(session, "my_udtf", python_bytes,
        return_type: return_schema,
        eval_type: 300,
        python_ver: "3.11"
      )
  """
  @spec register_udtf(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def register_udtf(session, name, python_command, opts \\ [])
      when is_binary(name) and is_binary(python_command) do
    validate_python_command!(python_command, :register_udtf)

    return_type = Keyword.get(opts, :return_type, nil)
    eval_type = require_opt!(opts, :eval_type, :register_udtf)
    python_ver = require_opt!(opts, :python_ver, :register_udtf)
    deterministic = Keyword.get(opts, :deterministic, true)

    with {:ok, normalized_return_type} <- normalize_return_type(session, return_type),
         {:ok, normalized_eval_type} <- normalize_eval_type(eval_type),
         {:ok, normalized_python_ver} <- normalize_python_ver(python_ver),
         {:ok, normalized_deterministic} <- normalize_deterministic(deterministic) do
      command =
        {:register_udtf, name, python_command, normalized_return_type, normalized_eval_type,
         normalized_python_ver, normalized_deterministic}

      SparkEx.Session.execute_command(session, command)
    end
  end

  @doc """
  Registers a custom data source.

  The data source is registered using the `register_data_source` command with the
  `CommonInlineUserDefinedDataSource` protocol.

  `:python_ver` must match the runtime that produced the bytes in
  `python_command` — there is no portable way to derive it from Elixir,
  so the caller is responsible for supplying it. There is no default.

  ## Parameters

  - `session` — the SparkEx session (GenServer reference).
  - `name` — the data source name.
  - `python_command` — serialized Python data source command bytes.
  - `opts` — keyword options:
    - `:python_ver` — Python version string of the worker that produced
      `python_command` (e.g. `"3.11"`). Required.
  """
  @spec register_data_source(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def register_data_source(session, name, python_command, opts \\ [])
      when is_binary(name) and is_binary(python_command) do
    validate_python_command!(python_command, :register_data_source)
    python_ver = require_opt!(opts, :python_ver, :register_data_source)

    with {:ok, normalized_python_ver} <- normalize_python_ver(python_ver) do
      command = {:register_data_source, name, python_command, normalized_python_ver}
      SparkEx.Session.execute_command(session, command)
    end
  end

  defp validate_python_command!(<<>>, fun) do
    raise ArgumentError,
          "#{fun}/4 requires non-empty python_command bytes; received an empty binary"
  end

  defp validate_python_command!(bytes, _fun) when is_binary(bytes), do: :ok

  defp normalize_return_type(_session, nil), do: {:ok, nil}

  defp normalize_return_type(_session, %Spark.Connect.DataType{} = return_type),
    do: {:ok, return_type}

  defp normalize_return_type(session, return_type) when is_binary(return_type) do
    case SparkEx.Session.analyze_ddl_parse(session, return_type) do
      {:ok, %Spark.Connect.DataType{} = parsed} -> {:ok, parsed}
      {:ok, other} -> {:error, {:invalid_return_type, other}}
      {:error, _} = err -> err
    end
  end

  defp normalize_return_type(_session, other),
    do: {:error, {:invalid_return_type, other}}

  defp normalize_python_ver(python_ver) when is_binary(python_ver), do: {:ok, python_ver}
  defp normalize_python_ver(other), do: {:error, {:invalid_python_ver, other}}

  defp normalize_eval_type(eval_type) when is_integer(eval_type), do: {:ok, eval_type}
  defp normalize_eval_type(other), do: {:error, {:invalid_eval_type, other}}

  defp normalize_deterministic(deterministic) when is_boolean(deterministic),
    do: {:ok, deterministic}

  defp normalize_deterministic(other), do: {:error, {:invalid_deterministic, other}}

  defp normalize_aggregate(aggregate) when is_boolean(aggregate), do: {:ok, aggregate}
  defp normalize_aggregate(other), do: {:error, {:invalid_aggregate, other}}

  defp require_opt!(opts, key, fun) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        raise ArgumentError,
              "#{fun}/4 requires the #{inspect(key)} option — there is no portable " <>
                "default because the value depends on the Python runtime that produced " <>
                "the bytes payload"
    end
  end
end
