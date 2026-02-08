defmodule SparkEx.UDFRegistration do
  @moduledoc """
  Registers user-defined functions (UDFs) and user-defined table functions (UDTFs)
  with a Spark session.

  Supports Java UDF registration via `register_java/4` and UDTF registration
  via `register_udtf/4`. Registered functions become available for SQL queries
  and DataFrame operations.
  """

  @doc """
  Registers a Java UDF class by fully qualified class name.

  ## Parameters

  - `session` — the SparkEx session (GenServer reference).
  - `name` — the SQL function name to register.
  - `class_name` — fully qualified Java class name (e.g. `"com.example.MyUDF"`).
  - `opts` — keyword options:
    - `:return_type` — optional `Spark.Connect.DataType` struct. If omitted,
      the server uses reflection to infer the return type.
    - `:aggregate` — boolean, set `true` for aggregate UDFs (UDAFs). Default: `false`.

  ## Examples

      SparkEx.UDFRegistration.register_java(session, "my_upper", "com.example.UpperUDF")

      SparkEx.UDFRegistration.register_java(session, "my_sum", "com.example.SumUDAF",
        aggregate: true
      )
  """
  @spec register_java(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, term()}
  def register_java(session, name, class_name, opts \\ [])
      when is_binary(name) and is_binary(class_name) do
    return_type = Keyword.get(opts, :return_type, nil)
    aggregate = Keyword.get(opts, :aggregate, false)
    command = {:register_java_udf, name, class_name, return_type, aggregate}
    SparkEx.Session.execute_command(session, command)
  end

  @doc """
  Registers a UDTF (User-Defined Table Function).

  UDTFs return tables instead of scalar values. The UDTF is registered using
  the `register_table_function` command with the `CommonInlineUserDefinedTableFunction`
  protocol.

  ## Parameters

  - `session` — the SparkEx session (GenServer reference).
  - `name` — the SQL function name to register.
  - `python_command` — serialized Python UDTF command bytes.
  - `opts` — keyword options:
    - `:return_type` — optional `Spark.Connect.DataType` struct for the UDTF output schema.
    - `:eval_type` — integer eval type (default: `0`).
    - `:python_ver` — Python version string (default: `"3.11"`).
    - `:deterministic` — boolean (default: `true`).

  ## Examples

      SparkEx.UDFRegistration.register_udtf(session, "my_udtf", python_bytes,
        return_type: return_schema,
        python_ver: "3.11"
      )
  """
  @spec register_udtf(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def register_udtf(session, name, python_command, opts \\ [])
      when is_binary(name) and is_binary(python_command) do
    return_type = Keyword.get(opts, :return_type, nil)
    eval_type = Keyword.get(opts, :eval_type, 0)
    python_ver = Keyword.get(opts, :python_ver, "3.11")
    deterministic = Keyword.get(opts, :deterministic, true)

    command =
      {:register_udtf, name, python_command, return_type, eval_type, python_ver, deterministic}

    SparkEx.Session.execute_command(session, command)
  end
end
