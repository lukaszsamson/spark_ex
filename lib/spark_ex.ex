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
  """
  @spec connect(keyword()) :: {:ok, pid()} | {:error, term()}
  def connect(opts) do
    url = Keyword.fetch!(opts, :url)

    with {:ok, _connect_opts} <- Channel.parse_uri(url) do
      SparkEx.Session.start_link(opts)
    end
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
  def sql(session, query, opts \\ []) do
    args = Keyword.get(opts, :args, nil)
    %SparkEx.DataFrame{session: session, plan: {:sql, query, args}}
  end

  @doc """
  Creates a DataFrame from a range of integers.

  ## Options

  - `:start` — range start (default: 0)
  - `:step` — step increment (default: 1)
  - `:num_partitions` — number of partitions (default: nil, server decides)

  ## Examples

      df = SparkEx.range(session, 10)
      df = SparkEx.range(session, 100, start: 10, step: 2)
  """
  @spec range(GenServer.server(), integer(), keyword()) :: SparkEx.DataFrame.t()
  def range(session, stop, opts \\ []) do
    start = Keyword.get(opts, :start, 0)
    step = Keyword.get(opts, :step, 1)
    num_partitions = Keyword.get(opts, :num_partitions, nil)
    %SparkEx.DataFrame{session: session, plan: {:range, start, stop, step, num_partitions}}
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
end
