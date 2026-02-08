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
end
