defmodule SparkEx.DataFrame do
  @moduledoc """
  A lazy reference to a Spark DataFrame.

  DataFrame structs hold a session reference and an internal plan representation.
  Transforms build up the plan; actions (`collect/1`, `count/1`, etc.) execute it
  via the Spark Connect server.
  """

  defstruct [:session, :plan]

  @type plan :: term()

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: plan()
        }

  @doc """
  Collects all rows from the DataFrame as a list of maps.

  ## Options

  - `:timeout` — gRPC call timeout in ms (default: 60_000)
  """
  @spec collect(t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def collect(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.execute_collect(df.session, df.plan, opts)
  end

  @doc """
  Returns the row count of the DataFrame.
  """
  @spec count(t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(%__MODULE__{} = df) do
    # Wrap the plan in an aggregate count(*) query
    SparkEx.Session.execute_count(df.session, df.plan)
  end

  @doc """
  Returns up to `n` rows from the DataFrame as a list of maps.
  """
  @spec take(t(), pos_integer(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def take(%__MODULE__{} = df, n, opts \\ []) when is_integer(n) and n > 0 do
    limit_plan = {:limit, df.plan, n}
    SparkEx.Session.execute_collect(df.session, limit_plan, opts)
  end

  @doc """
  Returns the schema of the DataFrame via AnalyzePlan.
  """
  @spec schema(t()) :: {:ok, term()} | {:error, term()}
  def schema(%__MODULE__{} = df) do
    SparkEx.Session.analyze_schema(df.session, df.plan)
  end

  @doc """
  Returns the explain string for the DataFrame's plan.

  Modes: `:simple`, `:extended`, `:codegen`, `:cost`, `:formatted`
  """
  @spec explain(t(), atom()) :: {:ok, String.t()} | {:error, term()}
  def explain(%__MODULE__{} = df, mode \\ :simple) do
    SparkEx.Session.analyze_explain(df.session, df.plan, mode)
  end
end
