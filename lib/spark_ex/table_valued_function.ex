defmodule SparkEx.TableValuedFunction do
  @moduledoc """
  Table-valued function (TVF) accessor.

  Mirrors PySpark's `spark.tvf` accessor and provides convenience helpers
  for common TVFs like `explode`, `inline`, and `stack`.
  """

  defstruct [:session]

  @type t :: %__MODULE__{session: GenServer.server()}

  @spec new(GenServer.server()) :: t()
  def new(session), do: %__MODULE__{session: session}

  @doc """
  Calls a table-valued function by name.
  """
  @spec call(t(), String.t(), [SparkEx.Column.t() | term()]) :: SparkEx.DataFrame.t()
  def call(%__MODULE__{} = tvf, name, args \\ []) when is_binary(name) and is_list(args) do
    SparkEx.DataFrame.table_function(tvf.session, name, args)
  end

  @doc """
  Returns a DataFrame from `explode`.
  """
  @spec explode(t(), SparkEx.Column.t() | term()) :: SparkEx.DataFrame.t()
  def explode(%__MODULE__{} = tvf, arg) do
    call(tvf, "explode", [arg])
  end

  @doc """
  Returns a DataFrame from `inline`.
  """
  @spec inline(t(), SparkEx.Column.t() | term()) :: SparkEx.DataFrame.t()
  def inline(%__MODULE__{} = tvf, arg) do
    call(tvf, "inline", [arg])
  end

  @doc """
  Returns a DataFrame from `stack`.

  `num_rows` is the number of rows in the stacked output.
  """
  @spec stack(t(), pos_integer(), [SparkEx.Column.t() | term()]) :: SparkEx.DataFrame.t()
  def stack(%__MODULE__{} = tvf, num_rows, args) when is_integer(num_rows) and is_list(args) do
    call(tvf, "stack", [num_rows | args])
  end
end
