defmodule SparkEx.GroupedData do
  @moduledoc """
  Represents a grouped DataFrame, created by `SparkEx.DataFrame.group_by/2`.

  Aggregation functions like `agg/2` can be applied to produce a new DataFrame.
  """

  alias SparkEx.Column
  alias SparkEx.DataFrame

  defstruct [:session, :plan, :grouping_exprs]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: term(),
          grouping_exprs: [Column.expr()]
        }

  @doc """
  Applies aggregate expressions to the grouped data, returning a new DataFrame.

  Accepts a list of `SparkEx.Column` structs representing aggregate expressions
  (e.g. `Functions.sum(col("amount"))`, `Functions.count(col("id"))`).

  ## Examples

      import SparkEx.Functions

      df
      |> DataFrame.group_by(["department"])
      |> SparkEx.GroupedData.agg([sum(col("salary")), avg(col("age"))])
  """
  @spec agg(t(), [Column.t()]) :: DataFrame.t()
  def agg(%__MODULE__{} = gd, agg_columns) when is_list(agg_columns) do
    if agg_columns == [] do
      raise ArgumentError, "expected at least one aggregate column"
    end

    unless Enum.all?(agg_columns, &match?(%Column{}, &1)) do
      raise ArgumentError, "expected all aggregate expressions to be SparkEx.Column"
    end

    agg_exprs = Enum.map(agg_columns, fn %Column{} = col -> col.expr end)

    %DataFrame{
      session: gd.session,
      plan: {:aggregate, gd.plan, :groupby, gd.grouping_exprs, agg_exprs}
    }
  end

  def agg(%__MODULE__{}, _other) do
    raise ArgumentError, "expected aggregate expressions as a non-empty list of SparkEx.Column"
  end
end
