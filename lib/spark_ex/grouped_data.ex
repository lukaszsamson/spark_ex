defmodule SparkEx.GroupedData do
  @moduledoc """
  Represents a grouped DataFrame, created by `SparkEx.DataFrame.group_by/2`.

  Aggregation functions like `agg/2` can be applied to produce a new DataFrame.
  Convenience methods `count/1`, `min/2`, `max/2`, `sum/2`, `avg/2`, `mean/2`
  apply common aggregations. `pivot/3` enables pivot-style aggregation.
  """

  alias SparkEx.Column
  alias SparkEx.DataFrame

  defstruct [:session, :plan, :grouping_exprs, :pivot_col, :pivot_values]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: term(),
          grouping_exprs: [Column.expr()],
          pivot_col: Column.expr() | nil,
          pivot_values: [term()] | nil
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

    case gd.pivot_col do
      nil ->
        %DataFrame{
          session: gd.session,
          plan: {:aggregate, gd.plan, :groupby, gd.grouping_exprs, agg_exprs}
        }

      pivot_col ->
        %DataFrame{
          session: gd.session,
          plan:
            {:aggregate, gd.plan, :pivot, gd.grouping_exprs, agg_exprs, pivot_col,
             gd.pivot_values}
        }
    end
  end

  def agg(%__MODULE__{}, _other) do
    raise ArgumentError, "expected aggregate expressions as a non-empty list of SparkEx.Column"
  end

  # ── Convenience aggregation methods ──

  @grouped_agg_shortcuts [
    {:count, "count", "Counts the number of records for each group."},
    {:min, "min", "Computes the minimum value for each group."},
    {:max, "max", "Computes the maximum value for each group."},
    {:sum, "sum", "Computes the sum for each group."},
    {:avg, "avg", "Computes the average for each group."},
    {:mean, "avg", "Computes the mean (alias for avg) for each group."}
  ]

  for {name, spark_fn, doc} <- @grouped_agg_shortcuts do
    @doc doc
    @spec unquote(name)(t(), [Column.t() | String.t()]) :: DataFrame.t()
    def unquote(name)(%__MODULE__{} = gd, cols \\ []) do
      agg_exprs =
        case cols do
          [] ->
            [%Column{expr: {:fn, unquote(spark_fn), [{:star}], false}}]

          cols ->
            Enum.map(cols, fn
              %Column{} = c ->
                %Column{expr: {:fn, unquote(spark_fn), [c.expr], false}}

              name when is_binary(name) ->
                %Column{expr: {:fn, unquote(spark_fn), [{:col, name}], false}}
            end)
        end

      agg(gd, agg_exprs)
    end
  end

  # ── Pivot ──

  @doc """
  Pivots on a column, enabling pivot-style aggregation.

  After calling `pivot/3`, use `agg/2` to specify the aggregation.

  ## Examples

      df
      |> DataFrame.group_by(["year"])
      |> SparkEx.GroupedData.pivot("course", ["dotNET", "Java"])
      |> SparkEx.GroupedData.agg([sum(col("earnings"))])
  """
  @spec pivot(t(), Column.t() | String.t(), [term()] | nil) :: t()
  def pivot(%__MODULE__{} = gd, pivot_col, values \\ nil) do
    col_expr =
      case pivot_col do
        %Column{expr: e} -> e
        name when is_binary(name) -> {:col, name}
      end

    %__MODULE__{gd | pivot_col: col_expr, pivot_values: values}
  end
end
