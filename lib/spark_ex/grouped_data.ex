defmodule SparkEx.GroupedData do
  @moduledoc """
  Represents a grouped DataFrame, created by `SparkEx.DataFrame.group_by/2`.

  Aggregation functions like `agg/2` can be applied to produce a new DataFrame.
  Convenience methods `count/1`, `min/2`, `max/2`, `sum/2`, `avg/2`, `mean/2`
  apply common aggregations. `pivot/3` enables pivot-style aggregation.
  """

  alias SparkEx.Column
  alias SparkEx.DataFrame

  defstruct [
    :session,
    :plan,
    :grouping_exprs,
    :group_type,
    :grouping_sets,
    :pivot_col,
    :pivot_values
  ]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: term(),
          grouping_exprs: [Column.expr()],
          group_type: atom(),
          grouping_sets: [[Column.expr()]] | nil,
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
        plan =
          case gd.group_type do
            :grouping_sets ->
              {:aggregate, gd.plan, :grouping_sets, gd.grouping_exprs, agg_exprs,
               gd.grouping_sets}

            group_type ->
              {:aggregate, gd.plan, group_type, gd.grouping_exprs, agg_exprs}
          end

        %DataFrame{session: gd.session, plan: plan}

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
    def unquote(name)(gd, cols \\ [])

    if name == :count do
      def unquote(name)(%__MODULE__{} = gd, cols) do
        agg_exprs =
          case cols do
            [] ->
              [%Column{expr: {:alias, {:fn, "count", [{:lit, 1}], false}, "count"}}]

            cols ->
              Enum.map(cols, fn
                %Column{} = c ->
                  %Column{expr: {:fn, "count", [c.expr], false}}

                name when is_binary(name) ->
                  %Column{expr: {:fn, "count", [{:col, name}], false}}
              end)
          end

        agg(gd, agg_exprs)
      end
    else
      def unquote(name)(%__MODULE__{} = gd, cols) do
        numeric_agg(gd, unquote(spark_fn), cols)
      end
    end
  end

  defp numeric_agg(%__MODULE__{} = gd, spark_fn, cols) do
    schema = fetch_schema!(gd.session, gd.plan)
    numeric_names = numeric_column_names(schema)

    agg_names =
      case cols do
        [] ->
          numeric_names

        cols when is_list(cols) ->
          normalize_string_columns(cols)
      end

    invalid = Enum.reject(agg_names, &(&1 in numeric_names))

    if invalid != [] do
      raise ArgumentError, "expected numeric columns, got: #{inspect(invalid)}"
    end

    if agg_names == [] do
      raise ArgumentError, "expected at least one numeric column"
    end

    agg_exprs =
      Enum.map(agg_names, fn name ->
        %Column{expr: {:fn, spark_fn, [{:col, name}], false}}
      end)

    agg(gd, agg_exprs)
  end

  defp normalize_string_columns(cols) do
    Enum.map(cols, fn
      %Column{expr: {:col, name}} ->
        name

      %Column{} ->
        raise ArgumentError, "expected column names when aggregating numeric columns"

      name when is_binary(name) ->
        name
    end)
  end

  defp fetch_schema!(session, plan) do
    case SparkEx.Session.analyze_schema(session, plan) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "failed to fetch schema: #{inspect(reason)}"
    end
  end

  defp numeric_column_names(%Spark.Connect.DataType{kind: {:struct, struct}}) do
    struct.fields
    |> Enum.filter(fn %Spark.Connect.DataType.StructField{data_type: dt} -> numeric_type?(dt) end)
    |> Enum.map(& &1.name)
  end

  defp numeric_column_names(_), do: []

  defp numeric_type?(%Spark.Connect.DataType{kind: {tag, _}}) do
    tag in [:byte, :short, :integer, :long, :float, :double, :decimal]
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
