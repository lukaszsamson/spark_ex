defmodule SparkEx.GroupedData do
  @moduledoc """
  Represents a grouped DataFrame, created by `SparkEx.DataFrame.group_by/2`.

  Aggregation functions like `agg/2` can be applied to produce a new DataFrame.
  Convenience methods `count/1`, `min/2`, `max/2`, `sum/2`, `avg/2`, `mean/2`
  apply common aggregations. `pivot/3` enables pivot-style aggregation.

  Note: convenience numeric aggregation methods (`sum/1`, `avg/1`, `min/1`, `max/1`, `mean/1`)
  called without explicit columns make an eager schema RPC to discover numeric columns.
  The schema is cached per process so repeated calls on the same GroupedData avoid
  redundant RPCs. Pass explicit column names to skip the schema lookup entirely.
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
    :pivot_values,
    :cached_schema
  ]

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: term(),
          grouping_exprs: [Column.expr()],
          group_type: atom(),
          grouping_sets: [[Column.expr()]] | nil,
          pivot_col: Column.expr() | nil,
          pivot_values: [term()] | nil,
          cached_schema: term() | nil
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
  @spec agg(t(), [Column.t()] | map()) :: DataFrame.t()
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

  def agg(%__MODULE__{} = gd, agg_map) when is_map(agg_map) do
    if map_size(agg_map) == 0 do
      raise ArgumentError, "expected at least one aggregate expression"
    end

    agg_cols =
      Enum.map(agg_map, fn {col_name, func_name} ->
        %Column{
          expr:
            {:alias, {:fn, to_string(func_name), [{:col, to_string(col_name)}], false},
             "#{func_name}(#{col_name})"}
        }
      end)

    agg(gd, agg_cols)
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
    @spec unquote(name)(t(), Column.t() | String.t() | [Column.t() | String.t()]) :: DataFrame.t()
    def unquote(name)(gd, cols \\ [])

    if name == :count do
      def unquote(name)(%__MODULE__{} = gd, cols) do
        agg_exprs =
          case cols do
            [] ->
              [%Column{expr: {:alias, {:fn, "count", [{:lit, 1}], false}, "count"}}]

            col when is_binary(col) ->
              [%Column{expr: {:fn, "count", [{:col, col}], false}}]

            %Column{} = c ->
              [%Column{expr: {:fn, "count", [c.expr], false}}]

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
    {agg_names, numeric_names} =
      case cols do
        [] ->
          case fetch_schema_cached(gd) do
            {:ok, schema} ->
              names = numeric_column_names(schema)
              {names, names}

            {:error, reason} ->
              raise ArgumentError, "failed to fetch schema: #{inspect(reason)}"
          end

        col when is_binary(col) ->
          {[col], nil}

        %Column{expr: {:col, name}} when is_binary(name) ->
          {[name], nil}

        %Column{} ->
          raise ArgumentError, "expected column names when aggregating numeric columns"

        cols when is_list(cols) ->
          {normalize_string_columns(cols), nil}

        other ->
          raise ArgumentError,
                "expected columns as string or list of strings, got: #{inspect(other)}"
      end

    if is_list(numeric_names) do
      invalid = Enum.reject(agg_names, &(&1 in numeric_names))

      if invalid != [] do
        raise ArgumentError, "expected numeric columns, got: #{inspect(invalid)}"
      end
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

  defp fetch_schema_cached(%__MODULE__{cached_schema: schema}) when not is_nil(schema) do
    {:ok, schema}
  end

  defp fetch_schema_cached(%__MODULE__{session: session, plan: plan}) do
    cache_key = {__MODULE__, :schema_cache, session, plan}

    case Process.get(cache_key) do
      nil ->
        case SparkEx.Session.analyze_schema(session, plan) do
          {:ok, schema} = result ->
            Process.put(cache_key, schema)
            result

          error ->
            error
        end

      cached ->
        {:ok, cached}
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
    if gd.group_type != :groupby do
      raise ArgumentError,
            "pivot is only supported after group_by, not after #{gd.group_type}"
    end

    if gd.pivot_col != nil do
      raise ArgumentError, "repeated pivot is not supported"
    end

    if values != nil and not is_list(values) do
      raise ArgumentError, "pivot values must be a list or nil, got: #{inspect(values)}"
    end

    if is_list(values) do
      Enum.each(values, fn v ->
        unless is_boolean(v) or is_number(v) or is_binary(v) do
          raise ArgumentError,
                "pivot values must be booleans, numbers, or strings, got: #{inspect(v)}"
        end
      end)
    end

    col_expr =
      case pivot_col do
        %Column{expr: e} ->
          e

        name when is_binary(name) ->
          {:col, name}

        other ->
          raise ArgumentError,
                "pivot column must be a string or SparkEx.Column, got: #{inspect(other)}"
      end

    %__MODULE__{gd | pivot_col: col_expr, pivot_values: values}
  end
end
