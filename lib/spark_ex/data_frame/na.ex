defmodule SparkEx.DataFrame.NA do
  @moduledoc """
  Null-value handling sub-API for DataFrames.

  Provides `fill/2`, `drop/1`, and `replace/3` operations that return
  new lazy DataFrames with the corresponding NA plan tuples.

  Accessed via `SparkEx.DataFrame.fillna/2`, `SparkEx.DataFrame.dropna/1`,
  `SparkEx.DataFrame.replace/3`, or directly.
  """

  alias SparkEx.DataFrame

  @doc """
  Fills null values with the given replacement.

  ## Parameters

  - `value` — scalar (int, float, string, bool) to fill all null values,
    or a map `%{"column_name" => replacement_value}` for column-specific fills.
  - `opts` — keyword options:
    - `:subset` — list of column names to restrict the fill to.

  ## Examples

      DataFrame.NA.fill(df, 0)
      DataFrame.NA.fill(df, %{"age" => 0, "name" => "unknown"})
      DataFrame.NA.fill(df, 0, subset: ["age", "salary"])
  """
  @spec fill(DataFrame.t(), term(), keyword()) :: DataFrame.t()
  def fill(df, value, opts \\ [])

  def fill(%DataFrame{} = df, value, _opts) when is_map(value) do
    cols = Map.keys(value)
    values = Map.values(value)

    unless Enum.all?(cols, &is_binary/1) do
      raise ArgumentError, "expected map keys to be column name strings"
    end

    validate_fill_values!(values)
    %DataFrame{df | plan: {:na_fill, df.plan, cols, values}}
  end

  def fill(%DataFrame{} = df, value, opts)
      when is_number(value) or is_binary(value) or is_boolean(value) do
    subset = Keyword.get(opts, :subset, nil)

    cols =
      case subset do
        nil ->
          []

        cols when is_list(cols) ->
          unless Enum.all?(cols, &is_binary/1) do
            raise ArgumentError, "expected subset to be a list of column name strings"
          end

          cols
      end

    # PySpark encodes a single literal value regardless of subset size
    %DataFrame{df | plan: {:na_fill, df.plan, cols, [value]}}
  end

  def fill(%DataFrame{}, value, _opts) do
    raise ArgumentError,
          "expected fill value to be a number, string, boolean, or map, got: #{inspect(value)}"
  end

  @doc """
  Drops rows containing null values.

  ## Options

  - `:how` — `:any` (default) drops rows with any null; `:all` drops rows where all values are null.
  - `:thresh` — minimum number of non-null values required to keep a row.
    Overrides `:how` when provided.
  - `:subset` — list of column names to consider.

  ## Examples

      DataFrame.NA.drop(df)
      DataFrame.NA.drop(df, how: :all)
      DataFrame.NA.drop(df, thresh: 2, subset: ["age", "name"])
  """
  @spec drop(DataFrame.t(), keyword()) :: DataFrame.t()
  def drop(%DataFrame{} = df, opts \\ []) do
    how = Keyword.get(opts, :how, :any)
    thresh = Keyword.get(opts, :thresh, nil)
    subset = Keyword.get(opts, :subset, nil)

    cols =
      case subset do
        nil -> []
        cols when is_list(cols) -> cols
      end

    min_non_nulls =
      cond do
        thresh != nil ->
          thresh

        how == :all ->
          1

        how == :any ->
          nil

        true ->
          raise ArgumentError, "expected :how to be :any or :all, got: #{inspect(how)}"
      end

    %DataFrame{df | plan: {:na_drop, df.plan, cols, min_non_nulls}}
  end

  @doc """
  Replaces values in the DataFrame.

  ## Forms

  - `replace(df, %{old => new, ...})` — replacement map
  - `replace(df, old_value, new_value)` — single replacement
  - `replace(df, [old1, old2], [new1, new2])` — parallel lists

  ## Options

  - `:subset` — list of column names to restrict replacements to.

  ## Examples

      DataFrame.NA.replace(df, %{0 => 100, -1 => 0})
      DataFrame.NA.replace(df, "N/A", nil)
      DataFrame.NA.replace(df, [1, 2], [10, 20], subset: ["score"])
  """
  @spec replace(DataFrame.t(), term(), term(), keyword()) :: DataFrame.t()
  def replace(df, to_replace, value \\ nil, opts \\ [])

  def replace(%DataFrame{} = df, to_replace, _value, opts) when is_map(to_replace) do
    subset = Keyword.get(opts, :subset, nil)
    cols = if subset, do: subset, else: []
    replacements = Enum.map(to_replace, fn {old, new} -> {old, new} end)
    %DataFrame{df | plan: {:na_replace, df.plan, cols, replacements}}
  end

  def replace(%DataFrame{} = df, to_replace, value, opts) when is_list(to_replace) do
    subset = Keyword.get(opts, :subset, nil)
    cols = if subset, do: subset, else: []

    values =
      cond do
        is_list(value) ->
          if length(to_replace) != length(value) do
            raise ArgumentError, "to_replace and value lists must have the same length"
          end

          value

        true ->
          List.duplicate(value, length(to_replace))
      end

    replacements = Enum.zip(to_replace, values)
    %DataFrame{df | plan: {:na_replace, df.plan, cols, replacements}}
  end

  def replace(%DataFrame{} = df, to_replace, value, opts) do
    subset = Keyword.get(opts, :subset, nil)
    cols = if subset, do: subset, else: []
    %DataFrame{df | plan: {:na_replace, df.plan, cols, [{to_replace, value}]}}
  end

  defp validate_fill_values!(values) do
    Enum.each(values, fn v ->
      unless is_number(v) or is_binary(v) or is_boolean(v) do
        raise ArgumentError,
              "expected fill values to be numbers, strings, or booleans, got: #{inspect(v)}"
      end
    end)
  end
end
