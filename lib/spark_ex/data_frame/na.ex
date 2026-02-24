defmodule SparkEx.DataFrame.NA do
  @moduledoc """
  Null-value handling sub-API for DataFrames.

  Provides `fill/2`, `drop/1`, and `replace/3` operations that return
  new lazy DataFrames with the corresponding NA plan tuples.

  Accessed via `SparkEx.DataFrame.fillna/2`, `SparkEx.DataFrame.dropna/1`,
  `SparkEx.DataFrame.replace/3`, or directly.
  """

  alias SparkEx.{Column, DataFrame}

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

  def fill(%DataFrame{} = df, value, opts) when is_map(value) do
    if Keyword.has_key?(opts, :subset) do
      raise ArgumentError,
            ":subset is not supported when value is a map (map keys already specify columns)"
    end

    if map_size(value) == 0 do
      raise ArgumentError, "value should not be empty"
    end

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

        col when is_binary(col) ->
          [col]

        cols when is_list(cols) ->
          unless Enum.all?(cols, &is_binary/1) do
            raise ArgumentError, "expected subset to be a list of column name strings"
          end

          cols

        other ->
          raise ArgumentError,
                "expected subset to be a column name string or list of column name strings, got: #{inspect(other)}"
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
        nil ->
          []

        col when is_binary(col) ->
          [col]

        cols when is_list(cols) ->
          unless Enum.all?(cols, &is_binary/1) do
            raise ArgumentError, "expected subset to be a list of column name strings"
          end

          cols

        other ->
          raise ArgumentError,
                "expected subset to be a column name string or list of column name strings, got: #{inspect(other)}"
      end

    min_non_nulls =
      cond do
        thresh != nil ->
          unless is_integer(thresh) and thresh >= 0 do
            raise ArgumentError, "expected :thresh to be a non-negative integer"
          end

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

  def replace(%DataFrame{} = df, to_replace, value_or_opts, opts) when is_map(to_replace) do
    {_value, effective_opts} = extract_replace_value_and_opts(value_or_opts, opts)
    cols = normalize_subset(Keyword.get(effective_opts, :subset, nil))
    replacements = Enum.map(to_replace, fn {old, new} -> {old, new} end)
    validate_replace_types!(replacements)
    build_replace_df(df, cols, replacements)
  end

  def replace(%DataFrame{} = df, to_replace, value_or_opts, opts) when is_list(to_replace) do
    {value, effective_opts} = extract_replace_value_and_opts(value_or_opts, opts)
    cols = normalize_subset(Keyword.get(effective_opts, :subset, nil))

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
    validate_replace_types!(replacements)
    build_replace_df(df, cols, replacements)
  end

  def replace(%DataFrame{} = df, to_replace, value_or_opts, opts) do
    {value, effective_opts} = extract_replace_value_and_opts(value_or_opts, opts)
    cols = normalize_subset(Keyword.get(effective_opts, :subset, nil))
    validate_replace_types!([{to_replace, value}])
    build_replace_df(df, cols, [{to_replace, value}])
  end

  defp normalize_subset(nil), do: []
  defp normalize_subset(col) when is_binary(col), do: [col]

  defp normalize_subset(cols) when is_list(cols) do
    unless Enum.all?(cols, &is_binary/1) do
      raise ArgumentError, "expected subset to be a list of column name strings"
    end

    cols
  end

  defp extract_replace_value_and_opts(value_or_opts, opts) do
    if opts == [] and is_list(value_or_opts) and Keyword.keyword?(value_or_opts) do
      {nil, value_or_opts}
    else
      {value_or_opts, opts}
    end
  end

  defp build_replace_df(%DataFrame{} = df, [], replacements) do
    %DataFrame{df | plan: {:na_replace, df.plan, [], replacements}}
  end

  defp build_replace_df(%DataFrame{} = df, cols, replacements) do
    Enum.reduce(cols, df, fn col_name, acc_df ->
      DataFrame.with_column(acc_df, col_name, subset_replace_expr(col_name, replacements))
    end)
  end

  defp subset_replace_expr(col_name, replacements) do
    base_expr = {:col, col_name}

    args =
      replacements
      |> Enum.flat_map(fn {old, new} ->
        [{:fn, "<=>", [base_expr, {:lit, old}], false}, {:lit, new}]
      end)
      |> Kernel.++([base_expr])

    %Column{expr: {:fn, "when", args, false}}
  end

  defp validate_fill_values!(values) do
    Enum.each(values, fn v ->
      unless is_number(v) or is_binary(v) or is_boolean(v) do
        raise ArgumentError,
              "expected fill values to be numbers, strings, or booleans, got: #{inspect(v)}"
      end
    end)
  end

  defp validate_replace_types!(replacements) do
    all_values =
      Enum.flat_map(replacements, fn {old, new} ->
        [old | if(is_nil(new), do: [], else: [new])]
      end)

    unless all_values == [] do
      type_family = replace_type_family(hd(all_values))

      unless Enum.all?(all_values, fn v -> replace_type_family(v) == type_family end) do
        raise ArgumentError,
              "mixed type replacements are not supported; all values must be the same type family (bool, numeric, or string)"
      end
    end
  end

  defp replace_type_family(v) when is_boolean(v), do: :bool
  defp replace_type_family(v) when is_number(v), do: :numeric
  defp replace_type_family(v) when is_binary(v), do: :string
  defp replace_type_family(nil), do: nil

  defp replace_type_family(v),
    do: raise(ArgumentError, "unsupported replace value type: #{inspect(v)}")
end
