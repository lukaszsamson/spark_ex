defmodule SparkEx.DataFrame.Stat do
  @moduledoc """
  Statistical operations sub-API for DataFrames.

  Provides descriptive statistics, correlation, covariance, crosstab,
  frequency items, approximate quantiles, and stratified sampling.

  Most methods return lazy DataFrames. Scalar-returning methods
  (`corr/4`, `cov/3`, `approx_quantile/4`) execute eagerly.
  """

  alias SparkEx.{Column, DataFrame}
  alias SparkEx.Internal.Random

  # ── Lazy methods (return DataFrame) ──

  @doc """
  Computes basic statistics (count, mean, stddev, min, max) for selected columns.

  If no columns are given, describes all columns.

  ## Examples

      DataFrame.Stat.describe(df)
      DataFrame.Stat.describe(df, ["age", "salary"])
  """
  @spec describe(DataFrame.t(), String.t() | [String.t()]) :: DataFrame.t()
  def describe(df, cols \\ [])

  def describe(%DataFrame{} = df, cols) when is_list(cols) do
    validate_string_list!(cols, "column names")
    DataFrame.update_plan(df, {:stat_describe, df.plan, cols})
  end

  def describe(%DataFrame{} = df, col) when is_binary(col) do
    describe(df, [col])
  end

  @doc """
  Computes specified statistics for numeric and string columns.

  Statistics can include: "count", "mean", "stddev", "min", "max",
  and percentiles like "25%", "50%", "75%".

  ## Examples

      DataFrame.Stat.summary(df)
      DataFrame.Stat.summary(df, ["count", "min", "max"])
  """
  @spec summary(DataFrame.t(), String.t() | [String.t()]) :: DataFrame.t()
  def summary(df, statistics \\ [])

  def summary(%DataFrame{} = df, statistics) when is_list(statistics) do
    validate_string_list!(statistics, "statistics")
    DataFrame.update_plan(df, {:stat_summary, df.plan, statistics})
  end

  def summary(%DataFrame{} = df, stat) when is_binary(stat) do
    summary(df, [stat])
  end

  @doc """
  Computes a contingency table (crosstab) of two columns.

  Returns a DataFrame with the frequency of each combination of values.

  ## Examples

      DataFrame.Stat.crosstab(df, "department", "gender")
  """
  @spec crosstab(DataFrame.t(), String.t(), String.t()) :: DataFrame.t()
  def crosstab(%DataFrame{} = df, col1, col2) when is_binary(col1) and is_binary(col2) do
    DataFrame.update_plan(df, {:stat_crosstab, df.plan, col1, col2})
  end

  @doc """
  Finds all items which have a frequency greater than or equal to `support`.

  ## Examples

      DataFrame.Stat.freq_items(df, ["category", "status"])
      DataFrame.Stat.freq_items(df, ["category"], 0.05)
  """
  @spec freq_items(DataFrame.t(), [String.t()], float() | keyword()) :: DataFrame.t()
  def freq_items(df, cols, support \\ 0.01)

  def freq_items(%DataFrame{} = df, cols, support) when is_list(cols) and is_number(support) do
    validate_string_list!(cols, "column names")
    validate_support!(support)
    DataFrame.update_plan(df, {:stat_freq_items, df.plan, cols, support})
  end

  def freq_items(%DataFrame{} = df, cols, opts) when is_list(cols) and is_list(opts) do
    validate_string_list!(cols, "column names")

    unless Keyword.keyword?(opts) do
      raise ArgumentError, "expected options as keyword list, got: #{inspect(opts)}"
    end

    support = Keyword.get(opts, :support, 0.01)

    unless is_number(support) do
      raise ArgumentError, "support must be a number, got: #{inspect(support)}"
    end

    freq_items(df, cols, support * 1.0)
  end

  @doc """
  Returns a stratified sample of the DataFrame.

  ## Parameters

  - `col` — column name (string or atom) or `Column` used for stratification.
  - `fractions` — map of `%{stratum_value => sampling_fraction}`.
  - `seed` — optional random seed. Auto-generated if not provided; pass an explicit seed for reproducibility.

  ## Examples

      DataFrame.Stat.sample_by(df, "label", %{0 => 0.1, 1 => 0.5})
      DataFrame.Stat.sample_by(df, :label, %{0 => 0.1, 1 => 0.5}, 42)
  """
  @spec sample_by(
          DataFrame.t(),
          Column.t() | String.t() | atom(),
          map(),
          integer() | keyword() | nil
        ) ::
          DataFrame.t()
  def sample_by(%DataFrame{} = df, col, fractions, seed \\ nil) when is_map(fractions) do
    validate_fractions!(fractions)
    col_expr = normalize_col_expr(col)
    frac_list = Enum.map(fractions, fn {k, v} -> {k, v * 1.0} end)
    seed = normalize_seed(seed)
    DataFrame.update_plan(df, {:stat_sample_by, df.plan, col_expr, frac_list, seed})
  end

  # ── Eager methods (return scalar values) ──

  @doc """
  Computes the Pearson correlation coefficient between two columns.

  Returns `{:ok, float}` or `{:error, reason}`.

  ## Examples

      {:ok, r} = DataFrame.Stat.corr(df, "height", "weight")
  """
  @spec corr(DataFrame.t(), String.t(), String.t(), String.t()) ::
          {:ok, float()} | {:error, term()}
  def corr(%DataFrame{} = df, col1, col2, method \\ "pearson")
      when is_binary(col1) and is_binary(col2) and is_binary(method) do
    unless method == "pearson" do
      raise ArgumentError,
            "currently only the Pearson correlation coefficient is supported, got: #{inspect(method)}"
    end

    plan = {:stat_corr, df.plan, col1, col2, method}
    collect_scalar(df.session, plan)
  end

  @doc """
  Computes the sample covariance between two columns.

  Returns `{:ok, float}` or `{:error, reason}`.

  ## Examples

      {:ok, c} = DataFrame.Stat.cov(df, "height", "weight")
  """
  @spec cov(DataFrame.t(), String.t(), String.t()) :: {:ok, float()} | {:error, term()}
  def cov(%DataFrame{} = df, col1, col2) when is_binary(col1) and is_binary(col2) do
    plan = {:stat_cov, df.plan, col1, col2}
    collect_scalar(df.session, plan)
  end

  @doc """
  Computes approximate quantiles for one or more columns.

  Returns `{:ok, [float]}` for a single column or `{:ok, [[float]]}` for multiple.

  ## Examples

      {:ok, quantiles} = DataFrame.Stat.approx_quantile(df, "age", [0.25, 0.5, 0.75], 0.0)
      {:ok, quantiles} = DataFrame.Stat.approx_quantile(df, ["age", "salary"], [0.5], 0.01)
  """
  @spec approx_quantile(
          DataFrame.t(),
          String.t() | [String.t()] | tuple(),
          [float()],
          float()
        ) :: {:ok, [float()] | [[float()]]} | {:error, term()}
  def approx_quantile(%DataFrame{} = df, col, probabilities, relative_error)
      when is_list(probabilities) do
    {cols, single?} =
      case col do
        c when is_binary(c) ->
          {[c], true}

        cs when is_list(cs) ->
          {cs, false}

        cs when is_tuple(cs) ->
          {Tuple.to_list(cs), false}

        _ ->
          raise ArgumentError,
                "col must be a string, list of strings, or tuple of strings, got: #{inspect(col)}"
      end

    unless Enum.all?(cols, &is_binary/1) do
      raise ArgumentError, "column names must all be strings"
    end

    validate_probabilities!(probabilities)

    unless is_number(relative_error) and relative_error >= 0 do
      raise ArgumentError, "relative_error must be a non-negative number"
    end

    plan = {:stat_approx_quantile, df.plan, cols, probabilities, relative_error / 1}

    case DataFrame.collect(DataFrame.new(df.session, plan)) do
      {:ok, [row]} ->
        # Result is a single row with one column containing a nested array:
        # single col: [[q1, q2, ...]], multi col: [[q1_a, q2_a], [q1_b, q2_b]]
        with {:ok, raw} <- first_column_value(row),
             {:ok, parsed} <- parse_nested_quantile(raw) do
          {:ok, if(single?, do: hd(parsed), else: parsed)}
        end

      {:error, _} = err ->
        err
    end
  end

  # ── Private helpers ──

  # Extracts the value of the first column from a single-column row map.
  # Equivalent to PySpark's `table[0][0].as_py()` positional access.
  # Returns `{:error, {:unexpected_columns, _}}` for multi-column rows so
  # the caller can surface the divergence rather than silently picking
  # whichever key Erlang's map iterator yields first.
  defp first_column_value(row) when is_map(row) and map_size(row) == 1 do
    [{_key, value}] = Map.to_list(row)
    {:ok, value}
  end

  defp first_column_value(row) when is_map(row) do
    {:error, {:unexpected_columns, Map.keys(row)}}
  end

  defp normalize_col_expr(%Column{expr: e}), do: e
  defp normalize_col_expr(name) when is_binary(name), do: {:col, name}

  defp normalize_col_expr(name) when is_atom(name) and not is_boolean(name) and not is_nil(name),
    do: {:col, Atom.to_string(name)}

  defp normalize_col_expr(other) do
    raise ArgumentError,
          "expected column name (string or atom) or %SparkEx.Column{}, got: #{inspect(other)}"
  end

  # Spark returns approx_quantile as a nested array (array of arrays). The
  # Arrow result decoder yields native lists, so the only shape we accept is
  # a list of lists; anything else is reported as an error tuple instead of
  # being coerced through ad-hoc string parsing.
  defp parse_nested_quantile(v) when is_list(v) do
    if Enum.all?(v, &is_list/1) do
      {:ok, v}
    else
      {:error, {:invalid_quantile_payload, v}}
    end
  end

  defp parse_nested_quantile(v), do: {:error, {:invalid_quantile_payload, v}}

  defp collect_scalar(session, plan) do
    case DataFrame.collect(DataFrame.new(session, plan)) do
      {:ok, [row]} when is_map(row) ->
        first_column_value(row)

      {:ok, []} ->
        {:ok, nil}

      {:ok, rows} when is_list(rows) ->
        {:error, {:unexpected_result, "expected 0 or 1 rows, got #{length(rows)}"}}

      {:error, _} = err ->
        err
    end
  end

  defp validate_fractions!(fractions) do
    Enum.each(fractions, fn {key, v} ->
      unless is_number(key) or is_binary(key) or is_boolean(key) do
        raise ArgumentError,
              "fraction keys must be numbers, strings, or booleans, got: #{inspect(key)}"
      end

      unless is_number(v) and v >= 0.0 and v <= 1.0 do
        raise ArgumentError,
              "each fraction must be in the range [0.0, 1.0], got: #{inspect(v)}"
      end
    end)
  end

  defp validate_probabilities!(probabilities) do
    Enum.each(probabilities, fn p ->
      unless is_number(p) and p >= 0.0 and p <= 1.0 do
        raise ArgumentError,
              "each probability must be a number between 0 and 1, got: #{inspect(p)}"
      end
    end)
  end

  defp validate_string_list!(values, label) do
    unless Enum.all?(values, &is_binary/1) do
      raise ArgumentError, "#{label} must all be strings"
    end
  end

  defp validate_support!(support) when is_number(support) do
    if support < 0.0 or support > 1.0 do
      raise ArgumentError, "support must be between 0 and 1, got: #{inspect(support)}"
    end
  end

  defp normalize_seed(nil), do: Random.long_seed()

  defp normalize_seed(seed) when is_integer(seed), do: seed

  defp normalize_seed(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "expected options as keyword list, got: #{inspect(opts)}"
    end

    case Keyword.get(opts, :seed, nil) do
      nil -> normalize_seed(nil)
      seed when is_integer(seed) -> seed
      other -> raise ArgumentError, "seed must be an integer, got: #{inspect(other)}"
    end
  end

  defp normalize_seed(other),
    do: raise(ArgumentError, "seed must be an integer, got: #{inspect(other)}")
end
