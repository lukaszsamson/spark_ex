defmodule SparkEx.DataFrame.Stat do
  @moduledoc """
  Statistical operations sub-API for DataFrames.

  Provides descriptive statistics, correlation, covariance, crosstab,
  frequency items, approximate quantiles, and stratified sampling.

  Most methods return lazy DataFrames. Scalar-returning methods
  (`corr/4`, `cov/3`, `approx_quantile/4`) execute eagerly.
  """

  alias SparkEx.{Column, DataFrame}

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
    %DataFrame{df | plan: {:stat_describe, df.plan, cols}}
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
    %DataFrame{df | plan: {:stat_summary, df.plan, statistics}}
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
    %DataFrame{df | plan: {:stat_crosstab, df.plan, col1, col2}}
  end

  @doc """
  Finds all items which have a frequency greater than or equal to `support`.

  ## Examples

      DataFrame.Stat.freq_items(df, ["category", "status"])
      DataFrame.Stat.freq_items(df, ["category"], 0.05)
  """
  @spec freq_items(DataFrame.t(), [String.t()], float()) :: DataFrame.t()
  def freq_items(%DataFrame{} = df, cols, support \\ 0.01) when is_list(cols) do
    %DataFrame{df | plan: {:stat_freq_items, df.plan, cols, support}}
  end

  @doc """
  Returns a stratified sample of the DataFrame.

  ## Parameters

  - `col` — column name (string) or `Column` used for stratification.
  - `fractions` — map of `%{stratum_value => sampling_fraction}`.
  - `seed` — optional random seed.

  ## Examples

      DataFrame.Stat.sample_by(df, "label", %{0 => 0.1, 1 => 0.5})
      DataFrame.Stat.sample_by(df, "label", %{0 => 0.1, 1 => 0.5}, 42)
  """
  @spec sample_by(DataFrame.t(), Column.t() | String.t(), map(), integer() | nil) :: DataFrame.t()
  def sample_by(%DataFrame{} = df, col, fractions, seed \\ nil) when is_map(fractions) do
    validate_fractions!(fractions)
    col_expr = normalize_col_expr(col)
    frac_list = Enum.map(fractions, fn {k, v} -> {k, v * 1.0} end)
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
    %DataFrame{df | plan: {:stat_sample_by, df.plan, col_expr, frac_list, seed}}
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

      {:ok, quantiles} = DataFrame.Stat.approx_quantile(df, "age", [0.25, 0.5, 0.75])
      {:ok, quantiles} = DataFrame.Stat.approx_quantile(df, ["age", "salary"], [0.5], 0.01)
  """
  @spec approx_quantile(
          DataFrame.t(),
          String.t() | [String.t()],
          [float()],
          float()
        ) :: {:ok, [float()] | [[float()]]} | {:error, term()}
  def approx_quantile(%DataFrame{} = df, col, probabilities, relative_error \\ 0.0)
      when is_list(probabilities) do
    {cols, single?} =
      case col do
        c when is_binary(c) -> {[c], true}
        cs when is_list(cs) -> {cs, false}
        cs when is_tuple(cs) -> {Tuple.to_list(cs), false}
      end

    unless Enum.all?(cols, &is_binary/1) do
      raise ArgumentError, "column names must all be strings"
    end

    validate_probabilities!(probabilities)

    unless is_number(relative_error) and relative_error >= 0 do
      raise ArgumentError, "relative_error must be a non-negative number"
    end

    plan = {:stat_approx_quantile, df.plan, cols, probabilities, relative_error / 1}

    case DataFrame.collect(%DataFrame{session: df.session, plan: plan}) do
      {:ok, [row]} ->
        # Result is a single row with one column containing a nested array:
        # single col: [[q1, q2, ...]], multi col: [[q1_a, q2_a], [q1_b, q2_b]]
        raw = row |> Map.values() |> hd()
        parsed = parse_nested_quantile(raw)

        {:ok, if(single?, do: hd(parsed), else: parsed)}

      {:error, _} = err ->
        err
    end
  end

  # ── Private helpers ──

  defp normalize_col_expr(%Column{expr: e}), do: e
  defp normalize_col_expr(name) when is_binary(name), do: {:col, name}

  # Spark returns approx_quantile as a nested array (array of arrays).
  # The result decoder may return this as a native list or as a string
  # representation like "[[1.0, 5.0, 10.0]]" or "[[3.0], [30.0]]".
  defp parse_nested_quantile(v) when is_list(v) do
    Enum.map(v, fn
      inner when is_list(inner) -> inner
      inner when is_binary(inner) -> parse_float_list(inner)
      inner -> [inner]
    end)
  end

  defp parse_nested_quantile(v) when is_binary(v) do
    # String like "[[1.0, 5.0, 10.0], [10.0, 50.0, 100.0]]"
    # Strip outer brackets, then split into inner arrays
    inner = v |> String.trim_leading("[") |> String.trim_trailing("]")

    # Split on "], [" to get individual array strings
    inner
    |> String.split(~r/\]\s*,\s*\[/)
    |> Enum.map(&parse_float_list/1)
  end

  defp parse_float_list(s) do
    s
    |> String.replace(~r/[\[\]]/, "")
    |> String.split(",")
    |> Enum.map(fn part ->
      case part |> String.trim() |> Float.parse() do
        {value, _rest} -> value
        :error -> raise ArgumentError, "could not parse float from: #{inspect(String.trim(part))}"
      end
    end)
  end

  defp collect_scalar(session, plan) do
    case DataFrame.collect(%DataFrame{session: session, plan: plan}) do
      {:ok, [row]} when is_map(row) ->
        value = row |> Map.values() |> hd()
        {:ok, value}

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

      unless is_number(v) and v >= 0.0 do
        raise ArgumentError,
              "each fraction must be a non-negative number, got: #{inspect(v)}"
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
end
