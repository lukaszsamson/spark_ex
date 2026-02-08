defmodule SparkEx.Functions do
  @moduledoc """
  Expression constructors for Spark DataFrame operations.

  Provides core constructors (`col/1`, `lit/1`, `expr/1`) and a comprehensive
  set of Spark SQL functions generated from a declarative registry.

  These functions create `SparkEx.Column` structs that can be used in
  DataFrame transforms like `select/2`, `filter/2`, `with_column/3`, etc.

  ## Examples

      import SparkEx.Functions

      df
      |> SparkEx.DataFrame.select([col("name"), col("age")])
      |> SparkEx.DataFrame.filter(col("age") |> SparkEx.Column.gt(lit(18)))
  """

  import Kernel, except: [abs: 1, ceil: 1, floor: 1, round: 1, length: 1, struct: 1, struct: 2]

  alias SparkEx.Column
  require SparkEx.Macros.FunctionGen

  # ── Core constructors (hand-written) ──

  @doc """
  Creates a column reference by name.

  ## Examples

      col("age")
      col("users.name")
  """
  @spec col(String.t()) :: Column.t()
  def col(name) when is_binary(name) do
    %Column{expr: {:col, name}}
  end

  @doc """
  Creates a literal value expression.

  Supports nil, booleans, integers, floats, and strings.

  ## Examples

      lit(42)
      lit("hello")
      lit(true)
  """
  @spec lit(term()) :: Column.t()
  def lit(value) do
    %Column{expr: {:lit, value}}
  end

  @doc """
  Creates an expression from a SQL expression string.

  This is a convenient escape hatch for expressions that are easier
  to write in SQL syntax.

  ## Examples

      expr("age + 1")
      expr("CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END")
  """
  @spec expr(String.t()) :: Column.t()
  def expr(expression) when is_binary(expression) do
    %Column{expr: {:expr, expression}}
  end

  @doc "Creates an unresolved star (*) expression for selecting all columns."
  @spec star() :: Column.t()
  def star do
    %Column{expr: {:star}}
  end

  # ── Sort helpers (hand-written delegates) ──

  @doc "Sort ascending by the given column"
  @spec asc(Column.t()) :: Column.t()
  defdelegate asc(col), to: Column

  @doc "Sort descending by the given column"
  @spec desc(Column.t()) :: Column.t()
  defdelegate desc(col), to: Column

  # ── Generated functions from registry ──

  SparkEx.Macros.FunctionGen.generate_functions()

  # ── Hand-written special functions ──

  @doc """
  Evaluates a list of conditions and returns one of multiple possible result expressions.
  If `otherwise/2` is not used, nil is returned for unmatched conditions.

  Equivalent to `CASE WHEN condition THEN value END` in SQL.

  ## Examples

      import SparkEx.Functions

      when_(col("age") |> Column.lt(13), lit("child"))
      |> otherwise(lit("adult"))
  """
  @spec when_(Column.t(), Column.t() | term()) :: Column.t()
  def when_(%Column{} = condition, %Column{} = value) do
    %Column{expr: {:fn, "when", [condition.expr, value.expr], false}}
  end

  def when_(%Column{} = condition, value) do
    %Column{expr: {:fn, "when", [condition.expr, {:lit, value}], false}}
  end

  @doc """
  Adds a fallback value to a `when_/2` expression chain.

  ## Examples

      when_(col("score") |> Column.gt(90), lit("A"))
      |> otherwise(lit("B"))
  """
  @spec otherwise(Column.t(), Column.t() | term()) :: Column.t()
  def otherwise(%Column{expr: {:fn, "when", args, false}} = _when_col, %Column{} = value) do
    %Column{expr: {:fn, "when", args ++ [value.expr], false}}
  end

  def otherwise(%Column{expr: {:fn, "when", args, false}} = _when_col, value) do
    %Column{expr: {:fn, "when", args ++ [{:lit, value}], false}}
  end

  # ── Higher-order functions (HOF) with lambda support ──

  @doc """
  Transforms each element in an array column using a function.

  The function receives a lambda variable `x` representing each element.

  ## Examples

      transform(col("arr"), fn x -> Column.plus(x, lit(1)) end)
  """
  @spec transform(Column.t() | String.t(), (Column.t() -> Column.t())) :: Column.t()
  def transform(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "transform", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Filters an array column using a predicate function.

  ## Examples

      filter(col("arr"), fn x -> Column.gt(x, lit(0)) end)
  """
  @spec filter(Column.t() | String.t(), (Column.t() -> Column.t())) :: Column.t()
  def filter(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "filter", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Returns true if any element in the array satisfies the predicate.

  ## Examples

      exists(col("arr"), fn x -> Column.gt(x, lit(0)) end)
  """
  @spec exists(Column.t() | String.t(), (Column.t() -> Column.t())) :: Column.t()
  def exists(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "exists", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Returns true if all elements in the array satisfy the predicate.

  ## Examples

      forall(col("arr"), fn x -> Column.gt(x, lit(0)) end)
  """
  @spec forall(Column.t() | String.t(), (Column.t() -> Column.t())) :: Column.t()
  def forall(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "forall", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Aggregates elements in an array column using an initial value and a merge function.

  The merge function receives two lambda variables: accumulator and element.

  ## Examples

      aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end)
  """
  @spec aggregate(Column.t() | String.t(), Column.t() | term(), (Column.t(), Column.t() -> Column.t())) ::
          Column.t()
  def aggregate(col, zero, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    zero_expr = to_expr_or_lit(zero)
    {body, vars} = build_lambda(func, ["acc", "x"])

    %Column{expr: {:fn, "aggregate", [col_expr, zero_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Alias for `aggregate/3`.
  """
  @spec reduce(Column.t() | String.t(), Column.t() | term(), (Column.t(), Column.t() -> Column.t())) ::
          Column.t()
  def reduce(col, zero, func), do: aggregate(col, zero, func)

  @doc """
  Filters entries in a map column using a predicate on key and value.

  ## Examples

      map_filter(col("m"), fn k, v -> Column.gt(v, lit(0)) end)
  """
  @spec map_filter(Column.t() | String.t(), (Column.t(), Column.t() -> Column.t())) :: Column.t()
  def map_filter(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["k", "v"])

    %Column{expr: {:fn, "map_filter", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Merges two maps using a function on overlapping keys.

  The function receives three lambda variables: key, value1, value2.

  ## Examples

      map_zip_with(col("m1"), col("m2"), fn k, v1, v2 -> Column.plus(v1, v2) end)
  """
  @spec map_zip_with(
          Column.t() | String.t(),
          Column.t() | String.t(),
          (Column.t(), Column.t(), Column.t() -> Column.t())
        ) :: Column.t()
  def map_zip_with(col1, col2, func) when is_function(func, 3) do
    col1_expr = to_expr(col1)
    col2_expr = to_expr(col2)
    {body, vars} = build_lambda(func, ["k", "v1", "v2"])

    %Column{expr: {:fn, "map_zip_with", [col1_expr, col2_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Transforms keys of a map column using a function on key and value.

  ## Examples

      transform_keys(col("m"), fn k, v -> Column.plus(k, lit(1)) end)
  """
  @spec transform_keys(Column.t() | String.t(), (Column.t(), Column.t() -> Column.t())) ::
          Column.t()
  def transform_keys(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["k", "v"])

    %Column{expr: {:fn, "transform_keys", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Transforms values of a map column using a function on key and value.

  ## Examples

      transform_values(col("m"), fn k, v -> Column.plus(v, lit(1)) end)
  """
  @spec transform_values(Column.t() | String.t(), (Column.t(), Column.t() -> Column.t())) ::
          Column.t()
  def transform_values(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["k", "v"])

    %Column{expr: {:fn, "transform_values", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Merges two arrays element-wise using a function.

  ## Examples

      zip_with(col("a1"), col("a2"), fn x, y -> Column.plus(x, y) end)
  """
  @spec zip_with(
          Column.t() | String.t(),
          Column.t() | String.t(),
          (Column.t(), Column.t() -> Column.t())
        ) :: Column.t()
  def zip_with(col1, col2, func) when is_function(func, 2) do
    col1_expr = to_expr(col1)
    col2_expr = to_expr(col2)
    {body, vars} = build_lambda(func, ["x", "y"])

    %Column{expr: {:fn, "zip_with", [col1_expr, col2_expr, {:lambda, body, vars}], false}}
  end

  # ── Internal helpers (used by generated functions) ──

  @doc false
  def to_expr(%Column{expr: e}), do: e
  def to_expr(name) when is_binary(name), do: {:col, name}
  def to_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}

  @doc false
  def lit_expr(%Column{expr: e}), do: e
  def lit_expr(value), do: {:lit, value}

  # ── Private helpers for HOF lambda construction ──

  defp build_lambda(func, var_names) do
    vars = Enum.map(var_names, fn name -> {:lambda_var, name} end)
    col_args = Enum.map(vars, fn var -> %Column{expr: var} end)
    %Column{expr: body} = apply(func, col_args)
    {body, vars}
  end

  defp to_expr_or_lit(%Column{expr: e}), do: e
  defp to_expr_or_lit(value), do: {:lit, value}
end
