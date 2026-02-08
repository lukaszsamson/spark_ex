defmodule SparkEx.Functions do
  @moduledoc """
  Expression constructors for Spark DataFrame operations.

  These functions create `SparkEx.Column` structs that can be used in
  DataFrame transforms like `select/2`, `filter/2`, `with_column/3`, etc.

  ## Examples

      import SparkEx.Functions

      df
      |> SparkEx.DataFrame.select([col("name"), col("age")])
      |> SparkEx.DataFrame.filter(col("age") |> SparkEx.Column.gt(lit(18)))
  """

  alias SparkEx.Column

  # --- Core constructors ---

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

  # --- Sort helpers ---

  @doc "Sort ascending by the given column"
  @spec asc(Column.t()) :: Column.t()
  defdelegate asc(col), to: Column

  @doc "Sort descending by the given column"
  @spec desc(Column.t()) :: Column.t()
  defdelegate desc(col), to: Column

  # --- Aggregate functions ---

  @doc "Count the number of non-null values"
  @spec count(Column.t()) :: Column.t()
  def count(%Column{} = col), do: agg_fn("count", col)

  @doc "Sum of values"
  @spec sum(Column.t()) :: Column.t()
  def sum(%Column{} = col), do: agg_fn("sum", col)

  @doc "Average of values"
  @spec avg(Column.t()) :: Column.t()
  def avg(%Column{} = col), do: agg_fn("avg", col)

  @doc "Minimum value"
  @spec min(Column.t()) :: Column.t()
  def min(%Column{} = col), do: agg_fn("min", col)

  @doc "Maximum value"
  @spec max(Column.t()) :: Column.t()
  def max(%Column{} = col), do: agg_fn("max", col)

  @doc "Count of distinct non-null values"
  @spec count_distinct(Column.t()) :: Column.t()
  def count_distinct(%Column{} = col) do
    %Column{expr: {:fn, "count", [col.expr], true}}
  end

  # --- Private ---

  defp agg_fn(name, %Column{} = col) do
    %Column{expr: {:fn, name, [col.expr], false}}
  end
end
