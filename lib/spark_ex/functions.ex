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

  # ── Internal helpers (used by generated functions) ──

  @doc false
  def to_expr(%Column{expr: e}), do: e
  def to_expr(name) when is_binary(name), do: {:col, name}
  def to_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}

  @doc false
  def lit_expr(%Column{expr: e}), do: e
  def lit_expr(value), do: {:lit, value}
end
