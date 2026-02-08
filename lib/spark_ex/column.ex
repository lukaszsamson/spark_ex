defmodule SparkEx.Column do
  @moduledoc """
  Expression wrapper for Spark DataFrame columns.

  A `Column` wraps an internal expression representation that gets encoded
  into Spark Connect protobuf `Expression` messages by the `PlanEncoder`.

  Columns are created via `SparkEx.Functions` constructors (`col/1`, `lit/1`, `expr/1`)
  and combined using the operations defined here.

  ## Examples

      import SparkEx.Functions, only: [col: 1, lit: 1]

      col("age") |> SparkEx.Column.gt(lit(18))
      col("name") |> SparkEx.Column.alias("user_name")
      col("score") |> SparkEx.Column.desc()
  """

  defstruct [:expr]

  @type t :: %__MODULE__{expr: expr()}

  @type expr ::
          {:col, String.t()}
          | {:lit, term()}
          | {:expr, String.t()}
          | {:fn, String.t(), [expr()], boolean()}
          | {:alias, expr(), String.t()}
          | {:sort_order, expr(), :asc | :desc, :nulls_first | :nulls_last | nil}
          | {:cast, expr(), String.t()}
          | {:star}
          | {:star, String.t()}
          | {:subquery, term(), :scalar | :exists}

  # --- Comparisons ---

  @doc "Equality: `col == other`"
  @spec eq(t(), t()) :: t()
  def eq(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("==", left, right)
  end

  @doc "Not equal: `col != other`"
  @spec neq(t(), t()) :: t()
  def neq(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("!=", left, right)
  end

  @doc "Greater than: `col > other`"
  @spec gt(t(), t()) :: t()
  def gt(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn(">", left, right)
  end

  @doc "Greater than or equal: `col >= other`"
  @spec gte(t(), t()) :: t()
  def gte(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn(">=", left, right)
  end

  @doc "Less than: `col < other`"
  @spec lt(t(), t()) :: t()
  def lt(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("<", left, right)
  end

  @doc "Less than or equal: `col <= other`"
  @spec lte(t(), t()) :: t()
  def lte(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("<=", left, right)
  end

  # --- Boolean ---

  @doc "Logical AND"
  @spec and_(t(), t()) :: t()
  def and_(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("and", left, right)
  end

  @doc "Logical OR"
  @spec or_(t(), t()) :: t()
  def or_(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("or", left, right)
  end

  @doc "Logical NOT"
  @spec not_(t()) :: t()
  def not_(%__MODULE__{} = col) do
    %__MODULE__{expr: {:fn, "not", [col.expr], false}}
  end

  # --- Null checks ---

  @doc "Returns true if the column is null"
  @spec is_null(t()) :: t()
  def is_null(%__MODULE__{} = col) do
    %__MODULE__{expr: {:fn, "isnull", [col.expr], false}}
  end

  @doc "Returns true if the column is not null"
  @spec is_not_null(t()) :: t()
  def is_not_null(%__MODULE__{} = col) do
    %__MODULE__{expr: {:fn, "isnotnull", [col.expr], false}}
  end

  # --- String operations ---

  @doc "Returns true if the column contains the given string"
  @spec contains(t(), t()) :: t()
  def contains(%__MODULE__{} = col, %__MODULE__{} = substr) do
    binary_fn("contains", col, substr)
  end

  @doc "Returns true if the column starts with the given string"
  @spec starts_with(t(), t()) :: t()
  def starts_with(%__MODULE__{} = col, %__MODULE__{} = prefix) do
    binary_fn("startswith", col, prefix)
  end

  @doc "Returns true if the column matches the given SQL LIKE pattern"
  @spec like(t(), t()) :: t()
  def like(%__MODULE__{} = col, %__MODULE__{} = pattern) do
    binary_fn("like", col, pattern)
  end

  # --- Arithmetic ---

  @doc "Addition: `col + other`"
  @spec plus(t(), t()) :: t()
  def plus(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("+", left, right)
  end

  @doc "Subtraction: `col - other`"
  @spec minus(t(), t()) :: t()
  def minus(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("-", left, right)
  end

  @doc "Multiplication: `col * other`"
  @spec multiply(t(), t()) :: t()
  def multiply(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("*", left, right)
  end

  @doc "Division: `col / other`"
  @spec divide(t(), t()) :: t()
  def divide(%__MODULE__{} = left, %__MODULE__{} = right) do
    binary_fn("/", left, right)
  end

  # --- Type casting ---

  @doc """
  Casts the column to the given type.

  The type is a Spark SQL type string (e.g. `"int"`, `"string"`, `"double"`).
  """
  @spec cast(t(), String.t()) :: t()
  def cast(%__MODULE__{} = col, type_str) when is_binary(type_str) do
    %__MODULE__{expr: {:cast, col.expr, type_str}}
  end

  # --- Sort ordering ---

  @doc "Sort ascending (nulls first by default)"
  @spec asc(t()) :: t()
  def asc(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :asc, nil}}
  end

  @doc "Sort descending (nulls last by default)"
  @spec desc(t()) :: t()
  def desc(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :desc, nil}}
  end

  @doc "Sort ascending with explicit null ordering"
  @spec asc_nulls_first(t()) :: t()
  def asc_nulls_first(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :asc, :nulls_first}}
  end

  @doc "Sort ascending with nulls last"
  @spec asc_nulls_last(t()) :: t()
  def asc_nulls_last(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :asc, :nulls_last}}
  end

  @doc "Sort descending with nulls first"
  @spec desc_nulls_first(t()) :: t()
  def desc_nulls_first(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :desc, :nulls_first}}
  end

  @doc "Sort descending with nulls last"
  @spec desc_nulls_last(t()) :: t()
  def desc_nulls_last(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :desc, :nulls_last}}
  end

  # --- Naming ---

  @doc "Assigns an alias (name) to this column expression"
  @spec alias_(t(), String.t()) :: t()
  def alias_(%__MODULE__{} = col, name) when is_binary(name) do
    %__MODULE__{expr: {:alias, col.expr, name}}
  end

  # --- Private helpers ---

  defp binary_fn(name, %__MODULE__{} = left, %__MODULE__{} = right) do
    %__MODULE__{expr: {:fn, name, [left.expr, right.expr], false}}
  end
end
