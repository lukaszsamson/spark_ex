defmodule SparkEx.DataFrame do
  @moduledoc """
  A lazy reference to a Spark DataFrame.

  DataFrame structs hold a session reference and an internal plan representation.
  Transforms build up the plan; actions (`collect/1`, `count/1`, etc.) execute it
  via the Spark Connect server.

  ## Transforms (lazy)

  - `select/2` — project columns
  - `filter/2` — filter rows by condition
  - `with_column/3` — add or replace a column
  - `drop/2` — drop columns
  - `order_by/2` — sort rows
  - `limit/2` — limit number of rows

  ## Actions (execute)

  - `collect/2` — collect all rows
  - `take/3` — collect up to N rows
  - `count/1` — count rows
  - `schema/1` — get the schema
  - `explain/2` — get the query plan
  - `show/2` — get a formatted string representation
  """

  alias SparkEx.Column

  defstruct [:session, :plan]

  @type plan :: term()

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: plan()
        }

  # ── Transforms (lazy — return new DataFrame) ──

  @doc """
  Projects a set of columns or expressions.

  Accepts a list of:
  - `SparkEx.Column` structs
  - strings (interpreted as column names)
  - atoms (interpreted as column names)

  ## Examples

      import SparkEx.Functions, only: [col: 1, lit: 1]

      df |> SparkEx.DataFrame.select([col("name"), col("age")])
      df |> SparkEx.DataFrame.select(["name", "age"])
      df |> SparkEx.DataFrame.select([:name, :age])
  """
  @spec select(t(), [Column.t() | String.t() | atom()]) :: t()
  def select(%__MODULE__{} = df, columns) when is_list(columns) do
    exprs = Enum.map(columns, &normalize_column_expr/1)
    %__MODULE__{df | plan: {:project, df.plan, exprs}}
  end

  @doc """
  Filters rows based on a boolean condition.

  ## Examples

      import SparkEx.Functions, only: [col: 1, lit: 1]

      df |> SparkEx.DataFrame.filter(col("age") |> SparkEx.Column.gt(lit(18)))
  """
  @spec filter(t(), Column.t()) :: t()
  def filter(%__MODULE__{} = df, %Column{} = condition) do
    %__MODULE__{df | plan: {:filter, df.plan, condition.expr}}
  end

  @doc """
  Adds or replaces a column with the given name and expression.

  ## Examples

      import SparkEx.Functions, only: [col: 1, lit: 1]

      df |> SparkEx.DataFrame.with_column("doubled", col("value") |> SparkEx.Column.multiply(lit(2)))
  """
  @spec with_column(t(), String.t(), Column.t()) :: t()
  def with_column(%__MODULE__{} = df, name, %Column{} = col) when is_binary(name) do
    %__MODULE__{df | plan: {:with_columns, df.plan, [{:alias, col.expr, name}]}}
  end

  @doc """
  Drops the specified columns.

  Accepts a list of column names as strings or atoms.

  ## Examples

      df |> SparkEx.DataFrame.drop(["temp_col", "debug_col"])
      df |> SparkEx.DataFrame.drop([:temp_col])
  """
  @spec drop(t(), [String.t() | atom()]) :: t()
  def drop(%__MODULE__{} = df, columns) when is_list(columns) do
    names = Enum.map(columns, &to_string/1)
    %__MODULE__{df | plan: {:drop, df.plan, names}}
  end

  @doc """
  Sorts the DataFrame by the given columns or sort orders.

  Accepts a list of:
  - `SparkEx.Column` structs (with optional `.asc()` / `.desc()`)
  - strings (ascending by default)
  - atoms (ascending by default)

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      df |> SparkEx.DataFrame.order_by([col("age") |> SparkEx.Column.desc()])
      df |> SparkEx.DataFrame.order_by(["name"])
  """
  @spec order_by(t(), [Column.t() | String.t() | atom()]) :: t()
  def order_by(%__MODULE__{} = df, columns) when is_list(columns) do
    sort_exprs = Enum.map(columns, &normalize_sort_expr/1)
    %__MODULE__{df | plan: {:sort, df.plan, sort_exprs}}
  end

  @doc """
  Limits the number of rows.

  ## Examples

      df |> SparkEx.DataFrame.limit(100)
  """
  @spec limit(t(), pos_integer()) :: t()
  def limit(%__MODULE__{} = df, n) when is_integer(n) and n > 0 do
    %__MODULE__{df | plan: {:limit, df.plan, n}}
  end

  # ── Actions (execute against Spark) ──

  @doc """
  Collects all rows from the DataFrame as a list of maps.

  ## Options

  - `:timeout` — gRPC call timeout in ms (default: 60_000)
  """
  @spec collect(t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def collect(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.execute_collect(df.session, df.plan, opts)
  end

  @doc """
  Returns the row count of the DataFrame.
  """
  @spec count(t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(%__MODULE__{} = df) do
    SparkEx.Session.execute_count(df.session, df.plan)
  end

  @doc """
  Returns up to `n` rows from the DataFrame as a list of maps.
  """
  @spec take(t(), pos_integer(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def take(%__MODULE__{} = df, n, opts \\ []) when is_integer(n) and n > 0 do
    limit_plan = {:limit, df.plan, n}
    SparkEx.Session.execute_collect(df.session, limit_plan, opts)
  end

  @doc """
  Returns the schema of the DataFrame via AnalyzePlan.
  """
  @spec schema(t()) :: {:ok, term()} | {:error, term()}
  def schema(%__MODULE__{} = df) do
    SparkEx.Session.analyze_schema(df.session, df.plan)
  end

  @doc """
  Returns the explain string for the DataFrame's plan.

  Modes: `:simple`, `:extended`, `:codegen`, `:cost`, `:formatted`
  """
  @spec explain(t(), atom()) :: {:ok, String.t()} | {:error, term()}
  def explain(%__MODULE__{} = df, mode \\ :simple) do
    SparkEx.Session.analyze_explain(df.session, df.plan, mode)
  end

  @doc """
  Returns a formatted string representation of the DataFrame (like PySpark's `show()`).

  ## Options

  - `:num_rows` — number of rows to show (default: 20)
  - `:truncate` — column width truncation (default: 20, 0 for no truncation)
  - `:vertical` — vertical display format (default: false)
  """
  @spec show(t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def show(%__MODULE__{} = df, opts \\ []) do
    num_rows = Keyword.get(opts, :num_rows, 20)
    truncate = Keyword.get(opts, :truncate, 20)
    vertical = Keyword.get(opts, :vertical, false)

    show_plan = {:show_string, df.plan, num_rows, truncate, vertical}
    SparkEx.Session.execute_show(df.session, show_plan)
  end

  # ── Private helpers ──

  defp normalize_column_expr(%Column{} = col), do: col.expr
  defp normalize_column_expr(name) when is_binary(name), do: {:col, name}
  defp normalize_column_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}

  defp normalize_sort_expr(%Column{expr: {:sort_order, _, _, _}} = col), do: col.expr

  defp normalize_sort_expr(%Column{} = col) do
    {:sort_order, col.expr, :asc, nil}
  end

  defp normalize_sort_expr(name) when is_binary(name) do
    {:sort_order, {:col, name}, :asc, nil}
  end

  defp normalize_sort_expr(name) when is_atom(name) do
    {:sort_order, {:col, Atom.to_string(name)}, :asc, nil}
  end
end
