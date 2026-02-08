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
  - `group_by/2` — group by columns (returns `SparkEx.GroupedData`)
  - `join/4` — join two DataFrames
  - `distinct/1` — deduplicate all rows
  - `union/2` — union two DataFrames

  ## Actions (execute)

  - `collect/2` — collect all rows
  - `take/3` — collect up to N rows
  - `count/1` — count rows
  - `schema/1` — get the schema
  - `explain/2` — get the query plan
  - `show/2` — get a formatted string representation
  """

  alias SparkEx.Column

  defstruct [:session, :plan, tags: []]

  @compile {:no_warn_undefined, Explorer.DataFrame}

  @type plan :: term()

  @type t :: %__MODULE__{
          session: GenServer.server(),
          plan: plan(),
          tags: [String.t()]
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

  @doc """
  Groups the DataFrame by the given columns, returning a `SparkEx.GroupedData`.

  Use `SparkEx.GroupedData.agg/2` to apply aggregate functions.

  Accepts a list of column names (strings or atoms) or `Column` structs.

  ## Examples

      import SparkEx.Functions

      df
      |> DataFrame.group_by(["department"])
      |> SparkEx.GroupedData.agg([sum(col("salary"))])
  """
  @spec group_by(t(), [Column.t() | String.t() | atom()]) :: SparkEx.GroupedData.t()
  def group_by(%__MODULE__{} = df, columns) when is_list(columns) do
    grouping_exprs = Enum.map(columns, &normalize_column_expr/1)

    %SparkEx.GroupedData{
      session: df.session,
      plan: df.plan,
      grouping_exprs: grouping_exprs
    }
  end

  @doc """
  Joins this DataFrame with another on the given condition.

  ## Join types

  - `:inner` (default)
  - `:left` — left outer join
  - `:right` — right outer join
  - `:full` — full outer join
  - `:cross` — cross join (no condition needed)
  - `:left_semi` — left semi join
  - `:left_anti` — left anti join

  ## Join conditions

  The `on` parameter can be:
  - A `Column` struct representing the join condition expression
  - A list of column name strings for a `USING` join

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      DataFrame.join(df1, df2, Column.eq(col("df1.id"), col("df2.id")), :inner)
      DataFrame.join(df1, df2, ["id"], :inner)
  """
  @spec join(
          t(),
          t(),
          Column.t() | String.t() | atom() | [Column.t() | String.t() | atom()],
          atom() | String.t()
        ) ::
          t()
  def join(%__MODULE__{} = left, %__MODULE__{} = right, on, join_type \\ :inner) do
    ensure_same_session!(left, right, :join)
    {condition, using_columns} = normalize_join_on(on)
    canonical_join_type = normalize_join_type(join_type)

    %__MODULE__{
      left
      | plan: {:join, left.plan, right.plan, condition, canonical_join_type, using_columns}
    }
  end

  @doc """
  Returns a new DataFrame with duplicate rows removed.

  ## Examples

      df |> SparkEx.DataFrame.distinct()
  """
  @spec distinct(t()) :: t()
  def distinct(%__MODULE__{} = df) do
    %__MODULE__{df | plan: {:deduplicate, df.plan, [], true}}
  end

  @doc """
  Returns a new DataFrame with the union of rows from both DataFrames.

  Both DataFrames must have the same schema. Duplicates are preserved
  (equivalent to SQL `UNION ALL`).

  ## Examples

      DataFrame.union(df1, df2)
  """
  @spec union(t(), t()) :: t()
  def union(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :union)

    %__MODULE__{
      left
      | plan: {:set_operation, left.plan, right.plan, :union, true}
    }
  end

  @doc """
  Returns a new DataFrame with the union of rows, removing duplicates
  (equivalent to SQL `UNION`).

  ## Examples

      DataFrame.union_distinct(df1, df2)
  """
  @spec union_distinct(t(), t()) :: t()
  def union_distinct(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :union_distinct)

    %__MODULE__{
      left
      | plan: {:set_operation, left.plan, right.plan, :union, false}
    }
  end

  @doc """
  Returns rows in this DataFrame that are also in the other DataFrame.

  ## Examples

      DataFrame.intersect(df1, df2)
  """
  @spec intersect(t(), t()) :: t()
  def intersect(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :intersect)

    %__MODULE__{
      left
      | plan: {:set_operation, left.plan, right.plan, :intersect, false}
    }
  end

  @doc """
  Returns rows in this DataFrame that are not in the other DataFrame.

  ## Examples

      DataFrame.except(df1, df2)
  """
  @spec except(t(), t()) :: t()
  def except(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :except)

    %__MODULE__{
      left
      | plan: {:set_operation, left.plan, right.plan, :except, false}
    }
  end

  # ── Metadata ──

  @doc """
  Tags the DataFrame with an operation tag for interrupt targeting.

  Tags are propagated to the `ExecutePlanRequest` when the DataFrame is
  executed. Multiple tags can be added by calling `tag/2` multiple times.

  ## Examples

      df = SparkEx.sql(session, "SELECT * FROM big_table")
      |> DataFrame.tag("etl-job-42")

      # Later, from another process:
      SparkEx.interrupt_tag(session, "etl-job-42")
  """
  @spec tag(t(), String.t()) :: t()
  def tag(%__MODULE__{} = df, tag) when is_binary(tag) do
    validate_tag!(tag)
    %{df | tags: df.tags ++ [tag]}
  end

  # ── Actions (execute against Spark) ──

  @doc """
  Materializes the DataFrame as an `Explorer.DataFrame`.

  By default, injects a `LIMIT` of `max_rows` into the Spark plan to prevent
  unbounded collection. Pass `unsafe: true` to skip the limit injection.
  Local decoder limits still apply unless you explicitly set `max_rows: :infinity`
  and/or `max_bytes: :infinity`.

  ## Options

  - `:max_rows` — maximum number of rows (default: 10_000)
  - `:max_bytes` — maximum Arrow data bytes (default: 64 MB)
  - `:unsafe` — skip LIMIT injection only (default: false)
  - `:timeout` — gRPC timeout in ms (default: 60_000)

  ## Examples

      {:ok, explorer_df} = DataFrame.to_explorer(df)
      {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 1_000)
      {:ok, explorer_df} = DataFrame.to_explorer(df, unsafe: true)
  """
  @spec to_explorer(t(), keyword()) :: {:ok, Explorer.DataFrame.t()} | {:error, term()}
  def to_explorer(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.execute_explorer(df.session, df.plan, merge_tags(df, opts))
  end

  @doc """
  Collects all rows from the DataFrame as a list of maps.

  ## Options

  - `:timeout` — gRPC call timeout in ms (default: 60_000)
  """
  @spec collect(t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def collect(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.execute_collect(df.session, df.plan, merge_tags(df, opts))
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
    SparkEx.Session.execute_collect(df.session, limit_plan, merge_tags(df, opts))
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

  defp merge_tags(%__MODULE__{tags: []}, opts), do: opts
  defp merge_tags(%__MODULE__{tags: tags}, opts), do: Keyword.put(opts, :tags, tags)

  defp validate_tag!("") do
    raise ArgumentError, "Spark Connect tag must be a non-empty string"
  end

  defp validate_tag!(tag) do
    if String.contains?(tag, ",") do
      raise ArgumentError, "Spark Connect tag cannot contain ','"
    end

    :ok
  end

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

  defp ensure_same_session!(%__MODULE__{session: left}, %__MODULE__{session: right}, _op)
       when left == right,
       do: :ok

  defp ensure_same_session!(%__MODULE__{}, %__MODULE__{}, op) do
    raise ArgumentError, "cannot #{op} DataFrames from different sessions"
  end

  defp normalize_join_on(%Column{} = col), do: {col.expr, []}

  defp normalize_join_on(name) when is_binary(name) or is_atom(name) do
    {nil, [to_string(name)]}
  end

  defp normalize_join_on(columns) when is_list(columns) do
    cond do
      columns == [] ->
        {nil, []}

      Enum.all?(columns, &(is_binary(&1) or is_atom(&1))) ->
        {nil, Enum.map(columns, &to_string/1)}

      Enum.all?(columns, &match?(%Column{}, &1)) ->
        {combine_join_conditions(columns), []}

      true ->
        raise ArgumentError,
              "expected join keys as a column name, list of names, Column, or list of Column conditions"
    end
  end

  defp normalize_join_on(_other) do
    raise ArgumentError,
          "expected join keys as a column name, list of names, Column, or list of Column conditions"
  end

  defp combine_join_conditions([%Column{expr: first_expr} | rest]) do
    Enum.reduce(rest, first_expr, fn %Column{expr: expr}, acc ->
      {:fn, "and", [acc, expr], false}
    end)
  end

  defp normalize_join_type(join_type) do
    normalized =
      case join_type do
        type when is_atom(type) ->
          type |> Atom.to_string() |> String.downcase() |> String.replace("_", "")

        type when is_binary(type) ->
          type |> String.downcase() |> String.replace("_", "")

        _ ->
          raise ArgumentError,
                "invalid join type: #{inspect(join_type)}. Expected one of: :inner, :outer, :full, :full_outer, :left, :left_outer, :right, :right_outer, :semi, :left_semi, :anti, :left_anti, :cross"
      end

    case normalized do
      "inner" -> :inner
      "outer" -> :full
      "full" -> :full
      "fullouter" -> :full
      "left" -> :left
      "leftouter" -> :left
      "right" -> :right
      "rightouter" -> :right
      "semi" -> :left_semi
      "leftsemi" -> :left_semi
      "anti" -> :left_anti
      "leftanti" -> :left_anti
      "cross" -> :cross
      _ -> raise ArgumentError, "invalid join type: #{inspect(join_type)}"
    end
  end
end
