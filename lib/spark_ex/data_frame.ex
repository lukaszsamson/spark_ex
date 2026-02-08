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

  # ── M10: Projection/Rename ──

  @doc """
  Projects columns using SQL expression strings.

  Each string is parsed as a SQL expression by Spark.

  ## Examples

      df |> DataFrame.select_expr(["name", "age + 1 AS age_plus"])
  """
  @spec select_expr(t(), [String.t()]) :: t()
  def select_expr(%__MODULE__{} = df, exprs) when is_list(exprs) do
    expr_nodes = Enum.map(exprs, fn e -> {:expr, e} end)
    %__MODULE__{df | plan: {:project, df.plan, expr_nodes}}
  end

  @doc """
  Adds or replaces multiple columns at once.

  Accepts a list of `{name, column}` tuples or a list of aliased Column expressions.

  ## Examples

      import SparkEx.Functions, only: [col: 1, lit: 1]

      df |> DataFrame.with_columns([
        {"doubled", Column.multiply(col("x"), lit(2))},
        {"const", lit(42)}
      ])
  """
  @spec with_columns(t(), [{String.t(), Column.t()}]) :: t()
  def with_columns(%__MODULE__{} = df, columns) when is_list(columns) do
    aliases =
      Enum.map(columns, fn
        {name, %Column{} = col} when is_binary(name) -> {:alias, col.expr, name}
        %Column{expr: {:alias, _, _} = expr} -> expr
      end)

    %__MODULE__{df | plan: {:with_columns, df.plan, aliases}}
  end

  @doc """
  Renames all columns in the DataFrame.

  ## Examples

      df |> DataFrame.to_df(["id", "full_name", "years"])
  """
  @spec to_df(t(), [String.t()]) :: t()
  def to_df(%__MODULE__{} = df, column_names) when is_list(column_names) do
    %__MODULE__{df | plan: {:to_df, df.plan, column_names}}
  end

  @doc """
  Renames a single column.

  ## Examples

      df |> DataFrame.with_column_renamed("old_name", "new_name")
  """
  @spec with_column_renamed(t(), String.t(), String.t()) :: t()
  def with_column_renamed(%__MODULE__{} = df, existing, new_name)
      when is_binary(existing) and is_binary(new_name) do
    %__MODULE__{df | plan: {:with_columns_renamed, df.plan, [{existing, new_name}]}}
  end

  @doc """
  Renames multiple columns using a map of old -> new names.

  ## Examples

      df |> DataFrame.with_columns_renamed(%{"old1" => "new1", "old2" => "new2"})
  """
  @spec with_columns_renamed(t(), %{String.t() => String.t()}) :: t()
  def with_columns_renamed(%__MODULE__{} = df, rename_map) when is_map(rename_map) do
    %__MODULE__{df | plan: {:with_columns_renamed, df.plan, Map.to_list(rename_map)}}
  end

  # ── M10: Extended Set Operations ──

  @doc """
  Union by column name rather than position.

  ## Options

  - `:allow_missing` — if true, missing columns are filled with nulls (default: false)

  ## Examples

      DataFrame.union_by_name(df1, df2)
      DataFrame.union_by_name(df1, df2, allow_missing: true)
  """
  @spec union_by_name(t(), t(), keyword()) :: t()
  def union_by_name(%__MODULE__{} = left, %__MODULE__{} = right, opts \\ []) do
    ensure_same_session!(left, right, :union_by_name)
    allow_missing = Keyword.get(opts, :allow_missing, false)

    %__MODULE__{
      left
      | plan:
          {:set_operation, left.plan, right.plan, :union, true,
           by_name: true, allow_missing_columns: allow_missing}
    }
  end

  @doc """
  Returns rows in this DataFrame that are not in the other, preserving duplicates
  (equivalent to SQL `EXCEPT ALL`).
  """
  @spec except_all(t(), t()) :: t()
  def except_all(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :except_all)
    %__MODULE__{left | plan: {:set_operation, left.plan, right.plan, :except, true}}
  end

  @doc """
  Returns rows common to both DataFrames, preserving duplicates
  (equivalent to SQL `INTERSECT ALL`).
  """
  @spec intersect_all(t(), t()) :: t()
  def intersect_all(%__MODULE__{} = left, %__MODULE__{} = right) do
    ensure_same_session!(left, right, :intersect_all)
    %__MODULE__{left | plan: {:set_operation, left.plan, right.plan, :intersect, true}}
  end

  # ── M10: Partitioning ──

  @doc """
  Repartitions the DataFrame.

  When `cols` is empty, does a hash repartition to `num_partitions`.
  When `cols` is provided, repartitions by those expressions.

  ## Examples

      df |> DataFrame.repartition(10)
      df |> DataFrame.repartition(10, [col("key")])
  """
  @spec repartition(t(), pos_integer(), [Column.t() | String.t() | atom()]) :: t()
  def repartition(%__MODULE__{} = df, num_partitions, cols \\ [])
      when is_integer(num_partitions) do
    case cols do
      [] ->
        %__MODULE__{df | plan: {:repartition, df.plan, num_partitions, true}}

      cols when is_list(cols) ->
        exprs = Enum.map(cols, &normalize_column_expr/1)
        %__MODULE__{df | plan: {:repartition_by_expression, df.plan, exprs, num_partitions}}
    end
  end

  @doc """
  Reduces the number of partitions without shuffling data.

  ## Examples

      df |> DataFrame.coalesce(1)
  """
  @spec coalesce(t(), pos_integer()) :: t()
  def coalesce(%__MODULE__{} = df, num_partitions) when is_integer(num_partitions) do
    %__MODULE__{df | plan: {:repartition, df.plan, num_partitions, false}}
  end

  @doc """
  Sorts within each partition by the given columns.

  ## Examples

      df |> DataFrame.sort_within_partitions(["key"])
  """
  @spec sort_within_partitions(t(), [Column.t() | String.t() | atom()]) :: t()
  def sort_within_partitions(%__MODULE__{} = df, columns) when is_list(columns) do
    sort_exprs = Enum.map(columns, &normalize_sort_expr/1)
    %__MODULE__{df | plan: {:sort, df.plan, sort_exprs, false}}
  end

  # ── M10: Sampling ──

  @doc """
  Returns a random sample of rows.

  ## Options

  - `:with_replacement` — sample with replacement (default: false)
  - `:seed` — random seed (default: nil)

  ## Examples

      df |> DataFrame.sample(0.1)
      df |> DataFrame.sample(0.5, with_replacement: true, seed: 42)
  """
  @spec sample(t(), float(), keyword()) :: t()
  def sample(%__MODULE__{} = df, fraction, opts \\ []) when is_float(fraction) do
    with_replacement = Keyword.get(opts, :with_replacement, false)
    seed = Keyword.get(opts, :seed, nil)

    %__MODULE__{
      df
      | plan: {:sample, df.plan, 0.0, fraction, with_replacement, seed, false}
    }
  end

  @doc """
  Randomly splits the DataFrame into multiple DataFrames using normalized weights.
  """
  @spec random_split(t(), [number()], integer() | nil) :: [t()]
  def random_split(%__MODULE__{} = df, weights, seed \\ nil) when is_list(weights) do
    Enum.each(weights, fn w ->
      if not is_number(w) or w < 0.0 do
        raise ArgumentError, "weights must be non-negative numbers"
      end
    end)

    total = Enum.sum(weights)

    if total <= 0.0 do
      raise ArgumentError, "sum(weights) must be > 0"
    end

    resolved_seed =
      case seed do
        nil -> System.unique_integer([:positive])
        s when is_integer(s) -> s
      end

    normalized = Enum.map(weights, &(&1 / total))

    {splits, _} =
      Enum.map_reduce(normalized, 0.0, fn w, lower ->
        upper = lower + w

        split = %__MODULE__{
          df
          | plan: {:sample, df.plan, lower, upper, false, resolved_seed, true}
        }

        {split, upper}
      end)

    splits
  end

  # ── M10: Row Operations ──

  @doc """
  Skips the first `n` rows.

  ## Examples

      df |> DataFrame.offset(10)
  """
  @spec offset(t(), non_neg_integer()) :: t()
  def offset(%__MODULE__{} = df, n) when is_integer(n) and n >= 0 do
    %__MODULE__{df | plan: {:offset, df.plan, n}}
  end

  @doc """
  Returns the last `n` rows.

  ## Examples

      df |> DataFrame.tail(5)
  """
  @spec tail(t(), pos_integer()) :: t()
  def tail(%__MODULE__{} = df, n) when is_integer(n) and n > 0 do
    %__MODULE__{df | plan: {:tail, df.plan, n}}
  end

  @doc """
  Returns the first `n` rows as a list of maps.

  Equivalent to `take/3` but follows PySpark naming.
  """
  @spec head(t(), pos_integer(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def head(%__MODULE__{} = df, n \\ 1, opts \\ []) when is_integer(n) and n > 0 do
    take(df, n, opts)
  end

  @doc """
  Returns the first row as a map, or `nil` if empty.
  """
  @spec first(t(), keyword()) :: {:ok, map() | nil} | {:error, term()}
  def first(%__MODULE__{} = df, opts \\ []) do
    case take(df, 1, opts) do
      {:ok, [row]} -> {:ok, row}
      {:ok, []} -> {:ok, nil}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns true if the DataFrame has no rows.
  """
  @spec is_empty(t()) :: {:ok, boolean()} | {:error, term()}
  def is_empty(%__MODULE__{} = df) do
    case take(df, 1) do
      {:ok, []} -> {:ok, true}
      {:ok, [_ | _]} -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  # ── M10: Query Shaping ──

  @doc """
  Adds a query optimization hint.

  Supports primitive values, `Column`s, and lists of primitive values.
  """
  @spec hint(t(), String.t(), term()) :: t()
  def hint(%__MODULE__{} = df, name, parameters \\ []) when is_binary(name) do
    %__MODULE__{df | plan: {:hint, df.plan, name, normalize_hint_parameters(parameters)}}
  end

  @doc """
  Adds or replaces metadata for an existing column.
  """
  @spec with_metadata(t(), String.t(), map()) :: t()
  def with_metadata(%__MODULE__{} = df, column_name, metadata)
      when is_binary(column_name) and is_map(metadata) do
    metadata_json = Jason.encode!(metadata)

    %__MODULE__{
      df
      | plan:
          {:with_columns, df.plan, [{:alias, {:col, column_name}, column_name, metadata_json}]}
    }
  end

  @doc """
  Adds a watermark for streaming event-time processing.

  ## Examples

      df |> DataFrame.with_watermark("event_time", "10 minutes")
  """
  @spec with_watermark(t(), String.t(), String.t()) :: t()
  def with_watermark(%__MODULE__{} = df, event_time, delay_threshold)
      when is_binary(event_time) and is_binary(delay_threshold) do
    %__MODULE__{df | plan: {:with_watermark, df.plan, event_time, delay_threshold}}
  end

  @doc """
  Drops duplicate rows based on a subset of columns.

  When `subset` is empty, deduplicates on all columns (like `distinct/1`).

  ## Examples

      df |> DataFrame.drop_duplicates(["id", "name"])
  """
  @spec drop_duplicates(t(), [String.t()]) :: t()
  def drop_duplicates(%__MODULE__{} = df, subset \\ []) when is_list(subset) do
    case subset do
      [] -> distinct(df)
      cols -> %__MODULE__{df | plan: {:deduplicate, df.plan, cols, false}}
    end
  end

  @doc """
  Drops duplicate rows within the watermark window.
  """
  @spec drop_duplicates_within_watermark(t(), [String.t()]) :: t()
  def drop_duplicates_within_watermark(%__MODULE__{} = df, subset \\ []) when is_list(subset) do
    case subset do
      [] -> %__MODULE__{df | plan: {:deduplicate, df.plan, [], true, true}}
      cols -> %__MODULE__{df | plan: {:deduplicate, df.plan, cols, false, true}}
    end
  end

  # ── M10: Reshaping ──

  @doc """
  Unpivots a DataFrame from wide to long format.

  ## Parameters

  - `ids` — columns to keep as identifier columns
  - `values` — columns to unpivot (nil for all non-id columns)
  - `variable_column_name` — name for the variable column
  - `value_column_name` — name for the value column

  ## Examples

      df |> DataFrame.unpivot(["id"], ["col1", "col2"], "variable", "value")
  """
  @spec unpivot(
          t(),
          [Column.t() | String.t() | atom()],
          [Column.t() | String.t() | atom()] | nil,
          String.t(),
          String.t()
        ) :: t()
  def unpivot(%__MODULE__{} = df, ids, values, variable_column_name, value_column_name) do
    id_exprs = Enum.map(ids, &normalize_column_expr/1)

    value_exprs =
      case values do
        nil -> nil
        vals -> Enum.map(vals, &normalize_column_expr/1)
      end

    %__MODULE__{
      df
      | plan: {:unpivot, df.plan, id_exprs, value_exprs, variable_column_name, value_column_name}
    }
  end

  @doc """
  Transposes the DataFrame.

  ## Options

  - `:index_column` — column(s) to use as index (default: nil)
  """
  @spec transpose(t(), keyword()) :: t()
  def transpose(%__MODULE__{} = df, opts \\ []) do
    index_columns =
      case Keyword.get(opts, :index_column) do
        nil -> []
        col when is_binary(col) -> [{:col, col}]
        cols when is_list(cols) -> Enum.map(cols, &normalize_column_expr/1)
      end

    %__MODULE__{df | plan: {:transpose, df.plan, index_columns}}
  end

  @doc """
  Aliases this DataFrame for use in subqueries.

  ## Examples

      df |> DataFrame.alias("t")
  """
  @spec alias_(t(), String.t()) :: t()
  def alias_(%__MODULE__{} = df, name) when is_binary(name) do
    %__MODULE__{df | plan: {:subquery_alias, df.plan, name}}
  end

  # ── M10: Convenience Aliases ──

  @doc "Alias for `filter/2`."
  @spec where(t(), Column.t()) :: t()
  def where(%__MODULE__{} = df, condition), do: filter(df, condition)

  @doc "Alias for `union/2`."
  @spec union_all(t(), t()) :: t()
  def union_all(%__MODULE__{} = left, %__MODULE__{} = right), do: union(left, right)

  @doc "Cross join — shorthand for `join(df, other, [], :cross)`."
  @spec cross_join(t(), t()) :: t()
  def cross_join(%__MODULE__{} = left, %__MODULE__{} = right) do
    join(left, right, [], :cross)
  end

  # ── M10: Display ──

  @doc """
  Prints the schema tree, mirroring PySpark `printSchema`.

  ## Options

  - `:level` — tree depth level (optional)
  """
  @spec print_schema(t(), keyword()) :: :ok | {:error, term()}
  def print_schema(%__MODULE__{} = df, opts \\ []) do
    case tree_string(df, opts) do
      {:ok, str} ->
        IO.puts(str)
        :ok

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Returns an HTML string representation of the DataFrame.

  ## Options

  - `:num_rows` — number of rows (default: 20)
  - `:truncate` — column width truncation (default: 20)
  """
  @spec html_string(t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def html_string(%__MODULE__{} = df, opts \\ []) do
    num_rows = Keyword.get(opts, :num_rows, 20)
    truncate = Keyword.get(opts, :truncate, 20)

    html_plan = {:html_string, df.plan, num_rows, truncate}
    SparkEx.Session.execute_show(df.session, html_plan)
  end

  # ── Writer entry points ──

  @doc """
  Returns a `SparkEx.Writer` builder for this DataFrame.

  ## Examples

      df
      |> DataFrame.write()
      |> SparkEx.Writer.format("parquet")
      |> SparkEx.Writer.mode(:overwrite)
      |> SparkEx.Writer.save("/data/output.parquet")
  """
  @spec write(t()) :: SparkEx.Writer.t()
  def write(%__MODULE__{} = df) do
    %SparkEx.Writer{df: df}
  end

  @doc """
  Returns a `SparkEx.WriterV2` builder for this DataFrame targeting the given table.

  ## Examples

      df
      |> DataFrame.write_v2("catalog.db.my_table")
      |> SparkEx.WriterV2.using("parquet")
      |> SparkEx.WriterV2.create()
  """
  @spec write_v2(t(), String.t()) :: SparkEx.WriterV2.t()
  def write_v2(%__MODULE__{} = df, table_name) when is_binary(table_name) do
    %SparkEx.WriterV2{df: df, table_name: table_name}
  end

  # ── Temp View creation ──

  @doc """
  Creates a temporary view with the given name.

  Raises an error if a view with this name already exists.
  """
  @spec create_temp_view(t(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_temp_view(%__MODULE__{} = df, name, opts \\ []) when is_binary(name) do
    SparkEx.Session.execute_command(
      df.session,
      {:create_dataframe_view, df.plan, name, false, false},
      opts
    )
  end

  @doc """
  Creates or replaces a temporary view with the given name.
  """
  @spec create_or_replace_temp_view(t(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_or_replace_temp_view(%__MODULE__{} = df, name, opts \\ []) when is_binary(name) do
    SparkEx.Session.execute_command(
      df.session,
      {:create_dataframe_view, df.plan, name, false, true},
      opts
    )
  end

  @doc """
  Creates a global temporary view with the given name.

  Global temp views are accessible across sessions within the same Spark application
  and are available in the `global_temp` database.
  """
  @spec create_global_temp_view(t(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_global_temp_view(%__MODULE__{} = df, name, opts \\ []) when is_binary(name) do
    SparkEx.Session.execute_command(
      df.session,
      {:create_dataframe_view, df.plan, name, true, false},
      opts
    )
  end

  @doc """
  Creates or replaces a global temporary view with the given name.
  """
  @spec create_or_replace_global_temp_view(t(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_or_replace_global_temp_view(%__MODULE__{} = df, name, opts \\ [])
      when is_binary(name) do
    SparkEx.Session.execute_command(
      df.session,
      {:create_dataframe_view, df.plan, name, true, true},
      opts
    )
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

  # ── M10: Subquery/DataFrame Expression Helpers ──

  @doc """
  Returns this DataFrame as a table-argument wrapper.
  """
  @spec as_table(t()) :: SparkEx.TableArg.t()
  def as_table(%__MODULE__{} = df) do
    %SparkEx.TableArg{plan: df.plan}
  end

  @doc """
  Returns this DataFrame as a scalar subquery expression.
  """
  @spec scalar(t()) :: Column.t()
  def scalar(%__MODULE__{} = df) do
    %Column{expr: {:subquery, df.plan, :scalar}}
  end

  @doc """
  Returns this DataFrame as an EXISTS subquery expression.
  """
  @spec exists(t()) :: Column.t()
  def exists(%__MODULE__{} = df) do
    %Column{expr: {:subquery, df.plan, :exists}}
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

  @doc """
  Returns the tree-string representation of the plan.

  ## Options

  - `:level` — tree depth level (optional)
  """
  @spec tree_string(t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def tree_string(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.analyze_tree_string(df.session, df.plan, opts)
  end

  @doc """
  Checks if the plan is local (can be computed without Spark).
  """
  @spec is_local(t()) :: {:ok, boolean()} | {:error, term()}
  def is_local(%__MODULE__{} = df) do
    SparkEx.Session.analyze_is_local(df.session, df.plan)
  end

  @doc """
  Checks if the plan represents a streaming query.
  """
  @spec is_streaming(t()) :: {:ok, boolean()} | {:error, term()}
  def is_streaming(%__MODULE__{} = df) do
    SparkEx.Session.analyze_is_streaming(df.session, df.plan)
  end

  @doc """
  Returns the input files for the plan.
  """
  @spec input_files(t()) :: {:ok, [String.t()]} | {:error, term()}
  def input_files(%__MODULE__{} = df) do
    SparkEx.Session.analyze_input_files(df.session, df.plan)
  end

  @doc """
  Checks if this DataFrame has the same semantics as another.
  """
  @spec same_semantics(t(), t()) :: {:ok, boolean()} | {:error, term()}
  def same_semantics(%__MODULE__{} = df1, %__MODULE__{} = df2) do
    ensure_same_session!(df1, df2, :same_semantics)
    SparkEx.Session.analyze_same_semantics(df1.session, df1.plan, df2.plan)
  end

  @doc """
  Returns the semantic hash of the plan.
  """
  @spec semantic_hash(t()) :: {:ok, integer()} | {:error, term()}
  def semantic_hash(%__MODULE__{} = df) do
    SparkEx.Session.analyze_semantic_hash(df.session, df.plan)
  end

  @doc """
  Persists the DataFrame with optional storage level.

  ## Options

  - `:storage_level` — a `Spark.Connect.StorageLevel` struct
  """
  @spec persist(t(), keyword()) :: :ok | {:error, term()}
  def persist(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.analyze_persist(df.session, df.plan, opts)
  end

  @doc """
  Unpersists the DataFrame.

  ## Options

  - `:blocking` — whether to block until unpersisted (default: false)
  """
  @spec unpersist(t(), keyword()) :: :ok | {:error, term()}
  def unpersist(%__MODULE__{} = df, opts \\ []) do
    SparkEx.Session.analyze_unpersist(df.session, df.plan, opts)
  end

  @doc """
  Returns the storage level of a persisted DataFrame.
  """
  @spec storage_level(t()) :: {:ok, Spark.Connect.StorageLevel.t()} | {:error, term()}
  def storage_level(%__MODULE__{} = df) do
    SparkEx.Session.analyze_get_storage_level(df.session, df.plan)
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

  defp normalize_hint_parameters(parameters) do
    parameters
    |> List.wrap()
    |> Enum.flat_map(fn
      vals when is_list(vals) -> if(Enum.all?(vals, &primitive_hint?/1), do: vals, else: [vals])
      val -> [val]
    end)
    |> Enum.map(fn
      %Column{} = c ->
        c.expr

      v ->
        if primitive_hint?(v) do
          {:lit, v}
        else
          raise ArgumentError, "invalid hint parameter: #{inspect(v)}"
        end
    end)
  end

  defp primitive_hint?(v), do: is_binary(v) or is_integer(v) or is_float(v)

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
