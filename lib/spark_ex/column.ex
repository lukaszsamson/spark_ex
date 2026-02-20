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
      col("name") |> SparkEx.Column.alias_("user_name")
      col("score") |> SparkEx.Column.desc()
  """

  alias SparkEx.DataFrame

  defstruct [:expr]

  @type t :: %__MODULE__{expr: expr()}

  @type expr ::
          {:col, String.t()}
          | {:col, String.t(), term()}
          | {:lit, term()}
          | {:expr, String.t()}
          | {:col_regex, String.t()}
          | {:col_regex, String.t(), term()}
          | {:metadata_col, String.t()}
          | {:metadata_col, String.t(), term()}
          | {:fn, String.t(), [expr()], boolean()}
          | {:alias, expr(), String.t()}
          | {:sort_order, expr(), :asc | :desc, :nulls_first | :nulls_last | nil}
          | {:cast, expr(), String.t()}
          | {:cast, expr(), String.t(), :try}
          | {:star}
          | {:star, String.t()}
          | {:star, String.t() | nil, term()}
          | {:window, expr(), [expr()], [expr()], term()}
          | {:unresolved_extract_value, expr(), expr()}
          | {:update_fields, expr(), String.t(), expr() | nil}
          | {:lambda, expr(), [{:lambda_var, String.t()}]}
          | {:lambda_var, String.t()}
          | {:subquery, atom(), term(), keyword()}

  # ── Generated binary operators ──

  @binary_operators [
    {:eq, "==", "Equality: `col == other`."},
    {:gt, ">", "Greater than: `col > other`."},
    {:gte, ">=", "Greater than or equal: `col >= other`."},
    {:lt, "<", "Less than: `col < other`."},
    {:lte, "<=", "Less than or equal: `col <= other`."},
    {:eq_null_safe, "<=>", "Null-safe equality."},
    {:and_, "and", "Logical AND."},
    {:or_, "or", "Logical OR."},
    {:plus, "+", "Addition: `col + other`."},
    {:minus, "-", "Subtraction: `col - other`."},
    {:multiply, "*", "Multiplication: `col * other`."},
    {:divide, "/", "Division: `col / other`."},
    {:mod, "%", "Modulo."},
    {:bitwise_and, "&", "Bitwise AND."},
    {:bitwise_or, "|", "Bitwise OR."},
    {:bitwise_xor, "^", "Bitwise XOR."},
    {:contains, "contains", "String contains."},
    {:starts_with, "startsWith", "String starts with."},
    {:ends_with, "endsWith", "String ends with."},
    {:like, "like", "SQL LIKE pattern match."},
    {:rlike, "rlike", "Regex pattern match."},
    {:ilike, "ilike", "Case-insensitive LIKE."}
  ]

  for {name, spark_name, doc} <- @binary_operators do
    @doc doc
    @spec unquote(name)(t(), t() | term()) :: t()
    def unquote(name)(%__MODULE__{} = left, %__MODULE__{} = right) do
      binary_fn(unquote(spark_name), left, right)
    end

    def unquote(name)(%__MODULE__{} = left, right) do
      binary_fn(unquote(spark_name), left, %__MODULE__{expr: {:lit, right}})
    end
  end

  @doc "Not equal: `col != other`. Encodes as `not(==(a, b))` matching PySpark."
  @spec neq(t(), t() | term()) :: t()
  def neq(%__MODULE__{} = left, right) do
    not_(eq(left, right))
  end

  # ── Generated unary operators ──

  @unary_operators [
    {:is_null, "isNull", "Returns true if the column is null."},
    {:is_not_null, "isNotNull", "Returns true if the column is not null."},
    {:is_nan, "isNaN", "Returns true if the column is NaN."},
    {:not_, "not", "Logical NOT."},
    {:bitwise_not, "~", "Bitwise NOT."},
    {:negate, "negative", "Unary negation."}
  ]

  for {name, spark_name, doc} <- @unary_operators do
    @doc doc
    @spec unquote(name)(t()) :: t()
    def unquote(name)(%__MODULE__{} = col) do
      %__MODULE__{expr: {:fn, unquote(spark_name), [col.expr], false}}
    end
  end

  # ── Type casting ──

  @doc """
  Casts the column to the given type.

  The type is a Spark SQL type string (e.g. `"int"`, `"string"`, `"double"`).
  """
  @spec cast(t(), String.t()) :: t()
  def cast(%__MODULE__{} = col, type_str) when is_binary(type_str) do
    %__MODULE__{expr: {:cast, col.expr, type_str}}
  end

  @doc """
  Try-casts the column to the given type. Returns null on cast failure instead of error.
  """
  @spec try_cast(t(), String.t()) :: t()
  def try_cast(%__MODULE__{} = col, type_str) when is_binary(type_str) do
    %__MODULE__{expr: {:cast, col.expr, type_str, :try}}
  end

  # ── Membership / range ──

  @doc "Returns true if the column value is in the given list of values or subquery DataFrame."
  @spec isin(t(), [term()] | DataFrame.t()) :: t()
  def isin(%__MODULE__{} = col, %DataFrame{} = df) do
    %__MODULE__{expr: {:subquery, :in, df.plan, [in_values: in_subquery_values(col.expr)]}}
  end

  def isin(%__MODULE__{} = col, [%DataFrame{} = df]), do: isin(col, df)

  def isin(%__MODULE__{} = col, values) when is_list(values) do
    value_exprs =
      Enum.map(values, fn
        %__MODULE__{expr: e} -> e
        v -> {:lit, v}
      end)

    %__MODULE__{expr: {:fn, "in", [col.expr | value_exprs], false}}
  end

  @doc "Returns true if the column value is between lower and upper (inclusive)."
  @spec between(t(), term(), term()) :: t()
  def between(%__MODULE__{} = col, lower, upper) do
    lower_expr = coerce_expr(lower)
    upper_expr = coerce_expr(upper)

    and_(
      %__MODULE__{expr: {:fn, ">=", [col.expr, lower_expr], false}},
      %__MODULE__{expr: {:fn, "<=", [col.expr, upper_expr], false}}
    )
  end

  # ── Struct/array/map access ──

  @doc "Extracts a value from an array by index or from a map by key."
  @spec get_item(t(), t() | term()) :: t()
  def get_item(%__MODULE__{} = col, %__MODULE__{} = key) do
    %__MODULE__{expr: {:unresolved_extract_value, col.expr, key.expr}}
  end

  def get_item(%__MODULE__{} = col, key) do
    %__MODULE__{expr: {:unresolved_extract_value, col.expr, {:lit, key}}}
  end

  @doc "Extracts a field from a struct column by name."
  @spec get_field(t(), String.t()) :: t()
  def get_field(%__MODULE__{} = col, field_name) when is_binary(field_name) do
    %__MODULE__{expr: {:unresolved_extract_value, col.expr, {:lit, field_name}}}
  end

  @doc """
  Adds or replaces a field in a struct column.
  """
  @spec with_field(t(), String.t(), t() | term()) :: t()
  def with_field(%__MODULE__{} = col, field_name, %__MODULE__{} = value)
      when is_binary(field_name) do
    %__MODULE__{expr: {:update_fields, col.expr, field_name, value.expr}}
  end

  def with_field(%__MODULE__{} = col, field_name, value) when is_binary(field_name) do
    %__MODULE__{expr: {:update_fields, col.expr, field_name, {:lit, value}}}
  end

  @doc """
  Drops fields from a struct column.
  """
  @spec drop_fields(t(), [String.t()]) :: t()
  def drop_fields(%__MODULE__{} = col, field_names) when is_list(field_names) do
    if field_names == [] do
      raise ArgumentError, "field names should not be empty"
    end

    Enum.reduce(field_names, col, fn field_name, acc ->
      if not is_binary(field_name) do
        raise ArgumentError, "field name must be a string, got: #{inspect(field_name)}"
      end

      %__MODULE__{expr: {:update_fields, acc.expr, field_name, nil}}
    end)
  end

  # ── String operations ──

  @doc "Returns a substring starting at `pos` for `len` characters."
  @spec substr(t(), t() | integer(), t() | integer()) :: t()
  def substr(%__MODULE__{} = col, %__MODULE__{} = pos, %__MODULE__{} = len) do
    %__MODULE__{
      expr: {:fn, "substr", [col.expr, pos.expr, len.expr], false}
    }
  end

  def substr(%__MODULE__{} = col, pos, len) when is_integer(pos) and is_integer(len) do
    %__MODULE__{
      expr: {:fn, "substr", [col.expr, {:lit, pos}, {:lit, len}], false}
    }
  end

  def substr(%__MODULE__{} = _col, _pos, _len) do
    raise ArgumentError, "startPos and length must be the same type: both Column or both integer"
  end

  @doc """
  Computes `col` raised to the given power.
  """
  @spec pow(t(), t() | term()) :: t()
  def pow(%__MODULE__{} = col, other) do
    %__MODULE__{expr: {:fn, "power", [col.expr, coerce_expr(other)], false}}
  end

  @doc """
  Alias for `pow/2`.
  """
  @spec power(t(), t() | term()) :: t()
  def power(%__MODULE__{} = col, other), do: pow(col, other)

  @doc """
  Adds a `when` condition to the column expression.
  """
  @spec when_(t(), t() | term()) :: t()
  def when_(%__MODULE__{} = col, %__MODULE__{} = value) do
    %__MODULE__{expr: {:fn, "when", [col.expr, value.expr], false}}
  end

  def when_(%__MODULE__{} = col, value) do
    %__MODULE__{expr: {:fn, "when", [col.expr, {:lit, value}], false}}
  end

  @doc """
  Adds a fallback value to a `when/2` expression chain.
  """
  @spec otherwise(t(), t() | term()) :: t()
  def otherwise(%__MODULE__{expr: {:fn, "when", args, false}} = _when_col, %__MODULE__{} = value) do
    if rem(length(args), 2) == 1 do
      raise ArgumentError, "otherwise() can only be called once on a when() expression"
    end

    %__MODULE__{expr: {:fn, "when", args ++ [value.expr], false}}
  end

  def otherwise(%__MODULE__{expr: {:fn, "when", args, false}} = _when_col, value) do
    if rem(length(args), 2) == 1 do
      raise ArgumentError, "otherwise() can only be called once on a when() expression"
    end

    %__MODULE__{expr: {:fn, "when", args ++ [{:lit, value}], false}}
  end

  # ── Window binding ──

  @doc """
  Defines a window specification for this column expression.

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      w = SparkEx.Window.partition_by(["dept"]) |> SparkEx.WindowSpec.order_by(["salary"])
      col("salary") |> SparkEx.Functions.row_number() |> SparkEx.Column.over(w)
  """
  @spec over(t(), SparkEx.WindowSpec.t()) :: t()
  def over(%__MODULE__{} = col, %SparkEx.WindowSpec{} = spec) do
    %__MODULE__{
      expr: {:window, col.expr, spec.partition_spec, spec.order_spec, spec.frame_spec}
    }
  end

  # ── Sort ordering ──

  @doc "Sort ascending (nulls first by default)"
  @spec asc(t()) :: t()
  def asc(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :asc, :nulls_first}}
  end

  @doc "Sort descending (nulls last by default)"
  @spec desc(t()) :: t()
  def desc(%__MODULE__{} = col) do
    %__MODULE__{expr: {:sort_order, col.expr, :desc, :nulls_last}}
  end

  @doc "Sort ascending with nulls first"
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

  # ── Naming ──

  @doc """
  Assigns an alias (name) to this column expression.

  Optionally accepts a `metadata` keyword with a JSON-serializable map.
  """
  @spec alias_(t(), String.t(), keyword()) :: t()
  def alias_(%__MODULE__{} = col, name, opts \\ []) when is_binary(name) do
    case Keyword.get(opts, :metadata) do
      nil ->
        %__MODULE__{expr: {:alias, col.expr, name}}

      metadata when is_map(metadata) ->
        metadata_json = Jason.encode!(metadata)
        %__MODULE__{expr: {:alias, col.expr, name, metadata_json}}
    end
  end

  @doc "Alias for `alias_/2`."
  @spec name(t(), String.t()) :: t()
  def name(%__MODULE__{} = col, alias_name) when is_binary(alias_name) do
    alias_(col, alias_name)
  end

  @doc "Alias for `cast/2`."
  @spec astype(t(), String.t()) :: t()
  def astype(%__MODULE__{} = col, type_str) when is_binary(type_str) do
    cast(col, type_str)
  end

  @doc "Marks this column for lateral join / generator context."
  @spec outer(t()) :: t()
  def outer(%__MODULE__{} = col) do
    %__MODULE__{expr: {:outer, col.expr}}
  end

  @doc """
  Applies a transformation function to this column.

  Equivalent to PySpark's `Column.transform(f)` which delegates to
  the `transform` SQL function.

  ## Examples

      col("arr") |> Column.transform(fn x -> Column.plus(x, lit(1)) end)
  """
  @spec transform(t(), (t() -> t())) :: t()
  def transform(%__MODULE__{} = col, func) when is_function(func, 1) do
    SparkEx.Functions.transform(col, func)
  end

  # ── Private helpers ──

  defp binary_fn(name, %__MODULE__{} = left, %__MODULE__{} = right) do
    %__MODULE__{expr: {:fn, name, [left.expr, right.expr], false}}
  end

  defp binary_fn(name, left, right) when is_number(left) do
    binary_fn(name, %__MODULE__{expr: {:lit, left}}, right)
  end

  defp binary_fn(name, left, right) when is_number(right) do
    binary_fn(name, left, %__MODULE__{expr: {:lit, right}})
  end

  defp binary_fn(name, left, %__MODULE__{} = right) do
    binary_fn(name, %__MODULE__{expr: {:lit, left}}, right)
  end

  defp binary_fn(name, %__MODULE__{} = left, right) do
    binary_fn(name, left, %__MODULE__{expr: {:lit, right}})
  end

  defp coerce_expr(%__MODULE__{expr: e}), do: e
  defp coerce_expr(value), do: {:lit, value}

  defp in_subquery_values({:fn, "struct", children, _is_distinct}), do: children
  defp in_subquery_values(expr), do: [expr]
end
