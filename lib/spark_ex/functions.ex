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

  # Excluded Kernel functions are redefined as Spark SQL equivalents below.
  # Note: Kernel.struct/1,2 is shadowed — use Kernel.struct/2 explicitly if needed in this module.
  import Kernel, except: [abs: 1, ceil: 1, floor: 1, round: 1, length: 1, struct: 1, struct: 2]

  alias SparkEx.Column
  alias SparkEx.Internal.ColumnName
  alias SparkEx.Internal.Random
  require SparkEx.Macros.FunctionGen

  # ── Core constructors (hand-written) ──

  @doc """
  Creates a column reference by name.

  ## Examples

      col("age")
      col("users.name")
  """
  @spec col(String.t() | atom()) :: Column.t()
  def col(name) when is_binary(name) or is_atom(name) do
    # Star routing lives in SparkEx.Internal.ColumnName: PySpark requires
    # UnresolvedStar.unparsed_target to end with ".*" (see
    # python/pyspark/sql/connect/expressions.py UnresolvedStar.__init__), so the
    # target keeps the full qualified name including the trailing ".*".
    %Column{expr: ColumnName.to_col_expr(name)}
  end

  @doc """
  Creates a literal value expression.

  If a `Column` is passed, it is returned as-is (pass-through).
  Supports nil, booleans, integers, floats, and strings.

  ## Examples

      lit(42)
      lit("hello")
      lit(true)
      lit(col("age"))  # returns the Column unchanged
  """
  @spec lit(term()) :: Column.t()
  def lit(%Column{} = col), do: col

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

  @doc """
  Builds a named argument expression.
  """
  @spec named_arg(String.t(), term()) :: Column.t()
  def named_arg(key, %Column{expr: expr}) when is_binary(key) do
    %Column{expr: {:named_arg, key, expr}}
  end

  def named_arg(key, value) when is_binary(key) do
    %Column{expr: {:named_arg, key, {:lit, value}}}
  end

  @doc """
  Calls a function with positional and named arguments.

  Positional binary (string) arguments are treated as column references
  (matching the rest of SparkEx's string-as-column convention); wrap with
  `lit/1` for literal strings.
  """
  @spec call_function(String.t(), list(), list()) :: Column.t()
  def call_function(name, args \\ [], named_args \\ [])
      when is_binary(name) and is_list(args) and is_list(named_args) do
    arg_exprs = Enum.map(args, &normalize_expr_arg/1)

    named_exprs =
      Enum.map(named_args, fn
        {key, %Column{expr: expr}} -> {:named_arg, to_string(key), expr}
        {key, value} -> {:named_arg, to_string(key), {:lit, value}}
      end)

    %Column{expr: {:call_function, name, arg_exprs ++ named_exprs}}
  end

  @doc """
  Returns the bucket number for a value and number of buckets.
  """
  @spec bucket(Column.t() | integer(), Column.t() | String.t()) :: Column.t()
  def bucket(num_buckets, col) do
    num_expr =
      case num_buckets do
        %Column{expr: expr} -> expr
        value when is_integer(value) -> {:lit, value}
      end

    %Column{expr: {:fn, "bucket", [num_expr, to_expr(col)], false}}
  end

  @doc """
  Extracts years from an interval expression.
  """
  @spec years(Column.t() | String.t()) :: Column.t()
  def years(col), do: %Column{expr: {:fn, "years", [to_expr(col)], false}}

  @doc """
  Extracts months from an interval expression.
  """
  @spec months(Column.t() | String.t()) :: Column.t()
  def months(col), do: %Column{expr: {:fn, "months", [to_expr(col)], false}}

  @doc """
  Extracts days from an interval expression.
  """
  @spec days(Column.t() | String.t()) :: Column.t()
  def days(col), do: %Column{expr: {:fn, "days", [to_expr(col)], false}}

  @doc """
  Extracts hours from an interval expression.
  """
  @spec hours(Column.t() | String.t()) :: Column.t()
  def hours(col), do: %Column{expr: {:fn, "hours", [to_expr(col)], false}}

  defp normalize_expr_arg(%Column{expr: expr}), do: expr
  defp normalize_expr_arg(value), do: to_expr(value)

  # ── Sort helpers (hand-written delegates) ──

  @doc "Sort ascending by the given column"
  @spec asc(Column.t()) :: Column.t()
  defdelegate asc(col), to: Column

  @doc "Sort ascending with nulls first"
  @spec asc_nulls_first(Column.t()) :: Column.t()
  defdelegate asc_nulls_first(col), to: Column

  @doc "Sort ascending with nulls last"
  @spec asc_nulls_last(Column.t()) :: Column.t()
  defdelegate asc_nulls_last(col), to: Column

  @doc "Sort descending by the given column"
  @spec desc(Column.t()) :: Column.t()
  defdelegate desc(col), to: Column

  @doc "Sort descending with nulls first"
  @spec desc_nulls_first(Column.t()) :: Column.t()
  defdelegate desc_nulls_first(col), to: Column

  @doc "Sort descending with nulls last"
  @spec desc_nulls_last(Column.t()) :: Column.t()
  defdelegate desc_nulls_last(col), to: Column

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
    if rem(Kernel.length(args), 2) == 1 do
      raise ArgumentError, "otherwise() has already been called on this when() expression"
    end

    %Column{expr: {:fn, "when", args ++ [value.expr], false}}
  end

  def otherwise(%Column{expr: {:fn, "when", args, false}} = _when_col, value) do
    if rem(Kernel.length(args), 2) == 1 do
      raise ArgumentError, "otherwise() has already been called on this when() expression"
    end

    %Column{expr: {:fn, "when", args ++ [{:lit, value}], false}}
  end

  @doc """
  Computes logarithm with the specified base.

  `log(col)` is defined in the registry as natural log (`ln`).
  `log(base, col)` computes `log_base(col)`.

  ## Examples

      log(2, col("x"))
      log(10, col("x"))
  """
  @spec log(number(), Column.t() | String.t()) :: Column.t()
  def log(%Column{expr: {:lit, base}}, col) when is_number(base) do
    log(base, col)
  end

  def log(base, col) when is_number(base) do
    %Column{expr: {:fn, "log", [lit_expr(base), to_expr(col)], false}}
  end

  def log(%Column{}, _col) do
    raise ArgumentError, "log/2 base must be a numeric literal or number"
  end

  @doc """
  Returns a DataFrame with a broadcast hint for join optimization.

  ## Examples

      broadcast(df)
  """
  @spec broadcast(SparkEx.DataFrame.t()) :: SparkEx.DataFrame.t()
  def broadcast(%SparkEx.DataFrame{} = df) do
    SparkEx.DataFrame.hint(df, "broadcast")
  end

  @doc """
  Computes atan2(y, x). Both arguments can be columns or numeric values.
  """
  @spec atan2(Column.t() | String.t() | number(), Column.t() | String.t() | number()) ::
          Column.t()
  def atan2(col1, col2) do
    %Column{expr: {:fn, "atan2", [to_col_or_lit(col1), to_col_or_lit(col2)], false}}
  end

  @doc """
  Computes x raised to the power of y. Both arguments can be columns or numeric values.
  """
  @spec pow(Column.t() | String.t() | number(), Column.t() | String.t() | number()) :: Column.t()
  def pow(col1, col2) do
    %Column{expr: {:fn, "power", [to_col_or_lit(col1), to_col_or_lit(col2)], false}}
  end

  @doc "Alias for `pow/2`."
  @spec power(Column.t() | String.t() | number(), Column.t() | String.t() | number()) ::
          Column.t()
  def power(col1, col2), do: pow(col1, col2)

  @doc """
  Returns any value from the group. Optionally ignores null values.
  """
  @spec any_value(Column.t() | String.t(), boolean()) :: Column.t()
  def any_value(col, ignore_nulls \\ false) do
    args = [to_expr(col), {:lit, ignore_nulls}]
    %Column{expr: {:fn, "any_value", args, false}}
  end

  @doc """
  Returns the nth value in a window frame. Optionally ignores null values.
  """
  @spec nth_value(Column.t() | String.t(), integer() | Column.t(), boolean()) :: Column.t()
  def nth_value(col, offset, ignore_nulls \\ false) do
    args = [to_expr(col), normalize_nth_value_offset(offset), {:lit, ignore_nulls}]
    %Column{expr: {:fn, "nth_value", args, false}}
  end

  @doc """
  Random value in [0, 1). Auto-generates a random seed when none given.
  Pass an explicit seed for reproducible results.
  """
  @spec rand(integer() | nil | keyword()) :: Column.t()
  def rand(seed \\ nil)

  def rand(opts) when is_list(opts) do
    rand(Keyword.get(opts, :seed))
  end

  def rand(seed) when is_integer(seed) or is_nil(seed) do
    seed = seed || Random.long_seed()
    %Column{expr: {:fn, "rand", [{:lit, seed}], false}}
  end

  @doc """
  Random value from standard normal distribution. Auto-generates a random seed when none given.
  Pass an explicit seed for reproducible results.
  """
  @spec randn(integer() | nil | keyword()) :: Column.t()
  def randn(seed \\ nil)

  def randn(opts) when is_list(opts) do
    randn(Keyword.get(opts, :seed))
  end

  def randn(seed) when is_integer(seed) or is_nil(seed) do
    seed = seed || Random.long_seed()
    %Column{expr: {:fn, "randn", [{:lit, seed}], false}}
  end

  @doc """
  Locates position of substring in a string column. Optional `pos` start position (default 1).
  """
  @spec locate(String.t(), Column.t() | String.t(), integer()) :: Column.t()
  def locate(substr, col, pos \\ 1)

  def locate(%Column{expr: {:lit, substr}}, col, pos) when is_binary(substr),
    do: locate(substr, col, pos)

  def locate(substr, col, %Column{expr: {:lit, pos}}) when is_integer(pos),
    do: locate(substr, col, pos)

  def locate(substr, col, pos) when is_binary(substr) and is_integer(pos) do
    %Column{expr: {:fn, "locate", [{:lit, substr}, to_expr(col), {:lit, pos}], false}}
  end

  def locate(_substr, _col, _pos) do
    raise ArgumentError, "locate/3 expects substr as string and pos as integer"
  end

  @doc """
  Returns the position of the first occurrence of `substr` within `str`.

  Both arguments are resolved as columns (string names become column refs).
  """
  @spec position(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
  def position(substr, str) do
    %Column{expr: {:fn, "position", [to_expr(substr), to_expr(str)], false}}
  end

  @doc """
  Returns the position of the first occurrence of `substr` within `str`,
  searching from `start`.

  `substr` and `str` resolve as columns; `start` may be an integer literal
  or a column / column-name string.
  """
  @spec position(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def position(substr, str, start) do
    %Column{
      expr: {:fn, "position", [to_expr(substr), to_expr(str), to_expr_or_lit_int(start)], false}
    }
  end

  @doc """
  Most frequent value in group.
  """
  @spec mode(Column.t() | String.t()) :: Column.t()
  def mode(col) do
    %Column{expr: {:fn, "mode", [to_expr(col)], false}}
  end

  @doc """
  Most frequent value in group. Optional deterministic parameter (Spark 4.x+).
  """
  @spec mode(Column.t() | String.t(), boolean()) :: Column.t()
  def mode(col, deterministic) when is_boolean(deterministic) do
    %Column{expr: {:fn, "mode", [to_expr(col), {:lit, deterministic}], false}}
  end

  @doc """
  Returns randomly shuffled array. PySpark generates a random seed when no
  seed is provided so the function is non-deterministic by default.
  """
  @spec shuffle(Column.t() | String.t()) :: Column.t()
  def shuffle(col) do
    shuffle(col, Random.long_seed())
  end

  @spec shuffle(Column.t() | String.t(), integer()) :: Column.t()
  def shuffle(col, seed) when is_integer(seed) do
    %Column{expr: {:fn, "shuffle", [to_expr(col), {:lit, seed}], false}}
  end

  @doc """
  Converts unix timestamp to string. Always sends format (default "yyyy-MM-dd HH:mm:ss").
  """
  @spec from_unixtime(Column.t() | String.t(), String.t()) :: Column.t()
  def from_unixtime(col, format \\ "yyyy-MM-dd HH:mm:ss")

  def from_unixtime(col, %Column{expr: {:lit, format}}) when is_binary(format) do
    from_unixtime(col, format)
  end

  def from_unixtime(col, format) when is_binary(format) do
    %Column{expr: {:fn, "from_unixtime", [to_expr(col), {:lit, format}], false}}
  end

  def from_unixtime(_col, _format) do
    raise ArgumentError, "from_unixtime/2 expects format as string literal"
  end

  @doc """
  Replaces occurrences of search string with empty string (2-arg form).
  """
  @spec replace(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
  def replace(src, search) do
    %Column{
      expr: {:fn, "replace", [to_expr(src), to_lit_string_or_expr(search)], false}
    }
  end

  @doc """
  Replaces occurrences of search string with the given replacement (3-arg form).
  """
  @spec replace(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t()) ::
          Column.t()
  def replace(src, search, replacement) do
    %Column{
      expr:
        {:fn, "replace",
         [to_expr(src), to_lit_string_or_expr(search), to_lit_string_or_expr(replacement)], false}
    }
  end

  defp to_col_or_lit(%Column{expr: e}), do: e
  defp to_col_or_lit(name) when is_binary(name), do: {:col, name}
  defp to_col_or_lit(value) when is_number(value), do: {:lit, value}

  @doc """
  Computes the ceiling of the given value.

  Optionally accepts a `scale` (Column or integer) controlling rounding precision.

  ## Examples

      ceil(col("x"))
      ceil(col("x"), 2)
  """
  @spec ceil(Column.t() | String.t()) :: Column.t()
  def ceil(col) do
    %Column{expr: {:fn, "ceil", [to_expr(col)], false}}
  end

  @spec ceil(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def ceil(col, scale) do
    %Column{expr: {:fn, "ceil", [to_expr(col), to_expr_or_lit_int(scale)], false}}
  end

  @doc """
  Alias for `ceil/1`.
  """
  @spec ceiling(Column.t() | String.t()) :: Column.t()
  def ceiling(col), do: ceil(col)

  @doc """
  Alias for `ceil/2`.
  """
  @spec ceiling(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def ceiling(col, scale), do: ceil(col, scale)

  @doc """
  Computes the floor of the given value.

  Optionally accepts a `scale` (Column or integer) controlling rounding precision.

  ## Examples

      floor(col("x"))
      floor(col("x"), 2)
  """
  @spec floor(Column.t() | String.t()) :: Column.t()
  def floor(col) do
    %Column{expr: {:fn, "floor", [to_expr(col)], false}}
  end

  @spec floor(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def floor(col, scale) do
    %Column{expr: {:fn, "floor", [to_expr(col), to_expr_or_lit_int(scale)], false}}
  end

  @doc """
  Splits string by regex pattern.

  ## Examples

      split(col("s"), "\\\\.")
      split(col("s"), "\\\\.", 3)
  """
  @spec split(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def split(col, pattern, limit \\ -1) do
    %Column{
      expr:
        {:fn, "split", [to_expr(col), to_lit_string_or_expr(pattern), to_expr_or_lit_int(limit)],
         false}
    }
  end

  @doc """
  Counts distinct non-null values.

  Accepts a single column or a list of columns for multi-column distinct count.

  ## Examples

      count_distinct(col("x"))
      count_distinct(["x", "y", "z"])
  """
  @spec count_distinct(Column.t() | String.t() | [Column.t() | String.t()]) :: Column.t()
  def count_distinct(cols) when is_list(cols) do
    %Column{expr: {:fn, "count", Enum.map(cols, &to_expr/1), true}}
  end

  def count_distinct(col) do
    %Column{expr: {:fn, "count", [to_expr(col)], true}}
  end

  @doc """
  Returns the number of months between two dates.

  Always sends 3 arguments with `roundOff` defaulting to `true`.

  ## Examples

      months_between(col("d1"), col("d2"))
      months_between(col("d1"), col("d2"), false)
  """
  @spec months_between(Column.t() | String.t(), Column.t() | String.t(), boolean()) :: Column.t()
  def months_between(date1, date2, round_off \\ true) do
    %Column{
      expr: {:fn, "months_between", [to_expr(date1), to_expr(date2), lit_expr(round_off)], false}
    }
  end

  @doc """
  Approximate count of distinct values.

  Optionally accepts a relative standard deviation parameter.

  ## Examples

      approx_count_distinct(col("x"))
      approx_count_distinct(col("x"), 0.05)
  """
  @spec approx_count_distinct(Column.t() | String.t(), float() | nil) :: Column.t()
  def approx_count_distinct(col, rsd \\ nil) do
    args =
      case rsd do
        nil -> [to_expr(col)]
        r -> [to_expr(col), lit_expr(r)]
      end

    %Column{expr: {:fn, "approx_count_distinct", args, false}}
  end

  @doc """
  Approximate percentile of a numeric column.

  `accuracy` defaults to `10000` (matching PySpark) and can be a Column or integer.
  `percentage` may be a single float, a list of floats, or a Column expression.

  ## Examples

      approx_percentile("value", [0.25, 0.5, 0.75])
      approx_percentile("value", 0.5, 1_000_000)
  """
  @spec approx_percentile(
          Column.t() | String.t(),
          Column.t() | float() | [float()],
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def approx_percentile(col, percentage, accuracy \\ 10_000) do
    pct_expr =
      case percentage do
        %Column{expr: e} -> e
        list when is_list(list) -> {:lit, list}
        other -> {:lit, other}
      end

    %Column{
      expr:
        {:fn, "approx_percentile", [to_expr(col), pct_expr, to_expr_or_lit_int(accuracy)], false}
    }
  end

  @doc """
  Extracts all matches for the given regex group.

  `idx` is optional; when omitted, the server defaults to group 1.

  `str` and `regexp` are both columns: a bare string names a column. Use
  `lit/1` to pass a literal pattern.

  ## Examples

      regexp_extract_all(col("s"), lit("(\\d+)"))
      regexp_extract_all(col("s"), lit("(\\d+)-(\\d+)"), 2)
      regexp_extract_all("s", "pattern_column")
  """
  @spec regexp_extract_all(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
  def regexp_extract_all(str, regexp) do
    %Column{
      expr: {:fn, "regexp_extract_all", [to_expr(str), to_expr(regexp)], false}
    }
  end

  @spec regexp_extract_all(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | integer()
        ) :: Column.t()
  def regexp_extract_all(str, regexp, idx) do
    %Column{
      expr:
        {:fn, "regexp_extract_all", [to_expr(str), to_expr(regexp), to_expr_or_lit_int(idx)],
         false}
    }
  end

  @doc """
  Builds a count-min sketch with the given relative error and confidence.

  `seed` is optional; when omitted, a random seed is generated client-side (matching PySpark).
  The server requires exactly 4 arguments (CountMinSketchAgg.scala:217-229).

  ## Examples

      count_min_sketch(col("id"), 1.0, 0.3)
      count_min_sketch(col("id"), 1.0, 0.3, 42)
  """
  @spec count_min_sketch(
          Column.t() | String.t(),
          Column.t() | number(),
          Column.t() | number()
        ) :: Column.t()
  def count_min_sketch(col, eps, confidence) do
    # PySpark always sends 4 args, generating a random seed client-side when none given.
    # builtin.py:1211: _seed = lit(py_random.randint(0, sys.maxsize)) if seed is None
    count_min_sketch(col, eps, confidence, Random.long_seed())
  end

  @spec count_min_sketch(
          Column.t() | String.t(),
          Column.t() | number(),
          Column.t() | number(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def count_min_sketch(col, eps, confidence, seed) do
    %Column{
      expr:
        {:fn, "count_min_sketch",
         [
           to_expr(col),
           to_expr_or_lit(eps),
           to_expr_or_lit(confidence),
           to_expr_or_lit_int(seed)
         ], false}
    }
  end

  @doc """
  Left-trims whitespace or specified characters.

  `trim_string` accepts a Column or string column name (bare strings resolve
  to column references, matching PySpark). Wrap with `lit/1` for a literal
  trim-character set.
  """
  @spec ltrim(Column.t() | String.t(), Column.t() | String.t() | nil) :: Column.t()
  def ltrim(col, trim_string \\ nil) do
    # PySpark sends (trim, src) — trim string FIRST — matching Spark's SQL form
    # ltrim(trimStr, srcStr) (catalyst StringTrimLeft 2-arg constructor).
    # builtin.py:2468-2472: _invoke_function_over_columns("ltrim", trim, col)
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(s), to_expr(col)]
      end

    %Column{expr: {:fn, "ltrim", args, false}}
  end

  @doc """
  Right-trims whitespace or specified characters.

  `trim_string` accepts a Column or string column name (bare strings resolve
  to column references, matching PySpark). Wrap with `lit/1` for a literal
  trim-character set.
  """
  @spec rtrim(Column.t() | String.t(), Column.t() | String.t() | nil) :: Column.t()
  def rtrim(col, trim_string \\ nil) do
    # PySpark sends (trim, src) — trim string FIRST.
    # builtin.py:2478-2482: _invoke_function_over_columns("rtrim", trim, col)
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(s), to_expr(col)]
      end

    %Column{expr: {:fn, "rtrim", args, false}}
  end

  @doc """
  Trims whitespace or specified characters from both ends.

  `trim_string` accepts a Column or string column name (bare strings resolve
  to column references, matching PySpark). Wrap with `lit/1` for a literal
  trim-character set.
  """
  @spec trim(Column.t() | String.t(), Column.t() | String.t() | nil) :: Column.t()
  def trim(col, trim_string \\ nil) do
    # PySpark sends (trim, src) — trim string FIRST.
    # builtin.py:2488-2492: _invoke_function_over_columns("trim", trim, col)
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(s), to_expr(col)]
      end

    %Column{expr: {:fn, "trim", args, false}}
  end

  @doc """
  Trims characters from both sides of a string.

  `trim_string` accepts a Column or string column name (bare strings resolve
  to column references, matching PySpark). Wrap with `lit/1` for a literal
  trim-character set.
  """
  @spec btrim(Column.t() | String.t(), Column.t() | String.t() | nil) :: Column.t()
  def btrim(col, trim_string \\ nil) do
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(col), to_expr(s)]
      end

    %Column{expr: {:fn, "btrim", args, false}}
  end

  @doc """
  Masks string characters, replacing uppercase letters, lowercase letters, digits,
  and other characters with configurable substitution characters.

  PySpark always sends all 5 args, substituting defaults when options are omitted:
  - `upper_char`: `"X"` (mask uppercase letters)
  - `lower_char`: `"x"` (mask lowercase letters)
  - `digit_char`: `"n"` (mask digits)
  - `other_char`: `nil` (retain other characters as-is, i.e. NULL = keep original)

  ## Examples

      mask(col("email"))
      mask(col("email"), upper_char: "A")
      mask(col("email"), digit_char: "0", other_char: "-")
  """
  @spec mask(Column.t() | String.t(), keyword()) :: Column.t()
  def mask(col, opts \\ [])

  def mask(col, opt_value) when not is_list(opt_value) do
    mask(col, upper_char: opt_value)
  end

  def mask(col, opts) when is_list(opts) do
    unless opts == [] or Keyword.keyword?(opts) do
      raise ArgumentError,
            "expected mask options to be a keyword list, got: #{inspect(opts)}"
    end

    # PySpark always sends all 4 optional args (builtin.py:3099-3106).
    # nil for other_char means "retain original character" (not "omit arg").
    upper = Keyword.get(opts, :upper_char, "X")
    lower = Keyword.get(opts, :lower_char, "x")
    digit = Keyword.get(opts, :digit_char, "n")
    other = Keyword.get(opts, :other_char, nil)

    %Column{
      expr:
        {:fn, "mask",
         [to_expr(col), lit_expr(upper), lit_expr(lower), lit_expr(digit), lit_expr(other)],
         false}
    }
  end

  @doc """
  Splits text into array of sentences.

  Optionally accepts language and country parameters.

  ## Examples

      sentences(col("text"))
      sentences(col("text"), "en", "US")
  """
  @spec sentences(Column.t() | String.t(), String.t() | nil, String.t() | nil) :: Column.t()
  def sentences(col, language \\ nil, country \\ nil) do
    # PySpark sends 3 args, defaulting language/country to lit("") when nil so the
    # locale-omitted overload is used server-side. Always emit 3 args here too.
    lang = if is_nil(language), do: "", else: language
    cty = if is_nil(country), do: "", else: country

    %Column{
      expr: {:fn, "sentences", [to_expr(col), lit_expr(lang), lit_expr(cty)], false}
    }
  end

  @doc """
  Levenshtein edit distance between strings.

  Optionally accepts a threshold parameter.

  ## Examples

      levenshtein(col("s1"), col("s2"))
      levenshtein(col("s1"), col("s2"), 5)
  """
  @spec levenshtein(Column.t() | String.t(), Column.t() | String.t(), integer() | nil) ::
          Column.t()
  def levenshtein(left, right, threshold \\ nil) do
    args =
      case threshold do
        nil -> [to_expr(left), to_expr(right)]
        t -> [to_expr(left), to_expr(right), lit_expr(t)]
      end

    %Column{expr: {:fn, "levenshtein", args, false}}
  end

  @doc """
  Joins array elements with delimiter.

  Optionally accepts a null_replacement string.

  ## Examples

      array_join(col("arr"), ",")
      array_join(col("arr"), ",", "NULL")
  """
  @spec array_join(Column.t() | String.t(), String.t(), String.t() | nil) :: Column.t()
  def array_join(col, delimiter, null_replacement \\ nil) do
    args =
      case null_replacement do
        nil -> [to_expr(col), lit_expr(delimiter)]
        nr -> [to_expr(col), lit_expr(delimiter), lit_expr(nr)]
      end

    %Column{expr: {:fn, "array_join", args, false}}
  end

  @doc """
  Returns slice of array from `start` for `length` elements.

  `start` and `length` may be Columns, string column names, or integer literals.
  Integer values are wrapped as literals.

  ## Examples

      slice(col("xs"), 1, 2)
      slice(col("xs"), col("s"), col("l"))
      slice(col("xs"), "s", "l")
  """
  @spec slice(
          Column.t() | String.t(),
          Column.t() | String.t() | integer(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def slice(col, start, length) do
    args = [to_expr(col), to_expr_or_lit_int(start), to_expr_or_lit_int(length)]
    %Column{expr: {:fn, "slice", args, false}}
  end

  @doc """
  Creates array of values from start to stop with optional step.

  ## Examples

      sequence(col("start"), col("stop"))
      sequence(col("start"), col("stop"), col("step"))
  """
  @spec sequence(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t() | nil) ::
          Column.t()
  def sequence(start, stop, step \\ nil) do
    args =
      case step do
        nil -> [to_expr(start), to_expr(stop)]
        s -> [to_expr(start), to_expr(stop), to_expr(s)]
      end

    %Column{expr: {:fn, "sequence", args, false}}
  end

  @doc """
  Raises error if condition is false.

  Optionally accepts an error message.

  ## Examples

      assert_true(col("cond"))
      assert_true(col("cond"), "Assertion failed!")
  """
  @spec assert_true(Column.t() | String.t(), String.t() | Column.t() | nil) :: Column.t()
  def assert_true(col, err_msg \\ nil) do
    args =
      case err_msg do
        nil -> [to_expr(col)]
        msg -> [to_expr(col), lit_expr(msg)]
      end

    %Column{expr: {:fn, "assert_true", args, false}}
  end

  @doc """
  Extracts fields from a JSON string column.

  First argument is the JSON column, remaining arguments are field name strings.

  ## Examples

      json_tuple(col("json_str"), ["name", "age"])
  """
  @spec json_tuple(Column.t() | String.t(), [String.t()]) :: Column.t()
  def json_tuple(col, fields) when is_list(fields) do
    args = [to_expr(col) | Enum.map(fields, &lit_expr/1)]
    %Column{expr: {:fn, "json_tuple", args, false}}
  end

  @doc """
  Calls a registered UDF by name with the given column arguments.

  Equivalent to PySpark's `call_udf`.
  """
  @spec call_udf(String.t(), [Column.t() | String.t()]) :: Column.t()
  def call_udf(name, cols) when is_binary(name) and is_list(cols) do
    %Column{expr: {:fn, name, Enum.map(cols, &to_expr/1), false}}
  end

  @doc """
  Returns the value of a user-defined type (UDT) as its underlying SQL representation.
  """
  @spec unwrap_udt(Column.t() | String.t()) :: Column.t()
  def unwrap_udt(col) do
    %Column{expr: {:fn, "unwrap_udt", [to_expr(col)], false}}
  end

  @doc """
  Decodes Avro binary using the provided JSON schema.
  """
  @spec from_avro(Column.t() | String.t(), String.t(), map() | nil) :: Column.t()
  def from_avro(col, json_schema, options \\ nil)
      when is_binary(json_schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, json_schema}]
        opts -> [to_expr(col), {:lit, json_schema}, options_expr(opts)]
      end

    %Column{expr: {:fn, "from_avro", args, false}}
  end

  @doc """
  Encodes a column to Avro binary using an optional JSON schema.
  """
  @spec to_avro(Column.t() | String.t(), String.t() | nil) :: Column.t()
  def to_avro(col, json_schema \\ nil) when is_binary(json_schema) or is_nil(json_schema) do
    args =
      case json_schema do
        nil -> [to_expr(col)]
        schema -> [to_expr(col), {:lit, schema}]
      end

    %Column{expr: {:fn, "to_avro", args, false}}
  end

  @doc """
  Decodes Protobuf binary using the provided message name and descriptor.

  Either `desc_file_path` or `binary_descriptor_set` can be provided (only one).
  """
  @spec from_protobuf(Column.t() | String.t(), String.t(), keyword()) :: Column.t()
  def from_protobuf(col, message_name, opts \\ [])
      when is_binary(message_name) and is_list(opts) do
    desc_file_path = Keyword.get(opts, :desc_file_path)
    binary_descriptor_set = Keyword.get(opts, :binary_descriptor_set)
    options = Keyword.get(opts, :options)

    if desc_file_path && binary_descriptor_set do
      raise ArgumentError, "provide only one of :desc_file_path or :binary_descriptor_set"
    end

    args =
      cond do
        binary_descriptor_set && options ->
          [
            to_expr(col),
            {:lit, message_name},
            {:lit, binary_descriptor_set},
            options_expr(options)
          ]

        binary_descriptor_set ->
          [to_expr(col), {:lit, message_name}, {:lit, binary_descriptor_set}]

        desc_file_path && options ->
          [
            to_expr(col),
            {:lit, message_name},
            {:lit, File.read!(desc_file_path)},
            options_expr(options)
          ]

        desc_file_path ->
          [to_expr(col), {:lit, message_name}, {:lit, File.read!(desc_file_path)}]

        options ->
          [to_expr(col), {:lit, message_name}, options_expr(options)]

        true ->
          [to_expr(col), {:lit, message_name}]
      end

    %Column{expr: {:fn, "from_protobuf", args, false}}
  end

  @doc """
  Encodes a column to Protobuf binary using the provided message name and descriptor.

  Either `desc_file_path` or `binary_descriptor_set` can be provided (only one).
  """
  @spec to_protobuf(Column.t() | String.t(), String.t(), keyword()) :: Column.t()
  def to_protobuf(col, message_name, opts \\ []) when is_binary(message_name) and is_list(opts) do
    desc_file_path = Keyword.get(opts, :desc_file_path)
    binary_descriptor_set = Keyword.get(opts, :binary_descriptor_set)
    options = Keyword.get(opts, :options)

    if desc_file_path && binary_descriptor_set do
      raise ArgumentError, "provide only one of :desc_file_path or :binary_descriptor_set"
    end

    args =
      cond do
        binary_descriptor_set && options ->
          [
            to_expr(col),
            {:lit, message_name},
            {:lit, binary_descriptor_set},
            options_expr(options)
          ]

        binary_descriptor_set ->
          [to_expr(col), {:lit, message_name}, {:lit, binary_descriptor_set}]

        desc_file_path && options ->
          [
            to_expr(col),
            {:lit, message_name},
            {:lit, File.read!(desc_file_path)},
            options_expr(options)
          ]

        desc_file_path ->
          [to_expr(col), {:lit, message_name}, {:lit, File.read!(desc_file_path)}]

        options ->
          [to_expr(col), {:lit, message_name}, options_expr(options)]

        true ->
          [to_expr(col), {:lit, message_name}]
      end

    %Column{expr: {:fn, "to_protobuf", args, false}}
  end

  # ── Higher-order functions (HOF) with lambda support ──

  @doc """
  Transforms each element in an array column using a function.

  The function receives a lambda variable `x` representing each element.

  ## Examples

      transform(col("arr"), fn x -> Column.plus(x, lit(1)) end)
  """
  @spec transform(
          Column.t() | String.t(),
          (Column.t() -> Column.t()) | (Column.t(), Column.t() -> Column.t())
        ) :: Column.t()
  def transform(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "transform", [col_expr, {:lambda, body, vars}], false}}
  end

  def transform(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x", "i"])

    %Column{expr: {:fn, "transform", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Filters an array column using a predicate function.

  ## Examples

      filter(col("arr"), fn x -> Column.gt(x, lit(0)) end)
  """
  @spec filter(
          Column.t() | String.t(),
          (Column.t() -> Column.t()) | (Column.t(), Column.t() -> Column.t())
        ) :: Column.t()
  def filter(col, func) when is_function(func, 1) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x"])

    %Column{expr: {:fn, "filter", [col_expr, {:lambda, body, vars}], false}}
  end

  def filter(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["x", "i"])

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
  An optional finish function can be applied to the final accumulator value.

  ## Examples

      aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end)
      aggregate(col("arr"), lit(0), fn acc, x -> Column.plus(acc, x) end, fn acc -> Column.cast(acc, "string") end)
  """
  @spec aggregate(
          Column.t() | String.t(),
          Column.t() | term(),
          (Column.t(), Column.t() -> Column.t()),
          (Column.t() -> Column.t()) | nil
        ) :: Column.t()
  def aggregate(col, zero, func, finish \\ nil)

  def aggregate(col, zero, func, nil) when is_function(func, 2) do
    col_expr = to_expr(col)
    zero_expr = to_expr_or_lit(zero)
    {body, vars} = build_lambda(func, ["acc", "x"])

    %Column{expr: {:fn, "aggregate", [col_expr, zero_expr, {:lambda, body, vars}], false}}
  end

  def aggregate(col, zero, func, finish) when is_function(func, 2) and is_function(finish, 1) do
    col_expr = to_expr(col)
    zero_expr = to_expr_or_lit(zero)
    {merge_body, merge_vars} = build_lambda(func, ["acc", "x"])
    {finish_body, finish_vars} = build_lambda(finish, ["acc"])

    %Column{
      expr:
        {:fn, "aggregate",
         [
           col_expr,
           zero_expr,
           {:lambda, merge_body, merge_vars},
           {:lambda, finish_body, finish_vars}
         ], false}
    }
  end

  @doc """
  Same shape as `aggregate/3`, but sends the SQL function name `reduce` on the wire,
  matching PySpark's `functions.reduce`.
  """
  @spec reduce(
          Column.t() | String.t(),
          Column.t() | term(),
          (Column.t(), Column.t() -> Column.t()),
          (Column.t() -> Column.t()) | nil
        ) :: Column.t()
  def reduce(col, zero, func, finish \\ nil)

  def reduce(col, zero, func, nil) when is_function(func, 2) do
    col_expr = to_expr(col)
    zero_expr = to_expr_or_lit(zero)
    {body, vars} = build_lambda(func, ["acc", "x"])

    %Column{expr: {:fn, "reduce", [col_expr, zero_expr, {:lambda, body, vars}], false}}
  end

  def reduce(col, zero, func, finish) when is_function(func, 2) and is_function(finish, 1) do
    col_expr = to_expr(col)
    zero_expr = to_expr_or_lit(zero)
    {merge_body, merge_vars} = build_lambda(func, ["acc", "x"])
    {finish_body, finish_vars} = build_lambda(finish, ["acc"])

    %Column{
      expr:
        {:fn, "reduce",
         [
           col_expr,
           zero_expr,
           {:lambda, merge_body, merge_vars},
           {:lambda, finish_body, finish_vars}
         ], false}
    }
  end

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

  # ── Windowing functions (hand-written due to complex signatures) ──

  @doc """
  Generates tumbling or sliding time window column for streaming aggregations.

  ## Examples

      window(col("timestamp"), "10 minutes")
      window(col("timestamp"), "10 minutes", "5 minutes")
      window(col("timestamp"), "10 minutes", "5 minutes", "2 minutes")
  """
  @spec window(Column.t() | String.t(), String.t(), String.t() | nil, String.t() | nil) ::
          Column.t()
  def window(time_col, window_duration, slide_duration \\ nil, start_time \\ nil) do
    check_window_duration!(window_duration, "window_duration")

    args =
      cond do
        slide_duration != nil and start_time != nil ->
          check_window_duration!(slide_duration, "slide_duration")
          check_window_duration!(start_time, "start_time")
          [to_expr(time_col), {:lit, window_duration}, {:lit, slide_duration}, {:lit, start_time}]

        slide_duration != nil ->
          check_window_duration!(slide_duration, "slide_duration")
          [to_expr(time_col), {:lit, window_duration}, {:lit, slide_duration}]

        start_time != nil ->
          check_window_duration!(start_time, "start_time")

          [
            to_expr(time_col),
            {:lit, window_duration},
            {:lit, window_duration},
            {:lit, start_time}
          ]

        true ->
          [to_expr(time_col), {:lit, window_duration}]
      end

    %Column{expr: {:fn, "window", args, false}}
  end

  defp check_window_duration!(value, _name) when is_binary(value) and value != "", do: :ok

  defp check_window_duration!(value, name) do
    raise ArgumentError,
          "expected #{name} to be a non-empty string duration, got: #{inspect(value)}"
  end

  # ── Timestamp construction functions (hand-written due to overloaded signatures) ──

  @doc """
  Creates a timestamp from individual components or from date+time columns.

  ## Examples

      make_timestamp(col("y"), col("m"), col("d"), col("h"), col("min"), col("sec"))
      make_timestamp(col("y"), col("m"), col("d"), col("h"), col("min"), col("sec"), col("tz"))
      make_timestamp(date: col("d"), time: col("t"))
      make_timestamp(date: col("d"), time: col("t"), timezone: col("tz"))
  """
  @spec make_timestamp([Column.t() | String.t()] | keyword()) :: Column.t()
  def make_timestamp(cols_or_opts) when is_list(cols_or_opts) do
    if Keyword.keyword?(cols_or_opts) do
      make_timestamp_from_keyword(cols_or_opts)
    else
      args = Enum.map(cols_or_opts, &to_expr/1)
      %Column{expr: {:fn, "make_timestamp", args, false}}
    end
  end

  @doc """
  Try version of `make_timestamp/1` — returns null on invalid input.
  """
  @spec try_make_timestamp([Column.t() | String.t()]) :: Column.t()
  def try_make_timestamp(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "try_make_timestamp", args, false}}
  end

  @doc """
  Creates a timestamp with local timezone from components.

  ## Examples

      make_timestamp_ltz([col("y"), col("m"), col("d"), col("h"), col("min"), col("sec")])
  """
  @spec make_timestamp_ltz([Column.t() | String.t()]) :: Column.t()
  def make_timestamp_ltz(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "make_timestamp_ltz", args, false}}
  end

  @doc """
  Try version of `make_timestamp_ltz/1` — returns null on invalid input.
  """
  @spec try_make_timestamp_ltz([Column.t() | String.t()]) :: Column.t()
  def try_make_timestamp_ltz(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "try_make_timestamp_ltz", args, false}}
  end

  @doc """
  Creates a timestamp without timezone from components.

  ## Examples

      make_timestamp_ntz([col("y"), col("m"), col("d"), col("h"), col("min"), col("sec")])
  """
  @spec make_timestamp_ntz([Column.t() | String.t()]) :: Column.t()
  def make_timestamp_ntz(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "make_timestamp_ntz", args, false}}
  end

  @doc """
  Try version of `make_timestamp_ntz/1` — returns null on invalid input.
  """
  @spec try_make_timestamp_ntz([Column.t() | String.t()]) :: Column.t()
  def try_make_timestamp_ntz(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "try_make_timestamp_ntz", args, false}}
  end

  defp make_timestamp_from_keyword(opts) do
    invalid_keys = Keyword.keys(opts) -- [:date, :time, :timezone]

    if invalid_keys != [] do
      raise ArgumentError, "unsupported make_timestamp options: #{inspect(invalid_keys)}"
    end

    if not (Keyword.has_key?(opts, :date) and Keyword.has_key?(opts, :time)) do
      raise ArgumentError, "make_timestamp keyword form requires :date and :time"
    end

    # Match PySpark 4.1+ behavior: make_timestamp(date, time[, timezone]) is a
    # native overload, so we send the date/time columns directly without
    # extracting year/month/day/hour/minute/second client-side.
    date_expr = to_expr(Keyword.fetch!(opts, :date))
    time_expr = to_expr(Keyword.fetch!(opts, :time))

    args = [date_expr, time_expr]

    args =
      if Keyword.has_key?(opts, :timezone) do
        args ++ [to_expr(Keyword.fetch!(opts, :timezone))]
      else
        args
      end

    %Column{expr: {:fn, "make_timestamp", args, false}}
  end

  # ── Interval construction functions ──

  @doc """
  Creates a day-time interval from optional components.

  ## Options

    * `:days` — days column (default: `lit(0)`)
    * `:hours` — hours column (default: `lit(0)`)
    * `:mins` — minutes column (default: `lit(0)`)
    * `:secs` — seconds column (default: `lit(0)`)
  """
  @spec make_dt_interval(keyword()) :: Column.t()
  def make_dt_interval(opts \\ []) do
    days = to_expr(Keyword.get(opts, :days, %Column{expr: {:lit, 0}}))
    hours = to_expr(Keyword.get(opts, :hours, %Column{expr: {:lit, 0}}))
    mins = to_expr(Keyword.get(opts, :mins, %Column{expr: {:lit, 0}}))
    secs = to_expr(Keyword.get(opts, :secs, %Column{expr: {:lit, Decimal.new(0)}}))

    %Column{expr: {:fn, "make_dt_interval", [days, hours, mins, secs], false}}
  end

  @doc """
  Creates an interval from optional components.

  ## Options

    * `:years`, `:months`, `:weeks`, `:days`, `:hours`, `:mins`, `:secs`
    All default to `lit(0)`.
  """
  @spec make_interval(keyword()) :: Column.t()
  def make_interval(opts \\ []) do
    int_fields = [:years, :months, :weeks, :days, :hours, :mins]

    int_args =
      Enum.map(int_fields, fn f ->
        to_expr(Keyword.get(opts, f, %Column{expr: {:lit, 0}}))
      end)

    secs = to_expr(Keyword.get(opts, :secs, %Column{expr: {:lit, Decimal.new(0)}}))
    %Column{expr: {:fn, "make_interval", int_args ++ [secs], false}}
  end

  @doc """
  Try version of `make_interval/1` — returns null on invalid input.
  """
  @spec try_make_interval(keyword()) :: Column.t()
  def try_make_interval(opts \\ []) do
    int_fields = [:years, :months, :weeks, :days, :hours, :mins]

    int_args =
      Enum.map(int_fields, fn f ->
        to_expr(Keyword.get(opts, f, %Column{expr: {:lit, 0}}))
      end)

    secs = to_expr(Keyword.get(opts, :secs, %Column{expr: {:lit, Decimal.new(0)}}))
    %Column{expr: {:fn, "try_make_interval", int_args ++ [secs], false}}
  end

  @doc """
  Creates a year-month interval from optional components.

  ## Options

    * `:years` — years column (default: `lit(0)`)
    * `:months` — months column (default: `lit(0)`)
  """
  @spec make_ym_interval(keyword()) :: Column.t()
  def make_ym_interval(opts \\ []) do
    years = to_expr(Keyword.get(opts, :years, %Column{expr: {:lit, 0}}))
    months = to_expr(Keyword.get(opts, :months, %Column{expr: {:lit, 0}}))
    %Column{expr: {:fn, "make_ym_interval", [years, months], false}}
  end

  # ── JSON/CSV/XML parsing functions ──

  @doc """
  Parses a JSON string column into a struct/array/map column using the given schema.

  The schema can be a DDL string, a `%Column{}` expression (e.g. from `schema_of_json/1`),
  or a Spark DataType protobuf struct.

  ## Examples

      from_json(col("json_str"), "a INT, b STRING")
      from_json(col("json_str"), schema_of_json(col("json_str")))
      from_json(col("json_str"), "a INT", %{"mode" => "FAILFAST"})
  """
  @spec from_json(
          Column.t() | String.t(),
          String.t() | Column.t() | SparkEx.Types.data_type_proto(),
          map() | nil
        ) ::
          Column.t()
  def from_json(col, schema, options \\ nil)

  def from_json(col, schema, options)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
      end

    %Column{expr: {:fn, "from_json", args, false}}
  end

  def from_json(col, %Column{} = schema, options)
      when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col), to_expr(schema)]
        opts -> [to_expr(col), to_expr(schema), options_expr(opts)]
      end

    %Column{expr: {:fn, "from_json", args, false}}
  end

  def from_json(col, %Spark.Connect.DataType{} = schema, options)
      when is_map(options) or is_nil(options) do
    from_json(col, SparkEx.Types.data_type_to_json(schema), options)
  end

  @doc """
  Converts a struct/array/map column to a JSON string.

  ## Examples

      to_json(col("struct_col"))
      to_json(col("struct_col"), %{"pretty" => "true"})
  """
  @spec to_json(Column.t() | String.t(), map() | nil) :: Column.t()
  def to_json(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "to_json", args, false}}
  end

  @doc """
  Parses a CSV string column into a struct column using the given schema.

  The schema can be a DDL string or a `%Column{}` expression (e.g. from `schema_of_csv/1`).

  ## Examples

      from_csv(col("csv_str"), "a INT, b STRING")
      from_csv(col("csv_str"), schema_of_csv(col("csv_str")))
      from_csv(col("csv_str"), "a INT, b STRING", %{"sep" => "|"})
  """
  @spec from_csv(Column.t() | String.t(), String.t() | Column.t(), map() | nil) :: Column.t()
  def from_csv(col, schema, options \\ nil)

  def from_csv(col, schema, options)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
      end

    %Column{expr: {:fn, "from_csv", args, false}}
  end

  def from_csv(col, %Column{} = schema, options)
      when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col), to_expr(schema)]
        opts -> [to_expr(col), to_expr(schema), options_expr(opts)]
      end

    %Column{expr: {:fn, "from_csv", args, false}}
  end

  @doc """
  Converts a struct column to a CSV string.

  ## Examples

      to_csv(col("struct_col"))
      to_csv(col("struct_col"), %{"sep" => "|"})
  """
  @spec to_csv(Column.t() | String.t(), map() | nil) :: Column.t()
  def to_csv(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "to_csv", args, false}}
  end

  @doc """
  Parses an XML string column into a struct column using the given schema.

  The schema can be a DDL string or a `%Column{}` expression (e.g. from `schema_of_xml/1`).

  ## Examples

      from_xml(col("xml_str"), "a INT, b STRING")
      from_xml(col("xml_str"), schema_of_xml(col("xml_str")))
      from_xml(col("xml_str"), "a INT, b STRING", %{"rowTag" => "item"})
  """
  @spec from_xml(Column.t() | String.t(), String.t() | Column.t(), map() | nil) :: Column.t()
  def from_xml(col, schema, options \\ nil)

  def from_xml(col, schema, options)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
      end

    %Column{expr: {:fn, "from_xml", args, false}}
  end

  def from_xml(col, %Column{} = schema, options)
      when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col), to_expr(schema)]
        opts -> [to_expr(col), to_expr(schema), options_expr(opts)]
      end

    %Column{expr: {:fn, "from_xml", args, false}}
  end

  @doc """
  Converts a struct column to an XML string.

  ## Examples

      to_xml(col("struct_col"))
      to_xml(col("struct_col"), %{"rowTag" => "item"})
  """
  @spec to_xml(Column.t() | String.t(), map() | nil) :: Column.t()
  def to_xml(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "to_xml", args, false}}
  end

  @doc """
  Returns DDL schema string of JSON string. Accepts optional options map.

  Binary inputs are encoded as a string literal (matching PySpark's
  `schema_of_json("...")`) rather than a column reference.
  """
  @spec schema_of_json(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_json(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [lit_expr(col)]
        opts -> [lit_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "schema_of_json", args, false}}
  end

  @doc "Parses a JSON string into a Variant value (Spark 4.x+)."
  @spec parse_json(Column.t() | String.t()) :: Column.t()
  def parse_json(col) do
    %Column{expr: {:fn, "parse_json", [to_expr(col)], false}}
  end

  @doc "Parses a JSON string into a Variant; returns NULL on failure (Spark 4.x+)."
  @spec try_parse_json(Column.t() | String.t()) :: Column.t()
  def try_parse_json(col) do
    %Column{expr: {:fn, "try_parse_json", [to_expr(col)], false}}
  end

  @doc "Returns true when the Variant value is JSON null (Spark 4.x+)."
  @spec is_variant_null(Column.t() | String.t()) :: Column.t()
  def is_variant_null(col) do
    %Column{expr: {:fn, "is_variant_null", [to_expr(col)], false}}
  end

  @doc """
  Extracts a sub-value from a Variant column at the given JSON path,
  cast to `target_type` (a DDL type string or column expression).
  """
  @spec variant_get(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t()) ::
          Column.t()
  def variant_get(col, path, target_type) do
    args = [to_expr(col), to_lit_string_or_expr(path), to_lit_string_or_expr(target_type)]
    %Column{expr: {:fn, "variant_get", args, false}}
  end

  @doc """
  Like `variant_get/3` but returns NULL on cast failure instead of erroring.
  """
  @spec try_variant_get(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t()) ::
          Column.t()
  def try_variant_get(col, path, target_type) do
    args = [to_expr(col), to_lit_string_or_expr(path), to_lit_string_or_expr(target_type)]
    %Column{expr: {:fn, "try_variant_get", args, false}}
  end

  @doc "Wraps a struct/map value into a Variant object (Spark 4.x+)."
  @spec to_variant_object(Column.t() | String.t()) :: Column.t()
  def to_variant_object(col) do
    %Column{expr: {:fn, "to_variant_object", [to_expr(col)], false}}
  end

  @doc "Returns the inferred DDL schema of a Variant column (Spark 4.x+)."
  @spec schema_of_variant(Column.t() | String.t()) :: Column.t()
  def schema_of_variant(col) do
    %Column{expr: {:fn, "schema_of_variant", [to_expr(col)], false}}
  end

  @doc "Aggregate variant of `schema_of_variant/1` (Spark 4.x+)."
  @spec schema_of_variant_agg(Column.t() | String.t()) :: Column.t()
  def schema_of_variant_agg(col) do
    %Column{expr: {:fn, "schema_of_variant_agg", [to_expr(col)], false}}
  end

  @doc """
  Returns DDL schema string of CSV string. Accepts optional options map.

  Binary inputs are encoded as a string literal.
  """
  @spec schema_of_csv(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_csv(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [lit_expr(col)]
        opts -> [lit_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "schema_of_csv", args, false}}
  end

  @doc """
  Returns DDL schema string of XML string. Accepts optional options map.

  Binary inputs are encoded as a string literal.
  """
  @spec schema_of_xml(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_xml(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [lit_expr(col)]
        opts -> [lit_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "schema_of_xml", args, false}}
  end

  # ── Additional hand-written functions (optional parameters, seed support, etc.) ──

  @doc "Converts timestamp to unix seconds. Can be called with no args for current timestamp."
  @spec unix_timestamp() :: Column.t()
  def unix_timestamp do
    %Column{expr: {:fn, "unix_timestamp", [], false}}
  end

  @spec unix_timestamp(Column.t() | String.t(), keyword()) :: Column.t()
  def unix_timestamp(col, opts \\ []) do
    format = Keyword.get(opts, :format)

    args =
      case format do
        nil -> [to_expr(col)]
        f -> [to_expr(col), lit_expr(f)]
      end

    %Column{expr: {:fn, "unix_timestamp", args, false}}
  end

  @doc "Converts timestamp between timezones. 2-arg form uses session timezone as source."
  @spec convert_timezone(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
  def convert_timezone(target_tz, source_ts) do
    %Column{expr: {:fn, "convert_timezone", [to_expr(target_tz), to_expr(source_ts)], false}}
  end

  @spec convert_timezone(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t()
        ) :: Column.t()
  def convert_timezone(source_tz, target_tz, source_ts) do
    %Column{
      expr:
        {:fn, "convert_timezone", [to_expr(source_tz), to_expr(target_tz), to_expr(source_ts)],
         false}
    }
  end

  @doc """
  Parses a string column to a TIME value (Spark 4.x+).

  Emits an unresolved `to_time` call so Spark returns the native TIME
  type rather than a formatted string.
  """
  @spec to_time(Column.t() | String.t(), keyword()) :: Column.t()
  def to_time(col, opts \\ []) when is_list(opts) do
    args =
      case Keyword.get(opts, :format) do
        nil -> [to_expr(col)]
        format -> [to_expr(col), lit_expr(format)]
      end

    %Column{expr: {:fn, "to_time", args, false}}
  end

  @doc "Like `to_time/2` but returns NULL on parse failure (Spark 4.x+)."
  @spec try_to_time(Column.t() | String.t(), keyword()) :: Column.t()
  def try_to_time(col, opts \\ []) when is_list(opts) do
    args =
      case Keyword.get(opts, :format) do
        nil -> [to_expr(col)]
        format -> [to_expr(col), lit_expr(format)]
      end

    %Column{expr: {:fn, "try_to_time", args, false}}
  end

  @doc """
  Returns the difference between two times measured in the specified units.

  Spark 4.1+. Unit is passed as a column expression (use `lit/1` for string literals).
  Supported units: "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND".

  ## Examples

      time_diff(lit("HOUR"), col("start_time"), col("end_time"))
  """
  @spec time_diff(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t()) ::
          Column.t()
  def time_diff(unit, start_time, end_time) do
    %Column{
      expr: {:fn, "time_diff", [to_expr(unit), to_expr(start_time), to_expr(end_time)], false}
    }
  end

  @doc """
  Truncates a TIME value to the given unit (Spark 4.x+).

  Emits an unresolved `time_trunc` call so Spark operates on TIME rather
  than re-routing through `date_trunc` (which expects a timestamp).

  Both `unit` and `time_col` accept a `Column` or a bare string (resolved as a
  column reference). To pass a literal unit string such as `"HOUR"`, wrap it
  with `lit/1`:

      time_trunc(lit("HOUR"), col("ts"))
  """
  @spec time_trunc(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
  def time_trunc(unit, time_col) do
    %Column{expr: {:fn, "time_trunc", [to_expr(unit), to_expr(time_col)], false}}
  end

  @doc """
  Extracts a part of a URL. Optional key for query string extraction.

  All string arguments are resolved as column references. To pass a literal
  part name such as `"HOST"`, wrap it with `lit/1`:

      parse_url(col("url"), lit("HOST"))
  """
  @spec parse_url(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t() | nil) ::
          Column.t()
  def parse_url(url, part, key \\ nil) do
    args =
      case key do
        nil -> [to_expr(url), to_expr(part)]
        k -> [to_expr(url), to_expr(part), to_expr(k)]
      end

    %Column{expr: {:fn, "parse_url", args, false}}
  end

  @doc """
  Try to extract a part of a URL, returns null on failure. Optional key for query string.

  All string arguments are resolved as column references. To pass a literal
  part name such as `"HOST"`, wrap it with `lit/1`:

      try_parse_url(col("url"), lit("HOST"))
  """
  @spec try_parse_url(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t() | nil
        ) :: Column.t()
  def try_parse_url(url, part, key \\ nil) do
    args =
      case key do
        nil -> [to_expr(url), to_expr(part)]
        k -> [to_expr(url), to_expr(part), to_expr(k)]
      end

    %Column{expr: {:fn, "try_parse_url", args, false}}
  end

  @doc "Returns substring from pos. Optional len parameter."
  @spec substr_(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t() | nil) ::
          Column.t()
  def substr_(str, pos, len \\ nil) do
    args =
      case len do
        nil -> [to_expr(str), to_expr(pos)]
        l -> [to_expr(str), to_expr(pos), to_expr(l)]
      end

    %Column{expr: {:fn, "substr", args, false}}
  end

  @doc """
  SQL LIKE pattern match. Optional escape character.

  All string arguments are resolved as column references. To pass a literal
  pattern such as `"%abc%"`, wrap it with `lit/1`:

      like_(col("name"), lit("%abc%"))
  """
  @spec like_(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t() | nil) ::
          Column.t()
  def like_(col, pattern, escape \\ nil) do
    args =
      case escape do
        nil -> [to_expr(col), to_expr(pattern)]
        e -> [to_expr(col), to_expr(pattern), to_expr(e)]
      end

    %Column{expr: {:fn, "like", args, false}}
  end

  @doc """
  Case-insensitive LIKE. Optional escape character.

  All string arguments are resolved as column references. To pass a literal
  pattern such as `"%abc%"`, wrap it with `lit/1`:

      ilike_(col("name"), lit("%abc%"))
  """
  @spec ilike_(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t() | nil) ::
          Column.t()
  def ilike_(col, pattern, escape \\ nil) do
    args =
      case escape do
        nil -> [to_expr(col), to_expr(pattern)]
        e -> [to_expr(col), to_expr(pattern), to_expr(e)]
      end

    %Column{expr: {:fn, "ilike", args, false}}
  end

  @doc "Sorts array in ascending order. Optional comparator function."
  @spec array_sort(Column.t() | String.t()) :: Column.t()
  def array_sort(col) do
    %Column{expr: {:fn, "array_sort", [to_expr(col)], false}}
  end

  @spec array_sort(Column.t() | String.t(), (Column.t(), Column.t() -> Column.t())) :: Column.t()
  def array_sort(col, func) when is_function(func, 2) do
    col_expr = to_expr(col)
    {body, vars} = build_lambda(func, ["l", "r"])
    %Column{expr: {:fn, "array_sort", [col_expr, {:lambda, body, vars}], false}}
  end

  @doc """
  Exact percentile. Supports single percentage or list/array of percentages.

  Optional frequency parameter (default 1).
  """
  @spec percentile(Column.t() | String.t(), number() | [number()], Column.t() | integer()) ::
          Column.t()
  def percentile(col, percentage, frequency \\ 1) do
    pct_expr =
      case percentage do
        pcts when is_list(pcts) -> {:fn, "array", Enum.map(pcts, &lit_expr/1), false}
        pct -> lit_expr(pct)
      end

    %Column{
      expr: {:fn, "percentile", [to_expr(col), pct_expr, to_col_or_lit(frequency)], false}
    }
  end

  @doc """
  Approximate percentile with PySpark's default accuracy (10_000).

  Equivalent to `percentile_approx(col, percentage, 10_000)`.
  """
  @spec percentile_approx(Column.t() | String.t(), number() | [number()]) :: Column.t()
  def percentile_approx(col, percentage) do
    percentile_approx(col, percentage, 10_000)
  end

  @doc """
  Generates a random UUID string.

  Mirrors PySpark's `uuid()` (connect/functions/builtin.py), which bakes a
  random long seed into the expression rather than sending a seedless call.
  """
  @spec uuid() :: Column.t()
  def uuid do
    uuid(Random.long_seed())
  end

  @doc """
  Generates a random UUID string with deterministic seed (Spark 4.x+).

  `seed` accepts a `Column`, a string column name, or an integer literal.
  """
  @spec uuid(Column.t() | String.t() | integer()) :: Column.t()
  def uuid(seed) do
    %Column{expr: {:fn, "uuid", [to_expr_or_lit_int(seed)], false}}
  end

  @doc """
  Random value uniformly distributed in [min, max) (Spark 4.x+).

  Auto-generates a seed when none given. `min`/`max` accept Column,
  numeric, or column-name binary inputs.
  """
  @spec uniform(
          Column.t() | String.t() | number(),
          Column.t() | String.t() | number(),
          Column.t() | integer() | nil
        ) :: Column.t()
  def uniform(min, max, seed \\ nil) do
    seed_expr =
      case seed do
        nil ->
          {:lit, Random.long_seed()}

        %Column{expr: e} ->
          e

        s when is_integer(s) ->
          {:lit, s}

        other ->
          raise ArgumentError, "seed must be a Column, integer, or nil; got #{inspect(other)}"
      end

    args = [
      normalize_uniform_bound(min),
      normalize_uniform_bound(max),
      seed_expr
    ]

    %Column{expr: {:fn, "uniform", args, false}}
  end

  @doc """
  Generates a random string of the given length (Spark 4.x+).

  Auto-generates a seed when none given. `length` accepts a column or
  integer.
  """
  @spec randstr(Column.t() | String.t() | integer(), Column.t() | integer() | nil) :: Column.t()
  def randstr(length, seed \\ nil) do
    seed_expr =
      case seed do
        nil ->
          {:lit, Random.long_seed()}

        %Column{expr: e} ->
          e

        s when is_integer(s) ->
          {:lit, s}

        other ->
          raise ArgumentError, "seed must be a Column, integer, or nil; got #{inspect(other)}"
      end

    %Column{expr: {:fn, "randstr", [to_col_or_lit(length), seed_expr], false}}
  end

  @doc """
  Returns the bucket number into which the value `v` would be assigned in an
  equi-width histogram with `num_bucket` buckets in the range `[min, max]`.

  `v`, `min`, and `max` accept Column or string column names (bare strings
  resolve to column references). `num_bucket` accepts a Column, string column
  name, or integer literal.
  """
  @spec width_bucket(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def width_bucket(v, min, max, num_bucket) do
    args = [to_expr(v), to_expr(min), to_expr(max), to_expr_or_lit_int(num_bucket)]
    %Column{expr: {:fn, "width_bucket", args, false}}
  end

  @doc """
  Returns the value of the bit at the given position.

  `col` accepts a Column or string column name. `pos` accepts a Column,
  string column name, or integer literal (bare strings resolve to column
  references).
  """
  @spec bit_get(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def bit_get(col, pos) do
    args = [to_expr(col), to_expr_or_lit_int(pos)]
    %Column{expr: {:fn, "bit_get", args, false}}
  end

  @doc "Alias for `bit_get/2`."
  @spec getbit(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def getbit(col, pos), do: bit_get(col, pos)

  @doc """
  Repeats string `s` `n` times.

  `s` accepts a Column or string column name. `n` accepts a Column, string
  column name, or integer literal (bare strings resolve to column references).
  """
  @spec repeat(Column.t() | String.t(), Column.t() | String.t() | integer()) :: Column.t()
  def repeat(s, n) do
    args = [to_expr(s), to_expr_or_lit_int(n)]
    %Column{expr: {:fn, "repeat", args, false}}
  end

  @doc """
  Returns the position of the n-th occurrence (capture group `idx`) of the
  regex `regexp` in `str`.

  `str` and `regexp` both accept a Column or string column name; a bare string
  names a column, so use `lit/1` for a literal pattern. `idx` is wrapped as a
  literal.
  """
  @spec regexp_instr(Column.t() | String.t(), Column.t() | String.t() | term(), term()) ::
          Column.t()
  def regexp_instr(str, regexp, idx) do
    args = [to_expr(str), to_expr(regexp), lit_expr(idx)]
    %Column{expr: {:fn, "regexp_instr", args, false}}
  end

  # ── Internal helpers (used by generated functions) ──

  @doc """
  Overlays `replace` over `src` starting at `pos` for `len` characters.

  All arguments accept Column or string column names.
  `len` defaults to `-1` (replace entire match length).
  """
  @spec overlay(
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t(),
          Column.t() | String.t() | integer()
        ) :: Column.t()
  def overlay(src, replace, pos, len \\ -1) do
    args = [to_expr(src), to_expr(replace), to_expr(pos), to_expr_or_lit(len)]
    %Column{expr: {:fn, "overlay", args, false}}
  end

  @doc false
  def to_expr(%Column{expr: e}), do: e
  def to_expr(nil), do: {:lit, nil}
  def to_expr(value) when is_boolean(value), do: {:lit, value}
  def to_expr(value) when is_number(value), do: {:lit, value}
  # Route through col/1 so "*" / "x.*" become UnresolvedStar rather than an
  # UnresolvedAttribute the server rejects (count("*"), count_distinct("*"), ...).
  def to_expr(name) when is_binary(name), do: col(name).expr
  def to_expr(name) when is_atom(name), do: col(Atom.to_string(name)).expr

  @doc false
  def lit_expr(%Column{expr: e}), do: e
  def lit_expr(value), do: {:lit, value}

  @doc false
  @spec fresh_lambda_name(String.t()) :: String.t()
  def fresh_lambda_name(base) when is_binary(base) do
    base <> "_" <> Integer.to_string(:erlang.unique_integer([:positive, :monotonic]))
  end

  # ── Private helpers for HOF lambda construction ──

  defp build_lambda(func, var_names) do
    vars = Enum.map(var_names, fn name -> {:lambda_var, fresh_lambda_name(name)} end)
    col_args = Enum.map(vars, fn var -> %Column{expr: var} end)
    %Column{expr: body} = apply(func, col_args)
    {body, vars}
  end

  defp to_expr_or_lit(%Column{expr: e}), do: e
  defp to_expr_or_lit(value), do: {:lit, value}

  defp to_expr_or_lit_int(%Column{expr: e}), do: e
  defp to_expr_or_lit_int(name) when is_binary(name), do: {:col, name}

  defp to_expr_or_lit_int(name) when is_atom(name) and not is_nil(name) and not is_boolean(name),
    do: {:col, Atom.to_string(name)}

  defp to_expr_or_lit_int(value) when is_integer(value), do: {:lit, value}

  defp to_expr_or_lit_int(other) do
    raise ArgumentError,
          "expected a Column, string/atom column name, or integer literal, got: #{inspect(other)}"
  end

  defp normalize_uniform_bound(%Column{expr: expr}), do: expr
  defp normalize_uniform_bound(name) when is_binary(name), do: {:col, name}
  defp normalize_uniform_bound(value), do: {:lit, value}

  defp to_lit_string_or_expr(%Column{expr: e}), do: e
  defp to_lit_string_or_expr(value) when is_binary(value), do: {:lit, value}
  defp to_lit_string_or_expr(value), do: to_expr(value)

  defp normalize_nth_value_offset(offset) when is_integer(offset), do: {:lit, offset}

  defp normalize_nth_value_offset(%Column{expr: {:lit, offset}}) when is_integer(offset),
    do: {:lit, offset}

  defp normalize_nth_value_offset(other) do
    raise ArgumentError,
          "nth_value/3 expects integer offset or literal column, got: #{inspect(other)}"
  end

  defp options_expr(options) when is_map(options) do
    kvs =
      options
      |> Enum.flat_map(fn {k, v} -> [lit_expr(to_string(k)), lit_expr(to_string(v))] end)

    {:fn, "map", kvs, false}
  end

  defp options_expr(options) do
    raise ArgumentError, "options must be a map, got: #{inspect(options)}"
  end
end
