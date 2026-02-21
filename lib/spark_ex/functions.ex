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
  def col("*"), do: %Column{expr: {:star}}

  def col(name) when is_binary(name) do
    %Column{expr: {:col, name}}
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
  defp normalize_expr_arg(value) when is_binary(value), do: {:lit, value}
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
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
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
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
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
  Most frequent value in group. Optional `deterministic` flag (default false).
  """
  @spec mode(Column.t() | String.t(), boolean()) :: Column.t()
  def mode(col, deterministic \\ false) do
    %Column{expr: {:fn, "mode", [to_expr(col), {:lit, deterministic}], false}}
  end

  @doc """
  Returns randomly shuffled array. Optional `seed` parameter.
  """
  @spec shuffle(Column.t() | String.t(), integer() | nil) :: Column.t()
  def shuffle(col, seed \\ nil) do
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
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
  Replaces occurrences of search string. When `replacement` is omitted, uses empty string.
  """
  @spec replace(Column.t() | String.t(), Column.t() | String.t(), Column.t() | String.t()) ::
          Column.t()
  def replace(src, search, replacement \\ "") do
    %Column{
      expr: {:fn, "replace", [to_expr(src), to_expr(search), to_expr(replacement)], false}
    }
  end

  defp to_col_or_lit(%Column{expr: e}), do: e
  defp to_col_or_lit(name) when is_binary(name), do: {:col, name}
  defp to_col_or_lit(value) when is_number(value), do: {:lit, value}

  @doc """
  Splits string by regex pattern.

  ## Examples

      split(col("s"), "\\\\.")
      split(col("s"), "\\\\.", 3)
  """
  @spec split(Column.t() | String.t(), String.t(), integer() | nil) :: Column.t()
  def split(col, pattern, limit \\ nil) do
    args =
      case limit do
        nil -> [to_expr(col), lit_expr(pattern)]
        n -> [to_expr(col), lit_expr(pattern), lit_expr(n)]
      end

    %Column{expr: {:fn, "split", args, false}}
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

  @doc "Left-trims whitespace or specified characters."
  @spec ltrim(Column.t() | String.t(), String.t() | nil) :: Column.t()
  def ltrim(col, trim_string \\ nil) do
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(col), lit_expr(s)]
      end

    %Column{expr: {:fn, "ltrim", args, false}}
  end

  @doc "Right-trims whitespace or specified characters."
  @spec rtrim(Column.t() | String.t(), String.t() | nil) :: Column.t()
  def rtrim(col, trim_string \\ nil) do
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(col), lit_expr(s)]
      end

    %Column{expr: {:fn, "rtrim", args, false}}
  end

  @doc "Trims whitespace or specified characters from both ends."
  @spec trim(Column.t() | String.t(), String.t() | nil) :: Column.t()
  def trim(col, trim_string \\ nil) do
    args =
      case trim_string do
        nil -> [to_expr(col)]
        s -> [to_expr(col), lit_expr(s)]
      end

    %Column{expr: {:fn, "trim", args, false}}
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
    args =
      case {language, country} do
        {nil, nil} -> [to_expr(col)]
        {l, c} -> [to_expr(col), lit_expr(l), lit_expr(c)]
      end

    %Column{expr: {:fn, "sentences", args, false}}
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
  Alias for `aggregate/3`.
  """
  @spec reduce(
          Column.t() | String.t(),
          Column.t() | term(),
          (Column.t(), Column.t() -> Column.t()),
          (Column.t() -> Column.t()) | nil
        ) :: Column.t()
  def reduce(col, zero, func, finish \\ nil), do: aggregate(col, zero, func, finish)

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
    args =
      cond do
        slide_duration != nil and start_time != nil ->
          [to_expr(time_col), {:lit, window_duration}, {:lit, slide_duration}, {:lit, start_time}]

        slide_duration != nil ->
          [to_expr(time_col), {:lit, window_duration}, {:lit, slide_duration}]

        start_time != nil ->
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

    args = [to_expr(Keyword.fetch!(opts, :date)), to_expr(Keyword.fetch!(opts, :time))]

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

  The schema can be a DDL string or `%Spark.Connect.DataType{}`.

  ## Examples

      from_json(col("json_str"), "a INT, b STRING")
      from_json(col("json_str"), "a INT", %{"mode" => "FAILFAST"})
  """
  @spec from_json(Column.t() | String.t(), String.t() | Spark.Connect.DataType.t(), map() | nil) ::
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

  ## Examples

      from_csv(col("csv_str"), "a INT, b STRING")
      from_csv(col("csv_str"), "a INT, b STRING", %{"sep" => "|"})
  """
  @spec from_csv(Column.t() | String.t(), String.t(), map() | nil) :: Column.t()
  def from_csv(col, schema, options \\ nil)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
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

  ## Examples

      from_xml(col("xml_str"), "a INT, b STRING")
      from_xml(col("xml_str"), "a INT, b STRING", %{"rowTag" => "item"})
  """
  @spec from_xml(Column.t() | String.t(), String.t(), map() | nil) :: Column.t()
  def from_xml(col, schema, options \\ nil)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
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

  @doc "Returns DDL schema string of JSON string. Accepts optional options map."
  @spec schema_of_json(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_json(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "schema_of_json", args, false}}
  end

  @doc "Returns DDL schema string of CSV string. Accepts optional options map."
  @spec schema_of_csv(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_csv(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
      end

    %Column{expr: {:fn, "schema_of_csv", args, false}}
  end

  @doc "Returns DDL schema string of XML string. Accepts optional options map."
  @spec schema_of_xml(Column.t() | String.t(), map() | nil) :: Column.t()
  def schema_of_xml(col, options \\ nil) when is_map(options) or is_nil(options) do
    args =
      case options do
        nil -> [to_expr(col)]
        opts -> [to_expr(col), options_expr(opts)]
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

  @doc "Extracts a part of a URL. Optional key for query string extraction."
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

  @doc "Try to extract a part of a URL, returns null on failure. Optional key for query string."
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

  @doc "SQL LIKE pattern match. Optional escape character."
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

  @doc "Case-insensitive LIKE. Optional escape character."
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

  @doc "Generates a random UUID string. Auto-generates seed when none given."
  @spec uuid(integer() | nil) :: Column.t()
  def uuid(seed \\ nil) do
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
    %Column{expr: {:fn, "uuid", [{:lit, seed}], false}}
  end

  @doc "Random value uniformly distributed in [min, max). Auto-generates seed when none given."
  @spec uniform(Column.t() | String.t(), term(), integer() | nil) :: Column.t()
  def uniform(min, max, seed \\ nil) do
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
    %Column{expr: {:fn, "uniform", [to_expr(min), lit_expr(max), {:lit, seed}], false}}
  end

  @doc "Generates random string of given length. Auto-generates seed when none given."
  @spec randstr(Column.t() | String.t(), term(), integer() | nil) :: Column.t()
  def randstr(length, charset_or_seed \\ nil, seed \\ nil)

  def randstr(length, nil, nil) do
    seed = :rand.uniform(9_223_372_036_854_775_807)
    %Column{expr: {:fn, "randstr", [to_expr(length), {:lit, seed}], false}}
  end

  def randstr(length, seed, nil) when is_integer(seed) do
    %Column{expr: {:fn, "randstr", [to_expr(length), {:lit, seed}], false}}
  end

  def randstr(length, charset, seed) do
    seed = seed || :rand.uniform(9_223_372_036_854_775_807)
    %Column{expr: {:fn, "randstr", [to_expr(length), lit_expr(charset), {:lit, seed}], false}}
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

  defp normalize_nth_value_offset(offset) when is_integer(offset), do: {:lit, offset}
  defp normalize_nth_value_offset(%Column{expr: {:lit, offset}}) when is_integer(offset), do: {:lit, offset}

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
