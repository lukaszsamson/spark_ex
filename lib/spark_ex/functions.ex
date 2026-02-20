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

  @doc """
  Builds a named argument expression.
  """
  @spec named_arg(String.t(), Column.t() | term()) :: Column.t()
  def named_arg(key, %Column{expr: expr}) when is_binary(key) do
    %Column{expr: {:named_arg, key, expr}}
  end

  def named_arg(key, value) when is_binary(key) do
    %Column{expr: {:named_arg, key, {:lit, value}}}
  end

  @doc """
  Calls a function with positional and named arguments.
  """
  @spec call_function(String.t(), [Column.t() | term()], keyword()) :: Column.t()
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
    %Column{expr: {:fn, "when", args ++ [value.expr], false}}
  end

  def otherwise(%Column{expr: {:fn, "when", args, false}} = _when_col, value) do
    %Column{expr: {:fn, "when", args ++ [{:lit, value}], false}}
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
  @spec aggregate(Column.t() | String.t(), Column.t() | term(), (Column.t(), Column.t() ->
                                                                   Column.t())) ::
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
  @spec reduce(Column.t() | String.t(), Column.t() | term(), (Column.t(), Column.t() ->
                                                                Column.t())) ::
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
  """
  @spec make_timestamp([Column.t() | String.t()]) :: Column.t()
  def make_timestamp(cols) when is_list(cols) do
    args = Enum.map(cols, &to_expr/1)
    %Column{expr: {:fn, "make_timestamp", args, false}}
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
    secs = to_expr(Keyword.get(opts, :secs, %Column{expr: {:lit, 0}}))

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
    fields = [:years, :months, :weeks, :days, :hours, :mins, :secs]
    args = Enum.map(fields, fn f -> to_expr(Keyword.get(opts, f, %Column{expr: {:lit, 0}})) end)
    %Column{expr: {:fn, "make_interval", args, false}}
  end

  @doc """
  Try version of `make_interval/1` — returns null on invalid input.
  """
  @spec try_make_interval(keyword()) :: Column.t()
  def try_make_interval(opts \\ []) do
    fields = [:years, :months, :weeks, :days, :hours, :mins, :secs]
    args = Enum.map(fields, fn f -> to_expr(Keyword.get(opts, f, %Column{expr: {:lit, 0}})) end)
    %Column{expr: {:fn, "try_make_interval", args, false}}
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

  ## Examples

      from_json(col("json_str"), "a INT, b STRING")
      from_json(col("json_str"), "a INT", %{"mode" => "FAILFAST"})
  """
  @spec from_json(Column.t() | String.t(), String.t(), map() | nil) :: Column.t()
  def from_json(col, schema, options \\ nil)
      when is_binary(schema) and (is_map(options) or is_nil(options)) do
    args =
      case options do
        nil -> [to_expr(col), {:lit, schema}]
        opts -> [to_expr(col), {:lit, schema}, options_expr(opts)]
      end

    %Column{expr: {:fn, "from_json", args, false}}
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
