defmodule SparkEx.Macros.FunctionRegistry do
  @moduledoc false

  # Declarative registry of Spark SQL functions.
  #
  # Each entry: {elixir_name, spark_name, arity_spec, opts}
  #
  # arity_spec is one of:
  #   :zero          — no arguments
  #   :one_col       — single Column|string argument
  #   :two_col       — two Column|string arguments
  #   :three_col     — three Column|string arguments
  #   :n_col         — variadic Column|string arguments (list)
  #   {:lit, n}      — n literal-coerced arguments (no column-leading argument)
  #   {:col_lit, n}  — first arg is Column, rest are literal-coerced
  #   {:lit_then_cols, n} — first n args are literal-coerced, rest is a Column list
  #   {:col_opt, defaults} — Column + optional keyword args with defaults
  #
  # opts (keyword):
  #   :doc       — function documentation string
  #   :group     — category atom for organization
  #   :aliases   — list of alternative Elixir function names
  #   :is_distinct — marks generated UnresolvedFunction as distinct aggregate

  @spec registry() :: [tuple()]
  def registry do
    math_functions() ++
      string_functions() ++
      date_functions() ++
      collection_functions() ++
      aggregate_functions() ++
      window_functions() ++
      conditional_functions() ++
      hash_functions() ++
      misc_functions()
  end

  defp math_functions do
    [
      {:abs, "abs", :one_col, group: :math, doc: "Computes the absolute value."},
      {:acos, "acos", :one_col, group: :math, doc: "Computes inverse cosine."},
      {:acosh, "acosh", :one_col, group: :math, doc: "Computes inverse hyperbolic cosine."},
      {:asin, "asin", :one_col, group: :math, doc: "Computes inverse sine."},
      {:asinh, "asinh", :one_col, group: :math, doc: "Computes inverse hyperbolic sine."},
      {:atan, "atan", :one_col, group: :math, doc: "Computes inverse tangent."},
      {:atan2, "atan2", :two_col, group: :math, doc: "Computes atan2(y, x)."},
      {:atanh, "atanh", :one_col, group: :math, doc: "Computes inverse hyperbolic tangent."},
      {:bin, "bin", :one_col, group: :math, doc: "Binary string representation of integer."},
      {:bround, "bround", {:col_opt, [scale: 0]}, group: :math,
       doc: "Banker's rounding to `scale` decimal places."},
      {:cbrt, "cbrt", :one_col, group: :math, doc: "Computes cube root."},
      {:ceil, "ceil", :one_col, group: :math, doc: "Computes ceiling.", aliases: [:ceiling]},
      {:conv, "conv", :three_col, group: :math, doc: "Converts number between bases."},
      {:cos, "cos", :one_col, group: :math, doc: "Computes cosine."},
      {:cosh, "cosh", :one_col, group: :math, doc: "Computes hyperbolic cosine."},
      {:cot, "cot", :one_col, group: :math, doc: "Computes cotangent."},
      {:csc, "csc", :one_col, group: :math, doc: "Computes cosecant."},
      {:degrees, "degrees", :one_col, group: :math, doc: "Converts radians to degrees.",
       aliases: [:to_degrees]},
      {:e, "e", :zero, group: :math, doc: "Returns Euler's number."},
      {:exp, "exp", :one_col, group: :math, doc: "Computes exponential."},
      {:expm1, "expm1", :one_col, group: :math, doc: "Computes exp(x) - 1."},
      {:factorial, "factorial", :one_col, group: :math, doc: "Computes factorial."},
      {:floor, "floor", :one_col, group: :math, doc: "Computes floor."},
      {:hex, "hex", :one_col, group: :math, doc: "Hex string of integer/binary."},
      {:hypot, "hypot", :two_col, group: :math, doc: "Computes sqrt(a^2 + b^2)."},
      {:log, "ln", :one_col, group: :math, doc: "Computes natural logarithm.", aliases: [:ln]},
      {:log2, "log2", :one_col, group: :math, doc: "Computes base-2 logarithm."},
      {:log10, "log10", :one_col, group: :math, doc: "Computes base-10 logarithm."},
      {:log1p, "log1p", :one_col, group: :math, doc: "Computes ln(1 + x)."},
      {:negative, "negative", :one_col, group: :math, doc: "Returns negation."},
      {:pi, "pi", :zero, group: :math, doc: "Returns pi."},
      {:pmod, "pmod", :two_col, group: :math, doc: "Positive modulo."},
      {:positive, "positive", :one_col, group: :math, doc: "Returns positive value."},
      {:pow, "power", :two_col, group: :math, doc: "Computes x raised to y.",
       aliases: [:power]},
      {:radians, "radians", :one_col, group: :math, doc: "Converts degrees to radians.",
       aliases: [:to_radians]},
      {:rint, "rint", :one_col, group: :math, doc: "Rounds to nearest integer."},
      {:round, "round", {:col_opt, [scale: 0]}, group: :math,
       doc: "Rounds to `scale` decimal places."},
      {:sec, "sec", :one_col, group: :math, doc: "Computes secant."},
      {:signum, "signum", :one_col, group: :math, doc: "Computes sign.", aliases: [:sign]},
      {:sin, "sin", :one_col, group: :math, doc: "Computes sine."},
      {:sinh, "sinh", :one_col, group: :math, doc: "Computes hyperbolic sine."},
      {:sqrt, "sqrt", :one_col, group: :math, doc: "Computes square root."},
      {:tan, "tan", :one_col, group: :math, doc: "Computes tangent."},
      {:tanh, "tanh", :one_col, group: :math, doc: "Computes hyperbolic tangent."},
      {:unhex, "unhex", :one_col, group: :math, doc: "Decodes hex string to binary."},
      {:width_bucket, "width_bucket", {:col_lit, 3}, group: :math,
       doc: "Returns bucket number for value in equi-width histogram."}
    ]
  end

  defp string_functions do
    [
      {:ascii, "ascii", :one_col, group: :string, doc: "ASCII value of first character."},
      {:char_length, "char_length", :one_col, group: :string,
       doc: "Character length of string.", aliases: [:character_length]},
      {:concat, "concat", :n_col, group: :string, doc: "Concatenates columns."},
      {:concat_ws, "concat_ws", {:lit_then_cols, 1}, group: :string,
       doc: "Concatenates with separator."},
      {:initcap, "initcap", :one_col, group: :string, doc: "Title-cases string."},
      {:lower, "lower", :one_col, group: :string, doc: "Converts to lowercase.",
       aliases: [:lcase]},
      {:upper, "upper", :one_col, group: :string, doc: "Converts to uppercase.",
       aliases: [:ucase]},
      {:ltrim, "ltrim", :one_col, group: :string, doc: "Left-trims whitespace."},
      {:rtrim, "rtrim", :one_col, group: :string, doc: "Right-trims whitespace."},
      {:trim, "trim", :one_col, group: :string, doc: "Trims whitespace from both ends."},
      {:lpad, "lpad", {:col_lit, 2}, group: :string,
       doc: "Left-pads string to length with pad string."},
      {:rpad, "rpad", {:col_lit, 2}, group: :string,
       doc: "Right-pads string to length with pad string."},
      {:repeat, "repeat", {:col_lit, 1}, group: :string, doc: "Repeats string n times."},
      {:reverse, "reverse", :one_col, group: :string, doc: "Reverses string or array."},
      {:soundex, "soundex", :one_col, group: :string, doc: "Soundex code."},
      {:substring, "substring", {:col_lit, 2}, group: :string,
       doc: "Returns substring from pos for len."},
      {:translate, "translate", :three_col, group: :string,
       doc: "Translates characters."},
      {:instr, "instr", :two_col, group: :string,
       doc: "Position of first occurrence of substr."},
      {:regexp_extract, "regexp_extract", {:col_lit, 2}, group: :string,
       doc: "Extracts regex group."},
      {:regexp_replace, "regexp_replace", :three_col, group: :string,
       doc: "Replaces regex matches."},
      {:format_string, "format_string", {:lit_then_cols, 1}, group: :string,
       doc: "printf-style formatting."},
      {:format_number, "format_number", {:col_lit, 1}, group: :string,
       doc: "Formats number with d decimal places."},
      {:base64, "base64", :one_col, group: :string, doc: "Base64 encodes binary."},
      {:unbase64, "unbase64", :one_col, group: :string, doc: "Decodes base64 string."},
      {:length, "length", :one_col, group: :string, doc: "Returns length of string or binary."}
    ]
  end

  defp date_functions do
    [
      {:current_date, "current_date", :zero, group: :datetime,
       doc: "Returns current date.", aliases: [:curdate]},
      {:current_timestamp, "current_timestamp", :zero, group: :datetime,
       doc: "Returns current timestamp.", aliases: [:now]},
      {:year, "year", :one_col, group: :datetime, doc: "Extracts year."},
      {:month, "month", :one_col, group: :datetime, doc: "Extracts month."},
      {:day, "day", :one_col, group: :datetime, doc: "Extracts day.",
       aliases: [:dayofmonth]},
      {:hour, "hour", :one_col, group: :datetime, doc: "Extracts hour."},
      {:minute, "minute", :one_col, group: :datetime, doc: "Extracts minute."},
      {:second, "second", :one_col, group: :datetime, doc: "Extracts second."},
      {:quarter, "quarter", :one_col, group: :datetime, doc: "Extracts quarter."},
      {:dayofweek, "dayofweek", :one_col, group: :datetime, doc: "Day of week (1=Sun)."},
      {:dayofyear, "dayofyear", :one_col, group: :datetime, doc: "Day of year."},
      {:weekofyear, "weekofyear", :one_col, group: :datetime, doc: "Week of year."},
      {:date_add, "date_add", {:col_lit, 1}, group: :datetime,
       doc: "Adds days to date.", aliases: [:dateadd]},
      {:date_sub, "date_sub", {:col_lit, 1}, group: :datetime,
       doc: "Subtracts days from date."},
      {:datediff, "datediff", :two_col, group: :datetime,
       doc: "Difference in days between dates.", aliases: [:date_diff]},
      {:date_format, "date_format", {:col_lit, 1}, group: :datetime,
       doc: "Formats date/timestamp with pattern."},
      {:add_months, "add_months", {:col_lit, 1}, group: :datetime,
       doc: "Adds months to date."},
      {:months_between, "months_between", :two_col, group: :datetime,
       doc: "Months between two dates."},
      {:last_day, "last_day", :one_col, group: :datetime,
       doc: "Last day of month for date."},
      {:to_date, "to_date", {:col_opt, [format: nil]}, group: :datetime,
       doc: "Converts to date, optionally with format."},
      {:to_timestamp, "to_timestamp", {:col_opt, [format: nil]}, group: :datetime,
       doc: "Converts to timestamp, optionally with format."},
      {:from_unixtime, "from_unixtime", {:col_opt, [format: nil]}, group: :datetime,
       doc: "Converts unix timestamp to string."},
      {:unix_timestamp, "unix_timestamp", {:col_opt, [format: nil]}, group: :datetime,
       doc: "Converts timestamp to unix seconds."}
    ]
  end

  defp collection_functions do
    [
      {:array, "array", :n_col, group: :collection, doc: "Creates array from columns."},
      {:create_map, "map", :n_col, group: :collection,
       doc: "Creates map from key-value column pairs."},
      {:struct, "struct", :n_col, group: :collection,
       doc: "Creates struct from columns."},
      {:array_contains, "array_contains", :two_col, group: :collection,
       doc: "Checks if array contains value."},
      {:array_distinct, "array_distinct", :one_col, group: :collection,
       doc: "Removes duplicates from array."},
      {:array_except, "array_except", :two_col, group: :collection,
       doc: "Returns elements in first but not second array."},
      {:array_intersect, "array_intersect", :two_col, group: :collection,
       doc: "Returns intersection of two arrays."},
      {:array_max, "array_max", :one_col, group: :collection,
       doc: "Returns max element of array."},
      {:array_min, "array_min", :one_col, group: :collection,
       doc: "Returns min element of array."},
      {:array_size, "array_size", :one_col, group: :collection,
       doc: "Returns array size.", aliases: [:size]},
      {:array_sort, "array_sort", :one_col, group: :collection,
       doc: "Sorts array in ascending order."},
      {:array_union, "array_union", :two_col, group: :collection,
       doc: "Returns union of two arrays."},
      {:element_at, "element_at", :two_col, group: :collection,
       doc: "Returns element at index/key."},
      {:explode, "explode", :one_col, group: :collection,
       doc: "Creates a row for each array/map element."},
      {:explode_outer, "explode_outer", :one_col, group: :collection,
       doc: "Like explode but preserves nulls."},
      {:flatten, "flatten", :one_col, group: :collection,
       doc: "Flattens nested array."},
      {:map_keys, "map_keys", :one_col, group: :collection, doc: "Returns map keys."},
      {:map_values, "map_values", :one_col, group: :collection,
       doc: "Returns map values."},
      {:map_entries, "map_entries", :one_col, group: :collection,
       doc: "Returns map entries as array of structs."},
      {:map_concat, "map_concat", :n_col, group: :collection,
       doc: "Concatenates maps."},
      {:map_from_arrays, "map_from_arrays", :two_col, group: :collection,
       doc: "Creates map from key and value arrays."},
      {:map_from_entries, "map_from_entries", :one_col, group: :collection,
       doc: "Creates map from array of entries."},
      {:slice, "slice", {:col_lit, 2}, group: :collection,
       doc: "Returns slice of array from start for length."},
      {:sort_array, "sort_array", {:col_opt, [asc: true]}, group: :collection,
       doc: "Sorts array."}
    ]
  end

  defp aggregate_functions do
    [
      {:count, "count", :one_col, group: :aggregate, doc: "Counts non-null values."},
      {:count_distinct, "count", :one_col, group: :aggregate,
       doc: "Counts distinct non-null values.", is_distinct: true},
      {:sum, "sum", :one_col, group: :aggregate, doc: "Computes sum."},
      {:avg, "avg", :one_col, group: :aggregate, doc: "Computes average.",
       aliases: [:mean]},
      {:min, "min", :one_col, group: :aggregate, doc: "Computes minimum."},
      {:max, "max", :one_col, group: :aggregate, doc: "Computes maximum."},
      {:first, "first", {:col_opt, [ignore_nulls: nil]}, group: :aggregate,
       doc: "Returns first value.", aliases: [:first_value]},
      {:last, "last", {:col_opt, [ignore_nulls: nil]}, group: :aggregate,
       doc: "Returns last value.", aliases: [:last_value]},
      {:collect_list, "collect_list", :one_col, group: :aggregate,
       doc: "Collects values into list.", aliases: [:array_agg]},
      {:collect_set, "collect_set", :one_col, group: :aggregate,
       doc: "Collects distinct values into set."},
      {:sum_distinct, "sum", :one_col, group: :aggregate,
       doc: "Computes sum of distinct values.", is_distinct: true},
      {:approx_count_distinct, "approx_count_distinct", :one_col, group: :aggregate,
       doc: "Approximate count of distinct values."},
      {:corr, "corr", :two_col, group: :aggregate, doc: "Pearson correlation."},
      {:covar_pop, "covar_pop", :two_col, group: :aggregate,
       doc: "Population covariance."},
      {:covar_samp, "covar_samp", :two_col, group: :aggregate,
       doc: "Sample covariance."},
      {:stddev, "stddev", :one_col, group: :aggregate,
       doc: "Sample standard deviation.", aliases: [:stddev_samp]},
      {:stddev_pop, "stddev_pop", :one_col, group: :aggregate,
       doc: "Population standard deviation."},
      {:variance, "variance", :one_col, group: :aggregate,
       doc: "Sample variance.", aliases: [:var_samp]},
      {:var_pop, "var_pop", :one_col, group: :aggregate, doc: "Population variance."},
      {:skewness, "skewness", :one_col, group: :aggregate, doc: "Skewness."},
      {:kurtosis, "kurtosis", :one_col, group: :aggregate, doc: "Kurtosis."},
      {:percentile_approx, "percentile_approx", {:col_lit, 2}, group: :aggregate,
       doc: "Approximate percentile."},
      {:mode, "mode", :one_col, group: :aggregate, doc: "Most frequent value."},
      {:any_value, "any_value", :one_col, group: :aggregate,
       doc: "Returns any value from group."},
      {:count_if, "count_if", :one_col, group: :aggregate,
       doc: "Counts rows where condition is true."},
      {:max_by, "max_by", :two_col, group: :aggregate,
       doc: "Value of first col at max of second."},
      {:min_by, "min_by", :two_col, group: :aggregate,
       doc: "Value of first col at min of second."},
      {:bool_and, "bool_and", :one_col, group: :aggregate,
       doc: "True if all values are true.", aliases: [:every]},
      {:bool_or, "bool_or", :one_col, group: :aggregate,
       doc: "True if any value is true.", aliases: [:some]},
      {:bit_and, "bit_and", :one_col, group: :aggregate, doc: "Bitwise AND aggregate."},
      {:bit_or, "bit_or", :one_col, group: :aggregate, doc: "Bitwise OR aggregate."},
      {:bit_xor, "bit_xor", :one_col, group: :aggregate, doc: "Bitwise XOR aggregate."},
      {:median, "median", :one_col, group: :aggregate, doc: "Median value."}
    ]
  end

  defp window_functions do
    [
      {:row_number, "row_number", :zero, group: :window,
       doc: "Row number within partition."},
      {:rank, "rank", :zero, group: :window, doc: "Rank within partition."},
      {:dense_rank, "dense_rank", :zero, group: :window,
       doc: "Dense rank within partition."},
      {:percent_rank, "percent_rank", :zero, group: :window,
       doc: "Percent rank within partition."},
      {:cume_dist, "cume_dist", :zero, group: :window,
       doc: "Cumulative distribution within partition."},
      {:ntile, "ntile", {:lit, 1}, group: :window,
       doc: "N-tile bucket number within partition."},
      {:lag, "lag", {:col_opt, [offset: 1, default: nil]}, group: :window,
       doc: "Value at offset rows before current."},
      {:lead, "lead", {:col_opt, [offset: 1, default: nil]}, group: :window,
       doc: "Value at offset rows after current."},
      {:nth_value, "nth_value", {:col_lit, 1}, group: :window,
       doc: "Returns nth value in window frame."}
    ]
  end

  defp conditional_functions do
    [
      {:coalesce, "coalesce", :n_col, group: :conditional,
       doc: "Returns first non-null value."},
      {:greatest, "greatest", :n_col, group: :conditional,
       doc: "Returns greatest value."},
      {:least, "least", :n_col, group: :conditional,
       doc: "Returns least value."},
      {:ifnull, "ifnull", :two_col, group: :conditional,
       doc: "Returns second value if first is null."},
      {:nullif, "nullif", :two_col, group: :conditional,
       doc: "Returns null if both values are equal."},
      {:nvl, "nvl", :two_col, group: :conditional,
       doc: "Returns second value if first is null."},
      {:nvl2, "nvl2", :three_col, group: :conditional,
       doc: "Returns second if first is not null, else third."},
      {:nanvl, "nanvl", :two_col, group: :conditional,
       doc: "Returns second value if first is NaN."},
      {:isnan, "isnan", :one_col, group: :conditional, doc: "True if NaN."},
      {:isnull, "isnull", :one_col, group: :conditional, doc: "True if null."},
      {:isnotnull, "isnotnull", :one_col, group: :conditional, doc: "True if not null."}
    ]
  end

  defp hash_functions do
    [
      {:md5, "md5", :one_col, group: :hash, doc: "MD5 hash."},
      {:sha1, "sha1", :one_col, group: :hash, doc: "SHA-1 hash."},
      {:sha2, "sha2", {:col_lit, 1}, group: :hash, doc: "SHA-2 hash with bit length."},
      {:crc32, "crc32", :one_col, group: :hash, doc: "CRC32 hash."},
      {:hash, "hash", :n_col, group: :hash, doc: "Murmur3 hash of columns."},
      {:xxhash64, "xxhash64", :n_col, group: :hash, doc: "xxHash64 of columns."}
    ]
  end

  defp misc_functions do
    [
      {:monotonically_increasing_id, "monotonically_increasing_id", :zero,
       group: :misc, doc: "Globally unique monotonically increasing ID."},
      {:spark_partition_id, "spark_partition_id", :zero,
       group: :misc, doc: "Partition ID of each row."},
      {:input_file_name, "input_file_name", :zero,
       group: :misc, doc: "Name of file being read."},
      {:typeof, "typeof", :one_col, group: :misc, doc: "Runtime data type string."},
      {:rand, "rand", {:lit_opt, [seed: nil]}, group: :misc,
       doc: "Random value in [0, 1)."},
      {:randn, "randn", {:lit_opt, [seed: nil]}, group: :misc,
       doc: "Random value from standard normal distribution."},
      {:grouping, "grouping", :one_col, group: :misc,
       doc: "Indicates whether column is aggregated in grouping set."},
      {:grouping_id, "grouping_id", :n_col, group: :misc,
       doc: "Grouping ID for grouping set."}
    ]
  end
end
