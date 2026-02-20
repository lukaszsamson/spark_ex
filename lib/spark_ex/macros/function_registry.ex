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
  #   {:lit_opt, defaults} — optional keyword args only, no column arg
  #
  # opts (keyword):
  #   :doc       — function documentation string
  #   :group     — category atom for organization
  #   :aliases   — list of alternative Elixir function names
  #   :is_distinct — marks generated UnresolvedFunction as distinct aggregate

  @spec registry() :: [tuple()]
  def registry do
    math_functions() ++
      bitwise_functions() ++
      string_functions() ++
      date_functions() ++
      collection_functions() ++
      aggregate_functions() ++
      window_functions() ++
      conditional_functions() ++
      hash_functions() ++
      json_functions() ++
      csv_xml_functions() ++
      type_functions() ++
      encryption_functions() ++
      session_functions() ++
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
      # atan2 hand-written in functions.ex to accept numeric literals
      {:atanh, "atanh", :one_col, group: :math, doc: "Computes inverse hyperbolic tangent."},
      {:bin, "bin", :one_col, group: :math, doc: "Binary string representation of integer."},
      {:bround, "bround", {:col_opt, [scale: 0]},
       group: :math, doc: "Banker's rounding to `scale` decimal places."},
      {:cbrt, "cbrt", :one_col, group: :math, doc: "Computes cube root."},
      {:ceil, "ceil", :one_col, group: :math, doc: "Computes ceiling.", aliases: [:ceiling]},
      {:conv, "conv", :three_col, group: :math, doc: "Converts number between bases."},
      {:cos, "cos", :one_col, group: :math, doc: "Computes cosine."},
      {:cosh, "cosh", :one_col, group: :math, doc: "Computes hyperbolic cosine."},
      {:cot, "cot", :one_col, group: :math, doc: "Computes cotangent."},
      {:csc, "csc", :one_col, group: :math, doc: "Computes cosecant."},
      {:degrees, "degrees", :one_col,
       group: :math, doc: "Converts radians to degrees.", aliases: [:to_degrees]},
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
      # pow hand-written in functions.ex to accept numeric literals
      {:radians, "radians", :one_col,
       group: :math, doc: "Converts degrees to radians.", aliases: [:to_radians]},
      {:rint, "rint", :one_col, group: :math, doc: "Rounds to nearest integer."},
      {:round, "round", {:col_opt, [scale: 0]},
       group: :math, doc: "Rounds to `scale` decimal places."},
      {:sec, "sec", :one_col, group: :math, doc: "Computes secant."},
      {:signum, "signum", :one_col, group: :math, doc: "Computes sign.", aliases: [:sign]},
      {:sin, "sin", :one_col, group: :math, doc: "Computes sine."},
      {:sinh, "sinh", :one_col, group: :math, doc: "Computes hyperbolic sine."},
      {:sqrt, "sqrt", :one_col, group: :math, doc: "Computes square root."},
      {:tan, "tan", :one_col, group: :math, doc: "Computes tangent."},
      {:tanh, "tanh", :one_col, group: :math, doc: "Computes hyperbolic tangent."},
      {:unhex, "unhex", :one_col, group: :math, doc: "Decodes hex string to binary."},
      {:width_bucket, "width_bucket", {:col_lit, 3},
       group: :math, doc: "Returns bucket number for value in equi-width histogram."},
      {:try_add, "try_add", :two_col,
       group: :math, doc: "Try addition, returns null on overflow."},
      {:try_divide, "try_divide", :two_col,
       group: :math, doc: "Try division, returns null on division by zero."},
      {:try_multiply, "try_multiply", :two_col,
       group: :math, doc: "Try multiplication, returns null on overflow."},
      {:try_subtract, "try_subtract", :two_col,
       group: :math, doc: "Try subtraction, returns null on overflow."},
      {:try_mod, "try_mod", :two_col,
       group: :math, doc: "Try modulo, returns null on division by zero."},
      {:try_sum, "try_sum", :one_col, group: :math, doc: "Try sum, returns null on overflow."},
      {:try_avg, "try_avg", :one_col,
       group: :math, doc: "Try average, returns null on overflow."},
      {:product, "product", :one_col, group: :math, doc: "Computes product of all values."},
      # uniform hand-written in functions.ex to support seed parameter
      # {:uniform, ...} — see Functions.uniform/2,3
    ]
  end

  defp bitwise_functions do
    [
      {:bit_count, "bit_count", :one_col, group: :bitwise, doc: "Counts number of set bits."},
      {:bit_get, "bit_get", {:col_lit, 1},
       group: :bitwise,
       doc: "Returns the value of the bit at the given position.",
       aliases: [:getbit]},
      {:bit_length, "bit_length", :one_col,
       group: :bitwise, doc: "Returns bit length of string."},
      {:shiftleft, "shiftleft", {:col_lit, 1}, group: :bitwise, doc: "Bitwise left shift."},
      {:shiftright, "shiftright", {:col_lit, 1}, group: :bitwise, doc: "Bitwise right shift."},
      {:shiftrightunsigned, "shiftrightunsigned", {:col_lit, 1},
       group: :bitwise, doc: "Bitwise unsigned right shift."},
      {:bitwise_not_, "~", :one_col,
       group: :bitwise, doc: "Bitwise NOT (standalone function)."}
    ]
  end

  defp string_functions do
    [
      {:ascii, "ascii", :one_col, group: :string, doc: "ASCII value of first character."},
      {:char_length, "char_length", :one_col,
       group: :string, doc: "Character length of string.", aliases: [:character_length]},
      {:concat, "concat", :n_col, group: :string, doc: "Concatenates columns."},
      {:concat_ws, "concat_ws", {:lit_then_cols, 1},
       group: :string, doc: "Concatenates with separator."},
      {:initcap, "initcap", :one_col, group: :string, doc: "Title-cases string."},
      {:lower, "lower", :one_col,
       group: :string, doc: "Converts to lowercase.", aliases: [:lcase]},
      {:upper, "upper", :one_col,
       group: :string, doc: "Converts to uppercase.", aliases: [:ucase]},
      # ltrim/rtrim/trim are hand-written in Functions to support optional trim character
      # See Functions.ltrim/1,2, Functions.rtrim/1,2, Functions.trim/1,2
      {:btrim, "btrim", {:col_opt, [trim_string: nil]},
       group: :string, doc: "Trims characters from both sides."},
      {:lpad, "lpad", {:col_lit, 2},
       group: :string, doc: "Left-pads string to length with pad string."},
      {:rpad, "rpad", {:col_lit, 2},
       group: :string, doc: "Right-pads string to length with pad string."},
      {:repeat, "repeat", {:col_lit, 1}, group: :string, doc: "Repeats string n times."},
      {:reverse, "reverse", :one_col, group: :string, doc: "Reverses string or array."},
      {:soundex, "soundex", :one_col, group: :string, doc: "Soundex code."},
      {:substring, "substring", {:col_lit, 2},
       group: :string, doc: "Returns substring from pos for len."},
      {:substring_index, "substring_index", {:col_lit, 2},
       group: :string, doc: "Returns substring before count occurrences of delimiter."},
      {:translate, "translate", :three_col, group: :string, doc: "Translates characters."},
      {:instr, "instr", :two_col, group: :string, doc: "Position of first occurrence of substr."},
      {:regexp_extract, "regexp_extract", {:col_lit, 2},
       group: :string, doc: "Extracts regex group."},
      {:regexp_replace, "regexp_replace", :three_col,
       group: :string, doc: "Replaces regex matches."},
      {:regexp_count, "regexp_count", :two_col,
       group: :string, doc: "Counts regex pattern occurrences."},
      {:regexp_extract_all, "regexp_extract_all", {:col_lit, 2},
       group: :string, doc: "Extracts all matches for regex group."},
      {:regexp_instr, "regexp_instr", {:col_lit, 1},
       group: :string, doc: "Returns position of first regex match."},
      {:regexp_substr, "regexp_substr", {:col_lit, 1},
       group: :string, doc: "Returns first substring matching regex."},
      {:regexp_like, "regexp_like", {:col_lit, 1},
       group: :string, doc: "Returns true if column matches regex.", aliases: [:regexp]},
      {:format_string, "format_string", {:lit_then_cols, 1},
       group: :string, doc: "printf-style formatting.", aliases: [:printf]},
      {:format_number, "format_number", {:col_lit, 1},
       group: :string, doc: "Formats number with d decimal places."},
      {:base64, "base64", :one_col, group: :string, doc: "Base64 encodes binary."},
      {:unbase64, "unbase64", :one_col, group: :string, doc: "Decodes base64 string."},
      {:length, "length", :one_col, group: :string, doc: "Returns length of string or binary."},
      {:octet_length, "octet_length", :one_col,
       group: :string, doc: "Returns byte length of string."},
      # overlay is hand-written in Functions to accept 3-4 column args with optional len
      # sentences is hand-written in Functions to support language/country parameters
      # {:sentences, ...} — see Functions.sentences/1,3
      # levenshtein is hand-written in Functions to support threshold parameter
      # {:levenshtein, ...} — see Functions.levenshtein/2,3
      # locate hand-written in functions.ex to support optional pos parameter
      # split is hand-written in Functions to support optional limit parameter
      # {:split, "split", ...} — see Functions.split/2 and split/3
      {:split_part, "split_part", {:col_lit, 2},
       group: :string, doc: "Splits string and returns the field at index."},
      {:char_, "char", {:lit, 1},
       group: :string, doc: "Returns character from ASCII code.", aliases: [:chr]},
      {:elt, "elt", :n_col, group: :string, doc: "Returns the n-th input string."},
      {:find_in_set, "find_in_set", :two_col,
       group: :string, doc: "Returns position of string in comma-delimited list."},
      {:left_, "left", :two_col, group: :string, doc: "Returns leftmost n characters."},
      {:right_, "right", :two_col, group: :string, doc: "Returns rightmost n characters."},
      {:endswith, "endsWith", :two_col,
       group: :string, doc: "Returns true if string ends with suffix."},
      {:startswith, "startsWith", :two_col,
       group: :string, doc: "Returns true if string starts with prefix."},
      {:position, "position", {:col_lit, 1},
       group: :string, doc: "Returns position of substring."},
      # replace hand-written in functions.ex to support optional replace parameter
      {:url_encode, "url_encode", :one_col, group: :string, doc: "URL-encodes string."},
      {:url_decode, "url_decode", :one_col, group: :string, doc: "URL-decodes string."},
      {:try_url_decode, "try_url_decode", :one_col,
       group: :string, doc: "Try URL-decode, returns null on failure."},
      {:mask, "mask",
       {:col_opt, [upper_char: nil, lower_char: nil, digit_char: nil, other_char: nil]},
       group: :string, doc: "Masks string characters."},
      {:encode, "encode", {:col_lit, 1}, group: :string, doc: "Encodes string with charset."},
      {:decode, "decode", {:col_lit, 1}, group: :string, doc: "Decodes binary with charset."},
      {:collate, "collate", {:col_lit, 1}, group: :string, doc: "Applies collation to string."},
      {:collation, "collation", :one_col,
       group: :string, doc: "Returns collation of string column."},
      {:is_valid_utf8, "is_valid_utf8", :one_col,
       group: :string, doc: "Returns true if string is valid UTF-8."},
      {:make_valid_utf8, "make_valid_utf8", :one_col,
       group: :string, doc: "Replaces invalid UTF-8 with replacement char."},
      {:validate_utf8, "validate_utf8", :one_col,
       group: :string, doc: "Validates UTF-8 and raises on invalid."},
      {:try_validate_utf8, "try_validate_utf8", :one_col,
       group: :string, doc: "Validates UTF-8 and returns null on invalid."},
      # randstr hand-written in functions.ex to support seed parameter
      # {:randstr, ...} — see Functions.randstr/2,3
      # parse_url/try_parse_url hand-written in functions.ex to support optional key parameter
      # {:parse_url, ...} — see Functions.parse_url/2,3
      # {:try_parse_url, ...} — see Functions.try_parse_url/2,3
      {:quote_, "quote", :one_col,
       group: :string, doc: "Quotes a string for use in SQL."},
      {:contains_, "contains", :two_col,
       group: :string, doc: "Returns true if string contains substring."},
      # like_/ilike_ hand-written in functions.ex to support escape character parameter
      # {:like_, ...} — see Functions.like_/2,3
      # {:ilike_, ...} — see Functions.ilike_/2,3
      {:rlike_, "rlike", :two_col,
       group: :string, doc: "Regex pattern match."},
      # substr_ hand-written in functions.ex to support optional len parameter
      # {:substr_, ...} — see Functions.substr_/2,3
    ]
  end

  defp date_functions do
    [
      {:current_date, "current_date", :zero,
       group: :datetime, doc: "Returns current date.", aliases: [:curdate]},
      {:current_timestamp, "current_timestamp", :zero,
       group: :datetime, doc: "Returns current timestamp.", aliases: [:now]},
      {:current_time, "current_time", :zero, group: :datetime, doc: "Returns current time."},
      {:current_timezone, "current_timezone", :zero,
       group: :datetime, doc: "Returns current timezone string."},
      {:localtimestamp_, "localtimestamp", :zero,
       group: :datetime, doc: "Returns current local timestamp."},
      {:year, "year", :one_col, group: :datetime, doc: "Extracts year."},
      {:month, "month", :one_col, group: :datetime, doc: "Extracts month."},
      {:day, "day", :one_col, group: :datetime, doc: "Extracts day.", aliases: [:dayofmonth]},
      {:hour, "hour", :one_col, group: :datetime, doc: "Extracts hour."},
      {:minute, "minute", :one_col, group: :datetime, doc: "Extracts minute."},
      {:second, "second", :one_col, group: :datetime, doc: "Extracts second."},
      {:quarter, "quarter", :one_col, group: :datetime, doc: "Extracts quarter."},
      {:dayofweek, "dayofweek", :one_col, group: :datetime, doc: "Day of week (1=Sun)."},
      {:dayofyear, "dayofyear", :one_col, group: :datetime, doc: "Day of year."},
      {:weekofyear, "weekofyear", :one_col, group: :datetime, doc: "Week of year."},
      {:weekday, "weekday", :one_col, group: :datetime, doc: "Day of week (0=Mon, 6=Sun)."},
      {:monthname, "monthname", :one_col, group: :datetime, doc: "Returns month name."},
      {:dayname, "dayname", :one_col, group: :datetime, doc: "Returns day name."},
      {:extract, "extract", :two_col,
       group: :datetime, doc: "Extracts date/time field.", aliases: [:date_part, :datepart]},
      {:date_add, "date_add", {:col_lit, 1},
       group: :datetime, doc: "Adds days to date.", aliases: [:dateadd]},
      {:date_sub, "date_sub", {:col_lit, 1}, group: :datetime, doc: "Subtracts days from date."},
      {:datediff, "datediff", :two_col,
       group: :datetime, doc: "Difference in days between dates.", aliases: [:date_diff]},
      {:date_format, "date_format", {:col_lit, 1},
       group: :datetime, doc: "Formats date/timestamp with pattern."},
      {:date_trunc, "date_trunc", {:lit_then_cols, 1},
       group: :datetime, doc: "Truncates date to specified unit."},
      {:trunc, "trunc", {:col_lit, 1},
       group: :datetime, doc: "Truncates date to specified format."},
      {:add_months, "add_months", {:col_lit, 1}, group: :datetime, doc: "Adds months to date."},
      # months_between is hand-written in Functions to support roundOff parameter
      # {:months_between, ...} — see Functions.months_between/2,3
      {:next_day, "next_day", {:col_lit, 1},
       group: :datetime, doc: "Next day of week after date."},
      {:last_day, "last_day", :one_col, group: :datetime, doc: "Last day of month for date."},
      {:to_date, "to_date", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts to date, optionally with format."},
      {:try_to_date, "try_to_date", {:col_opt, [format: nil]},
       group: :datetime, doc: "Try to convert to date, returns null on failure."},
      {:to_timestamp, "to_timestamp", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts to timestamp, optionally with format."},
      {:try_to_timestamp, "try_to_timestamp", {:col_opt, [format: nil]},
       group: :datetime, doc: "Try to convert to timestamp, returns null on failure."},
      {:to_timestamp_ltz, "to_timestamp_ltz", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts to timestamp with local timezone."},
      {:to_timestamp_ntz, "to_timestamp_ntz", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts to timestamp without timezone."},
      # from_unixtime hand-written in functions.ex to always send default format
      # unix_timestamp hand-written in functions.ex to support zero-arg form
      # {:unix_timestamp, ...} — see Functions.unix_timestamp/0,1,2
      {:to_unix_timestamp, "to_unix_timestamp", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts timestamp to unix seconds."},
      {:from_utc_timestamp, "from_utc_timestamp", {:col_lit, 1},
       group: :datetime, doc: "Converts UTC timestamp to timezone."},
      {:to_utc_timestamp, "to_utc_timestamp", {:col_lit, 1},
       group: :datetime, doc: "Converts timestamp from timezone to UTC."},
      {:date_from_unix_date, "date_from_unix_date", :one_col,
       group: :datetime, doc: "Creates date from days since epoch."},
      {:unix_date, "unix_date", :one_col,
       group: :datetime, doc: "Returns days since epoch for date."},
      {:unix_micros, "unix_micros", :one_col,
       group: :datetime, doc: "Returns microseconds since epoch."},
      {:unix_millis, "unix_millis", :one_col,
       group: :datetime, doc: "Returns milliseconds since epoch."},
      {:unix_seconds, "unix_seconds", :one_col,
       group: :datetime, doc: "Returns seconds since epoch."},
      {:timestamp_seconds, "timestamp_seconds", :one_col,
       group: :datetime, doc: "Creates timestamp from seconds."},
      {:timestamp_millis, "timestamp_millis", :one_col,
       group: :datetime, doc: "Creates timestamp from milliseconds."},
      {:timestamp_micros, "timestamp_micros", :one_col,
       group: :datetime, doc: "Creates timestamp from microseconds."},
      {:timestamp_diff, "timestampdiff", {:lit_then_cols, 1},
       group: :datetime, doc: "Returns difference between timestamps in given unit."},
      {:timestamp_add, "timestampadd", {:lit_then_cols, 1},
       group: :datetime, doc: "Adds interval to timestamp."},
      {:time_diff, "time_diff", {:lit_then_cols, 1},
       group: :datetime, doc: "Returns difference between times in given unit."},
      {:time_trunc, "time_trunc", {:lit_then_cols, 1},
       group: :datetime, doc: "Truncates time to specified unit."},
      {:make_date, "make_date", :three_col,
       group: :datetime, doc: "Creates date from year, month, day."},
      # convert_timezone hand-written in functions.ex to support 2-arg form
      # {:convert_timezone, ...} — see Functions.convert_timezone/2,3
      {:to_time, "to_time", {:col_opt, [format: nil]},
       group: :datetime, doc: "Converts to time type."},
      {:try_to_time, "try_to_time", {:col_opt, [format: nil]},
       group: :datetime, doc: "Try to convert to time, returns null on failure."},
      {:make_time, "make_time", :three_col,
       group: :datetime, doc: "Creates time from hour, minute, second."},
      {:window_time, "window_time", :one_col,
       group: :datetime, doc: "Extracts the time column from a window column."},
      {:session_window, "session_window", :two_col,
       group: :datetime, doc: "Generates session window for streaming aggregations."}
    ]
  end

  defp collection_functions do
    [
      {:array, "array", :n_col, group: :collection, doc: "Creates array from columns."},
      {:create_map, "map", :n_col,
       group: :collection, doc: "Creates map from key-value column pairs."},
      {:struct, "struct", :n_col, group: :collection, doc: "Creates struct from columns."},
      {:named_struct, "named_struct", :n_col,
       group: :collection, doc: "Creates struct with named fields."},
      {:array_contains, "array_contains", :two_col,
       group: :collection, doc: "Checks if array contains value."},
      {:array_distinct, "array_distinct", :one_col,
       group: :collection, doc: "Removes duplicates from array."},
      {:array_except, "array_except", :two_col,
       group: :collection, doc: "Returns elements in first but not second array."},
      {:array_intersect, "array_intersect", :two_col,
       group: :collection, doc: "Returns intersection of two arrays."},
      {:array_max, "array_max", :one_col,
       group: :collection, doc: "Returns max element of array."},
      {:array_min, "array_min", :one_col,
       group: :collection, doc: "Returns min element of array."},
      {:array_size, "array_size", :one_col,
       group: :collection, doc: "Returns array size.", aliases: [:size, :cardinality]},
      # array_sort hand-written in functions.ex to support comparator function
      # {:array_sort, ...} — see Functions.array_sort/1,2
      {:array_union, "array_union", :two_col,
       group: :collection, doc: "Returns union of two arrays."},
      {:array_append, "array_append", :two_col,
       group: :collection, doc: "Appends element to array."},
      {:array_prepend, "array_prepend", :two_col,
       group: :collection, doc: "Prepends element to array."},
      {:array_compact, "array_compact", :one_col,
       group: :collection, doc: "Removes null values from array."},
      {:array_insert, "array_insert", :three_col,
       group: :collection, doc: "Inserts element at position in array."},
      {:array_remove, "array_remove", :two_col,
       group: :collection, doc: "Removes all occurrences of element from array."},
      {:array_repeat, "array_repeat", {:col_lit, 1},
       group: :collection, doc: "Creates array with element repeated n times."},
      {:array_position, "array_position", :two_col,
       group: :collection, doc: "Locates element in array (1-based)."},
      {:arrays_overlap, "arrays_overlap", :two_col,
       group: :collection, doc: "Returns true if arrays have common elements."},
      {:arrays_zip, "arrays_zip", :n_col,
       group: :collection, doc: "Zips arrays into array of structs."},
      # array_join is hand-written in Functions to support null_replacement parameter
      # {:array_join, ...} — see Functions.array_join/2,3
      {:element_at, "element_at", :two_col,
       group: :collection, doc: "Returns element at index/key."},
      {:try_element_at, "try_element_at", :two_col,
       group: :collection, doc: "Returns element at index/key, null on out of bounds."},
      {:get, "get", {:col_lit, 1},
       group: :collection, doc: "Returns element at index from array."},
      {:explode, "explode", :one_col,
       group: :collection, doc: "Creates a row for each array/map element."},
      {:explode_outer, "explode_outer", :one_col,
       group: :collection, doc: "Like explode but preserves nulls."},
      {:posexplode, "posexplode", :one_col,
       group: :collection, doc: "Like explode but includes position."},
      {:posexplode_outer, "posexplode_outer", :one_col,
       group: :collection, doc: "Like posexplode but preserves nulls."},
      {:inline, "inline", :one_col,
       group: :collection, doc: "Explodes array of structs into columns."},
      {:inline_outer, "inline_outer", :one_col,
       group: :collection, doc: "Like inline but preserves nulls."},
      {:flatten, "flatten", :one_col, group: :collection, doc: "Flattens nested array."},
      {:map_keys, "map_keys", :one_col, group: :collection, doc: "Returns map keys."},
      {:map_values, "map_values", :one_col, group: :collection, doc: "Returns map values."},
      {:map_entries, "map_entries", :one_col,
       group: :collection, doc: "Returns map entries as array of structs."},
      {:map_concat, "map_concat", :n_col, group: :collection, doc: "Concatenates maps."},
      {:map_from_arrays, "map_from_arrays", :two_col,
       group: :collection, doc: "Creates map from key and value arrays."},
      {:map_from_entries, "map_from_entries", :one_col,
       group: :collection, doc: "Creates map from array of entries."},
      {:map_contains_key, "map_contains_key", :two_col,
       group: :collection, doc: "Returns true if map contains the given key."},
      {:str_to_map, "str_to_map", {:col_opt, [pair_delim: nil, key_value_delim: nil]},
       group: :collection, doc: "Creates map from delimited string."},
      {:slice, "slice", {:col_lit, 2},
       group: :collection, doc: "Returns slice of array from start for length."},
      {:sort_array, "sort_array", {:col_opt, [asc: true]},
       group: :collection, doc: "Sorts array."},
      # sequence is hand-written in Functions to make step optional
      # {:sequence, ...} — see Functions.sequence/2,3
      # shuffle hand-written in functions.ex to support seed parameter
      {:stack, "stack", :n_col, group: :collection, doc: "Separates column into n rows."}
    ]
  end

  defp aggregate_functions do
    [
      {:count, "count", :one_col, group: :aggregate, doc: "Counts non-null values."},
      # count_distinct is hand-written in Functions to support variadic columns
      # {:count_distinct, "count", ...} — see Functions.count_distinct/1
      {:sum, "sum", :one_col, group: :aggregate, doc: "Computes sum."},
      {:avg, "avg", :one_col, group: :aggregate, doc: "Computes average.", aliases: [:mean]},
      {:min, "min", :one_col, group: :aggregate, doc: "Computes minimum."},
      {:max, "max", :one_col, group: :aggregate, doc: "Computes maximum."},
      {:first, "first", {:col_opt, [ignore_nulls: nil]},
       group: :aggregate, doc: "Returns first value.", aliases: [:first_value]},
      {:last, "last", {:col_opt, [ignore_nulls: nil]},
       group: :aggregate, doc: "Returns last value.", aliases: [:last_value]},
      {:collect_list, "collect_list", :one_col,
       group: :aggregate, doc: "Collects values into list.", aliases: [:array_agg]},
      {:collect_set, "collect_set", :one_col,
       group: :aggregate, doc: "Collects distinct values into set."},
      {:sum_distinct, "sum", :one_col,
       group: :aggregate, doc: "Computes sum of distinct values.", is_distinct: true},
      # approx_count_distinct is hand-written in Functions to support rsd parameter
      # {:approx_count_distinct, ...} — see Functions.approx_count_distinct/1,2
      {:approx_percentile, "approx_percentile", {:col_lit, 2},
       group: :aggregate, doc: "Approximate percentile with accuracy parameter."},
      # percentile hand-written in functions.ex to support frequency parameter and list of percentages
      # {:percentile, ...} — see Functions.percentile/2,3
      {:corr, "corr", :two_col, group: :aggregate, doc: "Pearson correlation."},
      {:covar_pop, "covar_pop", :two_col, group: :aggregate, doc: "Population covariance."},
      {:covar_samp, "covar_samp", :two_col, group: :aggregate, doc: "Sample covariance."},
      {:stddev, "stddev", :one_col,
       group: :aggregate, doc: "Sample standard deviation.", aliases: [:stddev_samp, :std]},
      {:stddev_pop, "stddev_pop", :one_col,
       group: :aggregate, doc: "Population standard deviation."},
      {:variance, "variance", :one_col,
       group: :aggregate, doc: "Sample variance.", aliases: [:var_samp]},
      {:var_pop, "var_pop", :one_col, group: :aggregate, doc: "Population variance."},
      {:skewness, "skewness", :one_col, group: :aggregate, doc: "Skewness."},
      {:kurtosis, "kurtosis", :one_col, group: :aggregate, doc: "Kurtosis."},
      {:percentile_approx, "percentile_approx", {:col_lit, 2},
       group: :aggregate, doc: "Approximate percentile."},
      # mode hand-written in functions.ex to support deterministic parameter
      # any_value hand-written in functions.ex to support ignoreNulls
      {:count_if, "count_if", :one_col,
       group: :aggregate, doc: "Counts rows where condition is true."},
      {:max_by, "max_by", :two_col,
       group: :aggregate, doc: "Value of first col at max of second."},
      {:min_by, "min_by", :two_col,
       group: :aggregate, doc: "Value of first col at min of second."},
      {:bool_and, "bool_and", :one_col,
       group: :aggregate, doc: "True if all values are true.", aliases: [:every]},
      {:bool_or, "bool_or", :one_col,
       group: :aggregate, doc: "True if any value is true.", aliases: [:some]},
      {:bit_and, "bit_and", :one_col, group: :aggregate, doc: "Bitwise AND aggregate."},
      {:bit_or, "bit_or", :one_col, group: :aggregate, doc: "Bitwise OR aggregate."},
      {:bit_xor, "bit_xor", :one_col, group: :aggregate, doc: "Bitwise XOR aggregate."},
      {:median, "median", :one_col, group: :aggregate, doc: "Median value."},
      {:listagg, "listagg", {:col_opt, [delimiter: nil]},
       group: :aggregate, doc: "Concatenates values as string.", aliases: [:string_agg]},
      {:listagg_distinct, "listagg", {:col_opt, [delimiter: nil]},
       group: :aggregate,
       doc: "Concatenates distinct values as string.",
       is_distinct: true,
       aliases: [:string_agg_distinct]},
      {:regr_avgx, "regr_avgx", :two_col,
       group: :aggregate, doc: "Average of independent variable."},
      {:regr_avgy, "regr_avgy", :two_col,
       group: :aggregate, doc: "Average of dependent variable."},
      {:regr_count, "regr_count", :two_col, group: :aggregate, doc: "Count of non-null pairs."},
      {:regr_intercept, "regr_intercept", :two_col,
       group: :aggregate, doc: "Y-intercept of regression line."},
      {:regr_r2, "regr_r2", :two_col, group: :aggregate, doc: "Coefficient of determination."},
      {:regr_slope, "regr_slope", :two_col, group: :aggregate, doc: "Slope of regression line."},
      {:regr_sxx, "regr_sxx", :two_col,
       group: :aggregate, doc: "Sum of squares of independent variable."},
      {:regr_sxy, "regr_sxy", :two_col, group: :aggregate, doc: "Sum of products of deviations."},
      {:regr_syy, "regr_syy", :two_col,
       group: :aggregate, doc: "Sum of squares of dependent variable."},
      {:histogram_numeric, "histogram_numeric", {:col_lit, 1},
       group: :aggregate, doc: "Computes histogram of column."}
    ]
  end

  defp window_functions do
    [
      {:row_number, "row_number", :zero, group: :window, doc: "Row number within partition."},
      {:rank, "rank", :zero, group: :window, doc: "Rank within partition."},
      {:dense_rank, "dense_rank", :zero, group: :window, doc: "Dense rank within partition."},
      {:percent_rank, "percent_rank", :zero,
       group: :window, doc: "Percent rank within partition."},
      {:cume_dist, "cume_dist", :zero,
       group: :window, doc: "Cumulative distribution within partition."},
      {:ntile, "ntile", {:lit, 1}, group: :window, doc: "N-tile bucket number within partition."},
      {:lag, "lag", {:col_opt, [offset: 1, default: nil]},
       group: :window, doc: "Value at offset rows before current."},
      {:lead, "lead", {:col_opt, [offset: 1, default: nil]},
       group: :window, doc: "Value at offset rows after current."},
      # nth_value hand-written in functions.ex to support ignoreNulls
    ]
  end

  defp conditional_functions do
    [
      {:coalesce, "coalesce", :n_col, group: :conditional, doc: "Returns first non-null value."},
      {:greatest, "greatest", :n_col, group: :conditional, doc: "Returns greatest value."},
      {:least, "least", :n_col, group: :conditional, doc: "Returns least value."},
      {:ifnull, "ifnull", :two_col,
       group: :conditional, doc: "Returns second value if first is null."},
      {:nullif, "nullif", :two_col,
       group: :conditional, doc: "Returns null if both values are equal."},
      {:nullifzero, "nullifzero", :one_col,
       group: :conditional, doc: "Returns null if value is zero."},
      {:nvl, "nvl", :two_col, group: :conditional, doc: "Returns second value if first is null."},
      {:nvl2, "nvl2", :three_col,
       group: :conditional, doc: "Returns second if first is not null, else third."},
      {:nanvl, "nanvl", :two_col,
       group: :conditional, doc: "Returns second value if first is NaN."},
      {:isnan, "isNaN", :one_col, group: :conditional, doc: "True if NaN."},
      {:isnull, "isNull", :one_col, group: :conditional, doc: "True if null."},
      {:isnotnull, "isNotNull", :one_col, group: :conditional, doc: "True if not null."},
      {:equal_null, "equal_null", :two_col, group: :conditional, doc: "Null-safe equality."},
      {:zeroifnull, "zeroifnull", :one_col,
       group: :conditional, doc: "Returns zero if value is null."},
      # assert_true is hand-written in Functions to support errMsg parameter
      # {:assert_true, ...} — see Functions.assert_true/1,2
      {:raise_error, "raise_error", :one_col,
       group: :conditional, doc: "Raises a user-specified error message."}
    ]
  end

  defp hash_functions do
    [
      {:md5, "md5", :one_col, group: :hash, doc: "MD5 hash."},
      {:sha1, "sha1", :one_col, group: :hash, doc: "SHA-1 hash.", aliases: [:sha]},
      {:sha2, "sha2", {:col_lit, 1}, group: :hash, doc: "SHA-2 hash with bit length."},
      {:crc32, "crc32", :one_col, group: :hash, doc: "CRC32 hash."},
      {:hash, "hash", :n_col, group: :hash, doc: "Murmur3 hash of columns."},
      {:xxhash64, "xxhash64", :n_col, group: :hash, doc: "xxHash64 of columns."}
    ]
  end

  defp json_functions do
    [
      {:get_json_object, "get_json_object", {:col_lit, 1},
       group: :json, doc: "Extracts JSON object from path expression."},
      # json_tuple is hand-written in Functions because first arg is column, rest are string literals
      # {:json_tuple, "json_tuple", ...} — see Functions.json_tuple/2
      {:json_array_length, "json_array_length", :one_col,
       group: :json, doc: "Returns length of outermost JSON array."},
      {:json_object_keys, "json_object_keys", :one_col,
       group: :json, doc: "Returns keys of outermost JSON object."},
      {:schema_of_json, "schema_of_json", :one_col,
       group: :json, doc: "Returns DDL schema string of JSON string."},
      {:parse_json, "parse_json", :one_col,
       group: :json, doc: "Parses JSON string to variant type."},
      {:try_parse_json, "try_parse_json", :one_col,
       group: :json, doc: "Try parse JSON, returns null on failure."},
      {:is_variant_null, "is_variant_null", :one_col,
       group: :json, doc: "Returns true if variant value is null."},
      {:variant_get, "variant_get", {:col_lit, 2},
       group: :json, doc: "Gets variant value at path with type."},
      {:try_variant_get, "try_variant_get", {:col_lit, 2},
       group: :json, doc: "Try get variant value, returns null on failure."},
      {:to_variant_object, "to_variant_object", :one_col,
       group: :json, doc: "Converts map to variant object."},
      {:schema_of_variant, "schema_of_variant", :one_col,
       group: :json, doc: "Returns schema string of variant."},
      {:schema_of_variant_agg, "schema_of_variant_agg", :one_col,
       group: :json, doc: "Returns merged schema string of variant column."}
    ]
  end

  defp csv_xml_functions do
    [
      {:schema_of_csv, "schema_of_csv", :one_col,
       group: :csv_xml, doc: "Returns DDL schema string of CSV string."},
      {:schema_of_xml, "schema_of_xml", :one_col,
       group: :csv_xml, doc: "Returns DDL schema string of XML string."},
      {:xpath, "xpath", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning array of strings."},
      {:xpath_boolean, "xpath_boolean", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning boolean."},
      {:xpath_double, "xpath_double", {:col_lit, 1},
       group: :csv_xml,
       doc: "Evaluates XPath expression returning double.",
       aliases: [:xpath_number]},
      {:xpath_float, "xpath_float", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning float."},
      {:xpath_int, "xpath_int", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning integer."},
      {:xpath_long, "xpath_long", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning long."},
      {:xpath_short, "xpath_short", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning short."},
      {:xpath_string, "xpath_string", {:col_lit, 1},
       group: :csv_xml, doc: "Evaluates XPath expression returning string."}
    ]
  end

  defp type_functions do
    [
      {:typeof, "typeof", :one_col, group: :type, doc: "Runtime data type string."},
      {:to_binary, "to_binary", {:col_opt, [format: nil]},
       group: :type, doc: "Converts to binary."},
      {:try_to_binary, "try_to_binary", {:col_opt, [format: nil]},
       group: :type, doc: "Try to convert to binary, returns null on failure."},
      {:to_char_, "to_char", {:col_lit, 1},
       group: :type, doc: "Converts to character string with format.", aliases: [:to_varchar]},
      {:to_number, "to_number", {:col_lit, 1},
       group: :type, doc: "Converts string to number with format."},
      {:try_to_number, "try_to_number", {:col_lit, 1},
       group: :type, doc: "Try to convert to number, returns null on failure."}
    ]
  end

  defp encryption_functions do
    [
      {:aes_encrypt, "aes_encrypt", :n_col, group: :encryption, doc: "AES encrypts binary data."},
      {:aes_decrypt, "aes_decrypt", :n_col, group: :encryption, doc: "AES decrypts binary data."},
      {:try_aes_decrypt, "try_aes_decrypt", :n_col,
       group: :encryption, doc: "Try AES decrypt, returns null on failure."}
    ]
  end

  defp session_functions do
    [
      {:current_catalog, "current_catalog", :zero,
       group: :session, doc: "Returns current catalog name."},
      {:current_database, "current_database", :zero,
       group: :session, doc: "Returns current database name.", aliases: [:current_schema]},
      {:current_user_, "current_user", :zero,
       group: :session, doc: "Returns current user name.", aliases: [:user_]},
      {:session_user_, "session_user", :zero, group: :session, doc: "Returns session user name."},
      # uuid hand-written in functions.ex to support seed parameter
      # {:uuid, ...} — see Functions.uuid/0,1
      {:version_, "version", :zero, group: :session, doc: "Returns Spark version string."}
    ]
  end

  defp misc_functions do
    [
      {:monotonically_increasing_id, "monotonically_increasing_id", :zero,
       group: :misc, doc: "Globally unique monotonically increasing ID."},
      {:spark_partition_id, "spark_partition_id", :zero,
       group: :misc, doc: "Partition ID of each row."},
      {:input_file_name, "input_file_name", :zero, group: :misc, doc: "Name of file being read."},
      {:input_file_block_length, "input_file_block_length", :zero,
       group: :misc, doc: "Length of current file block."},
      {:input_file_block_start, "input_file_block_start", :zero,
       group: :misc, doc: "Start offset of current file block."},
      # rand/randn hand-written in functions.ex to generate random seed when none given
      {:grouping, "grouping", :one_col,
       group: :misc, doc: "Indicates whether column is aggregated in grouping set."},
      {:grouping_id, "grouping_id", :n_col, group: :misc, doc: "Grouping ID for grouping set."},
      {:count_min_sketch, "count_min_sketch", {:col_lit, 3},
       group: :misc, doc: "Creates a count-min sketch of a column with given eps, confidence, and seed."},
      {:reflect_, "reflect", :n_col,
       group: :misc, doc: "Calls a JVM method via reflection."},
      {:java_method, "java_method", :n_col,
       group: :misc, doc: "Calls a JVM method."},
      {:try_reflect, "try_reflect", :n_col,
       group: :misc, doc: "Try to call a JVM method, returns null on failure."}
    ] ++ hll_functions() ++ theta_sketch_functions() ++ kll_sketch_functions() ++ bitmap_functions() ++ geospatial_functions()
  end

  defp hll_functions do
    [
      {:hll_sketch_agg, "hll_sketch_agg", {:col_opt, [lg_config_k: nil]},
       group: :sketch, doc: "Aggregates values into an HLL sketch."},
      {:hll_sketch_estimate, "hll_sketch_estimate", :one_col,
       group: :sketch, doc: "Estimates distinct count from an HLL sketch."},
      {:hll_union, "hll_union", {:col_opt, [allow_different_lg_config_k: nil]},
       group: :sketch, doc: "Unions two HLL sketches."},
      {:hll_union_agg, "hll_union_agg", {:col_opt, [allow_different_lg_config_k: nil]},
       group: :sketch, doc: "Aggregate union of HLL sketches."}
    ]
  end

  defp theta_sketch_functions do
    [
      {:theta_sketch_agg, "theta_sketch_agg", {:col_opt, [lg_k: nil, seed: nil]},
       group: :sketch, doc: "Aggregates values into a theta sketch."},
      {:theta_sketch_estimate, "theta_sketch_estimate", :one_col,
       group: :sketch, doc: "Estimates distinct count from a theta sketch."},
      {:theta_union, "theta_union", {:col_opt, [lg_k: nil, seed: nil]},
       group: :sketch, doc: "Unions two theta sketches."},
      {:theta_union_agg, "theta_union_agg", {:col_opt, [lg_k: nil, seed: nil]},
       group: :sketch, doc: "Aggregate union of theta sketches."},
      {:theta_intersection_agg, "theta_intersection_agg", {:col_opt, [seed: nil]},
       group: :sketch, doc: "Aggregate intersection of theta sketches."},
      {:theta_intersection, "theta_intersection", {:col_opt, [seed: nil]},
       group: :sketch, doc: "Intersects two theta sketches."},
      {:theta_difference, "theta_difference", {:col_opt, [seed: nil]},
       group: :sketch, doc: "Computes difference of two theta sketches."}
    ]
  end

  defp kll_sketch_functions do
    kll_types = ["bigint", "float", "double"]

    Enum.flat_map(kll_types, fn type ->
      [
        {:"kll_sketch_agg_#{type}", "kll_sketch_agg_#{type}", {:col_opt, [k: nil]},
         group: :sketch, doc: "Aggregates #{type} values into a KLL sketch."},
        {:"kll_sketch_to_string_#{type}", "kll_sketch_to_string_#{type}", :one_col,
         group: :sketch, doc: "Converts a KLL sketch (#{type}) to a string."},
        {:"kll_sketch_get_n_#{type}", "kll_sketch_get_n_#{type}", :one_col,
         group: :sketch, doc: "Returns n (number of items) from a KLL sketch (#{type})."},
        {:"kll_sketch_merge_#{type}", "kll_sketch_merge_#{type}", {:col_opt, [k: nil]},
         group: :sketch, doc: "Merges KLL sketches (#{type})."},
        {:"kll_sketch_get_quantile_#{type}", "kll_sketch_get_quantile_#{type}", {:col_lit, 1},
         group: :sketch, doc: "Gets quantile from a KLL sketch (#{type})."},
        {:"kll_sketch_get_rank_#{type}", "kll_sketch_get_rank_#{type}", {:col_lit, 1},
         group: :sketch, doc: "Gets rank from a KLL sketch (#{type})."}
      ]
    end)
  end

  defp geospatial_functions do
    [
      {:st_asbinary, "ST_AsBinary", :one_col,
       group: :geospatial, doc: "Converts geometry/geography to WKB binary."},
      {:st_geogfromwkb, "ST_GeogFromWKB", :one_col,
       group: :geospatial, doc: "Creates geography from WKB binary."},
      {:st_geomfromwkb, "ST_GeomFromWKB", :one_col,
       group: :geospatial, doc: "Creates geometry from WKB binary."},
      {:st_setsrid, "ST_SetSRID", {:col_lit, 1},
       group: :geospatial, doc: "Sets the SRID of a geometry."},
      {:st_srid, "ST_SRID", :one_col,
       group: :geospatial, doc: "Returns the SRID of a geometry."}
    ]
  end

  defp bitmap_functions do
    [
      {:bitmap_bit_position, "bitmap_bit_position", :one_col,
       group: :bitmap, doc: "Returns bit position within a bitmap bucket."},
      {:bitmap_bucket_number, "bitmap_bucket_number", :one_col,
       group: :bitmap, doc: "Returns bitmap bucket number."},
      {:bitmap_construct_agg, "bitmap_construct_agg", :one_col,
       group: :bitmap, doc: "Constructs a bitmap from bit positions."},
      {:bitmap_count, "bitmap_count", :one_col,
       group: :bitmap, doc: "Counts set bits in a bitmap."},
      {:bitmap_or_agg, "bitmap_or_agg", :one_col,
       group: :bitmap, doc: "Aggregate OR of bitmaps."},
      {:bitmap_and_agg, "bitmap_and_agg", :one_col,
       group: :bitmap, doc: "Aggregate AND of bitmaps."}
    ]
  end
end
