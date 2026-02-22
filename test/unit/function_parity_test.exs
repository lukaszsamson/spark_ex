defmodule SparkEx.Unit.FunctionParityTest do
  @moduledoc """
  Compares SparkEx function registry against PySpark builtin.py source
  to detect missing functions (drift prevention).
  """
  use ExUnit.Case, async: true

  @pyspark_builtin_path Path.expand(
                          "../../../spark/python/pyspark/sql/connect/functions/builtin.py",
                          __DIR__
                        )

  # Functions that we intentionally exclude from parity checking.
  # Each has a documented reason.
  @known_exclusions MapSet.new([
                      # Python-specific UDF/UDTF registration
                      "pandas_udf",
                      "udf",
                      "udtf",
                      "arrow_udtf",
                      "unwrap_udt",
                      # DataFrame-level operations (not column functions)
                      "broadcast",
                      # Python-specific call_function
                      "call_function",
                      # Deprecated or internal
                      "call_udf",
                      # PySpark Column methods not top-level functions
                      "column",
                      # Spark Connect-specific internal
                      "_to_col",
                      "_to_col_with_plan_id",
                      "_invoke_function",
                      "_invoke_function_over_columns",
                      "_invoke_binary_math_function",
                      "_options_to_str",
                      # Will be implemented as hand-written special functions
                      "when",
                      # Complex struct-based functions requiring special handling
                      "from_json",
                      "to_json",
                      "from_csv",
                      "to_csv",
                      "from_xml",
                      "to_xml",
                      "from_avro",
                      "to_avro",
                      # Window function (time-based, not WindowSpec)
                      "window",
                      "window_time",
                      "session_window",
                      # Bitmap functions (specialized)
                      "bitmap_bit_position",
                      "bitmap_bucket_number",
                      "bitmap_construct_agg",
                      "bitmap_count",
                      "bitmap_or_agg",
                      "bitmap_and_agg",
                      # ML/complex functions
                      "bucket",
                      "years",
                      "months",
                      "days",
                      "hours",
                      # Observation / watermark / interval construction
                      "make_interval",
                      "make_dt_interval",
                      "make_ym_interval",
                      "make_timestamp",
                      "make_timestamp_ltz",
                      "make_timestamp_ntz",
                      "make_time",
                      "try_make_interval",
                      "try_make_timestamp",
                      "try_make_timestamp_ltz",
                      "try_make_timestamp_ntz",
                      # Try variants that may not be in all Spark versions
                      "try_reflect",
                      "try_parse_url",
                      # HLL functions
                      "hll_sketch_agg",
                      "hll_union_agg",
                      "hll_sketch_estimate",
                      "hll_union",
                      # KLL sketch functions (specialized)
                      "kll_sketch_agg_bigint",
                      "kll_sketch_agg_double",
                      "kll_sketch_agg_float",
                      "kll_sketch_get_n_bigint",
                      "kll_sketch_get_n_double",
                      "kll_sketch_get_n_float",
                      "kll_sketch_get_quantile_bigint",
                      "kll_sketch_get_quantile_double",
                      "kll_sketch_get_quantile_float",
                      "kll_sketch_get_rank_bigint",
                      "kll_sketch_get_rank_double",
                      "kll_sketch_get_rank_float",
                      "kll_sketch_merge_bigint",
                      "kll_sketch_merge_double",
                      "kll_sketch_merge_float",
                      "kll_sketch_to_string_bigint",
                      "kll_sketch_to_string_double",
                      "kll_sketch_to_string_float",
                      # Theta sketch functions (specialized)
                      "theta_sketch_agg",
                      "theta_sketch_estimate",
                      "theta_union",
                      "theta_union_agg",
                      "theta_intersection",
                      "theta_intersection_agg",
                      "theta_difference",
                      # Count-min sketch
                      "count_min_sketch",
                      # Spatial functions (specialized)
                      "st_asbinary",
                      "st_geogfromwkb",
                      "st_geomfromwkb",
                      "st_setsrid",
                      "st_srid",
                      # Other complex/niche functions
                      "java_method",
                      "reflect",
                      "parse_url",
                      "quote",
                      # literal / typedLit are constructors, not SQL functions
                      "typedLit",
                      "lit",
                      "col",
                      "struct",
                      # Column methods exposed as top-level in PySpark
                      # but implemented as Column operators in SparkEx
                      "asc_nulls_first",
                      "asc_nulls_last",
                      "desc_nulls_first",
                      "desc_nulls_last",
                      "bitwise_not",
                      "contains",
                      "like",
                      "rlike",
                      "ilike",
                      "substr",
                      # These are implemented via aliases or different names
                      "toDegrees",
                      "toRadians",
                      "bitwiseNOT",
                      "input_file_block_length",
                      "input_file_block_start",
                      "negate"
                    ])

  @tag :parity
  test "registry covers PySpark builtin functions" do
    unless File.exists?(@pyspark_builtin_path) do
      flunk("missing PySpark source: #{@pyspark_builtin_path}")
    end

    pyspark_fns = parse_pyspark_function_names(@pyspark_builtin_path)

    registry_names =
      SparkEx.Macros.FunctionRegistry.registry()
      |> Enum.flat_map(fn {name, _spark, _arity, opts} ->
        names = [Atom.to_string(name)]
        aliases = Keyword.get(opts, :aliases, []) |> Enum.map(&Atom.to_string/1)
        names ++ aliases
      end)
      |> MapSet.new()

    # Also add hand-written functions from Functions module
    hand_written =
      MapSet.new([
        "col",
        "lit",
        "expr",
        "star",
        "asc",
        "desc",
        "when_",
        "otherwise",
        "transform",
        "filter",
        "exists",
        "forall",
        "aggregate",
        "reduce",
        "map_filter",
        "map_zip_with",
        "transform_keys",
        "transform_values",
        "zip_with",
        "json_tuple",
        "split",
        "count_distinct",
        "months_between",
        "approx_count_distinct",
        "ltrim",
        "rtrim",
        "trim",
        "sentences",
        "levenshtein",
        "array_join",
        "sequence",
        "assert_true",
        "log",
        "broadcast",
        "atan2",
        "pow",
        "power",
        "any_value",
        "nth_value",
        "rand",
        "randn",
        "locate",
        "mode",
        "shuffle",
        "from_unixtime",
        "replace",
        "unix_timestamp",
        "convert_timezone",
        "parse_url",
        "try_parse_url",
        "substr_",
        "like_",
        "ilike_",
        "array_sort",
        "percentile",
        "uuid",
        "uniform",
        "randstr",
        "overlay",
         "schema_of_json",
         "schema_of_csv",
         "schema_of_xml",
         # Spark 3.5 compatibility wrappers implemented by hand
         "to_time",
         "try_to_time",
         "time_diff",
         "time_trunc",
         "parse_json",
         "try_parse_json",
         "is_variant_null",
         "variant_get",
         "try_variant_get",
         "to_variant_object",
         "schema_of_variant",
         "schema_of_variant_agg"
       ])

    all_names = MapSet.union(registry_names, hand_written)

    # Normalize: strip trailing _ from Elixir names (used for reserved words)
    normalized =
      all_names
      |> Enum.flat_map(fn name ->
        stripped = String.trim_trailing(name, "_")
        [name, stripped]
      end)
      |> MapSet.new()

    missing = MapSet.difference(pyspark_fns, normalized)
    actual_missing = MapSet.difference(missing, @known_exclusions)

    if MapSet.size(actual_missing) > 0 do
      missing_list = actual_missing |> MapSet.to_list() |> Enum.sort()

      IO.puts(
        "\nMissing #{length(missing_list)} PySpark functions from registry:\n" <>
          Enum.join(missing_list, ", ")
      )
    end

    missing_count = MapSet.size(actual_missing)
    max_allowed = 5

    assert missing_count <= max_allowed,
           "Too many missing functions (#{missing_count} > #{max_allowed}): #{inspect(MapSet.to_list(actual_missing) |> Enum.sort())}"
  end

  test "registry function count is substantial" do
    count = length(SparkEx.Macros.FunctionRegistry.registry())
    assert count >= 340, "Expected at least 340 registry entries, got #{count}"
  end

  test "no duplicate function names in registry" do
    entries = SparkEx.Macros.FunctionRegistry.registry()

    all_names =
      Enum.flat_map(entries, fn {name, _spark, _arity, opts} ->
        [name | Keyword.get(opts, :aliases, [])]
      end)

    duplicates =
      all_names
      |> Enum.frequencies()
      |> Enum.filter(fn {_, count} -> count > 1 end)
      |> Enum.map(fn {name, _} -> name end)

    assert duplicates == [], "Duplicate names found: #{inspect(duplicates)}"
  end

  # ── Helpers ──

  defp parse_pyspark_function_names(path) do
    content = File.read!(path)

    # Match Python function definitions: def function_name(
    Regex.scan(~r/^def\s+([a-z_][a-z0-9_]*)\s*\(/m, content)
    |> Enum.map(fn [_, name] -> name end)
    |> Enum.reject(fn name ->
      # Skip private/internal Python functions
      String.starts_with?(name, "_")
    end)
    |> MapSet.new()
  end
end
