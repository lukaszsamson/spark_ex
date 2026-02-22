defmodule SparkEx.M11.FunctionsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  # ── when_ / otherwise ──

  describe "when_/2" do
    test "creates case-when expression" do
      result =
        Functions.when_(
          Column.gt(Functions.col("age"), Functions.lit(18)),
          Functions.lit("adult")
        )

      assert %Column{
               expr:
                 {:fn, "when", [{:fn, ">", [{:col, "age"}, {:lit, 18}], false}, {:lit, "adult"}],
                  false}
             } = result
    end

    test "auto-coerces value to literal" do
      result = Functions.when_(Column.gt(Functions.col("age"), Functions.lit(18)), "adult")

      assert %Column{
               expr: {:fn, "when", [{:fn, ">", _, false}, {:lit, "adult"}], false}
             } = result
    end
  end

  describe "otherwise/2" do
    test "appends otherwise value to when chain" do
      result =
        Functions.when_(
          Column.gt(Functions.col("age"), Functions.lit(18)),
          Functions.lit("adult")
        )
        |> Functions.otherwise(Functions.lit("minor"))

      assert %Column{
               expr:
                 {:fn, "when",
                  [
                    {:fn, ">", [{:col, "age"}, {:lit, 18}], false},
                    {:lit, "adult"},
                    {:lit, "minor"}
                  ], false}
             } = result
    end

    test "auto-coerces otherwise value" do
      result =
        Functions.when_(Column.gt(Functions.col("x"), Functions.lit(0)), "positive")
        |> Functions.otherwise("non-positive")

      assert %Column{
               expr: {:fn, "when", [_, {:lit, "positive"}, {:lit, "non-positive"}], false}
             } = result
    end

    test "raises when otherwise called twice via Functions" do
      col =
        Functions.when_(Functions.col("x") |> Column.gt(0), Functions.lit("pos"))
        |> Functions.otherwise("zero")

      assert_raise ArgumentError, ~r/otherwise.*already been called/, fn ->
        Functions.otherwise(col, "neg")
      end
    end
  end

  # ── Higher-order functions (HOF) with lambda ──

  describe "transform/2" do
    test "creates transform with lambda expression" do
      result =
        Functions.transform(Functions.col("arr"), fn x -> Column.plus(x, Functions.lit(1)) end)

      assert %Column{
               expr:
                 {:fn, "transform",
                  [
                    {:col, "arr"},
                    {:lambda, {:fn, "+", [{:lambda_var, "x"}, {:lit, 1}], false},
                     [{:lambda_var, "x"}]}
                  ], false}
             } = result
    end

    test "accepts string column name" do
      result = Functions.transform("arr", fn x -> Column.plus(x, Functions.lit(1)) end)
      assert %Column{expr: {:fn, "transform", [{:col, "arr"}, {:lambda, _, _}], false}} = result
    end
  end

  describe "filter/2" do
    test "creates filter with lambda expression" do
      result = Functions.filter(Functions.col("arr"), fn x -> Column.gt(x, Functions.lit(0)) end)

      assert %Column{
               expr:
                 {:fn, "filter",
                  [
                    {:col, "arr"},
                    {:lambda, {:fn, ">", [{:lambda_var, "x"}, {:lit, 0}], false},
                     [{:lambda_var, "x"}]}
                  ], false}
             } = result
    end
  end

  describe "exists/2" do
    test "creates exists with lambda expression" do
      result = Functions.exists(Functions.col("arr"), fn x -> Column.gt(x, Functions.lit(0)) end)

      assert %Column{
               expr:
                 {:fn, "exists",
                  [
                    {:col, "arr"},
                    {:lambda, {:fn, ">", [{:lambda_var, "x"}, {:lit, 0}], false},
                     [{:lambda_var, "x"}]}
                  ], false}
             } = result
    end
  end

  describe "forall/2" do
    test "creates forall with lambda expression" do
      result = Functions.forall(Functions.col("arr"), fn x -> Column.gt(x, Functions.lit(0)) end)

      assert %Column{
               expr:
                 {:fn, "forall",
                  [
                    {:col, "arr"},
                    {:lambda, {:fn, ">", _, false}, [{:lambda_var, "x"}]}
                  ], false}
             } = result
    end
  end

  describe "aggregate/3" do
    test "creates aggregate with two-arg lambda" do
      result =
        Functions.aggregate(Functions.col("arr"), Functions.lit(0), fn acc, x ->
          Column.plus(acc, x)
        end)

      assert %Column{
               expr:
                 {:fn, "aggregate",
                  [
                    {:col, "arr"},
                    {:lit, 0},
                    {:lambda, {:fn, "+", [{:lambda_var, "acc"}, {:lambda_var, "x"}], false},
                     [{:lambda_var, "acc"}, {:lambda_var, "x"}]}
                  ], false}
             } = result
    end

    test "auto-coerces zero value to literal" do
      result =
        Functions.aggregate(Functions.col("arr"), 0, fn acc, x ->
          Column.plus(acc, x)
        end)

      assert %Column{
               expr: {:fn, "aggregate", [{:col, "arr"}, {:lit, 0}, {:lambda, _, _}], false}
             } = result
    end
  end

  describe "reduce/3" do
    test "is alias for aggregate/3" do
      result =
        Functions.reduce(Functions.col("arr"), Functions.lit(0), fn acc, x ->
          Column.plus(acc, x)
        end)

      assert %Column{expr: {:fn, "aggregate", _, false}} = result
    end
  end

  describe "map_filter/2" do
    test "creates map_filter with two-arg lambda" do
      result =
        Functions.map_filter(Functions.col("m"), fn _k, v ->
          Column.gt(v, Functions.lit(0))
        end)

      assert %Column{
               expr:
                 {:fn, "map_filter",
                  [
                    {:col, "m"},
                    {:lambda, {:fn, ">", [{:lambda_var, "v"}, {:lit, 0}], false},
                     [{:lambda_var, "k"}, {:lambda_var, "v"}]}
                  ], false}
             } = result
    end
  end

  describe "map_zip_with/3" do
    test "creates map_zip_with with three-arg lambda" do
      result =
        Functions.map_zip_with(Functions.col("m1"), Functions.col("m2"), fn _k, v1, v2 ->
          Column.plus(v1, v2)
        end)

      assert %Column{
               expr:
                 {:fn, "map_zip_with",
                  [
                    {:col, "m1"},
                    {:col, "m2"},
                    {:lambda, {:fn, "+", [{:lambda_var, "v1"}, {:lambda_var, "v2"}], false},
                     [{:lambda_var, "k"}, {:lambda_var, "v1"}, {:lambda_var, "v2"}]}
                  ], false}
             } = result
    end
  end

  describe "transform_keys/2" do
    test "creates transform_keys with two-arg lambda" do
      result =
        Functions.transform_keys(Functions.col("m"), fn k, _v ->
          Column.plus(k, Functions.lit(1))
        end)

      assert %Column{
               expr:
                 {:fn, "transform_keys",
                  [
                    {:col, "m"},
                    {:lambda, {:fn, "+", [{:lambda_var, "k"}, {:lit, 1}], false},
                     [{:lambda_var, "k"}, {:lambda_var, "v"}]}
                  ], false}
             } = result
    end
  end

  describe "transform_values/2" do
    test "creates transform_values with two-arg lambda" do
      result =
        Functions.transform_values(Functions.col("m"), fn _k, v ->
          Column.plus(v, Functions.lit(1))
        end)

      assert %Column{
               expr:
                 {:fn, "transform_values",
                  [
                    {:col, "m"},
                    {:lambda, {:fn, "+", [{:lambda_var, "v"}, {:lit, 1}], false},
                     [{:lambda_var, "k"}, {:lambda_var, "v"}]}
                  ], false}
             } = result
    end
  end

  describe "zip_with/3" do
    test "creates zip_with with two-arg lambda" do
      result =
        Functions.zip_with(Functions.col("a1"), Functions.col("a2"), fn x, y ->
          Column.plus(x, y)
        end)

      assert %Column{
               expr:
                 {:fn, "zip_with",
                  [
                    {:col, "a1"},
                    {:col, "a2"},
                    {:lambda, {:fn, "+", [{:lambda_var, "x"}, {:lambda_var, "y"}], false},
                     [{:lambda_var, "x"}, {:lambda_var, "y"}]}
                  ], false}
             } = result
    end
  end

  # ── Spot-check new generated functions ──

  describe "new function categories - spot checks" do
    test "bitwise: bit_count/1" do
      result = Functions.bit_count(Functions.col("x"))
      assert %Column{expr: {:fn, "bit_count", [{:col, "x"}], false}} = result
    end

    test "bitwise: shiftleft/2" do
      result = Functions.shiftleft(Functions.col("x"), 2)
      assert %Column{expr: {:fn, "shiftleft", [{:col, "x"}, {:lit, 2}], false}} = result
    end

    test "json: get_json_object/2" do
      result = Functions.get_json_object(Functions.col("json"), "$.name")

      assert %Column{expr: {:fn, "get_json_object", [{:col, "json"}, {:lit, "$.name"}], false}} =
               result
    end

    test "json: json_array_length/1" do
      result = Functions.json_array_length(Functions.col("json"))
      assert %Column{expr: {:fn, "json_array_length", [{:col, "json"}], false}} = result
    end

    test "csv_xml: xpath_string/2" do
      result = Functions.xpath_string(Functions.col("xml"), "//name")

      assert %Column{expr: {:fn, "xpath_string", [{:col, "xml"}, {:lit, "//name"}], false}} =
               result
    end

    test "type: typeof/1" do
      result = Functions.typeof(Functions.col("x"))
      assert %Column{expr: {:fn, "typeof", [{:col, "x"}], false}} = result
    end

    test "type: to_number/2" do
      result = Functions.to_number(Functions.col("x"), "999.99")
      assert %Column{expr: {:fn, "to_number", [{:col, "x"}, {:lit, "999.99"}], false}} = result
    end

    test "encryption: aes_encrypt/n variadic" do
      result = Functions.aes_encrypt([Functions.col("data"), Functions.col("key")])
      assert %Column{expr: {:fn, "aes_encrypt", [{:col, "data"}, {:col, "key"}], false}} = result
    end

    test "session: uuid/0" do
      result = Functions.uuid()
      assert %Column{expr: {:fn, "uuid", [], false}} = result
    end

    test "session: current_catalog/0" do
      result = Functions.current_catalog()
      assert %Column{expr: {:fn, "current_catalog", [], false}} = result
    end

    test "session: version_/0" do
      result = Functions.version_()
      assert %Column{expr: {:fn, "version", [], false}} = result
    end

    test "conditional: equal_null/2" do
      result = Functions.equal_null(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "equal_null", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "conditional: assert_true/1" do
      result = Functions.assert_true(Functions.col("cond"))
      assert %Column{expr: {:fn, "assert_true", [{:col, "cond"}], false}} = result
    end

    test "math: try_add/2" do
      result = Functions.try_add(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "try_add", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "math: product/1" do
      result = Functions.product(Functions.col("x"))
      assert %Column{expr: {:fn, "product", [{:col, "x"}], false}} = result
    end

    test "string: substring_index/3" do
      result = Functions.substring_index(Functions.col("s"), ".", 2)

      assert %Column{expr: {:fn, "substring_index", [{:col, "s"}, {:lit, "."}, {:lit, 2}], false}} =
               result
    end

    test "string: levenshtein/2" do
      result = Functions.levenshtein(Functions.col("a"), Functions.col("b"))
      assert %Column{expr: {:fn, "levenshtein", [{:col, "a"}, {:col, "b"}], false}} = result
    end

    test "date: current_time/0" do
      result = Functions.current_time()
      assert %Column{expr: {:fn, "current_time", [], false}} = result
    end

    test "date: try_to_date/1 with no format" do
      result = Functions.try_to_date(Functions.col("d"))
      assert %Column{expr: {:fn, "try_to_date", [{:col, "d"}], false}} = result
    end

    test "date: timestamp_diff/2 (lit_then_cols)" do
      result =
        Functions.timestamp_diff("HOUR", [Functions.col("ts1"), Functions.col("ts2")])

      assert %Column{
               expr: {:fn, "timestampdiff", [{:lit, "HOUR"}, {:col, "ts1"}, {:col, "ts2"}], false}
             } = result
    end

    test "collection: array_append/2" do
      result = Functions.array_append(Functions.col("arr"), Functions.col("elem"))
      assert %Column{expr: {:fn, "array_append", [{:col, "arr"}, {:col, "elem"}], false}} = result
    end

    test "collection: arrays_zip/1 variadic" do
      result = Functions.arrays_zip([Functions.col("a1"), Functions.col("a2")])
      assert %Column{expr: {:fn, "arrays_zip", [{:col, "a1"}, {:col, "a2"}], false}} = result
    end

    test "aggregate: regr_slope/2" do
      result = Functions.regr_slope(Functions.col("y"), Functions.col("x"))
      assert %Column{expr: {:fn, "regr_slope", [{:col, "y"}, {:col, "x"}], false}} = result
    end

    test "aggregate: histogram_numeric/2" do
      result = Functions.histogram_numeric(Functions.col("x"), 10)
      assert %Column{expr: {:fn, "histogram_numeric", [{:col, "x"}, {:lit, 10}], false}} = result
    end
  end

  describe "spark 3.5 compatibility fallbacks" do
    test "to_time/1 composes to_timestamp + date_format" do
      assert %Column{
               expr:
                 {:fn, "date_format",
                  [{:fn, "to_timestamp", [{:col, "ts"}], false}, {:lit, "HH:mm:ss"}], false}
             } = Functions.to_time("ts")
    end

    test "time_diff/3 maps to timestampdiff" do
      assert %Column{
               expr: {:fn, "timestampdiff", [{:lit, "HOUR"}, {:col, "start_ts"}, {:col, "end_ts"}], false}
             } = Functions.time_diff("HOUR", "start_ts", "end_ts")
    end

    test "parse_json/1 uses local fallback expression" do
      assert %Column{
               expr: {:fn, "coalesce", [{:col, "js"}, {:lit, nil}], false}
             } = Functions.parse_json("js")
    end

    test "variant_get/3 maps to get_json_object" do
      assert %Column{
               expr: {:fn, "get_json_object", [{:col, "js"}, {:lit, "$.a"}], false}
             } = Functions.variant_get("js", "$.a", "int")
    end
  end

  # ── Aliases ──

  describe "function aliases" do
    test "sha is alias for sha1" do
      result = Functions.sha(Functions.col("x"))
      assert %Column{expr: {:fn, "sha1", [{:col, "x"}], false}} = result
    end

    test "chr is alias for char_" do
      result = Functions.chr(65)
      assert %Column{expr: {:fn, "char", [{:lit, 65}], false}} = result
    end

    test "current_schema is alias for current_database" do
      result = Functions.current_schema()
      assert %Column{expr: {:fn, "current_database", [], false}} = result
    end

    test "user_ is alias for current_user_" do
      result = Functions.user_()
      assert %Column{expr: {:fn, "current_user", [], false}} = result
    end

    test "to_varchar is alias for to_char_" do
      result = Functions.to_varchar(Functions.col("x"), "FM9999")
      assert %Column{expr: {:fn, "to_char", [{:col, "x"}, {:lit, "FM9999"}], false}} = result
    end

    test "std is alias for stddev" do
      result = Functions.std(Functions.col("x"))
      assert %Column{expr: {:fn, "stddev", [{:col, "x"}], false}} = result
    end

    test "cardinality is alias for size" do
      result = Functions.cardinality(Functions.col("arr"))
      assert %Column{expr: {:fn, "size", [{:col, "arr"}], false}} = result
    end

    test "xpath_number is alias for xpath_double" do
      result = Functions.xpath_number(Functions.col("xml"), "//price")

      assert %Column{expr: {:fn, "xpath_double", [{:col, "xml"}, {:lit, "//price"}], false}} =
               result
    end

    test "date_part is alias for extract" do
      result = Functions.date_part(Functions.lit("YEAR"), Functions.col("d"))
      assert %Column{expr: {:fn, "extract", [{:lit, "YEAR"}, {:col, "d"}], false}} = result
    end

    test "string_agg is alias for listagg" do
      result = Functions.string_agg(Functions.col("x"))
      assert %Column{expr: {:fn, "listagg", [{:col, "x"}], false}} = result
    end
  end
end
