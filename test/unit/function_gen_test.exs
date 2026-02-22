defmodule SparkEx.Unit.FunctionGenTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  # Tests that the macro-generated functions produce correct expression tuples
  # for each arity pattern in the registry.

  # ── :zero arity ──

  describe "zero-arity functions" do
    test "pi/0 returns zero-arg function expression" do
      assert %Column{expr: {:fn, "pi", [], false}} = Functions.pi()
    end

    test "e/0 returns zero-arg function expression" do
      assert %Column{expr: {:fn, "e", [], false}} = Functions.e()
    end

    test "current_date/0" do
      assert %Column{expr: {:fn, "current_date", [], false}} = Functions.current_date()
    end

    test "row_number/0" do
      assert %Column{expr: {:fn, "row_number", [], false}} = Functions.row_number()
    end

    test "monotonically_increasing_id/0" do
      assert %Column{expr: {:fn, "monotonically_increasing_id", [], false}} =
               Functions.monotonically_increasing_id()
    end
  end

  # ── :one_col arity ──

  describe "one-col functions" do
    test "accepts Column struct" do
      col = Functions.col("x")
      assert %Column{expr: {:fn, "abs", [{:col, "x"}], false}} = Functions.abs(col)
    end

    test "accepts string shorthand" do
      assert %Column{expr: {:fn, "abs", [{:col, "x"}], false}} = Functions.abs("x")
    end

    test "accepts atom shorthand" do
      assert %Column{expr: {:fn, "lower", [{:col, "name"}], false}} = Functions.lower(:name)
    end

    test "sqrt/1" do
      assert %Column{expr: {:fn, "sqrt", [{:col, "v"}], false}} = Functions.sqrt("v")
    end

    test "year/1" do
      assert %Column{expr: {:fn, "year", [{:col, "ts"}], false}} = Functions.year("ts")
    end

    test "explode/1" do
      assert %Column{expr: {:fn, "explode", [{:col, "arr"}], false}} = Functions.explode("arr")
    end
  end

  # ── :two_col arity ──

  describe "two-col functions" do
    test "accepts two Column structs" do
      a = Functions.col("a")
      b = Functions.col("b")

      assert %Column{expr: {:fn, "atan2", [{:col, "a"}, {:col, "b"}], false}} =
               Functions.atan2(a, b)
    end

    test "accepts string shorthands" do
      assert %Column{expr: {:fn, "hypot", [{:col, "a"}, {:col, "b"}], false}} =
               Functions.hypot("a", "b")
    end

    test "corr/2" do
      assert %Column{expr: {:fn, "corr", [{:col, "x"}, {:col, "y"}], false}} =
               Functions.corr("x", "y")
    end

    test "datediff/2" do
      assert %Column{expr: {:fn, "datediff", [{:col, "d1"}, {:col, "d2"}], false}} =
               Functions.datediff("d1", "d2")
    end
  end

  # ── :three_col arity ──

  describe "three-col functions" do
    test "conv/3" do
      assert %Column{expr: {:fn, "conv", [{:col, "n"}, {:lit, "from"}, {:lit, "to"}], false}} =
               Functions.conv("n", "from", "to")
    end

    test "translate/3" do
      assert %Column{expr: {:fn, "translate", [{:col, "s"}, {:lit, "m"}, {:lit, "r"}], false}} =
               Functions.translate("s", "m", "r")
    end

    test "nvl2/3" do
      assert %Column{expr: {:fn, "nvl2", [{:col, "a"}, {:col, "b"}, {:col, "c"}], false}} =
               Functions.nvl2("a", "b", "c")
    end
  end

  # ── :n_col arity ──

  describe "n-col (variadic) functions" do
    test "coalesce accepts list of columns" do
      assert %Column{expr: {:fn, "coalesce", [{:col, "a"}, {:col, "b"}, {:col, "c"}], false}} =
               Functions.coalesce(["a", "b", "c"])
    end

    test "array accepts list of columns" do
      assert %Column{expr: {:fn, "array", [{:col, "x"}, {:col, "y"}], false}} =
               Functions.array(["x", "y"])
    end

    test "concat accepts list of columns" do
      assert %Column{expr: {:fn, "concat", [{:col, "a"}, {:col, "b"}], false}} =
               Functions.concat(["a", "b"])
    end

    test "hash accepts list" do
      assert %Column{expr: {:fn, "hash", [{:col, "a"}, {:col, "b"}], false}} =
               Functions.hash(["a", "b"])
    end

    test "empty list" do
      assert %Column{expr: {:fn, "coalesce", [], false}} = Functions.coalesce([])
    end
  end

  # ── {:lit, 1} arity ──

  describe "{:lit, 1} functions" do
    test "ntile/1 wraps argument as literal" do
      assert %Column{expr: {:fn, "ntile", [{:lit, 4}], false}} = Functions.ntile(4)
    end
  end

  # ── {:col_lit, 1} arity ──

  describe "{:col_lit, 1} functions" do
    test "repeat/2 - column + literal" do
      assert %Column{expr: {:fn, "repeat", [{:col, "s"}, {:lit, 3}], false}} =
               Functions.repeat("s", 3)
    end

    test "date_add/2" do
      assert %Column{expr: {:fn, "date_add", [{:col, "d"}, {:lit, 7}], false}} =
               Functions.date_add("d", 7)
    end

    test "format_number/2" do
      assert %Column{expr: {:fn, "format_number", [{:col, "v"}, {:lit, 2}], false}} =
               Functions.format_number("v", 2)
    end

    test "nth_value/2" do
      assert %Column{expr: {:fn, "nth_value", [{:col, "v"}, {:lit, 3}, {:lit, false}], false}} =
               Functions.nth_value("v", 3)
    end
  end

  # ── {:col_lit, 2} arity ──

  describe "{:col_lit, 2} functions" do
    test "substring/3 - column + two literals" do
      assert %Column{expr: {:fn, "substring", [{:col, "s"}, {:lit, 1}, {:lit, 5}], false}} =
               Functions.substring("s", 1, 5)
    end

    test "lpad/3" do
      assert %Column{expr: {:fn, "lpad", [{:col, "s"}, {:lit, 10}, {:lit, "0"}], false}} =
               Functions.lpad("s", 10, "0")
    end

    test "percentile_approx/3" do
      assert %Column{
               expr: {:fn, "percentile_approx", [{:col, "v"}, {:lit, 0.5}, {:lit, 1000}], false}
             } =
               Functions.percentile_approx("v", 0.5, 1000)
    end
  end

  # ── {:col_lit, 3} arity ──

  describe "{:col_lit, 3} functions" do
    test "width_bucket/4" do
      assert %Column{
               expr:
                 {:fn, "width_bucket", [{:col, "v"}, {:lit, 0}, {:lit, 100}, {:lit, 10}], false}
             } = Functions.width_bucket("v", 0, 100, 10)
    end
  end

  # ── {:lit_then_cols, 1} arity ──

  describe "{:lit_then_cols, 1} functions" do
    test "concat_ws/2 - literal separator + column list" do
      assert %Column{expr: {:fn, "concat_ws", [{:lit, ","}, {:col, "a"}, {:col, "b"}], false}} =
               Functions.concat_ws(",", ["a", "b"])
    end

    test "date_trunc/2 accepts positional column argument" do
      assert %Column{expr: {:fn, "date_trunc", [{:lit, "month"}, {:col, "ts"}], false}} =
               Functions.date_trunc("month", "ts")
    end

    test "format_string/2" do
      assert %Column{
               expr: {:fn, "format_string", [{:lit, "%s=%d"}, {:col, "k"}, {:col, "v"}], false}
             } = Functions.format_string("%s=%d", ["k", "v"])
    end
  end

  # ── {:col_opt, defaults} arity ──

  describe "{:col_opt, defaults} functions" do
    test "round/1 uses default scale=0" do
      assert %Column{expr: {:fn, "round", [{:col, "v"}, {:lit, 0}], false}} =
               Functions.round("v")
    end

    test "round/2 with explicit scale" do
      assert %Column{expr: {:fn, "round", [{:col, "v"}, {:lit, 2}], false}} =
               Functions.round("v", scale: 2)
    end

    test "bround/1 uses default" do
      assert %Column{expr: {:fn, "bround", [{:col, "v"}, {:lit, 0}], false}} =
               Functions.bround("v")
    end

    test "to_date/1 with nil default omits trailing nil" do
      assert %Column{expr: {:fn, "to_date", [{:col, "s"}], false}} =
               Functions.to_date("s")
    end

    test "to_date/2 with explicit format" do
      assert %Column{expr: {:fn, "to_date", [{:col, "s"}, {:lit, "yyyy-MM-dd"}], false}} =
               Functions.to_date("s", format: "yyyy-MM-dd")
    end

    test "to_date/2 accepts positional format argument" do
      assert %Column{expr: {:fn, "to_date", [{:col, "s"}, {:lit, "yyyy-MM-dd"}], false}} =
               Functions.to_date("s", Functions.lit("yyyy-MM-dd"))
    end

    test "to_timestamp/1 with nil default" do
      assert %Column{expr: {:fn, "to_timestamp", [{:col, "s"}], false}} =
               Functions.to_timestamp("s")
    end

    test "to_timestamp/2 with format" do
      assert %Column{
               expr: {:fn, "to_timestamp", [{:col, "s"}, {:lit, "yyyy-MM-dd HH:mm:ss"}], false}
             } =
               Functions.to_timestamp("s", format: "yyyy-MM-dd HH:mm:ss")
    end

    test "lag/1 uses defaults offset=1, default=nil" do
      assert %Column{expr: {:fn, "lag", [{:col, "v"}, {:lit, 1}], false}} =
               Functions.lag("v")
    end

    test "lag/2 with explicit offset, nil default trimmed" do
      assert %Column{expr: {:fn, "lag", [{:col, "v"}, {:lit, 3}], false}} =
               Functions.lag("v", offset: 3)
    end

    test "lag/2 accepts positional offset argument" do
      assert %Column{expr: {:fn, "lag", [{:col, "v"}, {:lit, 3}], false}} =
               Functions.lag("v", 3)
    end

    test "lag/2 with explicit offset and default" do
      assert %Column{expr: {:fn, "lag", [{:col, "v"}, {:lit, 2}, {:lit, 0}], false}} =
               Functions.lag("v", offset: 2, default: 0)
    end

    test "lead/1 uses defaults" do
      assert %Column{expr: {:fn, "lead", [{:col, "v"}, {:lit, 1}], false}} =
               Functions.lead("v")
    end

    test "rand/0 generates random seed" do
      assert %Column{expr: {:fn, "rand", [{:lit, seed}], false}} = Functions.rand()
      assert is_integer(seed)
    end

    test "rand/1 with explicit seed" do
      assert %Column{expr: {:fn, "rand", [{:lit, 42}], false}} = Functions.rand(42)
    end

    test "sort_array/1 with default asc=true" do
      assert %Column{expr: {:fn, "sort_array", [{:col, "arr"}, {:lit, true}], false}} =
               Functions.sort_array("arr")
    end

    test "sort_array/2 with asc=false" do
      assert %Column{expr: {:fn, "sort_array", [{:col, "arr"}, {:lit, false}], false}} =
               Functions.sort_array("arr", asc: false)
    end
  end

  # ── is_distinct flag ──

  describe "is_distinct flag" do
    test "count_distinct sets is_distinct=true" do
      assert %Column{expr: {:fn, "count", [{:col, "x"}], true}} =
               Functions.count_distinct("x")
    end

    test "sum_distinct sets is_distinct=true" do
      assert %Column{expr: {:fn, "sum", [{:col, "x"}], true}} =
               Functions.sum_distinct("x")
    end

    test "regular count is not distinct" do
      assert %Column{expr: {:fn, "count", [{:col, "x"}], false}} =
               Functions.count("x")
    end
  end

  # ── Aliases ──

  describe "aliases" do
    test "ceiling is alias for ceil" do
      assert Functions.ceiling("x") == Functions.ceil("x")
    end

    test "to_degrees is alias for degrees" do
      assert Functions.to_degrees("x") == Functions.degrees("x")
    end

    test "to_radians is alias for radians" do
      assert Functions.to_radians("x") == Functions.radians("x")
    end

    test "power is alias for pow" do
      assert Functions.power("a", "b") == Functions.pow("a", "b")
    end

    test "sign is alias for signum" do
      assert Functions.sign("x") == Functions.signum("x")
    end

    test "ln is alias for log" do
      assert Functions.ln("x") == Functions.log("x")
    end

    test "lcase is alias for lower" do
      assert Functions.lcase("x") == Functions.lower("x")
    end

    test "ucase is alias for upper" do
      assert Functions.ucase("x") == Functions.upper("x")
    end

    test "character_length is alias for char_length" do
      assert Functions.character_length("x") == Functions.char_length("x")
    end

    test "curdate is alias for current_date" do
      assert Functions.curdate() == Functions.current_date()
    end

    test "now is alias for current_timestamp" do
      assert Functions.now() == Functions.current_timestamp()
    end

    test "dayofmonth is alias for day" do
      assert Functions.dayofmonth("d") == Functions.day("d")
    end

    test "dateadd is alias for date_add" do
      assert Functions.dateadd("d", 1) == Functions.date_add("d", 1)
    end

    test "date_diff is alias for datediff" do
      assert Functions.date_diff("a", "b") == Functions.datediff("a", "b")
    end

    test "mean is alias for avg" do
      assert Functions.mean("x") == Functions.avg("x")
    end

    test "first_value is alias for first" do
      assert Functions.first_value("x") == Functions.first("x")
    end

    test "last_value is alias for last" do
      assert Functions.last_value("x") == Functions.last("x")
    end

    test "array_agg is alias for collect_list" do
      assert Functions.array_agg("x") == Functions.collect_list("x")
    end

    test "stddev_samp is alias for stddev" do
      assert Functions.stddev_samp("x") == Functions.stddev("x")
    end

    test "var_samp is alias for variance" do
      assert Functions.var_samp("x") == Functions.variance("x")
    end

    test "every is alias for bool_and" do
      assert Functions.every("x") == Functions.bool_and("x")
    end

    test "some is alias for bool_or" do
      assert Functions.some("x") == Functions.bool_or("x")
    end

    test "cardinality is alias for size" do
      assert Functions.cardinality("x") == Functions.size("x")
    end
  end

  # ── Column passthrough ──

  describe "column expression passthrough" do
    test "nested function expressions compose correctly" do
      inner = Functions.abs("x")
      outer = Functions.sqrt(inner)

      assert %Column{expr: {:fn, "sqrt", [{:fn, "abs", [{:col, "x"}], false}], false}} = outer
    end

    test "lit values as column arguments pass through" do
      lit_col = Functions.lit(42)
      result = Functions.abs(lit_col)

      assert %Column{expr: {:fn, "abs", [{:lit, 42}], false}} = result
    end
  end

  # ── Spot checks across categories ──

  describe "spot checks across categories" do
    test "math: factorial" do
      assert %Column{expr: {:fn, "factorial", [{:col, "n"}], false}} = Functions.factorial("n")
    end

    test "string: soundex" do
      assert %Column{expr: {:fn, "soundex", [{:col, "name"}], false}} = Functions.soundex("name")
    end

    test "collection: size uses generic size function" do
      assert %Column{expr: {:fn, "size", [{:col, "x"}], false}} = Functions.size("x")
    end

    test "datetime: quarter" do
      assert %Column{expr: {:fn, "quarter", [{:col, "d"}], false}} = Functions.quarter("d")
    end

    test "collection: array_distinct" do
      assert %Column{expr: {:fn, "array_distinct", [{:col, "arr"}], false}} =
               Functions.array_distinct("arr")
    end

    test "collection: create_map" do
      assert %Column{expr: {:fn, "map", [{:col, "k"}, {:col, "v"}], false}} =
               Functions.create_map(["k", "v"])
    end

    test "aggregate: median" do
      assert %Column{expr: {:fn, "median", [{:col, "v"}], false}} = Functions.median("v")
    end

    test "window: dense_rank" do
      assert %Column{expr: {:fn, "dense_rank", [], false}} = Functions.dense_rank()
    end

    test "conditional: greatest" do
      assert %Column{expr: {:fn, "greatest", [{:col, "a"}, {:col, "b"}], false}} =
               Functions.greatest(["a", "b"])
    end

    test "hash: md5" do
      assert %Column{expr: {:fn, "md5", [{:col, "data"}], false}} = Functions.md5("data")
    end

    test "hash: sha2" do
      assert %Column{expr: {:fn, "sha2", [{:col, "data"}, {:lit, 256}], false}} =
               Functions.sha2("data", 256)
    end

    test "misc: typeof" do
      assert %Column{expr: {:fn, "typeof", [{:col, "x"}], false}} = Functions.typeof("x")
    end
  end
end
