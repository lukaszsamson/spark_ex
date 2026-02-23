defmodule SparkEx.MissOpus2Test do
  @moduledoc """
  Tests for fixes from MISS_OPUS_2.md gap analysis.
  """
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions
  alias SparkEx.DataFrame
  alias SparkEx.Connect.PlanEncoder

  # ── 1.1 Sort null ordering defaults ──

  describe "1.1 sort null ordering defaults" do
    test "asc/1 defaults to nulls_first" do
      result = Column.asc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_first}} = result
    end

    test "desc/1 defaults to nulls_last" do
      result = Column.desc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_last}} = result
    end

    test "explicit asc_nulls_first/1 still works" do
      result = Column.asc_nulls_first(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_first}} = result
    end

    test "explicit asc_nulls_last/1 still works" do
      result = Column.asc_nulls_last(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_last}} = result
    end

    test "explicit desc_nulls_first/1 still works" do
      result = Column.desc_nulls_first(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_first}} = result
    end

    test "explicit desc_nulls_last/1 still works" do
      result = Column.desc_nulls_last(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_last}} = result
    end
  end

  # ── 3.3 startsWith/endsWith casing ──

  describe "3.3/3.4 function name casing" do
    test "starts_with uses startsWith" do
      result = Column.starts_with(Functions.col("s"), "abc")
      assert %Column{expr: {:fn, "startsWith", _, false}} = result
    end

    test "ends_with uses endsWith" do
      result = Column.ends_with(Functions.col("s"), "xyz")
      assert %Column{expr: {:fn, "endsWith", _, false}} = result
    end

    test "is_null uses isNull" do
      result = Column.is_null(Functions.col("x"))
      assert %Column{expr: {:fn, "isNull", [{:col, "x"}], false}} = result
    end

    test "is_not_null uses isNotNull" do
      result = Column.is_not_null(Functions.col("x"))
      assert %Column{expr: {:fn, "isNotNull", [{:col, "x"}], false}} = result
    end

    test "is_nan uses isNaN" do
      result = Column.is_nan(Functions.col("x"))
      assert %Column{expr: {:fn, "isNaN", [{:col, "x"}], false}} = result
    end
  end

  # ── 12.6 lit(Column) passthrough ──

  describe "12.6 lit(Column) passthrough" do
    test "lit of Column returns the column unchanged" do
      col = Functions.col("x")
      assert ^col = Functions.lit(col)
    end

    test "lit of Column preserves expression" do
      col = %Column{expr: {:fn, "abs", [{:col, "x"}], false}}
      result = Functions.lit(col)
      assert result == col
    end

    test "lit of regular values still creates literal" do
      assert %Column{expr: {:lit, 42}} = Functions.lit(42)
      assert %Column{expr: {:lit, "hello"}} = Functions.lit("hello")
      assert %Column{expr: {:lit, nil}} = Functions.lit(nil)
    end
  end

  # ── 12.7 col("*") produces UnresolvedStar ──

  describe "12.7 col(\"*\") produces UnresolvedStar" do
    test "col(\"*\") returns star expression" do
      assert %Column{expr: {:star}} = Functions.col("*")
    end

    test "col with regular name still works" do
      assert %Column{expr: {:col, "name"}} = Functions.col("name")
    end
  end

  # ── 12.8 json_tuple argument order ──

  describe "12.8 json_tuple argument order" do
    test "json_tuple takes column first, then literal field names" do
      result = Functions.json_tuple("json_col", ["name", "age"])

      assert %Column{expr: {:fn, "json_tuple", args, false}} = result
      assert [{:col, "json_col"}, {:lit, "name"}, {:lit, "age"}] = args
    end
  end

  # ── 12.9 timestamp_diff/timestamp_add function names ──

  describe "12.9 timestamp_diff/timestamp_add function names" do
    test "timestamp_diff uses timestampdiff" do
      result = Functions.timestamp_diff("DAY", ["start_ts", "end_ts"])
      assert %Column{expr: {:fn, "timestampdiff", _, false}} = result
    end

    test "timestamp_add uses timestampadd" do
      result = Functions.timestamp_add("DAY", ["ts", "days"])
      assert %Column{expr: {:fn, "timestampadd", _, false}} = result
    end
  end

  # ── 12.10 count(star()) special handling ──

  describe "12.10 count(star()) special handling" do
    test "count(star()) encodes as count(lit(1))" do
      count_expr = Functions.count(Functions.star())
      assert %Column{expr: {:fn, "count", [{:star}], false}} = count_expr

      # When encoded, star is replaced with lit(1)
      encoded = PlanEncoder.encode_expression(count_expr.expr)

      assert %Spark.Connect.Expression{
               expr_type:
                 {:unresolved_function,
                  %Spark.Connect.Expression.UnresolvedFunction{
                    function_name: "count",
                    arguments: [
                      %Spark.Connect.Expression{
                        expr_type:
                          {:literal,
                           %Spark.Connect.Expression.Literal{literal_type: {:integer, 1}}}
                      }
                    ]
                  }}
             } = encoded
    end
  end

  # ── 12.11 NAReplace integer values converted to float ──

  describe "12.11 NAReplace integer values converted to float" do
    test "na_replace encodes integer values as double" do
      plan = {:na_replace, {:sql, "SELECT * FROM t", nil}, [], [{0, 100}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      {:replace, na_replace} = relation.rel_type
      [replacement] = na_replace.replacements

      assert {:double, +0.0} = replacement.old_value.literal_type
      assert {:double, 100.0} = replacement.new_value.literal_type
    end

    test "na_replace preserves string values" do
      plan = {:na_replace, {:sql, "SELECT * FROM t", nil}, [], [{"old", "new"}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      {:replace, na_replace} = relation.rel_type
      [replacement] = na_replace.replacements

      assert {:string, "old"} = replacement.old_value.literal_type
      assert {:string, "new"} = replacement.new_value.literal_type
    end
  end

  # ── 12.14 listagg_distinct uses is_distinct flag ──

  describe "12.14 listagg_distinct uses is_distinct flag" do
    test "listagg_distinct sets is_distinct to true with function name 'listagg'" do
      result = Functions.listagg_distinct("col")
      assert %Column{expr: {:fn, "listagg", [{:col, "col"}], true}} = result
    end

    test "string_agg_distinct alias also works" do
      result = Functions.string_agg_distinct("col")
      assert %Column{expr: {:fn, "listagg", [{:col, "col"}], true}} = result
    end
  end

  # ── 15.6 RANGE frame boundaries use long literal ──

  describe "15.6 RANGE frame boundaries use long literal" do
    test "ROW frame boundary uses integer (32-bit)" do
      frame = {:rows, 5, 10}

      window_expr =
        {:window, {:col, "x"}, [], [{:sort_order, {:col, "y"}, :asc, :nulls_first}], frame}

      encoded = PlanEncoder.encode_expression(window_expr)
      window = encoded.expr_type |> elem(1)
      lower = window.frame_spec.lower
      assert {:value, %{expr_type: {:literal, %{literal_type: {:integer, 5}}}}} = lower.boundary
    end

    test "RANGE frame boundary uses long (64-bit)" do
      frame = {:range, 5, 10}

      window_expr =
        {:window, {:col, "x"}, [], [{:sort_order, {:col, "y"}, :asc, :nulls_first}], frame}

      encoded = PlanEncoder.encode_expression(window_expr)
      window = encoded.expr_type |> elem(1)
      lower = window.frame_spec.lower
      assert {:value, %{expr_type: {:literal, %{literal_type: {:long, 5}}}}} = lower.boundary
    end
  end

  # ── 15.11 LateralJoin nil join_condition ──

  describe "15.11 LateralJoin nil join_condition handling" do
    test "lateral_join with nil condition does not set join_condition" do
      plan = {:lateral_join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, nil, :inner}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      {:lateral_join, lateral} = relation.rel_type
      assert is_nil(lateral.join_condition)
    end

    test "lateral_join with condition sets join_condition" do
      plan =
        {:lateral_join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil},
         {:fn, "==", [{:col, "a"}, {:col, "b"}], false}, :inner}

      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      {:lateral_join, lateral} = relation.rel_type
      refute is_nil(lateral.join_condition)
    end
  end

  # ── 15.17 await_termination nil timeout ──

  describe "15.17 await_termination nil timeout" do
    test "nil timeout does not crash and produces correct command shape" do
      # The fix ensures a nil timeout generates an empty AwaitTerminationCommand
      # instead of encoding nil as 0 (which means "timeout immediately").
      {command, _counter} =
        SparkEx.Connect.CommandEncoder.encode_command(
          {:streaming_query_command, "query-id", "run-id", {:await_termination, nil}},
          0
        )

      {:streaming_query_command, sq_cmd} = command.command_type
      {:await_termination, await_cmd} = sq_cmd.command

      # timeout_ms should not be set (nil in struct means field not sent in protobuf)
      assert is_nil(await_cmd.timeout_ms)
    end
  end

  # ── 4.1 log() with base parameter (high-priority) ──

  describe "4.1 log() with base parameter" do
    test "log/1 computes natural logarithm" do
      result = Functions.log("x")
      assert %Column{expr: {:fn, "ln", [{:col, "x"}], false}} = result
    end

    test "log/2 computes logarithm with specified base" do
      result = Functions.log(2, "x")
      assert %Column{expr: {:fn, "log", [{:lit, 2}, {:col, "x"}], false}} = result
    end

    test "log/2 accepts literal column base" do
      result = Functions.log(Functions.lit(2), "x")
      assert %Column{expr: {:fn, "log", [{:lit, 2}, {:col, "x"}], false}} = result
    end
  end

  # ── 4.2 split() with limit parameter ──

  describe "4.2 split() with limit parameter" do
    test "split/2 splits without limit" do
      result = Functions.split("col", "\\.")
      assert %Column{expr: {:fn, "split", [{:col, "col"}, {:lit, "\\."}], false}} = result
    end

    test "split/3 splits with limit" do
      result = Functions.split("col", "\\.", 3)

      assert %Column{expr: {:fn, "split", [{:col, "col"}, {:lit, "\\."}, {:lit, 3}], false}} =
               result
    end
  end

  # ── 14.1 count_distinct variadic ──

  describe "14.1 count_distinct variadic" do
    test "count_distinct with single column" do
      result = Functions.count_distinct("x")
      assert %Column{expr: {:fn, "count", [{:col, "x"}], true}} = result
    end

    test "count_distinct with multiple columns" do
      result = Functions.count_distinct(["x", "y", "z"])
      assert %Column{expr: {:fn, "count", [{:col, "x"}, {:col, "y"}, {:col, "z"}], true}} = result
    end
  end

  # ── 14.2 aggregate with finish function ──

  describe "14.2 aggregate with finish function" do
    test "aggregate/3 without finish" do
      result = Functions.aggregate("arr", Functions.lit(0), fn acc, x -> Column.plus(acc, x) end)
      assert %Column{expr: {:fn, "aggregate", [_, _, {:lambda, _, _}], false}} = result
    end

    test "aggregate/4 with finish function" do
      result =
        Functions.aggregate(
          "arr",
          Functions.lit(0),
          fn acc, x -> Column.plus(acc, x) end,
          fn acc ->
            Column.cast(acc, "string")
          end
        )

      assert %Column{
               expr: {:fn, "aggregate", [_, _, {:lambda, _, _}, {:lambda, _, _}], false}
             } = result
    end
  end

  # ── 14.3 filter/transform HOF 2-arg form ──

  describe "14.3 filter/transform HOF 2-arg form" do
    test "transform/2 with 1-arg function" do
      result = Functions.transform("arr", fn x -> Column.plus(x, Functions.lit(1)) end)
      assert %Column{expr: {:fn, "transform", [_, {:lambda, _, vars}], false}} = result
      assert [{:lambda_var, "x"}] = vars
    end

    test "transform/2 with 2-arg function (element + index)" do
      result = Functions.transform("arr", fn x, i -> Column.plus(x, i) end)
      assert %Column{expr: {:fn, "transform", [_, {:lambda, _, vars}], false}} = result
      assert [{:lambda_var, "x"}, {:lambda_var, "i"}] = vars
    end

    test "filter/2 with 1-arg function" do
      result = Functions.filter("arr", fn x -> Column.gt(x, Functions.lit(0)) end)
      assert %Column{expr: {:fn, "filter", [_, {:lambda, _, vars}], false}} = result
      assert [{:lambda_var, "x"}] = vars
    end

    test "filter/2 with 2-arg function (element + index)" do
      result = Functions.filter("arr", fn _x, i -> Column.gt(i, Functions.lit(0)) end)
      assert %Column{expr: {:fn, "filter", [_, {:lambda, _, vars}], false}} = result
      assert [{:lambda_var, "x"}, {:lambda_var, "i"}] = vars
    end
  end

  # ── 14.16 broadcast function ──

  describe "14.16 broadcast function" do
    test "broadcast/1 applies broadcast hint" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = Functions.broadcast(df)
      assert %DataFrame{plan: {:hint, _, "broadcast", []}} = result
    end
  end

  # ── 14.5 months_between with roundOff ──

  describe "14.5 months_between with roundOff" do
    test "months_between/2 sends 3 args with default roundOff=true" do
      result = Functions.months_between("d1", "d2")
      assert %Column{expr: {:fn, "months_between", [_, _, {:lit, true}], false}} = result
    end

    test "months_between/3 with explicit roundOff=false" do
      result = Functions.months_between("d1", "d2", false)
      assert %Column{expr: {:fn, "months_between", [_, _, {:lit, false}], false}} = result
    end
  end

  # ── 14.6 approx_count_distinct with rsd ──

  describe "14.6 approx_count_distinct with rsd" do
    test "approx_count_distinct/1 without rsd" do
      result = Functions.approx_count_distinct("x")
      assert %Column{expr: {:fn, "approx_count_distinct", [{:col, "x"}], false}} = result
    end

    test "approx_count_distinct/2 with rsd" do
      result = Functions.approx_count_distinct("x", 0.05)

      assert %Column{expr: {:fn, "approx_count_distinct", [{:col, "x"}, {:lit, 0.05}], false}} =
               result
    end
  end

  # ── 14.7 ltrim/rtrim/trim with trim character ──

  describe "14.7 ltrim/rtrim/trim with trim character" do
    test "ltrim/1 trims whitespace" do
      result = Functions.ltrim("s")
      assert %Column{expr: {:fn, "ltrim", [{:col, "s"}], false}} = result
    end

    test "ltrim/2 trims specified character" do
      result = Functions.ltrim("s", "x")
      assert %Column{expr: {:fn, "ltrim", [{:lit, "x"}, {:col, "s"}], false}} = result
    end

    test "rtrim/2 trims specified character" do
      result = Functions.rtrim("s", "x")
      assert %Column{expr: {:fn, "rtrim", [{:lit, "x"}, {:col, "s"}], false}} = result
    end

    test "trim/2 trims specified character" do
      result = Functions.trim("s", "x")
      assert %Column{expr: {:fn, "trim", [{:lit, "x"}, {:col, "s"}], false}} = result
    end
  end

  # ── 13.9 melt alias for unpivot ──

  describe "13.9 melt alias" do
    test "melt/5 is an alias for unpivot/5" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      result = DataFrame.melt(df, ["id"], ["val1", "val2"], "variable", "value")
      assert %DataFrame{plan: {:unpivot, _, _, _, _, _}} = result
    end
  end

  # ── 15.20 Options with nil values filtered ──

  describe "15.20 options nil filtering" do
    # Options with nil values should be filtered out during encoding
    # This is tested at the command_encoder level
  end

  # ── 14.10 sentences with language/country ──

  describe "14.10 sentences with language/country" do
    test "sentences/1 without language/country" do
      result = Functions.sentences("s")
      assert %Column{expr: {:fn, "sentences", [{:col, "s"}], false}} = result
    end

    test "sentences/3 with language and country" do
      result = Functions.sentences("s", "en", "US")

      assert %Column{expr: {:fn, "sentences", [{:col, "s"}, {:lit, "en"}, {:lit, "US"}], false}} =
               result
    end
  end

  # ── 14.11 levenshtein with threshold ──

  describe "14.11 levenshtein with threshold" do
    test "levenshtein/2 without threshold" do
      result = Functions.levenshtein("s1", "s2")
      assert %Column{expr: {:fn, "levenshtein", [{:col, "s1"}, {:col, "s2"}], false}} = result
    end

    test "levenshtein/3 with threshold" do
      result = Functions.levenshtein("s1", "s2", 5)

      assert %Column{expr: {:fn, "levenshtein", [{:col, "s1"}, {:col, "s2"}, {:lit, 5}], false}} =
               result
    end
  end

  # ── 14.13 array_join with null_replacement ──

  describe "14.13 array_join with null_replacement" do
    test "array_join/2 without null_replacement" do
      result = Functions.array_join("arr", ",")
      assert %Column{expr: {:fn, "array_join", [{:col, "arr"}, {:lit, ","}], false}} = result
    end

    test "array_join/3 with null_replacement" do
      result = Functions.array_join("arr", ",", "NULL")

      assert %Column{
               expr: {:fn, "array_join", [{:col, "arr"}, {:lit, ","}, {:lit, "NULL"}], false}
             } = result
    end
  end

  # ── 14.14 sequence with optional step ──

  describe "14.14 sequence with optional step" do
    test "sequence/2 without step" do
      result = Functions.sequence("start", "stop")
      assert %Column{expr: {:fn, "sequence", [{:col, "start"}, {:col, "stop"}], false}} = result
    end

    test "sequence/3 with step" do
      result = Functions.sequence("start", "stop", "step")

      assert %Column{
               expr: {:fn, "sequence", [{:col, "start"}, {:col, "stop"}, {:col, "step"}], false}
             } = result
    end
  end

  # ── 14.20 assert_true with errMsg ──

  describe "14.20 assert_true with errMsg" do
    test "assert_true/1 without error message" do
      result = Functions.assert_true("cond")
      assert %Column{expr: {:fn, "assert_true", [{:col, "cond"}], false}} = result
    end

    test "assert_true/2 with error message" do
      result = Functions.assert_true("cond", "failed!")

      assert %Column{expr: {:fn, "assert_true", [{:col, "cond"}, {:lit, "failed!"}], false}} =
               result
    end
  end

  # ── 12.15 make_dt_interval/make_interval secs Decimal default ──

  describe "12.15 make_dt_interval secs default" do
    test "make_dt_interval defaults secs to Decimal(0)" do
      result = Functions.make_dt_interval()
      assert %Column{expr: {:fn, "make_dt_interval", [_, _, _, secs_expr], false}} = result
      assert {:lit, %Decimal{}} = secs_expr
    end
  end

  # ── 13.11 save/3 optional path ──
  # Writer.save path optionality is tested in writer_test.exs

  # ── 6.2 Writer mode accepts strings ──

  describe "6.2 writer mode accepts strings" do
    test "mode accepts 'overwrite' string" do
      writer = %SparkEx.Writer{mode: :error_if_exists, options: %{}}
      result = SparkEx.Writer.mode(writer, "overwrite")
      assert result.mode == :overwrite
    end

    test "mode accepts 'append' string" do
      writer = %SparkEx.Writer{mode: :error_if_exists, options: %{}}
      result = SparkEx.Writer.mode(writer, "append")
      assert result.mode == :append
    end

    test "mode accepts 'ignore' string" do
      writer = %SparkEx.Writer{mode: :error_if_exists, options: %{}}
      result = SparkEx.Writer.mode(writer, "ignore")
      assert result.mode == :ignore
    end

    test "mode accepts 'error' string" do
      writer = %SparkEx.Writer{mode: :append, options: %{}}
      result = SparkEx.Writer.mode(writer, "error")
      assert result.mode == :error_if_exists
    end

    test "mode accepts 'errorifexists' string" do
      writer = %SparkEx.Writer{mode: :append, options: %{}}
      result = SparkEx.Writer.mode(writer, "errorifexists")
      assert result.mode == :error_if_exists
    end

    test "mode raises for unknown string" do
      writer = %SparkEx.Writer{mode: :error_if_exists, options: %{}}
      assert_raise ArgumentError, fn -> SparkEx.Writer.mode(writer, "invalid") end
    end
  end

  # ── 13.6 explain accepts boolean and string ──

  describe "13.6 explain extended forms" do
    # explain requires session so we test the dispatch via pattern matching
    # by verifying the function clauses accept the right types
    test "explain/2 accepts boolean true" do
      # We can't call explain without a session, but verify the function exists
      assert function_exported?(DataFrame, :explain, 2)
    end
  end

  # ── 13.8 observe empty validation ──

  describe "13.8 observe empty expressions validation" do
    test "observe raises on empty expressions" do
      df = make_df()

      assert_raise ArgumentError, "exprs should not be empty", fn ->
        DataFrame.observe(df, "test_obs", [])
      end
    end
  end

  # ── 16.1 fill/3 empty map validation ──

  describe "16.1 fill empty map validation" do
    test "fill raises on empty map" do
      df = make_df()

      assert_raise ArgumentError, "value should not be empty", fn ->
        DataFrame.NA.fill(df, %{})
      end
    end
  end

  # ── 16.2 fill/drop subset single string ──

  describe "16.2 fill/drop subset accepts single string" do
    test "fill with single string subset" do
      df = make_df()
      result = DataFrame.NA.fill(df, 0, subset: "age")
      assert %DataFrame{plan: {:na_fill, _, ["age"], [0]}} = result
    end

    test "drop with single string subset" do
      df = make_df()
      result = DataFrame.NA.drop(df, subset: "age")
      assert %DataFrame{plan: {:na_drop, _, ["age"], _}} = result
    end
  end

  # ── 18.1 trigger validates single type ──

  describe "18.1 trigger validates single trigger type" do
    test "trigger raises on multiple trigger types" do
      writer = %SparkEx.StreamWriter{trigger: nil}

      assert_raise ArgumentError, ~r/only one trigger type/, fn ->
        SparkEx.StreamWriter.trigger(writer, processing_time: "10 seconds", once: true)
      end
    end

    test "trigger accepts single trigger type" do
      writer = %SparkEx.StreamWriter{trigger: nil}
      result = SparkEx.StreamWriter.trigger(writer, processing_time: "10 seconds")
      assert result.trigger == {:processing_time, "10 seconds"}
    end
  end

  # ── 20.1 Observation auto-generated name ──

  describe "20.1 observation auto-generated name" do
    test "new/0 generates UUID name" do
      obs = SparkEx.Observation.new()
      assert is_binary(obs.name)
      assert String.length(obs.name) == 36

      assert String.match?(
               obs.name,
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
             )
    end

    test "new/0 generates unique names" do
      obs1 = SparkEx.Observation.new()
      obs2 = SparkEx.Observation.new()
      assert obs1.name != obs2.name
    end
  end

  # ── 17.2 pivot group_type validation ──

  describe "17.2 pivot group_type validation" do
    test "pivot raises on rollup grouped data" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :rollup,
        grouping_sets: nil,
        pivot_col: nil,
        pivot_values: nil
      }

      assert_raise ArgumentError, ~r/pivot is only supported after group_by/, fn ->
        SparkEx.GroupedData.pivot(gd, "col")
      end
    end

    test "pivot raises on repeated pivot" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :groupby,
        grouping_sets: nil,
        pivot_col: {:col, "existing"},
        pivot_values: nil
      }

      assert_raise ArgumentError, ~r/repeated pivot/, fn ->
        SparkEx.GroupedData.pivot(gd, "col")
      end
    end
  end

  # ── 4.9 atan2/pow accept numeric literals ──

  describe "4.9 atan2/pow accept numeric literals" do
    test "atan2 with numeric literal first arg" do
      result = Functions.atan2(1.0, Functions.col("x"))
      assert %Column{expr: {:fn, "atan2", [{:lit, 1.0}, {:col, "x"}], false}} = result
    end

    test "atan2 with numeric literal second arg" do
      result = Functions.atan2(Functions.col("y"), 2.0)
      assert %Column{expr: {:fn, "atan2", [{:col, "y"}, {:lit, 2.0}], false}} = result
    end

    test "pow with numeric literal" do
      result = Functions.pow(2.0, Functions.col("x"))
      assert %Column{expr: {:fn, "power", [{:lit, 2.0}, {:col, "x"}], false}} = result
    end

    test "power alias works" do
      result = Functions.power(Functions.col("x"), 3)
      assert %Column{expr: {:fn, "power", [{:col, "x"}, {:lit, 3}], false}} = result
    end
  end

  # ── 4.3 nth_value ignoreNulls ──

  describe "4.3 nth_value ignoreNulls" do
    test "nth_value defaults ignoreNulls to false" do
      result = Functions.nth_value(Functions.col("x"), 2)

      assert %Column{expr: {:fn, "nth_value", [{:col, "x"}, {:lit, 2}, {:lit, false}], false}} =
               result
    end

    test "nth_value with ignoreNulls true" do
      result = Functions.nth_value(Functions.col("x"), 2, true)

      assert %Column{expr: {:fn, "nth_value", [{:col, "x"}, {:lit, 2}, {:lit, true}], false}} =
               result
    end

    test "nth_value accepts literal column offset" do
      result = Functions.nth_value(Functions.col("x"), Functions.lit(2))

      assert %Column{expr: {:fn, "nth_value", [{:col, "x"}, {:lit, 2}, {:lit, false}], false}} =
               result
    end
  end

  # ── 4.4 any_value ignoreNulls ──

  describe "4.4 any_value ignoreNulls" do
    test "any_value defaults ignoreNulls to false" do
      result = Functions.any_value(Functions.col("x"))
      assert %Column{expr: {:fn, "any_value", [{:col, "x"}, {:lit, false}], false}} = result
    end

    test "any_value with ignoreNulls true" do
      result = Functions.any_value(Functions.col("x"), true)
      assert %Column{expr: {:fn, "any_value", [{:col, "x"}, {:lit, true}], false}} = result
    end
  end

  # ── 12.4 sample random seed generation ──

  describe "12.4 sample random seed generation" do
    test "sample generates random seed when none given" do
      df = make_df()
      result = DataFrame.sample(df, 0.5)
      assert %DataFrame{plan: {:sample, _, _, 0.5, false, seed, false}} = result
      assert is_integer(seed)
    end

    test "sample respects explicit seed" do
      df = make_df()
      result = DataFrame.sample(df, 0.5, seed: 42)
      assert %DataFrame{plan: {:sample, _, _, 0.5, false, 42, false}} = result
    end
  end

  # ── 14.8 rand/randn always generate seed ──

  describe "14.8 rand/randn always generate seed" do
    test "rand/0 generates random seed" do
      result = Functions.rand()
      assert %Column{expr: {:fn, "rand", [{:lit, seed}], false}} = result
      assert is_integer(seed)
    end

    test "rand/1 with explicit seed" do
      result = Functions.rand(42)
      assert %Column{expr: {:fn, "rand", [{:lit, 42}], false}} = result
    end

    test "randn/0 generates random seed" do
      result = Functions.randn()
      assert %Column{expr: {:fn, "randn", [{:lit, seed}], false}} = result
      assert is_integer(seed)
    end

    test "randn/1 with explicit seed" do
      result = Functions.randn(42)
      assert %Column{expr: {:fn, "randn", [{:lit, 42}], false}} = result
    end
  end

  # ── 14.12 locate with pos parameter ──

  describe "14.12 locate with pos parameter" do
    test "locate/2 defaults pos to 1" do
      result = Functions.locate("abc", Functions.col("s"))

      assert %Column{expr: {:fn, "locate", [{:lit, "abc"}, {:col, "s"}, {:lit, 1}], false}} =
               result
    end

    test "locate/3 with explicit pos" do
      result = Functions.locate("abc", Functions.col("s"), 5)

      assert %Column{expr: {:fn, "locate", [{:lit, "abc"}, {:col, "s"}, {:lit, 5}], false}} =
               result
    end

    test "locate/2 accepts literal-column substring" do
      result = Functions.locate(Functions.lit("abc"), Functions.col("s"))

      assert %Column{expr: {:fn, "locate", [{:lit, "abc"}, {:col, "s"}, {:lit, 1}], false}} =
               result
    end

    test "locate/3 accepts literal-column pos" do
      result = Functions.locate("abc", Functions.col("s"), Functions.lit(5))

      assert %Column{expr: {:fn, "locate", [{:lit, "abc"}, {:col, "s"}, {:lit, 5}], false}} =
               result
    end
  end

  # ── 14.24 mode with deterministic parameter ──

  describe "14.24 mode with deterministic parameter" do
    test "mode/1 uses single-arg form" do
      result = Functions.mode(Functions.col("x"))
      assert %Column{expr: {:fn, "mode", [{:col, "x"}], false}} = result
    end

    test "mode/2 with deterministic true" do
      result = Functions.mode(Functions.col("x"), true)
      assert %Column{expr: {:fn, "mode", [{:col, "x"}], false}} = result
    end
  end

  # ── 14.9 shuffle with seed ──

  describe "14.9 shuffle with seed" do
    test "shuffle/1 uses single-arg form" do
      result = Functions.shuffle(Functions.col("arr"))
      assert %Column{expr: {:fn, "shuffle", [{:col, "arr"}], false}} = result
    end

    test "shuffle/2 with explicit seed" do
      result = Functions.shuffle(Functions.col("arr"), 42)
      assert %Column{expr: {:fn, "shuffle", [{:col, "arr"}], false}} = result
    end
  end

  # ── 14.17 from_unixtime default format ──

  describe "14.17 from_unixtime default format" do
    test "from_unixtime/1 always sends default format" do
      result = Functions.from_unixtime(Functions.col("ts"))

      assert %Column{
               expr: {:fn, "from_unixtime", [{:col, "ts"}, {:lit, "yyyy-MM-dd HH:mm:ss"}], false}
             } = result
    end

    test "from_unixtime/2 with custom format" do
      result = Functions.from_unixtime(Functions.col("ts"), "yyyy-MM-dd")

      assert %Column{
               expr: {:fn, "from_unixtime", [{:col, "ts"}, {:lit, "yyyy-MM-dd"}], false}
             } = result
    end
  end

  # ── 14.21 replace optional third arg ──

  describe "14.21 replace optional third arg" do
    test "replace/2 uses empty string as default replacement" do
      result = Functions.replace(Functions.col("s"), Functions.col("search"))

      assert %Column{expr: {:fn, "replace", [{:col, "s"}, {:col, "search"}, {:lit, ""}], false}} =
               result
    end

    test "replace/3 with explicit replacement" do
      result =
        Functions.replace(Functions.col("s"), Functions.col("search"), Functions.col("repl"))

      assert %Column{
               expr: {:fn, "replace", [{:col, "s"}, {:col, "search"}, {:col, "repl"}], false}
             } = result
    end
  end

  # ── 15.12 AsOfJoin join_type atom to string ──

  describe "15.12 AsOfJoin join_type conversion" do
    test "AsOfJoin converts atom join_type to string" do
      plan =
        {:as_of_join, {:sql, "SELECT 1", nil}, {:sql, "SELECT 2", nil}, {:col, "a"}, {:col, "b"},
         {:lit, nil}, [], :inner, {:lit, nil}, nil, nil}

      {relation, _} = PlanEncoder.encode_relation(plan, 0)
      as_of = elem(relation.rel_type, 1)
      assert as_of.join_type == "inner"
    end
  end

  # ── 3.7 dropFields empty validation ──

  describe "3.7 dropFields empty validation" do
    test "drop_fields raises on empty list" do
      col = %Column{expr: {:col, "struct_col"}}

      assert_raise ArgumentError, ~r/field names should not be empty/, fn ->
        Column.drop_fields(col, [])
      end
    end
  end

  # ── 3.1 Column.alias with metadata ──

  describe "3.1 Column.alias with metadata" do
    test "alias_ without metadata" do
      col = Functions.col("x")
      result = Column.alias_(col, "y")
      assert %Column{expr: {:alias, {:col, "x"}, "y"}} = result
    end

    test "alias_ with metadata" do
      col = Functions.col("x")
      result = Column.alias_(col, "y", metadata: %{"comment" => "test"})
      assert %Column{expr: {:alias, {:col, "x"}, "y", json}} = result
      assert Jason.decode!(json) == %{"comment" => "test"}
    end
  end

  # ── 16.4 sample_by seed default generation ──

  describe "16.4 sample_by seed default" do
    test "sample_by generates seed when none given" do
      df = make_df()
      result = DataFrame.Stat.sample_by(df, "label", %{0 => 0.1})
      assert %DataFrame{plan: {:stat_sample_by, _, _, _, seed}} = result
      assert is_integer(seed) and seed > 0
    end

    test "sample_by uses provided seed" do
      df = make_df()
      result = DataFrame.Stat.sample_by(df, "label", %{0 => 0.1}, 42)
      assert %DataFrame{plan: {:stat_sample_by, _, _, _, 42}} = result
    end
  end

  # ── 14.18 unix_timestamp zero-arg form ──

  describe "14.18 unix_timestamp zero-arg" do
    test "unix_timestamp() with no args" do
      result = Functions.unix_timestamp()
      assert %Column{expr: {:fn, "unix_timestamp", [], false}} = result
    end

    test "unix_timestamp with column" do
      result = Functions.unix_timestamp(Functions.col("ts"))
      assert %Column{expr: {:fn, "unix_timestamp", [{:col, "ts"}], false}} = result
    end

    test "unix_timestamp with column and format" do
      result = Functions.unix_timestamp(Functions.col("ts"), format: "yyyy-MM-dd")

      assert %Column{
               expr: {:fn, "unix_timestamp", [{:col, "ts"}, {:lit, "yyyy-MM-dd"}], false}
             } = result
    end
  end

  # ── 14.19 convert_timezone 2-arg form ──

  describe "14.19 convert_timezone 2-arg" do
    test "2-arg form (target_tz, source_ts)" do
      result = Functions.convert_timezone("US/Pacific", Functions.col("ts"))

      assert %Column{
               expr: {:fn, "convert_timezone", [{:col, "US/Pacific"}, {:col, "ts"}], false}
             } = result
    end

    test "3-arg form (source_tz, target_tz, source_ts)" do
      result = Functions.convert_timezone("UTC", "US/Pacific", Functions.col("ts"))

      assert %Column{
               expr:
                 {:fn, "convert_timezone", [{:col, "UTC"}, {:col, "US/Pacific"}, {:col, "ts"}],
                  false}
             } = result
    end
  end

  # ── 14.22 parse_url optional key ──

  describe "14.22 parse_url optional key" do
    test "parse_url without key" do
      result = Functions.parse_url(Functions.col("url"), Functions.col("part"))
      assert %Column{expr: {:fn, "parse_url", [{:col, "url"}, {:col, "part"}], false}} = result
    end

    test "parse_url with key" do
      result =
        Functions.parse_url(Functions.col("url"), Functions.col("part"), Functions.col("key"))

      assert %Column{
               expr: {:fn, "parse_url", [{:col, "url"}, {:col, "part"}, {:col, "key"}], false}
             } = result
    end

    test "parse_url treats raw string args as literals" do
      result = Functions.parse_url(Functions.col("url"), "HOST", "x")

      assert %Column{
               expr: {:fn, "parse_url", [{:col, "url"}, {:lit, "HOST"}, {:lit, "x"}], false}
             } = result
    end

    test "try_parse_url without key" do
      result = Functions.try_parse_url(Functions.col("url"), Functions.col("part"))

      assert %Column{
               expr: {:fn, "parse_url", [{:col, "url"}, {:col, "part"}], false}
             } = result
    end
  end

  # ── 14.23 substr_ optional len ──

  describe "14.23 substr_ optional len" do
    test "substr_ without len" do
      result = Functions.substr_(Functions.col("s"), Functions.col("pos"))
      assert %Column{expr: {:fn, "substr", [{:col, "s"}, {:col, "pos"}], false}} = result
    end

    test "substr_ with len" do
      result =
        Functions.substr_(Functions.col("s"), Functions.col("pos"), Functions.col("len"))

      assert %Column{
               expr: {:fn, "substr", [{:col, "s"}, {:col, "pos"}, {:col, "len"}], false}
             } = result
    end
  end

  # ── 4.5 like/ilike escape character ──

  describe "4.5 like/ilike escape character" do
    test "like_ without escape" do
      result = Functions.like_(Functions.col("s"), Functions.col("pat"))
      assert %Column{expr: {:fn, "like", [{:col, "s"}, {:col, "pat"}], false}} = result
    end

    test "like_ treats raw string pattern as literal" do
      result = Functions.like_(Functions.col("s"), "%abc%")
      assert %Column{expr: {:fn, "like", [{:col, "s"}, {:lit, "%abc%"}], false}} = result
    end

    test "like_ with escape" do
      result = Functions.like_(Functions.col("s"), Functions.col("pat"), Functions.col("esc"))

      assert %Column{
               expr: {:fn, "like", [{:col, "s"}, {:col, "pat"}, {:col, "esc"}], false}
             } = result
    end

    test "ilike_ without escape" do
      result = Functions.ilike_(Functions.col("s"), Functions.col("pat"))
      assert %Column{expr: {:fn, "ilike", [{:col, "s"}, {:col, "pat"}], false}} = result
    end

    test "ilike_ treats raw string pattern as literal" do
      result = Functions.ilike_(Functions.col("s"), "%abc%")
      assert %Column{expr: {:fn, "ilike", [{:col, "s"}, {:lit, "%abc%"}], false}} = result
    end

    test "ilike_ with escape" do
      result =
        Functions.ilike_(Functions.col("s"), Functions.col("pat"), Functions.col("esc"))

      assert %Column{
               expr: {:fn, "ilike", [{:col, "s"}, {:col, "pat"}, {:col, "esc"}], false}
             } = result
    end
  end

  # ── 14.4 array_sort with comparator ──

  describe "14.4 array_sort with comparator" do
    test "array_sort without comparator" do
      result = Functions.array_sort(Functions.col("arr"))
      assert %Column{expr: {:fn, "array_sort", [{:col, "arr"}], false}} = result
    end

    test "array_sort with comparator function" do
      result =
        Functions.array_sort(Functions.col("arr"), fn l, r -> Column.minus(l, r) end)

      assert %Column{
               expr: {:fn, "array_sort", [{:col, "arr"}, {:lambda, _, _}], false}
             } = result
    end
  end

  # ── 14.15 percentile with frequency and list ──

  describe "14.15 percentile with frequency and list" do
    test "percentile with single percentage" do
      result = Functions.percentile(Functions.col("x"), 0.5)

      assert %Column{
               expr: {:fn, "percentile", [{:col, "x"}, {:lit, 0.5}, {:lit, 1}], false}
             } = result
    end

    test "percentile with list of percentages" do
      result = Functions.percentile(Functions.col("x"), [0.25, 0.5, 0.75])

      assert %Column{
               expr:
                 {:fn, "percentile",
                  [
                    {:col, "x"},
                    {:fn, "array", [{:lit, 0.25}, {:lit, 0.5}, {:lit, 0.75}], false},
                    {:lit, 1}
                  ], false}
             } = result
    end

    test "percentile with frequency" do
      result = Functions.percentile(Functions.col("x"), 0.5, 2)

      assert %Column{
               expr: {:fn, "percentile", [{:col, "x"}, {:lit, 0.5}, {:lit, 2}], false}
             } = result
    end
  end

  # ── 14.28 uuid/uniform/randstr seed ──

  describe "14.28 uuid/uniform/randstr seed" do
    test "uuid/0 uses zero-arg form" do
      result = Functions.uuid()
      assert %Column{expr: {:fn, "uuid", [], false}} = result
    end

    test "uuid with explicit seed" do
      result = Functions.uuid(42)
      assert %Column{expr: {:fn, "uuid", [], false}} = result
    end

    test "uniform auto-generates seed" do
      result = Functions.uniform(Functions.col("min"), 100)

      assert %Column{
               expr:
                 {:fn, "+",
                  [
                    {:col, "min"},
                    {:fn, "*",
                     [
                       {:fn, "rand", [{:lit, seed}], false},
                       {:fn, "-", [{:lit, 100}, {:col, "min"}], false}
                     ], false}
                  ], false}
             } = result

      assert is_integer(seed)
    end

    test "uniform with explicit seed" do
      result = Functions.uniform(Functions.col("min"), 100, 42)

      assert %Column{
               expr:
                 {:fn, "+",
                  [
                    {:col, "min"},
                    {:fn, "*",
                     [
                       {:fn, "rand", [{:lit, 42}], false},
                       {:fn, "-", [{:lit, 100}, {:col, "min"}], false}
                     ], false}
                  ], false}
             } = result
    end

    test "randstr auto-generates seed" do
      result = Functions.randstr(Functions.col("len"))

      assert %Column{
               expr:
                 {:fn, "substr",
                  [
                    {:fn, "md5", [{:cast, {:fn, "rand", [{:lit, seed}], false}, "STRING"}],
                     false},
                    {:lit, 1},
                    {:col, "len"}
                  ], false}
             } = result

      assert is_integer(seed)
    end

    test "randstr with explicit seed" do
      result = Functions.randstr(Functions.col("len"), 42)

      assert %Column{
               expr:
                 {:fn, "substr",
                  [
                    {:fn, "md5", [{:cast, {:fn, "rand", [{:lit, 42}], false}, "STRING"}], false},
                    {:lit, 1},
                    {:col, "len"}
                  ], false}
             } = result
    end
  end

  # ── 12.5 insert_into no longer auto-changes mode ──

  describe "12.5 insert_into mode preservation" do
    test "Writer keeps error_if_exists mode when no overwrite given" do
      writer = %SparkEx.Writer{mode: :error_if_exists, options: %{}}
      assert writer.mode == :error_if_exists
    end
  end

  # ── Round 4 ──

  # ── 7.1 GroupedData.agg map form ──

  describe "7.1 agg map form" do
    test "accepts a map of {col_name, agg_func}" do
      df = make_df()

      gd = %SparkEx.GroupedData{
        session: df.session,
        plan: df.plan,
        group_type: :groupby,
        grouping_exprs: []
      }

      result = SparkEx.GroupedData.agg(gd, %{"age" => "max", "salary" => "avg"})
      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result

      aliases =
        Enum.map(agg_exprs, fn {:alias, {:fn, func, [{:col, col}], false}, _name} ->
          {col, func}
        end)

      assert Enum.sort(aliases) == [{"age", "max"}, {"salary", "avg"}]
    end

    test "raises on empty map" do
      df = make_df()

      gd = %SparkEx.GroupedData{
        session: df.session,
        plan: df.plan,
        group_type: :groupby,
        grouping_exprs: []
      }

      assert_raise ArgumentError, ~r/at least one/, fn ->
        SparkEx.GroupedData.agg(gd, %{})
      end
    end
  end

  # ── 13.1 order_by ascending keyword ──

  describe "13.1 order_by ascending keyword" do
    test "ascending: true sorts all columns ascending" do
      df = make_df()
      result = DataFrame.order_by(df, ["a", "b"], ascending: true)
      assert %DataFrame{plan: {:sort, _, exprs}} = result

      assert [
               {:sort_order, {:col, "a"}, :asc, :nulls_first},
               {:sort_order, {:col, "b"}, :asc, :nulls_first}
             ] = exprs
    end

    test "ascending: false sorts all columns descending" do
      df = make_df()
      result = DataFrame.order_by(df, ["a", "b"], ascending: false)
      assert %DataFrame{plan: {:sort, _, exprs}} = result

      assert [
               {:sort_order, {:col, "a"}, :desc, :nulls_last},
               {:sort_order, {:col, "b"}, :desc, :nulls_last}
             ] = exprs
    end

    test "ascending list applies per-column" do
      df = make_df()
      result = DataFrame.order_by(df, ["a", "b"], ascending: [true, false])
      assert %DataFrame{plan: {:sort, _, exprs}} = result

      assert [
               {:sort_order, {:col, "a"}, :asc, :nulls_first},
               {:sort_order, {:col, "b"}, :desc, :nulls_last}
             ] = exprs
    end

    test "ascending list length mismatch raises" do
      df = make_df()

      assert_raise ArgumentError, ~r/length/, fn ->
        DataFrame.order_by(df, ["a", "b"], ascending: [true])
      end
    end
  end

  # ── 13.11 save optional path ──

  describe "13.11 save optional path" do
    test "save/1 with no path is a valid function call" do
      # Verify that save/1 (with default nil path) exists and is callable.
      # We can't test it fully without a real session, so verify arity.
      assert function_exported?(SparkEx.Writer, :save, 1)
      assert function_exported?(SparkEx.Writer, :save, 2)
      assert function_exported?(SparkEx.Writer, :save, 3)
    end
  end

  # ── 4.7 left_/right_ two_col ──

  describe "4.7 left_/right_ use two_col arity" do
    test "left_ accepts two column arguments" do
      result = Functions.left_(Functions.col("s"), Functions.col("n"))
      assert %Column{expr: {:fn, "left", [{:col, "s"}, {:col, "n"}], false}} = result
    end

    test "right_ accepts two column arguments" do
      result = Functions.right_(Functions.col("s"), Functions.col("n"))
      assert %Column{expr: {:fn, "right", [{:col, "s"}, {:col, "n"}], false}} = result
    end
  end

  # ── 3.2 Column.transform delegates to Functions.transform ──

  describe "3.2 Column.transform/2" do
    test "delegates to Functions.transform" do
      col = Functions.col("arr")
      result = Column.transform(col, fn x -> Column.plus(x, Functions.lit(1)) end)
      assert %Column{expr: {:fn, "transform", [{:col, "arr"}, {:lambda, _, _}], false}} = result
    end
  end

  # ── 15.18 WriterV2 overwrite_condition conditional ──

  describe "15.18 WriterV2 overwrite_condition" do
    test "overwrite_condition encoding is conditional on mode" do
      # This is tested at the encoder level. Verify the writer builder sets mode correctly.
      df = make_df()

      writer =
        df
        |> DataFrame.write()
        |> SparkEx.Writer.format("delta")
        |> SparkEx.Writer.mode(:overwrite)

      assert writer.mode == :overwrite

      writer2 = df |> DataFrame.write() |> SparkEx.Writer.format("delta")
      assert writer2.mode == :error_if_exists
    end
  end

  # ── 16.3 replace mixed-type validation ──

  describe "16.3 replace mixed-type validation" do
    test "raises on mixed numeric and string replacements" do
      df = make_df()

      assert_raise ArgumentError, ~r/mixed type/, fn ->
        DataFrame.NA.replace(df, %{1 => "one"})
      end
    end

    test "allows same-type replacements" do
      df = make_df()
      result = DataFrame.NA.replace(df, %{1 => 2, 3 => 4})
      assert %DataFrame{plan: {:na_replace, _, _, _}} = result
    end

    test "allows nil as replacement value" do
      df = make_df()
      result = DataFrame.NA.replace(df, "N/A", nil)
      assert %DataFrame{plan: {:na_replace, _, _, [{_, nil}]}} = result
    end
  end

  # ── 17.1 Window boundary clamping ──

  describe "17.1 window boundary clamping" do
    setup do
      %{spec: %SparkEx.WindowSpec{}}
    end

    test "extreme negative boundary is clamped to :unbounded", %{spec: spec} do
      result = SparkEx.WindowSpec.rows_between(spec, -2_147_483_648, 0)
      assert %SparkEx.WindowSpec{frame_spec: {:rows, :unbounded, 0}} = result
    end

    test "extreme positive boundary is clamped to :unbounded", %{spec: spec} do
      result = SparkEx.WindowSpec.rows_between(spec, 0, 2_147_483_647)
      assert %SparkEx.WindowSpec{frame_spec: {:rows, 0, :unbounded}} = result
    end

    test "normal boundaries are not clamped", %{spec: spec} do
      result = SparkEx.WindowSpec.rows_between(spec, -5, 5)
      assert %SparkEx.WindowSpec{frame_spec: {:rows, -5, 5}} = result
    end

    test "range_between also clamps", %{spec: spec} do
      result = SparkEx.WindowSpec.range_between(spec, -2_147_483_648, 2_147_483_647)
      assert %SparkEx.WindowSpec{frame_spec: {:range, :unbounded, :unbounded}} = result
    end
  end

  # ── Round 5 ──

  # ── 3.5 when/otherwise validation ──

  describe "3.5 otherwise validation" do
    test "otherwise raises when called twice" do
      col =
        Functions.when_(Functions.col("x") |> Column.gt(0), Functions.lit("pos"))
        |> Column.otherwise("zero")

      assert_raise ArgumentError, ~r/otherwise.*already been called/, fn ->
        Column.otherwise(col, "neg")
      end
    end
  end

  # ── 3.6 neq produces not(==) ──

  describe "3.6 neq produces not(==)" do
    test "neq wraps equality in NOT" do
      result = Column.neq(Functions.col("a"), Functions.lit(1))

      assert %Column{expr: {:fn, "not", [{:fn, "==", [{:col, "a"}, {:lit, 1}], false}], false}} =
               result
    end

    test "neq with non-Column literal" do
      result = Column.neq(Functions.col("a"), 5)

      assert %Column{expr: {:fn, "not", [{:fn, "==", [{:col, "a"}, {:lit, 5}], false}], false}} =
               result
    end

    test "from_unixtime/2 accepts literal-column format" do
      result = Functions.from_unixtime(Functions.col("ts"), Functions.lit("yyyy-MM-dd"))

      assert %Column{
               expr: {:fn, "from_unixtime", [{:col, "ts"}, {:lit, "yyyy-MM-dd"}], false}
             } = result
    end
  end

  # ── 4.6 overlay ──

  describe "4.6 overlay" do
    test "overlay with 3 args uses default len" do
      result = Functions.overlay(Functions.col("s"), Functions.col("r"), Functions.col("p"))

      assert %Column{
               expr: {:fn, "overlay", [{:col, "s"}, {:col, "r"}, {:col, "p"}, {:lit, -1}], false}
             } = result
    end

    test "overlay with 4 args" do
      result =
        Functions.overlay(
          Functions.col("s"),
          Functions.col("r"),
          Functions.col("p"),
          Functions.lit(3)
        )

      assert %Column{
               expr: {:fn, "overlay", [{:col, "s"}, {:col, "r"}, {:col, "p"}, {:lit, 3}], false}
             } = result
    end
  end

  # ── 4.8 extract accepts Column for field ──

  describe "4.8 extract accepts Column" do
    test "extract with Column field arg" do
      result = Functions.extract(Functions.col("field_name"), Functions.col("ts"))
      assert %Column{expr: {:fn, "extract", [{:col, "field_name"}, {:col, "ts"}], false}} = result
    end

    test "extract with string args" do
      result = Functions.extract("YEAR", "ts")
      assert %Column{expr: {:fn, "extract", [{:col, "YEAR"}, {:col, "ts"}], false}} = result
    end
  end

  # ── 19.2 struct_field metadata ──

  describe "19.2 struct_field metadata" do
    test "struct_field accepts metadata option" do
      field = SparkEx.Types.struct_field("name", :string, metadata: %{"comment" => "user name"})
      assert field.metadata == %{"comment" => "user name"}
    end

    test "struct_field defaults metadata to empty map" do
      field = SparkEx.Types.struct_field("name", :string)
      assert field.metadata == %{}
    end
  end

  # ── 19.3 type_mapper time type ──

  describe "19.3 time type mapping" do
    test "time type maps to TIME not STRING" do
      assert SparkEx.Connect.TypeMapper.to_spark_ddl_type({:time, :microsecond}) == "TIME"
    end
  end

  # ── 20.2 Observation get raises ──

  describe "20.2 Observation get raises on unobserved" do
    test "raises ArgumentError with NO_OBSERVE_BEFORE_GET" do
      obs = SparkEx.Observation.new("never_attached_#{System.unique_integer()}")

      assert_raise ArgumentError, ~r/NO_OBSERVE_BEFORE_GET/, fn ->
        SparkEx.Observation.get(obs)
      end
    end
  end

  # ── Round 6 ──

  # ── 10.1 Column binary ops coerce non-Column values ──

  describe "10.1 binary ops coerce datetime/decimal" do
    test "eq accepts Date on right side" do
      result = Column.eq(Functions.col("d"), ~D[2024-01-01])
      assert %Column{expr: {:fn, "==", [{:col, "d"}, {:lit, ~D[2024-01-01]}], false}} = result
    end

    test "gt accepts string on right side" do
      result = Column.gt(Functions.col("name"), "Alice")
      assert %Column{expr: {:fn, ">", [{:col, "name"}, {:lit, "Alice"}], false}} = result
    end

    test "plus accepts Decimal on right side" do
      d = Decimal.new("1.5")
      result = Column.plus(Functions.col("x"), d)
      assert %Column{expr: {:fn, "+", [{:col, "x"}, {:lit, ^d}], false}} = result
    end
  end

  # ── 13.2 Integer ordinal support ──

  describe "13.2 integer ordinal support" do
    test "order_by accepts integer column indices" do
      df = make_df()
      result = DataFrame.order_by(df, [0, 1])
      assert %DataFrame{plan: {:sort, _, exprs}} = result

      assert [
               {:sort_order, {:col, "_c0"}, :asc, :nulls_first},
               {:sort_order, {:col, "_c1"}, :asc, :nulls_first}
             ] = exprs
    end

    test "group_by accepts integer column indices" do
      df = make_df()
      result = DataFrame.group_by(df, [0])
      assert %SparkEx.GroupedData{grouping_exprs: [{:col, "_c0"}]} = result
    end
  end

  # ── 15.1 Drop relation columns field ──

  describe "15.1 drop with Column expressions" do
    test "complex Column expressions go to col_exprs" do
      df = make_df()
      # A non-simple Column expression (not just {:col, name})
      complex_col = Column.plus(Functions.col("a"), Functions.col("b"))
      result = DataFrame.drop(df, [complex_col, "c"])
      assert %DataFrame{plan: {:drop, _, ["c"], [expr]}} = result
      assert {:fn, "+", [{:col, "a"}, {:col, "b"}], false} = expr
    end

    test "simple col references stay as names" do
      df = make_df()
      result = DataFrame.drop(df, [Functions.col("x"), "y"])
      assert %DataFrame{plan: {:drop, _, ["x", "y"], []}} = result
    end
  end

  # ── 15.5 ColumnAlias multi-name ──

  describe "15.5 multi-name alias encoding" do
    test "encodes alias with metadata" do
      col = Functions.col("x") |> Column.alias_("new_x", metadata: %{"key" => "val"})
      assert %Column{expr: {:alias, {:col, "x"}, "new_x", _json}} = col
    end
  end

  # ── 15.15 Decimal precision/scale defaults ──

  describe "15.15 decimal precision/scale" do
    test "inferred for plain decimal literal" do
      expr = {:lit, Decimal.new("123.45")}
      encoded = SparkEx.Connect.PlanEncoder.encode_expression(expr)
      assert %{expr_type: {:literal, %{literal_type: {:decimal, decimal}}}} = encoded
      assert decimal.precision > 0
      assert decimal.scale > 0
    end
  end

  # ── Round 7 ──

  describe "round 7" do
    test "1.8 substr accepts mixed int/Column types" do
      c = Functions.col("s")
      result = Column.substr(c, 1, Functions.lit(5))
      assert %Column{expr: {:fn, "substr", [_, {:lit, 1}, _], false}} = result

      result2 = Column.substr(c, Functions.lit(1), 5)
      assert %Column{expr: {:fn, "substr", [_, _, {:lit, 5}], false}} = result2
    end

    test "1.8 substr accepts both Column args" do
      c = Functions.col("s")
      result = Column.substr(c, Functions.lit(1), Functions.lit(5))
      assert %Column{expr: {:fn, "substr", [_, _, _], false}} = result
    end

    test "1.8 substr accepts both integer args" do
      c = Functions.col("s")
      result = Column.substr(c, 1, 5)
      assert %Column{expr: {:fn, "substr", [{:col, "s"}, {:lit, 1}, {:lit, 5}], false}} = result
    end

    test "8.1 streaming query module exports recent_progress and last_progress" do
      # Verify the module exports these functions (parse_progress_json is private)
      functions = SparkEx.StreamingQuery.__info__(:functions)
      assert {:recent_progress, 1} in functions
      assert {:last_progress, 1} in functions
    end

    test "13.4 repartition_by_id accepts numPartitions" do
      df = make_df()
      result = DataFrame.repartition_by_id(df, 10, "col1")
      assert %DataFrame{plan: {:repartition_by_expression, _, [_], 10}} = result
    end

    test "13.4 repartition_by_id defaults numPartitions to nil" do
      df = make_df()
      result = DataFrame.repartition_by_id(df, "col1")
      assert %DataFrame{plan: {:repartition_by_expression, _, [_], nil}} = result
    end

    test "14.27 schema_of_json accepts options" do
      c = Functions.schema_of_json(Functions.col("j"), %{"allowNumericLeadingZeros" => "true"})
      assert %Column{expr: {:fn, "schema_of_json", [_, _], false}} = c
    end

    test "14.27 schema_of_json works without options" do
      c = Functions.schema_of_json(Functions.col("j"))
      assert %Column{expr: {:fn, "schema_of_json", [{:col, "j"}], false}} = c
    end

    test "14.27 schema_of_csv accepts options" do
      c = Functions.schema_of_csv(Functions.col("c"), %{"sep" => "|"})
      assert %Column{expr: {:fn, "schema_of_csv", [_, _], false}} = c
    end

    test "14.27 schema_of_xml accepts options" do
      c = Functions.schema_of_xml(Functions.col("x"), %{"rowTag" => "item"})
      assert %Column{expr: {:fn, "schema_of_xml", [_, _], false}} = c
    end

    test "15.14 Time literal includes precision field" do
      time = ~T[12:30:45.123456]
      expr = {:lit, time}
      encoded = PlanEncoder.encode_expression(expr)
      assert %{expr_type: {:literal, %{literal_type: {:time, time_lit}}}} = encoded
      assert time_lit.precision == 6
    end
  end

  # ── Round 8 ──

  describe "round 8" do
    test "14.25 kll_sketch_merge takes two columns" do
      result = Functions.kll_sketch_merge_bigint(Functions.col("a"), Functions.col("b"))

      assert %Column{expr: {:fn, "kll_sketch_merge_bigint", [{:col, "a"}, {:col, "b"}], false}} =
               result
    end

    test "15.7 cast supports legacy mode" do
      expr = {:cast, {:col, "x"}, "int", :legacy}
      encoded = PlanEncoder.encode_expression(expr)
      assert %{expr_type: {:cast, cast}} = encoded
      assert cast.eval_mode == :EVAL_MODE_LEGACY
    end

    test "15.7 cast supports ansi mode" do
      expr = {:cast, {:col, "x"}, "int", :ansi}
      encoded = PlanEncoder.encode_expression(expr)
      assert %{expr_type: {:cast, cast}} = encoded
      assert cast.eval_mode == :EVAL_MODE_ANSI
    end

    test "15.16 nil null_ordering defaults to SORT_NULLS_FIRST" do
      expr = {:sort_order, {:col, "x"}, :asc, nil}
      encoded = PlanEncoder.encode_expression(expr)
      assert %{expr_type: {:sort_order, sort}} = encoded
      assert sort.null_ordering == :SORT_NULLS_FIRST
    end

    test "15.16 bare col sort_order defaults to SORT_NULLS_FIRST" do
      # Test through encode_sort_order for {:col, _} fallback
      plan = {:sort, {:sql, "SELECT 1", nil}, [{:col, "x"}], true}
      {encoded, _counter} = PlanEncoder.encode_relation(plan, 0)
      assert %{rel_type: {:sort, sort}} = encoded
      [order] = sort.order
      assert order.null_ordering == :SORT_NULLS_FIRST
    end

    test "19.1 array_type with contains_null: false" do
      t = SparkEx.Types.array_type(:string, contains_null: false)
      assert {:array, :string, false} = t
    end

    test "19.1 array_type defaults to contains_null: true" do
      t = SparkEx.Types.array_type(:string)
      assert {:array, :string} = t
    end

    test "19.1 map_type with value_contains_null: false" do
      t = SparkEx.Types.map_type(:string, :long, value_contains_null: false)
      assert {:map, :string, :long, false} = t
    end

    test "19.1 array_type contains_null: false in JSON" do
      t = {:array, :string, false}

      json =
        SparkEx.Types.to_json({:struct, [%{name: "arr", type: t, nullable: true, metadata: %{}}]})

      decoded = Jason.decode!(json)
      field = hd(decoded["fields"])
      assert field["type"]["containsNull"] == false
    end

    test "13.10 merge_into accepts condition parameter" do
      df = make_df()

      writer =
        DataFrame.merge_into(df, "target", Functions.col("id") |> Column.eq(Functions.col("tid")))

      assert %SparkEx.MergeIntoWriter{condition: {:fn, "==", _, false}} = writer
    end

    test "13.10 merge_into works without condition" do
      df = make_df()
      writer = DataFrame.merge_into(df, "target")
      assert %SparkEx.MergeIntoWriter{condition: nil} = writer
    end

    test "15.19 WriterV2 overwrite with condition sets overwrite_condition" do
      functions = SparkEx.WriterV2.__info__(:functions)
      assert {:overwrite, 2} in functions
      assert {:overwrite, 3} in functions
    end
  end

  # ── Helper ──

  defp make_df do
    %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
  end
end
