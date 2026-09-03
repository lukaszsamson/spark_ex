defmodule SparkEx.MissCodex2Test do
  @moduledoc """
  Tests for fixes from MISS_CODEX_2.md gap analysis.
  """
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers, only: [unwrap: 1, unwrap_plan: 1]

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions

  # ── #1 Non-positive partition counts (already fixed in MISS_OPUS_2) ──

  describe "#1 coalesce/repartition positive validation" do
    test "coalesce rejects 0" do
      assert_raise FunctionClauseError, fn ->
        DataFrame.coalesce(make_df(), 0)
      end
    end

    test "repartition with num_partitions rejects negative" do
      # repartition(df, n) when n > 0 - negative won't match, falls to list clause
      assert_raise FunctionClauseError, fn ->
        DataFrame.repartition(make_df(), -1, [Functions.col("x")])
      end
    end
  end

  # ── #2 repartition_by_range empty sort expressions ──

  describe "#2 repartition_by_range empty validation" do
    test "rejects empty columns" do
      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        DataFrame.repartition_by_range(make_df(), [])
      end
    end

    test "accepts non-empty columns" do
      result = DataFrame.repartition_by_range(make_df(), ["col1"])

      assert {:repartition_by_expression, _, _, nil} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #6 Streaming trigger validation (already fixed in MISS_OPUS_2) ──

  describe "#6 trigger multiple types validation" do
    test "rejects multiple trigger types" do
      writer = %SparkEx.StreamWriter{trigger: nil}

      assert_raise ArgumentError, ~r/only one trigger/, fn ->
        SparkEx.StreamWriter.trigger(writer, processing_time: "10 seconds", once: true)
      end
    end
  end

  # ── #7 Streaming query name blank strings ──

  describe "#7 streaming query name blank validation" do
    test "rejects empty string" do
      writer = %SparkEx.StreamWriter{query_name: nil}

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.StreamWriter.query_name(writer, "")
      end
    end

    test "rejects blank string" do
      writer = %SparkEx.StreamWriter{query_name: nil}

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.StreamWriter.query_name(writer, "   ")
      end
    end

    test "accepts valid name" do
      writer = %SparkEx.StreamWriter{query_name: nil}
      result = SparkEx.StreamWriter.query_name(writer, "my_query")
      assert result.query_name == "my_query"
    end
  end

  # ── #8 NA.fill empty map (already fixed) ──

  describe "#8 NA.fill empty map" do
    test "rejects empty map" do
      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        DataFrame.NA.fill(make_df(), %{})
      end
    end
  end

  # ── #10 GroupedData.pivot validation (already fixed) ──

  describe "#10 pivot group_type validation" do
    test "rejects pivot on rollup" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :rollup,
        grouping_sets: nil,
        pivot_col: nil,
        pivot_values: nil
      }

      assert_raise ArgumentError, ~r/only supported after group_by/, fn ->
        SparkEx.GroupedData.pivot(gd, "col")
      end
    end
  end

  # ── #12 NA.fill/drop single string subset (already fixed) ──

  describe "#12 NA.fill/drop single string subset" do
    test "fill accepts single string subset" do
      result = DataFrame.NA.fill(make_df(), 0, subset: "age")
      assert {:na_fill, _, ["age"], [0]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "drop accepts single string subset" do
      result = DataFrame.NA.drop(make_df(), subset: "age")
      assert {:na_drop, _, ["age"], _} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #13 NA.replace string subset normalization ──

  describe "#13 NA.replace string subset normalization" do
    test "replace normalizes string subset to list" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1}, nil, subset: "col1")
      assert {:na_replace, _, ["col1"], [{0, 1}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "replace with list subset still works" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1}, nil, subset: ["col1", "col2"])

      assert {:na_replace, _, ["col1", "col2"], [{0, 1}]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "replace with nil subset uses empty list" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1})
      assert {:na_replace, _, [], _} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #26 order_by empty validation ──

  describe "#26 order_by empty validation" do
    test "rejects empty columns" do
      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        DataFrame.order_by(make_df(), [])
      end
    end

    test "accepts non-empty columns" do
      result = DataFrame.order_by(make_df(), ["name"])
      assert {:sort, _, _} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #27 to_df string-only validation ──

  describe "#27 to_df string-only column names" do
    test "rejects non-string column names" do
      assert_raise ArgumentError, ~r/must all be strings/, fn ->
        DataFrame.to_df(make_df(), ["a", 1])
      end
    end

    test "accepts all string column names" do
      result = DataFrame.to_df(make_df(), ["a", "b"])
      assert {:to_df, _, ["a", "b"]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #32 observe empty expressions (already fixed) ──

  describe "#32 observe empty expressions" do
    test "rejects empty expressions" do
      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        DataFrame.observe(make_df(), "obs", [])
      end
    end
  end

  # ── #33 with_watermark blank strings ──

  describe "#33 with_watermark blank string validation" do
    test "rejects empty event_time" do
      assert_raise ArgumentError, ~r/event_time should not be empty/, fn ->
        DataFrame.with_watermark(make_df(), "", "10 minutes")
      end
    end

    test "rejects blank event_time" do
      assert_raise ArgumentError, ~r/event_time should not be empty/, fn ->
        DataFrame.with_watermark(make_df(), "   ", "10 minutes")
      end
    end

    test "rejects empty delay_threshold" do
      assert_raise ArgumentError, ~r/delay_threshold should not be empty/, fn ->
        DataFrame.with_watermark(make_df(), "event_time", "")
      end
    end

    test "accepts valid parameters" do
      result = DataFrame.with_watermark(make_df(), "event_time", "10 minutes")

      assert {:with_watermark, _, "event_time", "10 minutes"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #34 unpivot scalar ids/values ──

  describe "#34 unpivot scalar ids/values" do
    test "accepts scalar string id" do
      result = DataFrame.unpivot(make_df(), "id", ["v1", "v2"], "key", "value")

      assert {:unpivot, _, [{:col, "id"}], _, "key", "value"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts scalar Column id" do
      result = DataFrame.unpivot(make_df(), Functions.col("id"), ["v1"], "key", "value")

      assert {:unpivot, _, [{:col, "id"}], _, "key", "value"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts list ids" do
      result = DataFrame.unpivot(make_df(), ["id1", "id2"], ["v1"], "key", "value")

      assert {:unpivot, _, [{:col, "id1"}, {:col, "id2"}], _, "key", "value"} =
               unwrap_plan(result)
    end
  end

  # ── #38 Writer.cluster_by empty validation ──

  describe "#38 Writer.cluster_by empty validation" do
    test "rejects empty columns" do
      writer = %SparkEx.Writer{cluster_by: ["a"], options: %{}}

      # T-51: [] now clears the repeated field instead of raising.
      assert SparkEx.Writer.cluster_by(writer, []).cluster_by == []

      assert_raise ArgumentError, ~r/list of column names/, fn ->
        SparkEx.Writer.cluster_by(writer, "a")
      end
    end
  end

  # ── #47 Writer.bucket_by and sort_by empty validation ──

  describe "#47 Writer.bucket_by/sort_by empty validation" do
    test "bucket_by rejects empty columns" do
      writer = %SparkEx.Writer{bucket_by: {4, ["a"]}, options: %{}}

      # T-51: [] now clears bucketing instead of raising.
      assert SparkEx.Writer.bucket_by(writer, 8, []).bucket_by == nil

      assert_raise ArgumentError, ~r/positive number of buckets/, fn ->
        SparkEx.Writer.bucket_by(writer, 0, ["a"])
      end
    end

    test "sort_by rejects empty columns" do
      writer = %SparkEx.Writer{sort_by: ["a"], options: %{}}

      # T-51: [] now clears the repeated field instead of raising.
      assert SparkEx.Writer.sort_by(writer, []).sort_by == []

      assert_raise ArgumentError, ~r/list of column names/, fn ->
        SparkEx.Writer.sort_by(writer, "a")
      end
    end
  end

  # ── #62 Missing melt alias (already fixed) ──

  describe "#62 melt alias" do
    test "melt delegates to unpivot" do
      result = DataFrame.melt(make_df(), ["id"], ["v1"], "key", "value")
      assert {:unpivot, _, _, _, "key", "value"} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #9 NA.drop subset type validation ──

  describe "#9 NA.drop subset type validation" do
    test "rejects non-string subset elements" do
      assert_raise ArgumentError, ~r/column name strings/, fn ->
        DataFrame.NA.drop(make_df(), subset: [1, 2])
      end
    end

    test "accepts string subset elements" do
      result = DataFrame.NA.drop(make_df(), subset: ["a", "b"])
      assert {:na_drop, _, ["a", "b"], _} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #15 as_of_join accepts string columns ──

  describe "#15 as_of_join string columns" do
    test "accepts string left_as_of (binds to left plan)" do
      left = make_df()
      right = make_df()
      result = DataFrame.as_of_join(left, right, "t1", Functions.col("t2"))

      left_plan = unwrap(left.plan)

      assert {:as_of_join, _, _, {:col, "t1", ^left_plan}, {:col, "t2"}, _, _, _, _, _, _} =
               unwrap_plan(result)
    end

    test "accepts string right_as_of (binds to right plan)" do
      left = make_df()
      right = make_df()
      result = DataFrame.as_of_join(left, right, Functions.col("t1"), "t2")

      right_plan = unwrap(right.plan)

      assert {:as_of_join, _, _, {:col, "t1"}, {:col, "t2", ^right_plan}, _, _, _, _, _, _} =
               unwrap_plan(result)
    end

    test "accepts both strings (binds each to its own plan)" do
      left = make_df()
      right = make_df()
      result = DataFrame.as_of_join(left, right, "t1", "t2")

      left_plan = unwrap(left.plan)
      right_plan = unwrap(right.plan)

      assert {:as_of_join, _, _, {:col, "t1", ^left_plan}, {:col, "t2", ^right_plan}, _, _, _, _,
              _, _} = unwrap_plan(result)
    end
  end

  # ── #17 drop supports Column operands ──

  describe "#17 drop Column support" do
    test "accepts Column in list (kept as an expression, PySpark parity)" do
      result = DataFrame.drop(make_df(), [Functions.col("x")])
      assert {:drop, _, [], [{:col, "x"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts scalar string" do
      result = DataFrame.drop(make_df(), "x")
      assert {:drop, _, ["x"], []} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts scalar Column (kept as an expression, PySpark parity)" do
      result = DataFrame.drop(make_df(), Functions.col("x"))
      assert {:drop, _, [], [{:col, "x"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #39 pivot value type validation ──

  describe "#39 pivot value type validation" do
    test "stores pivot values verbatim and defers literal validation to the encoder" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :groupby,
        grouping_sets: nil,
        pivot_col: nil,
        pivot_values: nil
      }

      # The construction-time guard no longer eagerly rejects shapes the
      # encoder might accept; encode-time errors surface from
      # encode_literal/1 instead. Verify pivot stores the raw value.
      result = SparkEx.GroupedData.pivot(gd, "col", [Decimal.new("1.5")])
      assert [%Decimal{}] = result.pivot_values
    end

    test "accepts valid pivot values" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :groupby,
        grouping_sets: nil,
        pivot_col: nil,
        pivot_values: nil
      }

      result = SparkEx.GroupedData.pivot(gd, "col", [1, "a", true])
      assert result.pivot_values == [1, "a", true]
    end
  end

  # ── #40 sort_within_partitions empty validation ──

  describe "#40 sort_within_partitions empty validation" do
    test "rejects empty columns" do
      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        DataFrame.sort_within_partitions(make_df(), [])
      end
    end

    test "accepts non-empty columns" do
      result = DataFrame.sort_within_partitions(make_df(), ["x"])
      assert {:sort, _, _, false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #41 select scalar support ──

  describe "#41 select scalar support" do
    test "accepts scalar string" do
      result = DataFrame.select(make_df(), "x")
      assert {:project, _, [{:col, "x"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts scalar Column" do
      result = DataFrame.select(make_df(), Functions.col("x"))
      assert {:project, _, [{:col, "x"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts list" do
      result = DataFrame.select(make_df(), ["a", "b"])

      assert {:project, _, [{:col, "a"}, {:col, "b"}]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #42 select_expr scalar support ──

  describe "#42 select_expr scalar support" do
    test "accepts scalar string" do
      result = DataFrame.select_expr(make_df(), "x + 1")
      assert {:project, _, [{:expr, "x + 1"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts list" do
      result = DataFrame.select_expr(make_df(), ["a", "b + 1"])

      assert {:project, _, [{:expr, "a"}, {:expr, "b + 1"}]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #50 describe/summary scalar support ──

  describe "#50 describe/summary scalar support" do
    test "describe accepts scalar string" do
      result = DataFrame.describe(make_df(), "age")
      assert {:stat_describe, _, ["age"]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "summary accepts scalar string" do
      result = DataFrame.summary(make_df(), "count")
      assert {:stat_summary, _, ["count"]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #53 group_by/rollup/cube scalar support ──

  describe "#53 group_by/rollup/cube scalar support" do
    test "group_by accepts scalar string" do
      result = DataFrame.group_by(make_df(), "x")
      assert %SparkEx.GroupedData{grouping_exprs: [{:col, "x"}]} = result
    end

    test "rollup accepts scalar string" do
      result = DataFrame.rollup(make_df(), "x")
      assert %SparkEx.GroupedData{grouping_exprs: [{:col, "x"}], group_type: :rollup} = result
    end

    test "cube accepts scalar string" do
      result = DataFrame.cube(make_df(), "x")
      assert %SparkEx.GroupedData{grouping_exprs: [{:col, "x"}], group_type: :cube} = result
    end
  end

  # ── #54/#55 WriterV2 empty validation ──

  describe "#54/#55 WriterV2 empty validation" do
    test "partitioned_by rejects empty list" do
      writer = %SparkEx.WriterV2{
        df: make_df(),
        table_name: "t",
        options: %{},
        table_properties: %{},
        partitioned_by: [],
        cluster_by: []
      }

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.WriterV2.partitioned_by(writer, [])
      end
    end

    test "cluster_by rejects empty list" do
      writer = %SparkEx.WriterV2{
        df: make_df(),
        table_name: "t",
        options: %{},
        table_properties: %{},
        partitioned_by: [],
        cluster_by: []
      }

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.WriterV2.cluster_by(writer, [])
      end
    end
  end

  # ── #60 transpose scalar index_column ──

  describe "#60 transpose scalar index_column" do
    test "accepts scalar string" do
      result = DataFrame.transpose(make_df(), "id")
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts Column" do
      result = DataFrame.transpose(make_df(), Functions.col("id"))
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts Column via keyword" do
      result = DataFrame.transpose(make_df(), index_column: Functions.col("id"))
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts keyword opts" do
      result = DataFrame.transpose(make_df(), index_column: "id")
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #11 join no-condition overload ──

  describe "#11 join nil condition" do
    test "accepts nil on condition" do
      result = DataFrame.join(make_df(), make_df(), nil, :cross)
      assert {:join, _, _, nil, :cross, []} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts Column condition" do
      result = DataFrame.join(make_df(), make_df(), Functions.col("id"), :inner)

      assert {:join, _, _, {:col, "id"}, :inner, []} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #16 lateral_join optional condition ──

  describe "#16 lateral_join optional condition" do
    test "accepts nil condition" do
      result = DataFrame.lateral_join(make_df(), make_df(), nil, :inner)
      assert {:lateral_join, _, _, nil, :inner} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts no condition (default nil)" do
      result = DataFrame.lateral_join(make_df(), make_df())
      assert {:lateral_join, _, _, nil, :inner} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #20 CSV sep keyword ──

  describe "#20 CSV sep keyword" do
    test "reader csv builds with :sep option" do
      # Verify :sep option is passed through to the data source plan options
      df = SparkEx.Reader.csv(nil, "/tmp/test.csv", sep: "|")
      {:read_data_source, "csv", ["/tmp/test.csv"], _schema, options} = unwrap(df.plan)
      assert options["sep"] == "|"
    end
  end

  # ── #23 tail(0) valid ──

  describe "#23 tail(0)" do
    test "accepts 0" do
      assert catch_exit(DataFrame.tail(make_df(), 0))
    end

    test "still accepts positive" do
      assert catch_exit(DataFrame.tail(make_df(), 5))
    end
  end

  describe "#23 tail_df(0)" do
    test "builds lazy tail relation for 0" do
      result = DataFrame.tail_df(make_df(), 0)
      assert {:tail, _, 0} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still builds lazy relation for positive" do
      result = DataFrame.tail_df(make_df(), 5)
      assert {:tail, _, 5} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #30 grouping_sets *cols overload ──

  describe "#30 grouping_sets with cols" do
    test "accepts explicit grouping columns" do
      result = DataFrame.grouping_sets(make_df(), [["a"], ["b"]], ["a", "b", "c"])

      assert %SparkEx.GroupedData{
               grouping_exprs: [{:col, "a"}, {:col, "b"}, {:col, "c"}],
               group_type: :grouping_sets
             } = result
    end

    test "still works without explicit cols" do
      result = DataFrame.grouping_sets(make_df(), [["a"], ["b"]])

      assert %SparkEx.GroupedData{
               grouping_exprs: [{:col, "a"}, {:col, "b"}],
               group_type: :grouping_sets
             } = result
    end
  end

  # ── #31 sample overload parity ──

  describe "#31 sample overload" do
    test "accepts (with_replacement, fraction, seed) form" do
      result = DataFrame.sample(make_df(), true, 0.5, 42)

      assert {:sample, _, +0.0, 0.5, true, 42, false} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "still accepts (fraction, opts) form" do
      result = DataFrame.sample(make_df(), 0.1, seed: 42)

      assert {:sample, _, +0.0, 0.1, false, 42, false} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #48 sample_by key type validation ──

  describe "#48 sample_by key type validation" do
    test "rejects invalid key types" do
      assert_raise ArgumentError, ~r/fraction keys must be/, fn ->
        SparkEx.DataFrame.Stat.sample_by(make_df(), "col", %{%{bad: 1} => 0.5})
      end
    end
  end

  # ── #49 approx_quantile tuple support ──

  describe "#49 approx_quantile tuple support" do
    test "rejects non-string column names in tuple" do
      assert_raise ArgumentError, ~r/column names must all be strings/, fn ->
        SparkEx.DataFrame.Stat.approx_quantile(make_df(), {1, 2}, [0.5], 0.0)
      end
    end
  end

  # ── #51/#52 col_regex/metadata_column return Column ──

  describe "#51/#52 col_regex and metadata_column return Column" do
    test "col_regex returns Column" do
      result = DataFrame.col_regex(make_df(), "^x.*")
      assert {:col_regex, "^x.*", {:sql, "SELECT 1", nil}} = unwrap(result.expr)
    end

    test "metadata_column returns Column" do
      result = DataFrame.metadata_column(make_df(), "_metadata")
      assert {:metadata_col, "_metadata", {:sql, "SELECT 1", nil}} = unwrap(result.expr)
    end
  end

  # ── #63 DataFrame.to schema validation ──

  describe "#63 DataFrame.to schema validation" do
    test "rejects invalid schema types" do
      assert_raise ArgumentError, ~r/expected schema/, fn ->
        DataFrame.to(make_df(), 123)
      end
    end

    test "accepts DDL string" do
      result = DataFrame.to(make_df(), "id LONG, name STRING")

      assert {:to_schema, _, "id LONG, name STRING"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #22 DataFrame.agg map shorthand ──

  describe "#22 DataFrame.agg map shorthand" do
    test "accepts map of column => function" do
      result = DataFrame.agg(make_df(), %{"age" => "max"})
      assert {:aggregate, _, :groupby, [], _} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "GroupedData.agg also accepts map" do
      gd = DataFrame.group_by(make_df(), ["dept"])
      result = SparkEx.GroupedData.agg(gd, %{"salary" => "sum"})

      assert {:aggregate, _, :groupby, [{:col, "dept"}], _} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #28 drop_duplicates nil vs [] semantics ──

  describe "#28 drop_duplicates nil vs empty list semantics" do
    test "nil subset sets all_columns_as_keys=true" do
      result = DataFrame.drop_duplicates(make_df(), nil)
      assert {:deduplicate, _, [], true} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "default (no subset) sets all_columns_as_keys=true" do
      result = DataFrame.drop_duplicates(make_df())
      assert {:deduplicate, _, [], true} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "explicit empty list sets all_columns_as_keys=false" do
      result = DataFrame.drop_duplicates(make_df(), [])
      assert {:deduplicate, _, [], false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "non-empty subset sets all_columns_as_keys=false" do
      result = DataFrame.drop_duplicates(make_df(), ["a", "b"])
      assert {:deduplicate, _, ["a", "b"], false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── #40 sort_within_partitions ascending/ordinal support ──

  describe "#40 sort_within_partitions ascending keyword and ordinals" do
    test "rejects integer ordinal columns" do
      assert_raise ArgumentError, ~r/integer sort keys are not supported/, fn ->
        DataFrame.sort_within_partitions(make_df(), [0, 1])
      end
    end

    test "rejects integer ordinal columns with explicit ascending flag" do
      assert_raise ArgumentError, ~r/integer sort keys are not supported/, fn ->
        DataFrame.sort_within_partitions(make_df(), [0], ascending: true)
      end
    end

    test "ascending: false reverses all columns" do
      result = DataFrame.sort_within_partitions(make_df(), ["a", "b"], ascending: false)
      assert {:sort, _, sort_exprs, false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)

      Enum.each(sort_exprs, fn {:sort_order, _, dir, _} ->
        assert dir == :desc
      end)
    end

    test "ascending: list sets per-column direction" do
      result =
        DataFrame.sort_within_partitions(make_df(), ["a", "b"], ascending: [true, false])

      assert {:sort, _, [order_a, order_b], false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
      assert {:sort_order, _, :asc, _} = order_a
      assert {:sort_order, _, :desc, _} = order_b
    end

    test "ascending list length mismatch raises" do
      assert_raise ArgumentError, ~r/ascending list length/, fn ->
        DataFrame.sort_within_partitions(make_df(), ["a", "b"], ascending: [true])
      end
    end
  end

  # ── #59 MERGE assignment key validation ──

  describe "#59 MERGE assignment key string validation" do
    test "rejects non-string keys" do
      writer = %SparkEx.MergeIntoWriter{
        source_df: make_df(),
        target_table: "t",
        condition: {:col, "id"},
        match_actions: [],
        not_matched_actions: [],
        not_matched_by_source_actions: []
      }

      assert_raise ArgumentError, ~r/must be strings/, fn ->
        SparkEx.MergeIntoWriter.when_matched_update(writer, %{1 => Functions.col("x")})
      end
    end

    test "accepts string keys" do
      writer = %SparkEx.MergeIntoWriter{
        source_df: make_df(),
        target_table: "t",
        condition: {:col, "id"},
        match_actions: [],
        not_matched_actions: [],
        not_matched_by_source_actions: []
      }

      result =
        SparkEx.MergeIntoWriter.when_matched_update(writer, %{"col1" => Functions.col("x")})

      assert length(result.match_actions) == 1
    end
  end

  # ── #61 transpose single index only ──

  describe "#61 transpose single index column only" do
    test "accepts single index via keyword" do
      result = DataFrame.transpose(make_df(), index_column: "id")
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "no index column produces empty list" do
      result = DataFrame.transpose(make_df())
      assert {:transpose, _, []} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts Column via keyword" do
      result = DataFrame.transpose(make_df(), index_column: Functions.col("id"))
      assert {:transpose, _, [{:col, "id"}]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  # ── Helper ──

  defp make_df do
    DataFrame.new(self(), {:sql, "SELECT 1", nil})
  end
end
