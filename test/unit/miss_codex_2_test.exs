defmodule SparkEx.MissCodex2Test do
  @moduledoc """
  Tests for fixes from MISS_CODEX_2.md gap analysis.
  """
  use ExUnit.Case, async: true

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
      assert %DataFrame{plan: {:repartition_by_expression, _, _, nil}} = result
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
      assert %DataFrame{plan: {:na_fill, _, ["age"], [0]}} = result
    end

    test "drop accepts single string subset" do
      result = DataFrame.NA.drop(make_df(), subset: "age")
      assert %DataFrame{plan: {:na_drop, _, ["age"], _}} = result
    end
  end

  # ── #13 NA.replace string subset normalization ──

  describe "#13 NA.replace string subset normalization" do
    test "replace normalizes string subset to list" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1}, nil, subset: "col1")
      assert %DataFrame{plan: {:na_replace, _, ["col1"], _}} = result
    end

    test "replace with list subset still works" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1}, nil, subset: ["col1", "col2"])
      assert %DataFrame{plan: {:na_replace, _, ["col1", "col2"], _}} = result
    end

    test "replace with nil subset uses empty list" do
      result = DataFrame.NA.replace(make_df(), %{0 => 1})
      assert %DataFrame{plan: {:na_replace, _, [], _}} = result
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
      assert %DataFrame{plan: {:sort, _, _}} = result
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
      assert %DataFrame{plan: {:to_df, _, ["a", "b"]}} = result
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
      assert %DataFrame{plan: {:with_watermark, _, "event_time", "10 minutes"}} = result
    end
  end

  # ── #34 unpivot scalar ids/values ──

  describe "#34 unpivot scalar ids/values" do
    test "accepts scalar string id" do
      result = DataFrame.unpivot(make_df(), "id", ["v1", "v2"], "key", "value")
      assert %DataFrame{plan: {:unpivot, _, [{:col, "id"}], _, "key", "value"}} = result
    end

    test "accepts scalar Column id" do
      result = DataFrame.unpivot(make_df(), Functions.col("id"), ["v1"], "key", "value")
      assert %DataFrame{plan: {:unpivot, _, [{:col, "id"}], _, "key", "value"}} = result
    end

    test "still accepts list ids" do
      result = DataFrame.unpivot(make_df(), ["id1", "id2"], ["v1"], "key", "value")
      assert %DataFrame{plan: {:unpivot, _, [{:col, "id1"}, {:col, "id2"}], _, "key", "value"}} = result
    end
  end

  # ── #38 Writer.cluster_by empty validation ──

  describe "#38 Writer.cluster_by empty validation" do
    test "rejects empty columns" do
      writer = %SparkEx.Writer{cluster_by: [], options: %{}}

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.Writer.cluster_by(writer, [])
      end
    end
  end

  # ── #47 Writer.bucket_by and sort_by empty validation ──

  describe "#47 Writer.bucket_by/sort_by empty validation" do
    test "bucket_by rejects empty columns" do
      writer = %SparkEx.Writer{bucket_by: nil, options: %{}}

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.Writer.bucket_by(writer, 8, [])
      end
    end

    test "sort_by rejects empty columns" do
      writer = %SparkEx.Writer{sort_by: [], options: %{}}

      assert_raise ArgumentError, ~r/should not be empty/, fn ->
        SparkEx.Writer.sort_by(writer, [])
      end
    end
  end

  # ── #62 Missing melt alias (already fixed) ──

  describe "#62 melt alias" do
    test "melt delegates to unpivot" do
      result = DataFrame.melt(make_df(), ["id"], ["v1"], "key", "value")
      assert %DataFrame{plan: {:unpivot, _, _, _, "key", "value"}} = result
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
      assert %DataFrame{plan: {:na_drop, _, ["a", "b"], _}} = result
    end
  end

  # ── #15 as_of_join accepts string columns ──

  describe "#15 as_of_join string columns" do
    test "accepts string left_as_of" do
      result = DataFrame.as_of_join(make_df(), make_df(), "t1", Functions.col("t2"))
      assert %DataFrame{plan: {:as_of_join, _, _, {:col, "t1"}, {:col, "t2"}, _, _, _, _, _, _}} =
               result
    end

    test "accepts string right_as_of" do
      result = DataFrame.as_of_join(make_df(), make_df(), Functions.col("t1"), "t2")
      assert %DataFrame{plan: {:as_of_join, _, _, {:col, "t1"}, {:col, "t2"}, _, _, _, _, _, _}} =
               result
    end

    test "accepts both strings" do
      result = DataFrame.as_of_join(make_df(), make_df(), "t1", "t2")
      assert %DataFrame{plan: {:as_of_join, _, _, {:col, "t1"}, {:col, "t2"}, _, _, _, _, _, _}} =
               result
    end
  end

  # ── #17 drop supports Column operands ──

  describe "#17 drop Column support" do
    test "accepts Column in list" do
      result = DataFrame.drop(make_df(), [Functions.col("x")])
      assert %DataFrame{plan: {:drop, _, ["x"]}} = result
    end

    test "accepts scalar string" do
      result = DataFrame.drop(make_df(), "x")
      assert %DataFrame{plan: {:drop, _, ["x"]}} = result
    end

    test "accepts scalar Column" do
      result = DataFrame.drop(make_df(), Functions.col("x"))
      assert %DataFrame{plan: {:drop, _, ["x"]}} = result
    end
  end

  # ── #39 pivot value type validation ──

  describe "#39 pivot value type validation" do
    test "rejects invalid pivot values" do
      gd = %SparkEx.GroupedData{
        session: self(),
        plan: {:sql, "SELECT 1", nil},
        grouping_exprs: [{:col, "x"}],
        group_type: :groupby,
        grouping_sets: nil,
        pivot_col: nil,
        pivot_values: nil
      }

      assert_raise ArgumentError, ~r/pivot values must be/, fn ->
        SparkEx.GroupedData.pivot(gd, "col", [%{bad: 1}])
      end
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
      assert %DataFrame{plan: {:sort, _, _, false}} = result
    end
  end

  # ── #41 select scalar support ──

  describe "#41 select scalar support" do
    test "accepts scalar string" do
      result = DataFrame.select(make_df(), "x")
      assert %DataFrame{plan: {:project, _, [{:col, "x"}]}} = result
    end

    test "accepts scalar Column" do
      result = DataFrame.select(make_df(), Functions.col("x"))
      assert %DataFrame{plan: {:project, _, [{:col, "x"}]}} = result
    end

    test "still accepts list" do
      result = DataFrame.select(make_df(), ["a", "b"])
      assert %DataFrame{plan: {:project, _, [{:col, "a"}, {:col, "b"}]}} = result
    end
  end

  # ── #42 select_expr scalar support ──

  describe "#42 select_expr scalar support" do
    test "accepts scalar string" do
      result = DataFrame.select_expr(make_df(), "x + 1")
      assert %DataFrame{plan: {:project, _, [{:expr, "x + 1"}]}} = result
    end

    test "still accepts list" do
      result = DataFrame.select_expr(make_df(), ["a", "b + 1"])
      assert %DataFrame{plan: {:project, _, [{:expr, "a"}, {:expr, "b + 1"}]}} = result
    end
  end

  # ── #50 describe/summary scalar support ──

  describe "#50 describe/summary scalar support" do
    test "describe accepts scalar string" do
      result = DataFrame.describe(make_df(), "age")
      assert %DataFrame{plan: {:stat_describe, _, ["age"]}} = result
    end

    test "summary accepts scalar string" do
      result = DataFrame.summary(make_df(), "count")
      assert %DataFrame{plan: {:stat_summary, _, ["count"]}} = result
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
      assert %DataFrame{plan: {:transpose, _, [{:col, "id"}]}} = result
    end

    test "accepts Column" do
      result = DataFrame.transpose(make_df(), Functions.col("id"))
      assert %DataFrame{plan: {:transpose, _, [{:col, "id"}]}} = result
    end

    test "accepts Column via keyword" do
      result = DataFrame.transpose(make_df(), index_column: Functions.col("id"))
      assert %DataFrame{plan: {:transpose, _, [{:col, "id"}]}} = result
    end

    test "still accepts keyword opts" do
      result = DataFrame.transpose(make_df(), index_column: "id")
      assert %DataFrame{plan: {:transpose, _, [{:col, "id"}]}} = result
    end
  end

  # ── Helper ──

  defp make_df do
    %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
  end
end
