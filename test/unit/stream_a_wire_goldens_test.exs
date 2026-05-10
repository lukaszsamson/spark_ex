defmodule SparkEx.Test.StreamAWireGoldensTest do
  @moduledoc """
  Wire-byte goldens for Stream A (BUGS_PLAN_5) relations whose top-level
  expressions are now remapped to the encoded child input plan_id.

  Regenerate with `UPDATE_GOLDENS=1 mix test test/unit/stream_a_wire_goldens_test.exs`.
  """

  use ExUnit.Case, async: true

  require SparkEx.Test.WireGoldens
  alias SparkEx.Test.WireGoldens

  # A common SQL child used across goldens — plan_id 0 after encoding.
  @child {:sql, "SELECT 1 AS id, 2 AS val, 3 AS dept", nil}
  # A foreign plan_id used to assert the relation rewrites it to the input id.
  @foreign 999

  describe "Stream A relation goldens (foreign plan_id rewrites to input)" do
    test "sort with foreign-bound sort_order" do
      plan =
        {:sort, @child, [{:sort_order, {:col, "id", @foreign}, :asc, :nulls_first}]}

      WireGoldens.assert_golden("stream_a/sort_foreign_col", plan)
    end

    test "with_columns with foreign-bound alias body" do
      plan =
        {:with_columns, @child, [{:alias, {:col, "id", @foreign}, "id2"}]}

      WireGoldens.assert_golden("stream_a/with_columns_foreign_col", plan)
    end

    test "drop with foreign-bound col_exprs" do
      plan = {:drop, @child, [], [{:col, "val", @foreign}]}

      WireGoldens.assert_golden("stream_a/drop_foreign_col", plan)
    end

    test "aggregate (groupby) with foreign-bound grouping/agg" do
      grouping = [{:col, "dept", @foreign}]
      agg = [{:fn, "sum", [{:col, "val", @foreign}], false}]
      plan = {:aggregate, @child, :groupby, grouping, agg}

      WireGoldens.assert_golden("stream_a/aggregate_groupby_foreign_cols", plan)
    end

    test "repartition_by_expression with foreign-bound exprs" do
      plan = {:repartition_by_expression, @child, [{:col, "id", @foreign}], 4}

      WireGoldens.assert_golden("stream_a/repartition_by_expression_foreign", plan)
    end

    test "hint with foreign-bound parameters" do
      plan = {:hint, @child, "broadcast", [{:col, "id", @foreign}]}

      WireGoldens.assert_golden("stream_a/hint_foreign_param", plan)
    end

    test "unpivot with foreign-bound ids and values" do
      plan =
        {:unpivot, @child, [{:col, "id", @foreign}], [{:col, "val", @foreign}], "var", "value"}

      WireGoldens.assert_golden("stream_a/unpivot_foreign_cols", plan)
    end

    test "transpose with foreign-bound index_columns" do
      plan = {:transpose, @child, [{:col, "id", @foreign}]}

      WireGoldens.assert_golden("stream_a/transpose_foreign_index", plan)
    end

    test "stat_sample_by with foreign-bound col" do
      plan = {:stat_sample_by, @child, {:col, "dept", @foreign}, [{"a", 0.1}], 42}

      WireGoldens.assert_golden("stream_a/sample_by_foreign_col", plan)
    end

    test "collect_metrics with foreign-bound metrics" do
      metrics = [{:alias, {:fn, "sum", [{:col, "val", @foreign}], false}, "total"}]
      plan = {:collect_metrics, @child, "obs", metrics}

      WireGoldens.assert_golden("stream_a/collect_metrics_foreign", plan)
    end
  end
end
