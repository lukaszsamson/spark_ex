defmodule SparkEx.Unit.PlanEncoderM10Test do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Relation, Expression}

  @base_plan {:sql, "SELECT 1", nil}

  # ── Offset ──

  describe "encode_relation :offset" do
    test "encodes offset relation" do
      {relation, _} = PlanEncoder.encode_relation({:offset, @base_plan, 10}, 0)

      assert %Relation{rel_type: {:offset, offset}} = relation
      assert %Spark.Connect.Offset{offset: 10} = offset
      assert offset.input != nil
    end
  end

  # ── Tail ──

  describe "encode_relation :tail" do
    test "encodes tail relation" do
      {relation, _} = PlanEncoder.encode_relation({:tail, @base_plan, 5}, 0)

      assert %Relation{rel_type: {:tail, tail}} = relation
      assert %Spark.Connect.Tail{limit: 5} = tail
      assert tail.input != nil
    end
  end

  # ── ToDF ──

  describe "encode_relation :to_df" do
    test "encodes to_df relation" do
      {relation, _} =
        PlanEncoder.encode_relation({:to_df, @base_plan, ["id", "name"]}, 0)

      assert %Relation{rel_type: {:to_df, to_df}} = relation
      assert %Spark.Connect.ToDF{column_names: ["id", "name"]} = to_df
      assert to_df.input != nil
    end
  end

  # ── WithColumnsRenamed ──

  describe "encode_relation :with_columns_renamed" do
    test "encodes rename pairs" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:with_columns_renamed, @base_plan, [{"old", "new"}, {"a", "b"}]},
          0
        )

      assert %Relation{rel_type: {:with_columns_renamed, renamed}} = relation
      assert length(renamed.renames) == 2

      [r1, r2] = renamed.renames
      assert %Spark.Connect.WithColumnsRenamed.Rename{col_name: "old", new_col_name: "new"} = r1
      assert %Spark.Connect.WithColumnsRenamed.Rename{col_name: "a", new_col_name: "b"} = r2
    end
  end

  describe "encode_relation :with_columns metadata alias" do
    test "encodes alias metadata when present" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:with_columns, @base_plan, [{:alias, {:col, "name"}, "name", ~s({"k":"v"})}]},
          0
        )

      assert %Relation{rel_type: {:with_columns, with_cols}} = relation
      assert [alias_expr] = with_cols.aliases
      assert alias_expr.name == ["name"]
      assert alias_expr.metadata == ~s({"k":"v"})
    end
  end

  # ── Repartition ──

  describe "encode_relation :repartition" do
    test "encodes repartition with shuffle" do
      {relation, _} =
        PlanEncoder.encode_relation({:repartition, @base_plan, 10, true}, 0)

      assert %Relation{rel_type: {:repartition, repart}} = relation
      assert %Spark.Connect.Repartition{num_partitions: 10, shuffle: true} = repart
    end

    test "encodes coalesce (shuffle=false)" do
      {relation, _} =
        PlanEncoder.encode_relation({:repartition, @base_plan, 1, false}, 0)

      assert %Relation{rel_type: {:repartition, repart}} = relation
      assert %Spark.Connect.Repartition{num_partitions: 1, shuffle: false} = repart
    end
  end

  # ── RepartitionByExpression ──

  describe "encode_relation :repartition_by_expression" do
    test "encodes repartition by expression" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:repartition_by_expression, @base_plan, [{:col, "key"}], 10},
          0
        )

      assert %Relation{rel_type: {:repartition_by_expression, repart}} = relation
      assert %Spark.Connect.RepartitionByExpression{num_partitions: 10} = repart
      assert [%Expression{expr_type: {:unresolved_attribute, _}}] = repart.partition_exprs
    end

    test "encodes repartition by multiple expressions" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:repartition_by_expression, @base_plan, [{:col, "a"}, {:col, "b"}], 5},
          0
        )

      assert %Relation{rel_type: {:repartition_by_expression, repart}} = relation
      assert length(repart.partition_exprs) == 2
    end
  end

  # ── Sample ──

  describe "encode_relation :sample" do
    test "encodes sample relation" do
      lower = +0.0

      {relation, _} =
        PlanEncoder.encode_relation(
          {:sample, @base_plan, lower, 0.1, false, nil, false},
          0
        )

      assert %Relation{rel_type: {:sample, sample}} = relation
      assert sample.lower_bound == +0.0
      assert sample.upper_bound == 0.1
      assert sample.with_replacement == false
      assert sample.seed == nil
    end

    test "encodes sample with seed and replacement" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:sample, @base_plan, 0.0, 0.5, true, 42, true},
          0
        )

      assert %Relation{rel_type: {:sample, sample}} = relation
      assert sample.with_replacement == true
      assert sample.seed == 42
      assert sample.deterministic_order == true
    end
  end

  # ── Hint ──

  describe "encode_relation :hint" do
    test "encodes hint with no parameters" do
      {relation, _} =
        PlanEncoder.encode_relation({:hint, @base_plan, "broadcast", []}, 0)

      assert %Relation{rel_type: {:hint, hint}} = relation
      assert %Spark.Connect.Hint{name: "broadcast", parameters: []} = hint
    end

    test "encodes hint with parameters" do
      {relation, _} =
        PlanEncoder.encode_relation({:hint, @base_plan, "coalesce", [{:lit, 3}]}, 0)

      assert %Relation{rel_type: {:hint, hint}} = relation
      assert hint.name == "coalesce"
      assert [%Expression{expr_type: {:literal, _}}] = hint.parameters
    end
  end

  # ── Unpivot ──

  describe "encode_relation :unpivot" do
    test "encodes unpivot with explicit values" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:unpivot, @base_plan, [{:col, "id"}], [{:col, "c1"}, {:col, "c2"}], "variable",
           "value"},
          0
        )

      assert %Relation{rel_type: {:unpivot, unpivot}} = relation

      assert %Spark.Connect.Unpivot{variable_column_name: "variable", value_column_name: "value"} =
               unpivot

      assert length(unpivot.ids) == 1
      assert %Spark.Connect.Unpivot.Values{values: vals} = unpivot.values
      assert length(vals) == 2
    end

    test "encodes unpivot with nil values" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:unpivot, @base_plan, [{:col, "id"}], nil, "var", "val"},
          0
        )

      assert %Relation{rel_type: {:unpivot, unpivot}} = relation
      assert unpivot.values == nil
    end
  end

  # ── Transpose ──

  describe "encode_relation :transpose" do
    test "encodes transpose with no index columns" do
      {relation, _} =
        PlanEncoder.encode_relation({:transpose, @base_plan, []}, 0)

      assert %Relation{rel_type: {:transpose, transpose}} = relation
      assert %Spark.Connect.Transpose{index_columns: []} = transpose
    end

    test "encodes transpose with index column" do
      {relation, _} =
        PlanEncoder.encode_relation({:transpose, @base_plan, [{:col, "id"}]}, 0)

      assert %Relation{rel_type: {:transpose, transpose}} = relation
      assert [%Expression{}] = transpose.index_columns
    end
  end

  # ── HtmlString ──

  describe "encode_relation :html_string" do
    test "encodes html_string relation" do
      {relation, _} =
        PlanEncoder.encode_relation({:html_string, @base_plan, 20, 20}, 0)

      assert %Relation{rel_type: {:html_string, html}} = relation
      assert %Spark.Connect.HtmlString{num_rows: 20, truncate: 20} = html
    end
  end

  # ── WithWatermark ──

  describe "encode_relation :with_watermark" do
    test "encodes with_watermark relation" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:with_watermark, @base_plan, "event_time", "10 minutes"},
          0
        )

      assert %Relation{rel_type: {:with_watermark, wm}} = relation

      assert %Spark.Connect.WithWatermark{event_time: "event_time", delay_threshold: "10 minutes"} =
               wm
    end
  end

  # ── SubqueryAlias ──

  describe "encode_relation :subquery_alias" do
    test "encodes subquery alias" do
      {relation, _} =
        PlanEncoder.encode_relation({:subquery_alias, @base_plan, "t"}, 0)

      assert %Relation{rel_type: {:subquery_alias, sa}} = relation
      assert %Spark.Connect.SubqueryAlias{alias: "t"} = sa
    end
  end

  # ── Extended SetOperation ──

  describe "encode_relation :set_operation extended" do
    test "encodes set_operation with by_name" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:set_operation, @base_plan, @base_plan, :union, true,
           by_name: true, allow_missing_columns: false},
          0
        )

      assert %Relation{rel_type: {:set_op, set_op}} = relation
      assert set_op.by_name == true
      assert set_op.allow_missing_columns == false
    end

    test "encodes set_operation with allow_missing_columns" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:set_operation, @base_plan, @base_plan, :union, true,
           by_name: true, allow_missing_columns: true},
          0
        )

      assert %Relation{rel_type: {:set_op, set_op}} = relation
      assert set_op.by_name == true
      assert set_op.allow_missing_columns == true
    end

    test "existing 5-tuple still works" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:set_operation, @base_plan, @base_plan, :union, true},
          0
        )

      assert %Relation{rel_type: {:set_op, set_op}} = relation
      assert set_op.by_name == false
      assert set_op.allow_missing_columns == false
    end
  end

  # ── Extended Sort ──

  describe "encode_relation :sort extended" do
    test "3-tuple sort defaults to is_global=true" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:sort, @base_plan, [{:sort_order, {:col, "a"}, :asc, nil}]},
          0
        )

      assert %Relation{rel_type: {:sort, sort}} = relation
      assert sort.is_global == true
    end

    test "4-tuple sort with is_global=false" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:sort, @base_plan, [{:sort_order, {:col, "a"}, :asc, nil}], false},
          0
        )

      assert %Relation{rel_type: {:sort, sort}} = relation
      assert sort.is_global == false
    end
  end

  # ── Extended Deduplicate ──

  describe "encode_relation :deduplicate extended" do
    test "4-tuple deduplicate defaults to within_watermark=false" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:deduplicate, @base_plan, ["id"], false},
          0
        )

      assert %Relation{rel_type: {:deduplicate, dedup}} = relation
      assert dedup.within_watermark == false
    end

    test "5-tuple deduplicate with within_watermark=true" do
      {relation, _} =
        PlanEncoder.encode_relation(
          {:deduplicate, @base_plan, ["id"], false, true},
          0
        )

      assert %Relation{rel_type: {:deduplicate, dedup}} = relation
      assert dedup.within_watermark == true
    end
  end

  # ── Plan ID counter progression ──

  describe "plan ID counter" do
    test "counter increments correctly for new relation types" do
      {_, counter} = PlanEncoder.encode_relation({:offset, @base_plan, 10}, 0)
      assert counter > 1

      {_, counter} = PlanEncoder.encode_relation({:hint, @base_plan, "broadcast", []}, 0)
      assert counter > 1
    end
  end

  # ── Full Plan encoding ──

  describe "encode/2 with M10 relations" do
    test "encodes a complex M10 plan" do
      plan =
        {:sort,
         {:repartition,
          {:with_columns_renamed, {:project, @base_plan, [{:col, "a"}, {:col, "b"}]},
           [{"a", "x"}]}, 4, true}, [{:sort_order, {:col, "x"}, :asc, nil}], false}

      {encoded, _} = PlanEncoder.encode(plan, 0)
      assert %Spark.Connect.Plan{op_type: {:root, %Relation{}}} = encoded
    end
  end
end
