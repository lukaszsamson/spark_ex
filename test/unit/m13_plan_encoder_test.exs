defmodule SparkEx.M13.PlanEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder

  # Helper to make a simple child plan
  defp child_plan, do: {:sql, "SELECT 1", nil}

  # ── NA Fill ──

  describe "encode_relation for na_fill" do
    test "encodes na_fill with scalar value and no cols" do
      plan = {:na_fill, child_plan(), [], [0]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:fill_na, na_fill} = relation.rel_type
      assert na_fill.cols == []
      assert [%{literal_type: {:long, 0}}] = na_fill.values
      assert na_fill.input != nil
    end

    test "encodes na_fill with multiple cols and values" do
      plan = {:na_fill, child_plan(), ["age", "name"], [0, "unknown"]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:fill_na, na_fill} = relation.rel_type
      assert na_fill.cols == ["age", "name"]
      assert [%{literal_type: {:long, 0}}, %{literal_type: {:string, "unknown"}}] = na_fill.values
    end

    test "encodes na_fill with float value" do
      plan = {:na_fill, child_plan(), [], [3.14]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:fill_na, na_fill} = relation.rel_type
      assert [%{literal_type: {:double, 3.14}}] = na_fill.values
    end

    test "encodes na_fill with boolean value" do
      plan = {:na_fill, child_plan(), [], [true]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:fill_na, na_fill} = relation.rel_type
      assert [%{literal_type: {:boolean, true}}] = na_fill.values
    end
  end

  # ── NA Drop ──

  describe "encode_relation for na_drop" do
    test "encodes na_drop with no cols and no min_non_nulls" do
      plan = {:na_drop, child_plan(), [], nil}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:drop_na, na_drop} = relation.rel_type
      assert na_drop.cols == []
      assert na_drop.min_non_nulls == nil
      assert na_drop.input != nil
    end

    test "encodes na_drop with cols and min_non_nulls" do
      plan = {:na_drop, child_plan(), ["a", "b"], 2}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:drop_na, na_drop} = relation.rel_type
      assert na_drop.cols == ["a", "b"]
      assert na_drop.min_non_nulls == 2
    end

    test "encodes na_drop with how=:all (min_non_nulls=1)" do
      plan = {:na_drop, child_plan(), [], 1}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:drop_na, na_drop} = relation.rel_type
      assert na_drop.min_non_nulls == 1
    end
  end

  # ── NA Replace ──

  describe "encode_relation for na_replace" do
    test "encodes na_replace with single replacement" do
      plan = {:na_replace, child_plan(), [], [{0, 100}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:replace, na_replace} = relation.rel_type
      assert na_replace.cols == []
      assert [replacement] = na_replace.replacements
      assert %{literal_type: {:double, +0.0}} = replacement.old_value
      assert %{literal_type: {:double, 100.0}} = replacement.new_value
    end

    test "encodes na_replace with multiple replacements" do
      plan = {:na_replace, child_plan(), ["score"], [{1, 10}, {2, 20}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:replace, na_replace} = relation.rel_type
      assert na_replace.cols == ["score"]
      assert length(na_replace.replacements) == 2
    end

    test "encodes na_replace with nil new_value" do
      plan = {:na_replace, child_plan(), [], [{"N/A", nil}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:replace, na_replace} = relation.rel_type
      [replacement] = na_replace.replacements
      assert %{literal_type: {:string, "N/A"}} = replacement.old_value
      assert %{literal_type: {:null, _}} = replacement.new_value
    end
  end

  # ── Stat Describe ──

  describe "encode_relation for stat_describe" do
    test "encodes describe with no cols" do
      plan = {:stat_describe, child_plan(), []}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:describe, stat} = relation.rel_type
      assert stat.cols == []
      assert stat.input != nil
    end

    test "encodes describe with cols" do
      plan = {:stat_describe, child_plan(), ["age", "salary"]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:describe, stat} = relation.rel_type
      assert stat.cols == ["age", "salary"]
    end
  end

  # ── Stat Summary ──

  describe "encode_relation for stat_summary" do
    test "encodes summary with no statistics" do
      plan = {:stat_summary, child_plan(), []}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:summary, stat} = relation.rel_type
      assert stat.statistics == []
    end

    test "encodes summary with statistics" do
      plan = {:stat_summary, child_plan(), ["count", "min", "max"]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:summary, stat} = relation.rel_type
      assert stat.statistics == ["count", "min", "max"]
    end
  end

  # ── Stat Corr ──

  describe "encode_relation for stat_corr" do
    test "encodes corr" do
      plan = {:stat_corr, child_plan(), "height", "weight", "pearson"}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:corr, stat} = relation.rel_type
      assert stat.col1 == "height"
      assert stat.col2 == "weight"
      assert stat.method == "pearson"
      assert stat.input != nil
    end
  end

  # ── Stat Cov ──

  describe "encode_relation for stat_cov" do
    test "encodes cov" do
      plan = {:stat_cov, child_plan(), "x", "y"}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:cov, stat} = relation.rel_type
      assert stat.col1 == "x"
      assert stat.col2 == "y"
      assert stat.input != nil
    end
  end

  # ── Stat Crosstab ──

  describe "encode_relation for stat_crosstab" do
    test "encodes crosstab" do
      plan = {:stat_crosstab, child_plan(), "dept", "gender"}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:crosstab, stat} = relation.rel_type
      assert stat.col1 == "dept"
      assert stat.col2 == "gender"
    end
  end

  # ── Stat FreqItems ──

  describe "encode_relation for stat_freq_items" do
    test "encodes freq_items" do
      plan = {:stat_freq_items, child_plan(), ["category"], 0.05}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:freq_items, stat} = relation.rel_type
      assert stat.cols == ["category"]
      assert stat.support == 0.05
    end
  end

  # ── Stat ApproxQuantile ──

  describe "encode_relation for stat_approx_quantile" do
    test "encodes approx_quantile" do
      plan = {:stat_approx_quantile, child_plan(), ["age"], [0.25, 0.5, 0.75], 0.01}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:approx_quantile, stat} = relation.rel_type
      assert stat.cols == ["age"]
      assert stat.probabilities == [0.25, 0.5, 0.75]
      assert stat.relative_error == 0.01
    end
  end

  # ── Stat SampleBy ──

  describe "encode_relation for stat_sample_by" do
    test "encodes sample_by with fractions and seed" do
      plan =
        {:stat_sample_by, child_plan(), {:col, "label"}, [{0, 0.1}, {1, 0.5}], 42}

      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:sample_by, stat} = relation.rel_type
      assert stat.col != nil
      assert length(stat.fractions) == 2
      assert stat.seed == 42

      [f1, f2] = stat.fractions
      assert f1.fraction == 0.1
      assert f2.fraction == 0.5
    end

    test "encodes sample_by without seed" do
      plan =
        {:stat_sample_by, child_plan(), {:col, "label"}, [{0, 0.1}], nil}

      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:sample_by, stat} = relation.rel_type
      assert stat.seed == nil
    end
  end

  # ── Table-Valued Functions ──

  describe "encode_relation for table_valued_function" do
    test "encodes TVF with no arguments" do
      plan = {:table_valued_function, "range", []}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:unresolved_table_valued_function, tvf} = relation.rel_type
      assert tvf.function_name == "range"
      assert tvf.arguments == []
    end

    test "encodes TVF with literal arguments" do
      plan = {:table_valued_function, "range", [{:lit, 0}, {:lit, 10}, {:lit, 2}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:unresolved_table_valued_function, tvf} = relation.rel_type
      assert tvf.function_name == "range"
      assert length(tvf.arguments) == 3
    end

    test "encodes TVF with column arguments" do
      plan = {:table_valued_function, "explode", [{:col, "my_array"}]}
      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:unresolved_table_valued_function, tvf} = relation.rel_type
      assert tvf.function_name == "explode"
      assert length(tvf.arguments) == 1
    end
  end

  # ── Inline UDTF as Relation ──

  describe "encode_relation for inline_udtf" do
    test "encodes inline UDTF as relation" do
      plan =
        {:inline_udtf, "my_udtf", [{:lit, 1}], <<1, 2, 3>>, nil, 0, "3.11", true}

      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:common_inline_user_defined_table_function, udtf} = relation.rel_type
      assert udtf.function_name == "my_udtf"
      assert udtf.deterministic == true
      assert length(udtf.arguments) == 1
      assert {:python_udtf, python_udtf} = udtf.function
      assert python_udtf.command == <<1, 2, 3>>
      assert python_udtf.eval_type == 0
      assert python_udtf.python_ver == "3.11"
    end

    test "encodes inline UDTF with return type" do
      return_type = %Spark.Connect.DataType{
        kind: {:struct, %Spark.Connect.DataType.Struct{}}
      }

      plan =
        {:inline_udtf, "my_udtf", [], <<1, 2, 3>>, return_type, 0, "3.11", true}

      {relation, _counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:common_inline_user_defined_table_function, udtf} = relation.rel_type
      assert {:python_udtf, python_udtf} = udtf.function
      assert python_udtf.return_type == return_type
    end
  end

  # ── Counter increment ──

  describe "counter management" do
    test "counter increments correctly through NA/Stat encoding" do
      plan = {:na_fill, {:sql, "SELECT 1", nil}, [], [0]}
      {_relation, counter} = PlanEncoder.encode_relation(plan, 0)
      # sql child uses 1 id, na_fill uses 1 id
      assert counter == 2
    end
  end
end
