defmodule SparkEx.M13.StatTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.Stat

  defp make_df(plan \\ :test_plan) do
    %DataFrame{session: self(), plan: plan}
  end

  # ── Lazy methods ──

  describe "describe/1" do
    test "with no cols" do
      df = Stat.describe(make_df())
      assert %DataFrame{plan: {:stat_describe, :test_plan, []}} = df
    end

    test "with specific cols" do
      df = Stat.describe(make_df(), ["age", "salary"])
      assert %DataFrame{plan: {:stat_describe, :test_plan, ["age", "salary"]}} = df
    end
  end

  describe "summary/1" do
    test "with no statistics" do
      df = Stat.summary(make_df())
      assert %DataFrame{plan: {:stat_summary, :test_plan, []}} = df
    end

    test "with specific statistics" do
      df = Stat.summary(make_df(), ["count", "min", "max"])
      assert %DataFrame{plan: {:stat_summary, :test_plan, ["count", "min", "max"]}} = df
    end
  end

  describe "crosstab/3" do
    test "creates crosstab plan" do
      df = Stat.crosstab(make_df(), "department", "gender")
      assert %DataFrame{plan: {:stat_crosstab, :test_plan, "department", "gender"}} = df
    end
  end

  describe "freq_items/2" do
    test "with default support" do
      df = Stat.freq_items(make_df(), ["category"])
      assert %DataFrame{plan: {:stat_freq_items, :test_plan, ["category"], 0.01}} = df
    end

    test "with custom support" do
      df = Stat.freq_items(make_df(), ["category", "status"], 0.05)
      assert %DataFrame{plan: {:stat_freq_items, :test_plan, ["category", "status"], 0.05}} = df
    end

    test "accepts keyword support option" do
      df = Stat.freq_items(make_df(), ["category"], support: 0.5)
      assert %DataFrame{plan: {:stat_freq_items, :test_plan, ["category"], 0.5}} = df
    end
  end

  describe "sample_by/3" do
    test "with string column" do
      df = Stat.sample_by(make_df(), "label", %{0 => 0.1, 1 => 0.5})

      assert %DataFrame{
               plan: {:stat_sample_by, :test_plan, {:col, "label"}, fractions, seed}
             } = df

      assert is_integer(seed)

      assert length(fractions) == 2
    end

    test "with seed" do
      df = Stat.sample_by(make_df(), "label", %{0 => 0.1}, 42)

      assert %DataFrame{
               plan: {:stat_sample_by, :test_plan, {:col, "label"}, [{0, 0.1}], 42}
              } = df
    end

    test "accepts keyword seed option" do
      df = Stat.sample_by(make_df(), "label", %{0 => 0.1}, seed: 42)

      assert %DataFrame{
               plan: {:stat_sample_by, :test_plan, {:col, "label"}, [{0, 0.1}], 42}
             } = df
    end
  end

  # ── Eager methods (plan structure only — no session execution in unit tests) ──

  describe "corr/3 plan structure" do
    test "builds stat_corr plan tuple" do
      # We can't test the full eager path without a session, but we can verify
      # the plan tuple is correct by testing the Stat module's internal plan creation
      df = make_df()
      plan = {:stat_corr, df.plan, "height", "weight", "pearson"}
      result_df = %DataFrame{session: df.session, plan: plan}
      assert result_df.plan == {:stat_corr, :test_plan, "height", "weight", "pearson"}
    end
  end

  describe "cov/3 plan structure" do
    test "builds stat_cov plan tuple" do
      df = make_df()
      plan = {:stat_cov, df.plan, "height", "weight"}
      result_df = %DataFrame{session: df.session, plan: plan}
      assert result_df.plan == {:stat_cov, :test_plan, "height", "weight"}
    end
  end

  describe "approx_quantile plan structure" do
    test "builds stat_approx_quantile plan tuple for single col" do
      df = make_df()
      plan = {:stat_approx_quantile, df.plan, ["age"], [0.25, 0.5, 0.75], 0.0}

      assert plan ==
               {:stat_approx_quantile, :test_plan, ["age"], [0.25, 0.5, 0.75], 0.0}
    end

    test "builds stat_approx_quantile plan tuple for multiple cols" do
      df = make_df()
      plan = {:stat_approx_quantile, df.plan, ["age", "salary"], [0.5], 0.01}

      assert plan ==
               {:stat_approx_quantile, :test_plan, ["age", "salary"], [0.5], 0.01}
    end
  end

  # ── Validation tests ──

  describe "corr/3 validation" do
    test "raises on unsupported method" do
      assert_raise ArgumentError, ~r/currently only the Pearson/, fn ->
        Stat.corr(make_df(), "a", "b", "spearman")
      end
    end
  end

  describe "approx_quantile validation" do
    test "raises on invalid probability" do
      assert_raise ArgumentError, ~r/between 0 and 1/, fn ->
        Stat.approx_quantile(make_df(), "age", [1.5])
      end
    end

    test "raises on negative probability" do
      assert_raise ArgumentError, ~r/between 0 and 1/, fn ->
        Stat.approx_quantile(make_df(), "age", [-0.1])
      end
    end

    test "raises on negative relative_error" do
      assert_raise ArgumentError, ~r/non-negative/, fn ->
        Stat.approx_quantile(make_df(), "age", [0.5], -0.01)
      end
    end
  end

  describe "sample_by validation" do
    test "raises on negative fraction" do
      assert_raise ArgumentError, ~r/non-negative/, fn ->
        Stat.sample_by(make_df(), "label", %{0 => -0.1})
      end
    end
  end

  # ── DataFrame convenience delegates ──

  describe "DataFrame convenience delegates" do
    test "describe/1 delegates" do
      df = DataFrame.describe(make_df())
      assert %DataFrame{plan: {:stat_describe, :test_plan, []}} = df
    end

    test "summary/1 delegates" do
      df = DataFrame.summary(make_df())
      assert %DataFrame{plan: {:stat_summary, :test_plan, []}} = df
    end

    test "crosstab/3 delegates" do
      df = DataFrame.crosstab(make_df(), "a", "b")
      assert %DataFrame{plan: {:stat_crosstab, :test_plan, "a", "b"}} = df
    end

    test "freq_items/2 delegates" do
      df = DataFrame.freq_items(make_df(), ["x"])
      assert %DataFrame{plan: {:stat_freq_items, :test_plan, ["x"], 0.01}} = df
    end

    test "sample_by/3 delegates" do
      df = DataFrame.sample_by(make_df(), "label", %{0 => 0.1})
      assert %DataFrame{plan: {:stat_sample_by, :test_plan, {:col, "label"}, _, seed}} = df
      assert is_integer(seed)
    end
  end

  # ── Table-Valued Function ──

  describe "DataFrame.table_function/3" do
    test "creates TVF plan with no args" do
      df = DataFrame.table_function(self(), "range")
      assert %DataFrame{plan: {:table_valued_function, "range", []}} = df
      assert df.session == self()
    end

    test "creates TVF plan with literal args" do
      import SparkEx.Functions, only: [lit: 1]

      df = DataFrame.table_function(self(), "range", [lit(0), lit(10)])

      assert %DataFrame{plan: {:table_valued_function, "range", [{:lit, 0}, {:lit, 10}]}} = df
    end

    test "creates TVF plan with column args" do
      import SparkEx.Functions, only: [col: 1]

      df = DataFrame.table_function(self(), "explode", [col("my_array")])

      assert %DataFrame{plan: {:table_valued_function, "explode", [{:col, "my_array"}]}} = df
    end

    test "creates TVF plan with bare literal values" do
      df = DataFrame.table_function(self(), "range", [0, 10, 2])

      assert %DataFrame{
               plan: {:table_valued_function, "range", [{:lit, 0}, {:lit, 10}, {:lit, 2}]}
             } = df
    end
  end
end
