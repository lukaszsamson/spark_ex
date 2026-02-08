defmodule SparkEx.M13.NATest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.NA

  defp make_df(plan \\ :test_plan) do
    %DataFrame{session: self(), plan: plan}
  end

  # ── fill/2 ──

  describe "fill/2 with scalar" do
    test "fills with integer, no subset" do
      df = NA.fill(make_df(), 0)
      assert %DataFrame{plan: {:na_fill, :test_plan, [], [0]}} = df
    end

    test "fills with float, no subset" do
      df = NA.fill(make_df(), 0.0)
      assert %DataFrame{plan: {:na_fill, :test_plan, [], [val]}} = df
      assert val == 0.0
    end

    test "fills with string, no subset" do
      df = NA.fill(make_df(), "unknown")
      assert %DataFrame{plan: {:na_fill, :test_plan, [], ["unknown"]}} = df
    end

    test "fills with boolean, no subset" do
      df = NA.fill(make_df(), false)
      assert %DataFrame{plan: {:na_fill, :test_plan, [], [false]}} = df
    end

    test "fills with scalar and subset encodes single value (PySpark parity)" do
      df = NA.fill(make_df(), 0, subset: ["age", "salary"])
      # PySpark encodes a single literal value with multiple cols, not duplicated values
      assert %DataFrame{plan: {:na_fill, :test_plan, ["age", "salary"], [0]}} = df
    end
  end

  describe "fill/2 with map" do
    test "fills with column-specific values" do
      df = NA.fill(make_df(), %{"age" => 0, "name" => "unknown"})

      assert %DataFrame{plan: {:na_fill, :test_plan, cols, values}} = df
      # Map ordering may vary, so check contents
      assert length(cols) == 2
      assert Enum.sort(Enum.zip(cols, values)) == [{"age", 0}, {"name", "unknown"}]
    end
  end

  # ── drop/1 ──

  describe "drop/1" do
    test "default drops with how=:any (min_non_nulls=nil)" do
      df = NA.drop(make_df())
      assert %DataFrame{plan: {:na_drop, :test_plan, [], nil}} = df
    end

    test "how: :all sets min_non_nulls=1" do
      df = NA.drop(make_df(), how: :all)
      assert %DataFrame{plan: {:na_drop, :test_plan, [], 1}} = df
    end

    test "thresh overrides how" do
      df = NA.drop(make_df(), how: :all, thresh: 3)
      assert %DataFrame{plan: {:na_drop, :test_plan, [], 3}} = df
    end

    test "with subset" do
      df = NA.drop(make_df(), subset: ["a", "b"])
      assert %DataFrame{plan: {:na_drop, :test_plan, ["a", "b"], nil}} = df
    end

    test "combined options" do
      df = NA.drop(make_df(), how: :all, thresh: 2, subset: ["x"])
      assert %DataFrame{plan: {:na_drop, :test_plan, ["x"], 2}} = df
    end
  end

  # ── replace/2 ──

  describe "replace/2 with map" do
    test "creates replacements from map" do
      df = NA.replace(make_df(), %{0 => 100, -1 => 0})
      assert %DataFrame{plan: {:na_replace, :test_plan, [], replacements}} = df
      assert length(replacements) == 2
      assert Enum.sort(replacements) == [{-1, 0}, {0, 100}]
    end

    test "replace map with subset" do
      df = NA.replace(make_df(), %{"N/A" => "unknown"}, nil, subset: ["name"])
      assert %DataFrame{plan: {:na_replace, :test_plan, ["name"], [{"N/A", "unknown"}]}} = df
    end
  end

  describe "replace/3 with scalar" do
    test "single replacement" do
      df = NA.replace(make_df(), "N/A", nil)
      assert %DataFrame{plan: {:na_replace, :test_plan, [], [{"N/A", nil}]}} = df
    end
  end

  describe "replace/3 with lists" do
    test "parallel lists" do
      df = NA.replace(make_df(), [1, 2], [10, 20])
      assert %DataFrame{plan: {:na_replace, :test_plan, [], [{1, 10}, {2, 20}]}} = df
    end

    test "scalar value expanded for list" do
      df = NA.replace(make_df(), [1, 2, 3], 0)
      assert %DataFrame{plan: {:na_replace, :test_plan, [], [{1, 0}, {2, 0}, {3, 0}]}} = df
    end

    test "raises on mismatched list lengths" do
      assert_raise ArgumentError, ~r/same length/, fn ->
        NA.replace(make_df(), [1, 2], [10])
      end
    end
  end

  # ── DataFrame convenience delegates ──

  describe "DataFrame convenience delegates" do
    test "fillna/2 delegates to NA.fill" do
      df = DataFrame.fillna(make_df(), 0)
      assert %DataFrame{plan: {:na_fill, :test_plan, [], [0]}} = df
    end

    test "dropna/1 delegates to NA.drop" do
      df = DataFrame.dropna(make_df())
      assert %DataFrame{plan: {:na_drop, :test_plan, [], nil}} = df
    end

    test "dropna/2 with options" do
      df = DataFrame.dropna(make_df(), how: :all)
      assert %DataFrame{plan: {:na_drop, :test_plan, [], 1}} = df
    end

    test "replace/3 delegates to NA.replace" do
      df = DataFrame.replace(make_df(), "N/A", nil)
      assert %DataFrame{plan: {:na_replace, :test_plan, [], [{"N/A", nil}]}} = df
    end

    test "replace/2 with map delegates to NA.replace" do
      df = DataFrame.replace(make_df(), %{0 => 100})
      assert %DataFrame{plan: {:na_replace, :test_plan, [], [{0, 100}]}} = df
    end
  end
end
