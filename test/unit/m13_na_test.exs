defmodule SparkEx.M13.NATest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.NA

  defp make_df(plan \\ :test_plan) do
    DataFrame.new(self(), plan)
  end

  # ── fill/2 ──

  describe "fill/2 with scalar" do
    test "fills with integer, no subset" do
      df = NA.fill(make_df(), 0)
      assert {:na_fill, :test_plan, [], [0]} = unwrap_plan(df)
    end

    test "fills with float, no subset" do
      df = NA.fill(make_df(), 0.0)
      assert {:na_fill, :test_plan, [], [val]} = unwrap_plan(df)
      assert val == 0.0
    end

    test "fills with string, no subset" do
      df = NA.fill(make_df(), "unknown")
      assert {:na_fill, :test_plan, [], ["unknown"]} = unwrap_plan(df)
    end

    test "fills with boolean, no subset" do
      df = NA.fill(make_df(), false)
      assert {:na_fill, :test_plan, [], [false]} = unwrap_plan(df)
    end

    test "fills with scalar and subset encodes single value (PySpark parity)" do
      df = NA.fill(make_df(), 0, subset: ["age", "salary"])
      # PySpark encodes a single literal value with multiple cols, not duplicated values
      assert {:na_fill, :test_plan, ["age", "salary"], [0]} = unwrap_plan(df)
    end

    test "raises on unsupported scalar value type" do
      assert_raise ArgumentError, ~r/expected fill value/, fn ->
        NA.fill(make_df(), [:invalid])
      end
    end
  end

  describe "fill/2 with map" do
    test "fills with column-specific values" do
      df = NA.fill(make_df(), %{"age" => 0, "name" => "unknown"})

      assert {:na_fill, :test_plan, cols, values} = unwrap_plan(df)
      # Map ordering may vary, so check contents
      assert length(cols) == 2
      assert Enum.sort(Enum.zip(cols, values)) == [{"age", 0}, {"name", "unknown"}]
    end
  end

  # ── drop/1 ──

  describe "drop/1" do
    test "default drops with how=:any (min_non_nulls=nil)" do
      df = NA.drop(make_df())
      assert {:na_drop, :test_plan, [], nil} = unwrap_plan(df)
    end

    test "how: :all sets min_non_nulls=1" do
      df = NA.drop(make_df(), how: :all)
      assert {:na_drop, :test_plan, [], 1} = unwrap_plan(df)
    end

    test "thresh overrides how" do
      df = NA.drop(make_df(), how: :all, thresh: 3)
      assert {:na_drop, :test_plan, [], 3} = unwrap_plan(df)
    end

    test "raises when thresh is not an integer" do
      assert_raise ArgumentError, ~r/expected :thresh to be an integer/, fn ->
        NA.drop(make_df(), thresh: 1.5)
      end
    end

    test "accepts negative thresh (server-side semantics)" do
      df = NA.drop(make_df(), thresh: -1)
      assert {:na_drop, :test_plan, [], -1} = unwrap_plan(df)
    end

    test "with subset" do
      df = NA.drop(make_df(), subset: ["a", "b"])
      assert {:na_drop, :test_plan, ["a", "b"], nil} = unwrap_plan(df)
    end

    test "raises when subset is neither string nor list of strings" do
      assert_raise ArgumentError, ~r/column name \(string or atom\) or a list/, fn ->
        NA.drop(make_df(), subset: 42)
      end
    end

    test "combined options" do
      df = NA.drop(make_df(), how: :all, thresh: 2, subset: ["x"])
      assert {:na_drop, :test_plan, ["x"], 2} = unwrap_plan(df)
    end
  end

  # ── replace/2 ──

  describe "replace/2 with map" do
    test "creates replacements from map" do
      df = NA.replace(make_df(), %{0 => 100, -1 => 0})
      assert {:na_replace, :test_plan, [], replacements} = unwrap_plan(df)
      assert length(replacements) == 2
      assert Enum.sort(replacements) == [{-1, 0}, {0, 100}]
    end

    test "replace map with subset" do
      df = NA.replace(make_df(), %{"N/A" => "unknown"}, nil, subset: ["name"])

      assert {:na_replace, :test_plan, ["name"], [{"N/A", "unknown"}]} = unwrap_plan(df)
    end

    test "replace map supports keyword subset in 3-arg call" do
      df = NA.replace(make_df(), %{1 => 100}, subset: ["a"])

      assert {:na_replace, :test_plan, ["a"], [{1, 100}]} = unwrap_plan(df)
    end
  end

  describe "replace/3 with scalar" do
    test "single replacement" do
      df = NA.replace(make_df(), "N/A", nil)
      assert {:na_replace, :test_plan, [], [{"N/A", nil}]} = unwrap_plan(df)
    end
  end

  describe "replace/3 with lists" do
    test "parallel lists" do
      df = NA.replace(make_df(), [1, 2], [10, 20])
      assert {:na_replace, :test_plan, [], [{1, 10}, {2, 20}]} = unwrap_plan(df)
    end

    test "scalar value expanded for list" do
      df = NA.replace(make_df(), [1, 2, 3], 0)
      assert {:na_replace, :test_plan, [], [{1, 0}, {2, 0}, {3, 0}]} = unwrap_plan(df)
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
      assert {:na_fill, :test_plan, [], [0]} = unwrap_plan(df)
    end

    test "dropna/1 delegates to NA.drop" do
      df = DataFrame.dropna(make_df())
      assert {:na_drop, :test_plan, [], nil} = unwrap_plan(df)
    end

    test "dropna/2 with options" do
      df = DataFrame.dropna(make_df(), how: :all)
      assert {:na_drop, :test_plan, [], 1} = unwrap_plan(df)
    end

    test "replace/3 delegates to NA.replace" do
      df = DataFrame.replace(make_df(), "N/A", nil)
      assert {:na_replace, :test_plan, [], [{"N/A", nil}]} = unwrap_plan(df)
    end

    test "replace/2 with map delegates to NA.replace" do
      df = DataFrame.replace(make_df(), %{0 => 100})
      assert {:na_replace, :test_plan, [], [{0, 100}]} = unwrap_plan(df)
    end

    test "replace/3 treats keyword third argument as opts" do
      df = DataFrame.replace(make_df(), %{"a" => "b"}, subset: ["col1"])

      assert {:na_replace, :test_plan, ["col1"], [{"a", "b"}]} = unwrap_plan(df)
    end

    test "replace raises when subset list contains non-string elements" do
      assert_raise ArgumentError, ~r/column name \(string or atom\) or a list/, fn ->
        DataFrame.replace(make_df(), 1, 2, subset: [123])
      end
    end
  end
end
