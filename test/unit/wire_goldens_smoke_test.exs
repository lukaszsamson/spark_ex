defmodule SparkEx.Test.WireGoldensSmokeTest do
  @moduledoc """
  Sanity tests for `SparkEx.Test.WireGoldens`.

  Stream A (BUGS_PLAN_5) will add many more goldens. This file only verifies
  the helper itself: encode → fixture round-trip + canonicalization of
  lambda variable names so that goldens are stable across runs.
  """

  use ExUnit.Case, async: true

  alias SparkEx.Test.WireGoldens
  alias SparkEx.{DataFrame, Functions, Column}

  describe "encode_canonical/2" do
    test "produces deterministic bytes for SQL relation" do
      {b1, _} = WireGoldens.encode_canonical({:sql, "SELECT 1", nil})
      {b2, _} = WireGoldens.encode_canonical({:sql, "SELECT 1", nil})
      assert b1 == b2
      assert byte_size(b1) > 0
    end

    test "canonicalizes lambda variable names so HOF plans are stable" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      # Two independent builds use different `unique_integer` suffixes.
      build = fn ->
        col =
          Functions.transform(DataFrame.col(df, "arr"), fn x ->
            Column.plus(x, Functions.lit(1))
          end)

        DataFrame.select(df, [col]).plan
      end

      {b1, _} = WireGoldens.encode_canonical(build.())
      {b2, _} = WireGoldens.encode_canonical(build.())

      assert b1 == b2
    end
  end

  describe "assert_golden/2" do
    @golden_name "smoke/sql_select_1"

    test "creates fixture on first run, asserts on second" do
      path = WireGoldens.fixture_path(@golden_name)
      File.rm_rf!(Path.dirname(path))

      # First run: fixture missing → flunks with "golden created".
      assert_raise ExUnit.AssertionError, ~r/golden created/, fn ->
        WireGoldens.__assert_golden__(@golden_name, {:sql, "SELECT 1", nil})
      end

      assert File.exists?(path)

      # Second run: matches.
      assert {:ok, _plan} =
               WireGoldens.__assert_golden__(@golden_name, {:sql, "SELECT 1", nil})

      # Divergent plan flunks with diff.
      assert_raise ExUnit.AssertionError, ~r/wire bytes diverged/, fn ->
        WireGoldens.__assert_golden__(@golden_name, {:sql, "SELECT 2", nil})
      end

      File.rm_rf!(Path.dirname(path))
    end
  end
end
