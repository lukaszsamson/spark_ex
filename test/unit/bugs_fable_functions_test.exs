defmodule SparkEx.BugsFableFunctionsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  # ── FABLE-01: ltrim/rtrim/trim arg order with trim_string ──────────────────
  # PySpark sends (trim_string, col) — trim string FIRST — matching Spark SQL's
  # ltrim(trimStr, srcStr) constructor. Previously spark_ex sent (col, trim_string).

  describe "ltrim/2 arg order (FABLE-01)" do
    test "no trim_string sends only col" do
      result = Functions.ltrim(Functions.col("s"))
      assert %Column{expr: {:fn, "ltrim", [{:col, "s"}], false}} = result
    end

    test "with trim_string sends (trim_string, col)" do
      result = Functions.ltrim(Functions.col("s"), Functions.lit("-"))
      # trim string FIRST, then src col
      assert %Column{
               expr: {:fn, "ltrim", [{:lit, "-"}, {:col, "s"}], false}
             } = result
    end

    test "with bare string trim_string resolves to column ref (not literal)" do
      result = Functions.ltrim(Functions.col("s"), "chars")

      assert %Column{
               expr: {:fn, "ltrim", [{:col, "chars"}, {:col, "s"}], false}
             } = result
    end
  end

  describe "rtrim/2 arg order (FABLE-01)" do
    test "no trim_string sends only col" do
      result = Functions.rtrim(Functions.col("s"))
      assert %Column{expr: {:fn, "rtrim", [{:col, "s"}], false}} = result
    end

    test "with trim_string sends (trim_string, col)" do
      result = Functions.rtrim(Functions.col("s"), Functions.lit("-"))
      # trim string FIRST, then src col
      assert %Column{
               expr: {:fn, "rtrim", [{:lit, "-"}, {:col, "s"}], false}
             } = result
    end
  end

  describe "trim/2 arg order (FABLE-01)" do
    test "no trim_string sends only col" do
      result = Functions.trim(Functions.col("s"))
      assert %Column{expr: {:fn, "trim", [{:col, "s"}], false}} = result
    end

    test "with trim_string sends (trim_string, col)" do
      result = Functions.trim(Functions.col("s"), Functions.lit("-"))
      # trim string FIRST, then src col
      assert %Column{
               expr: {:fn, "trim", [{:lit, "-"}, {:col, "s"}], false}
             } = result
    end
  end

  describe "btrim/2 arg order unchanged (FABLE-01)" do
    test "btrim without trim_string sends only col" do
      result = Functions.btrim(Functions.col("s"))
      assert %Column{expr: {:fn, "btrim", [{:col, "s"}], false}} = result
    end

    test "btrim with trim_string sends (col, trim_string) — btrim is correct as-is" do
      result = Functions.btrim(Functions.col("s"), Functions.lit("-"))

      assert %Column{
               expr: {:fn, "btrim", [{:col, "s"}, {:lit, "-"}], false}
             } = result
    end
  end

  # ── FABLE-02: mask always sends all 5 args with PySpark defaults ───────────
  # PySpark sends: mask(col, lit("X"), lit("x"), lit("n"), lit(None))
  # Previously spark_ex omitted args with nil/default values, silently leaving
  # characters unmasked.

  describe "mask/2 PySpark defaults (FABLE-02)" do
    test "mask with no options sends all 5 args with PySpark defaults" do
      result = Functions.mask(Functions.col("email"))

      assert %Column{
               expr:
                 {:fn, "mask",
                  [
                    {:col, "email"},
                    {:lit, "X"},
                    {:lit, "x"},
                    {:lit, "n"},
                    {:lit, nil}
                  ], false}
             } = result
    end

    test "mask with only digit_char still sends all 5 args" do
      result = Functions.mask(Functions.col("phone"), digit_char: "0")

      assert %Column{
               expr:
                 {:fn, "mask",
                  [
                    {:col, "phone"},
                    {:lit, "X"},
                    {:lit, "x"},
                    {:lit, "0"},
                    {:lit, nil}
                  ], false}
             } = result
    end

    test "mask with all opts explicit sends all 5 args" do
      result =
        Functions.mask(Functions.col("s"),
          upper_char: "A",
          lower_char: "a",
          digit_char: "1",
          other_char: "-"
        )

      assert %Column{
               expr:
                 {:fn, "mask",
                  [
                    {:col, "s"},
                    {:lit, "A"},
                    {:lit, "a"},
                    {:lit, "1"},
                    {:lit, "-"}
                  ], false}
             } = result
    end

    test "mask shorthand (non-list second arg) sets upper_char" do
      result = Functions.mask(Functions.col("s"), "Y")

      assert %Column{
               expr:
                 {:fn, "mask",
                  [
                    {:col, "s"},
                    {:lit, "Y"},
                    {:lit, "x"},
                    {:lit, "n"},
                    {:lit, nil}
                  ], false}
             } = result
    end

    test "mask with explicit other_char nil still sends lit(nil)" do
      result = Functions.mask(Functions.col("s"), other_char: nil)

      assert %Column{
               expr:
                 {:fn, "mask",
                  [
                    {:col, "s"},
                    {:lit, "X"},
                    {:lit, "x"},
                    {:lit, "n"},
                    {:lit, nil}
                  ], false}
             } = result
    end
  end

  # ── FABLE-24: count_min_sketch/3 auto-generates seed ─────────────────────
  # PySpark always sends 4 args, generating a random seed client-side.
  # The server (CountMinSketchAgg.scala:217-229) requires exactly 4 expressions.
  # Previously the 3-arity form sent only 3 args, causing an analysis error.

  describe "count_min_sketch/3 auto-seed (FABLE-24)" do
    test "count_min_sketch/3 sends 4 args including an auto-generated integer seed" do
      result = Functions.count_min_sketch(Functions.col("id"), 0.01, 0.95)

      assert %Column{
               expr:
                 {:fn, "count_min_sketch",
                  [
                    {:col, "id"},
                    {:lit, eps},
                    {:lit, conf},
                    {:lit, seed}
                  ], false}
             } = result

      assert eps == 0.01
      assert conf == 0.95
      assert is_integer(seed)
    end

    test "count_min_sketch/3 generates an integer seed" do
      result = Functions.count_min_sketch(Functions.col("id"), 0.01, 0.95)
      assert %Column{expr: {:fn, "count_min_sketch", [_, _, _, {:lit, seed}], false}} = result
      assert is_integer(seed)
    end

    test "count_min_sketch/4 sends explicit seed unchanged" do
      result = Functions.count_min_sketch(Functions.col("id"), 0.01, 0.95, 42)

      assert %Column{
               expr:
                 {:fn, "count_min_sketch",
                  [
                    {:col, "id"},
                    {:lit, 0.01},
                    {:lit, 0.95},
                    {:lit, 42}
                  ], false}
             } = result
    end

    test "count_min_sketch/3 two calls produce different seeds (probabilistically)" do
      %Column{expr: {:fn, _, [_, _, _, {:lit, seed1}], _}} =
        Functions.count_min_sketch(Functions.col("id"), 0.01, 0.95)

      %Column{expr: {:fn, _, [_, _, _, {:lit, seed2}], _}} =
        Functions.count_min_sketch(Functions.col("id"), 0.01, 0.95)

      # Seeds should be different (probabilistically true for long random ints)
      refute seed1 == seed2
    end
  end
end
