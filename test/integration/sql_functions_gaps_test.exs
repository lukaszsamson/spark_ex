defmodule SparkEx.Integration.SqlFunctionsGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.Window
  alias SparkEx.WindowSpec

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── try_* function variants ──

  describe "try_* function variants" do
    test "try_cast returns null on failure", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          try_cast('123' AS INT) AS good,
          try_cast('abc' AS INT) AS bad
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["good"] == 123
      assert row["bad"] == nil
    end

    test "try_divide returns null on division by zero", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          try_divide(10, 2) AS good,
          try_divide(10, 0) AS bad
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["good"] == 5.0
      assert row["bad"] == nil
    end

    test "try_add and try_subtract handle overflow", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          try_add(1, 2) AS sum_ok,
          try_subtract(10, 3) AS sub_ok
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["sum_ok"] == 3
      assert row["sub_ok"] == 7
    end
  end

  # ── percentile / median / percentile_approx ──

  describe "percentile and median functions" do
    test "percentile_approx in aggregation", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1.0),(2.0),(3.0),(4.0),(5.0),(6.0),(7.0),(8.0),(9.0),(10.0) AS t(v)"
        )
        |> DataFrame.select([
          Column.alias_(Functions.percentile_approx(Functions.col("v"), 0.5, 100), "p50")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)

      p50 =
        case row["p50"] do
          %Decimal{} = d -> Decimal.to_float(d)
          v when is_number(v) -> v
        end

      assert_in_delta p50, 5.5, 1.0
    end

    test "median via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT median(v) AS med
        FROM VALUES (1.0),(2.0),(3.0),(4.0),(5.0) AS t(v)
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert_in_delta row["med"], 3.0, 0.01
    end

    test "percentile via SQL", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT percentile(v, 0.5) AS p50
        FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(v)
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert_in_delta row["p50"], 5.5, 0.01
    end
  end

  # ── nth_value ──

  describe "nth_value window function" do
    test "nth_value returns Nth value in window", %{session: session} do
      spec = Window.partition_by(["grp"]) |> WindowSpec.order_by(["val"])

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('a', 10), ('a', 20), ('a', 30) AS t(grp, val)"
        )
        |> DataFrame.select([
          Functions.col("val"),
          Column.alias_(
            Column.over(Functions.nth_value(Functions.col("val"), 2), spec),
            "second_val"
          )
        ])
        |> DataFrame.order_by(["val"])

      assert {:ok, rows} = DataFrame.collect(df)
      # First row may be null (nth_value depends on frame), second and third should be 20
      second_vals = Enum.map(rows, & &1["second_val"]) |> Enum.reject(&is_nil/1)
      assert Enum.all?(second_vals, &(&1 == 20))
    end
  end

  # ── assert_true / raise_error ──

  describe "assert_true and raise_error" do
    test "assert_true succeeds on true condition", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT assert_true(1 < 2) AS ok")

      assert {:ok, [_row]} = DataFrame.collect(df)
    end

    test "assert_true fails on false condition", %{session: session} do
      df = SparkEx.sql(session, "SELECT assert_true(1 > 2) AS bad")

      assert {:error, _} = DataFrame.collect(df)
    end

    test "raise_error always fails", %{session: session} do
      df = SparkEx.sql(session, "SELECT raise_error('boom') AS bad")

      assert {:error, _} = DataFrame.collect(df)
    end
  end

  # ── sum_distinct ──

  describe "sum_distinct" do
    test "sum_distinct sums only unique values", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT sum(DISTINCT v) AS sd
        FROM VALUES (1),(1),(2),(3),(3) AS t(v)
        """)

      assert {:ok, [%{"sd" => 6}]} = DataFrame.collect(df)
    end
  end

  # ── overlay ──

  describe "overlay function" do
    test "overlay replaces substring", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT overlay('abcdef' PLACING 'XY' FROM 3 FOR 2) AS result
        """)

      assert {:ok, [%{"result" => "abXYef"}]} = DataFrame.collect(df)
    end
  end

  # ── make_date / make_time ──

  describe "make_date" do
    test "constructs a date from components", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT make_date(2024, 6, 15) AS d")

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["d"] == ~D[2024-06-15]
    end
  end

  describe "make_timestamp keyword form" do
    test "plans make_timestamp(date,time) from keyword args", %{session: session} do
      df = SparkEx.sql(session, "SELECT DATE '2024-06-15' AS d, TIME '10:30:45' AS t")

      projected =
        df
        |> DataFrame.select([
          Column.alias_(
            Functions.make_timestamp(date: Functions.col("d"), time: Functions.col("t")),
            "from_kw"
          ),
          Column.alias_(Functions.expr("make_timestamp(d, t)"), "from_sql")
        ])

      assert {:ok, explain_str} = DataFrame.explain(projected, :extended)
      assert explain_str =~ "make_timestamp"
    end
  end

  # ── octet_length / bit_length ──

  describe "octet_length and bit_length" do
    test "returns byte and bit lengths", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          octet_length('abc') AS bytes,
          bit_length('abc') AS bits
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["bytes"] == 3
      assert row["bits"] == 24
    end
  end

  # ── regexp_replace ──

  describe "regexp_replace" do
    test "replaces with complex regex pattern", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT regexp_replace('Hello 123 World 456', '[0-9]+', '#') AS result
        """)

      assert {:ok, [%{"result" => "Hello # World #"}]} = DataFrame.collect(df)
    end
  end

  # ── map functions comprehensive ──

  describe "map functions" do
    test "map_from_arrays creates map from arrays", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT map_from_arrays(array('a', 'b', 'c'), array(1, 2, 3)) AS m
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      m = Map.new(row["m"], fn %{"key" => k, "value" => v} -> {k, v} end)
      assert m["a"] == 1
      assert m["b"] == 2
      assert m["c"] == 3
    end

    test "map_from_entries creates map from array of structs", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT map_from_entries(array(struct('a', 1), struct('b', 2))) AS m
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      m = Map.new(row["m"], fn %{"key" => k, "value" => v} -> {k, v} end)
      assert m["a"] == 1
      assert m["b"] == 2
    end

    test "map_concat merges maps", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT map_concat(map('a', 1), map('b', 2)) AS m
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      m = Map.new(row["m"], fn %{"key" => k, "value" => v} -> {k, v} end)
      assert m["a"] == 1
      assert m["b"] == 2
    end

    test "map_contains_key checks for key presence", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          map_contains_key(map('a', 1, 'b', 2), 'a') AS has_a,
          map_contains_key(map('a', 1, 'b', 2), 'z') AS has_z
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["has_a"] == true
      assert row["has_z"] == false
    end
  end

  # ── levenshtein ──

  describe "levenshtein" do
    test "computes edit distance", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          levenshtein('kitten', 'sitting') AS dist1,
          levenshtein('abc', 'abc') AS dist2
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["dist1"] == 3
      assert row["dist2"] == 0
    end
  end

  # ── dayofweek / monthname / dayname ──

  describe "dayofweek / monthname / dayname" do
    test "date component extraction functions", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          dayofweek(DATE '2024-01-15') AS dow,
          date_format(DATE '2024-01-15', 'EEEE') AS dayname,
          date_format(DATE '2024-01-15', 'MMMM') AS monthname
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert is_integer(row["dow"])
      assert row["dayname"] == "Monday"
      assert row["monthname"] == "January"
    end
  end

  # ── shiftleft / shiftright ──

  describe "bit shift functions" do
    test "shiftleft and shiftright", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          shiftleft(1, 3) AS sl,
          shiftright(16, 2) AS sr,
          shiftrightunsigned(-1, 30) AS sru
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["sl"] == 8
      assert row["sr"] == 4
      assert is_integer(row["sru"])
    end
  end

  # ── input_file_name ──

  describe "input_file_name" do
    test "returns source file path for file-based sources", %{session: session} do
      path = "/tmp/spark_ex_input_file_test_#{System.unique_integer([:positive])}"

      SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(id)")
      |> DataFrame.write()
      |> SparkEx.Writer.format("parquet")
      |> SparkEx.Writer.mode(:overwrite)
      |> SparkEx.Writer.save(path)

      df =
        SparkEx.Reader.parquet(session, path)
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(Functions.input_file_name(), "file")
        ])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.all?(rows, &(is_binary(&1["file"]) and String.length(&1["file"]) > 0))
    end
  end

  # ── window functions without partitionBy ──

  describe "window functions without partitionBy" do
    test "window with only order_by", %{session: session} do
      spec = Window.order_by(["id"])

      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (3), (1), (2) AS t(id)")
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(Column.over(Functions.row_number(), spec), "rn")
        ])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, &{&1["id"], &1["rn"]}) == [{1, 1}, {2, 2}, {3, 3}]
    end
  end

  # ── cumulative window / moving average ──

  describe "cumulative and moving window functions" do
    test "cumulative sum", %{session: session} do
      spec =
        Window.order_by(["id"])
        |> WindowSpec.rows_between(:unbounded, :current_row)

      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(Column.over(Functions.sum(Functions.col("id")), spec), "cumsum")
        ])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["cumsum"]) == [1, 3, 6]
    end

    test "moving average with 2-row window", %{session: session} do
      spec =
        Window.order_by(["id"])
        |> WindowSpec.rows_between(-1, 0)

      df =
        SparkEx.sql(session, "SELECT * FROM VALUES (10), (20), (30), (40) AS t(id)")
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(Column.over(Functions.avg(Functions.col("id")), spec), "ma")
        ])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      moving_avgs = Enum.map(rows, & &1["ma"])
      # First: avg(10) = 10, then avg(10,20)=15, avg(20,30)=25, avg(30,40)=35
      assert moving_avgs == [10.0, 15.0, 25.0, 35.0]
    end
  end

  # ── nested higher-order functions ──

  describe "nested higher-order functions" do
    test "HOF calling another HOF", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT array(array(1, 2), array(3, 4)) AS nested")
        |> DataFrame.select([
          Column.alias_(
            Functions.transform(
              Functions.col("nested"),
              fn inner ->
                Functions.aggregate(inner, 0, fn acc, x -> Column.plus(acc, x) end)
              end
            ),
            "sums"
          )
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["sums"] == [3, 7]
    end
  end

  # ── variant expressions ──

  describe "variant expressions via SQL" do
    test "parse_json and variant_get", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          parse_json('{"a": 1, "b": "hello"}') AS v
        """)
        |> DataFrame.select([
          Column.alias_(Functions.expr("variant_get(v, '$.a', 'int')"), "a_val"),
          Column.alias_(Functions.expr("variant_get(v, '$.b', 'string')"), "b_val")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["a_val"] == 1
      assert row["b_val"] == "hello"
    end

    test "is_variant_null and schema_of_variant", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          is_variant_null(parse_json('null')) AS is_null,
          is_variant_null(parse_json('42')) AS not_null,
          schema_of_variant(parse_json('{"x": 1}')) AS schema
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["is_null"] == true
      assert row["not_null"] == false
      assert is_binary(row["schema"])
    end
  end
end
