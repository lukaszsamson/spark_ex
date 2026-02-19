defmodule SparkEx.Integration.M13.NAStatTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.{NA, Stat}

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── NA.fill ──

  describe "NA.fill" do
    test "fills null integers with scalar", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 10), (2, CAST(NULL AS INT)), (3, 30) AS t(id, value)"
        )

      filled = NA.fill(df, 0)
      {:ok, rows} = DataFrame.collect(filled)
      values = Enum.map(rows, & &1["value"])
      assert Enum.sort(values) == [0, 10, 30]
    end

    test "fills null strings with scalar", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('Alice', 'eng'), ('Bob', CAST(NULL AS STRING)) AS t(name, dept)"
        )

      filled = NA.fill(df, "unknown")
      {:ok, rows} = DataFrame.collect(filled)
      depts = Enum.map(rows, & &1["dept"]) |> Enum.sort()
      assert depts == ["eng", "unknown"]
    end

    test "fills with map of column-specific values", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, CAST(NULL AS INT), 'a'),
          (2, 20, CAST(NULL AS STRING))
        AS t(id, score, grade)
        """)

      filled = NA.fill(df, %{"score" => 0, "grade" => "F"})
      {:ok, rows} = DataFrame.collect(filled)

      row1 = Enum.find(rows, &(&1["id"] == 1))
      row2 = Enum.find(rows, &(&1["id"] == 2))

      assert row1["score"] == 0
      assert row2["grade"] == "F"
    end

    test "fills with subset restriction", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (CAST(NULL AS INT), CAST(NULL AS INT))
        AS t(a, b)
        """)

      filled = NA.fill(df, 99, subset: ["a"])
      {:ok, [row]} = DataFrame.collect(filled)

      assert row["a"] == 99
      # b should still be null
      assert row["b"] == nil
    end

    test "DataFrame.fillna/2 convenience delegate", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (CAST(NULL AS INT)), (42) AS t(x)"
        )

      filled = DataFrame.fillna(df, 0)
      {:ok, rows} = DataFrame.collect(filled)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [0, 42]
    end
  end

  # ── NA.drop ──

  describe "NA.drop" do
    test "drops rows with any null (default)", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, 'a'),
          (2, CAST(NULL AS STRING)),
          (CAST(NULL AS INT), 'c')
        AS t(id, name)
        """)

      dropped = NA.drop(df)
      {:ok, rows} = DataFrame.collect(dropped)
      assert length(rows) == 1
      assert hd(rows)["id"] == 1
    end

    test "drops rows with all null (how: :all)", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, CAST(NULL AS STRING)),
          (CAST(NULL AS INT), CAST(NULL AS STRING)),
          (3, 'c')
        AS t(id, name)
        """)

      dropped = NA.drop(df, how: :all)
      {:ok, rows} = DataFrame.collect(dropped)
      # Only the row with both nulls should be dropped
      assert length(rows) == 2
    end

    test "drops with thresh", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, 'a', 100),
          (2, CAST(NULL AS STRING), CAST(NULL AS INT)),
          (CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS INT))
        AS t(id, name, score)
        """)

      # thresh=2 means keep rows with at least 2 non-null values
      # Row 1: 3 non-null (keep), Row 2: 1 non-null (drop), Row 3: 0 non-null (drop)
      dropped = NA.drop(df, thresh: 2)
      {:ok, rows} = DataFrame.collect(dropped)
      assert length(rows) == 1
    end

    test "DataFrame.dropna/1 convenience delegate", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1, 'a'), (2, CAST(NULL AS STRING)) AS t(id, name)
        """)

      dropped = DataFrame.dropna(df)
      {:ok, rows} = DataFrame.collect(dropped)
      assert length(rows) == 1
    end
  end

  # ── NA.replace ──

  describe "NA.replace" do
    test "replaces single value", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2), (3) AS t(x)"
        )

      replaced = NA.replace(df, 2, 20)
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [1, 3, 20]
    end

    test "replaces with map", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2), (3) AS t(x)"
        )

      replaced = NA.replace(df, %{1 => 10, 3 => 30})
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [2, 10, 30]
    end

    test "replaces with parallel lists", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('a'), ('b'), ('c') AS t(letter)"
        )

      replaced = NA.replace(df, ["a", "c"], ["x", "z"])
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["letter"]) |> Enum.sort()
      assert values == ["b", "x", "z"]
    end

    test "DataFrame.replace/3 convenience delegate", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (2) AS t(x)"
        )

      replaced = DataFrame.replace(df, 1, 100)
      {:ok, rows} = DataFrame.collect(replaced)
      values = Enum.map(rows, & &1["x"]) |> Enum.sort()
      assert values == [2, 100]
    end
  end

  # ── Stat.describe ──

  describe "Stat.describe" do
    test "computes basic statistics", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e') AS t(num, letter)
        """)

      desc = Stat.describe(df)
      {:ok, rows} = DataFrame.collect(desc)

      summaries = Map.new(rows, &{&1["summary"], &1})
      assert summaries["count"]["num"] == "5"
      assert summaries["min"]["num"] == "1"
      assert summaries["max"]["num"] == "5"
      assert summaries["mean"]["num"] == "3.0"
    end

    test "describe with specific columns", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id, score)
        """)

      desc = Stat.describe(df, ["score"])
      {:ok, rows} = DataFrame.collect(desc)
      summaries = Map.new(rows, &{&1["summary"], &1})
      assert summaries["count"]["score"] == "3"
      assert summaries["min"]["score"] == "10"
      assert summaries["max"]["score"] == "30"
    end

    test "DataFrame.describe/1 convenience delegate", %{session: session} do
      df = SparkEx.range(session, 10)
      desc = DataFrame.describe(df)
      {:ok, rows} = DataFrame.collect(desc)
      assert length(rows) > 0
    end
  end

  # ── Stat.summary ──

  describe "Stat.summary" do
    test "computes summary statistics", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(value)
        """)

      summ = Stat.summary(df, ["count", "min", "max"])
      {:ok, rows} = DataFrame.collect(summ)

      summaries = Map.new(rows, &{&1["summary"], &1})
      assert summaries["count"]["value"] == "5"
      assert summaries["min"]["value"] == "1"
      assert summaries["max"]["value"] == "5"
    end
  end

  # ── Stat.corr ──

  describe "Stat.corr" do
    test "computes Pearson correlation", %{session: session} do
      # Perfect positive correlation: y = 2*x
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0) AS t(x, y)
        """)

      assert {:ok, r} = Stat.corr(df, "x", "y")
      assert is_float(r)
      assert_in_delta r, 1.0, 0.001
    end
  end

  # ── Stat.cov ──

  describe "Stat.cov" do
    test "computes sample covariance", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0) AS t(x, y)
        """)

      assert {:ok, c} = Stat.cov(df, "x", "y")
      assert is_float(c)
      assert c > 0
    end
  end

  # ── Stat.crosstab ──

  describe "Stat.crosstab" do
    test "computes contingency table", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          ('eng', 'M'), ('eng', 'F'), ('eng', 'M'),
          ('hr', 'F'), ('hr', 'F')
        AS t(dept, gender)
        """)

      ct = Stat.crosstab(df, "dept", "gender")
      {:ok, rows} = DataFrame.collect(ct)
      assert length(rows) > 0
      # The crosstab produces a row per dept with columns for each gender value
    end
  end

  # ── Stat.freq_items ──

  describe "Stat.freq_items" do
    test "finds frequent items", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          ('a'), ('a'), ('a'), ('b'), ('b'), ('c'),
          ('a'), ('a'), ('b'), ('a')
        AS t(item)
        """)

      fi = Stat.freq_items(df, ["item"], 0.3)
      {:ok, rows} = DataFrame.collect(fi)
      assert length(rows) > 0
    end
  end

  # ── Stat.approx_quantile ──

  describe "Stat.approx_quantile" do
    test "computes approximate quantiles for single column", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST(id AS DOUBLE) AS value FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10) AS t(id)
        """)

      assert {:ok, quantiles} = Stat.approx_quantile(df, "value", [0.0, 0.5, 1.0])
      assert is_list(quantiles)
      assert length(quantiles) == 3
      # Min should be ~1.0, median ~5.5, max ~10.0
      [q0, q50, q100] = quantiles
      assert_in_delta q0, 1.0, 0.001
      assert_in_delta q50, 5.5, 0.5
      assert_in_delta q100, 10.0, 0.001
    end

    test "computes approximate quantiles for multiple columns", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST(id AS DOUBLE) AS a, CAST(id * 10 AS DOUBLE) AS b
        FROM VALUES (1),(2),(3),(4),(5) AS t(id)
        """)

      assert {:ok, result} = Stat.approx_quantile(df, ["a", "b"], [0.5])
      assert is_list(result)
      assert length(result) == 2

      [a_quantiles, b_quantiles] = result
      assert is_list(a_quantiles)
      assert is_list(b_quantiles)
      # b values are 10x a values
      [a_median] = a_quantiles
      [b_median] = b_quantiles
      assert b_median > a_median
    end
  end

  # ── Stat.sample_by ──

  describe "Stat.sample_by" do
    test "stratified sampling", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          ('a'), ('a'), ('a'), ('a'), ('a'),
          ('b'), ('b'), ('b'), ('b'), ('b'),
          ('b'), ('b'), ('b'), ('b'), ('b')
        AS t(label)
        """)

      sampled = Stat.sample_by(df, "label", %{"a" => 1.0, "b" => 0.2}, 42)
      {:ok, rows} = DataFrame.collect(sampled)
      # All 'a' rows should be present (fraction=1.0), 'b' rows should be fewer
      a_count = Enum.count(rows, &(&1["label"] == "a"))
      assert a_count == 5
    end
  end
end
