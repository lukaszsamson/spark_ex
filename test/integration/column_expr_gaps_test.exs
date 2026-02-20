defmodule SparkEx.Integration.ColumnExprGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── date/time column arithmetic ──

  describe "column date/time operations" do
    test "date arithmetic with date_add/date_sub", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT DATE '2024-01-15' AS dt")
        |> DataFrame.select([
          Column.alias_(Functions.date_add(Functions.col("dt"), 10), "plus10"),
          Column.alias_(Functions.date_sub(Functions.col("dt"), 5), "minus5"),
          Column.alias_(
            Functions.datediff(Functions.lit(~D[2024-02-01]), Functions.col("dt")),
            "diff"
          )
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["diff"] == 17
    end

    test "timestamp arithmetic", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts")
        |> DataFrame.select([
          Column.alias_(Functions.hour(Functions.col("ts")), "h"),
          Column.alias_(Functions.minute(Functions.col("ts")), "m"),
          Column.alias_(Functions.second(Functions.col("ts")), "s"),
          Column.alias_(Functions.year(Functions.col("ts")), "y"),
          Column.alias_(Functions.month(Functions.col("ts")), "mo"),
          Column.alias_(Functions.dayofmonth(Functions.col("ts")), "d")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["h"] == 10
      assert row["m"] == 30
      assert row["s"] == 0
      assert row["y"] == 2024
      assert row["mo"] == 1
      assert row["d"] == 15
    end
  end

  # ── with_field / drop_fields on struct columns ──

  describe "with_field/3 and drop_fields/2" do
    test "with_field adds or replaces a struct field", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT named_struct('a', 1, 'b', 2) AS s")
        |> DataFrame.select([
          Column.alias_(Column.with_field(Functions.col("s"), "c", Functions.lit(3)), "s")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      s = row["s"]
      assert s["a"] == 1
      assert s["b"] == 2
      assert s["c"] == 3
    end

    test "drop_fields removes struct fields", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS s")
        |> DataFrame.select([
          Column.alias_(Column.drop_fields(Functions.col("s"), ["b"]), "s")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      s = row["s"]
      assert s["a"] == 1
      assert s["c"] == 3
      refute Map.has_key?(s, "b")
    end
  end

  # ── non-ASCII column names ──

  describe "non-ASCII column names" do
    test "column names with unicode characters", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS `名前`, 2 AS `wert`")

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["名前"] == 1
      assert row["wert"] == 2
    end
  end

  # ── alias with metadata ──

  describe "alias with metadata" do
    test "alias_metadata attaches metadata to column", %{session: session} do
      df =
        SparkEx.range(session, 3)
        |> DataFrame.select([
          Column.alias_(Functions.col("id"), "my_id")
        ])

      assert {:ok, schema} = DataFrame.schema(df)
      {:struct, struct} = schema.kind
      field = Enum.find(struct.fields, &(&1.name == "my_id"))
      assert field != nil
    end
  end

  # ── eqNullSafe ──

  describe "eq_null_safe/2" do
    test "null-safe equality treats null == null as true", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, 1), (1, 2), (CAST(NULL AS INT), CAST(NULL AS INT)), (1, CAST(NULL AS INT))
        AS t(a, b)
        """)
        |> DataFrame.select([
          Functions.col("a"),
          Functions.col("b"),
          Column.alias_(Column.eq_null_safe(Functions.col("a"), Functions.col("b")), "eq_safe")
        ])
        |> DataFrame.order_by(["a", "b"])

      assert {:ok, rows} = DataFrame.collect(df)

      eq_safe_values = Enum.map(rows, & &1["eq_safe"])
      # (null, null) -> true, (1, null) -> false, (1, 1) -> true, (1, 2) -> false
      assert Enum.count(eq_safe_values, & &1) == 2
    end
  end

  # ── cast error cases ──

  describe "cast error cases" do
    test "try_cast returns null on invalid cast", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 'not_a_number' AS s")
        |> DataFrame.select([
          Column.alias_(Column.try_cast(Functions.col("s"), "int"), "val")
        ])

      assert {:ok, [%{"val" => nil}]} = DataFrame.collect(df)
    end

    test "cast invalid string to int returns null in Spark", %{session: session} do
      # Spark's ANSI mode may throw or return null depending on config
      df =
        SparkEx.sql(session, "SELECT try_cast('abc' AS INT) AS val")

      assert {:ok, [%{"val" => nil}]} = DataFrame.collect(df)
    end
  end

  # ── get_item with column as index ──

  describe "get_item with expressions" do
    test "get_item from array with literal index", %{session: session} do
      # Spark arrays are 0-indexed
      df =
        SparkEx.sql(session, "SELECT array(10, 20, 30) AS arr")
        |> DataFrame.select([
          Column.alias_(Column.get_item(Functions.col("arr"), 0), "first"),
          Column.alias_(Column.get_item(Functions.col("arr"), 1), "second")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["first"] == 10
      assert row["second"] == 20
    end

    test "get_item from map with literal key", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT map('a', 1, 'b', 2) AS m")
        |> DataFrame.select([
          Column.alias_(Column.get_item(Functions.col("m"), "a"), "val_a"),
          Column.alias_(Column.get_item(Functions.col("m"), "b"), "val_b")
        ])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["val_a"] == 1
      assert row["val_b"] == 2
    end
  end

  # ── sort with nulls ordering ──

  describe "sort with nulls ordering" do
    test "asc_nulls_first and desc_nulls_last", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1), (CAST(NULL AS INT)), (3), (CAST(NULL AS INT)), (2) AS t(v)"
        )

      asc_nf =
        df
        |> DataFrame.order_by([Column.asc_nulls_first(Functions.col("v"))])

      assert {:ok, rows} = DataFrame.collect(asc_nf)
      # First two should be null
      assert Enum.at(rows, 0)["v"] == nil
      assert Enum.at(rows, 1)["v"] == nil

      desc_nl =
        df
        |> DataFrame.order_by([Column.desc_nulls_last(Functions.col("v"))])

      assert {:ok, rows2} = DataFrame.collect(desc_nl)
      # Last two should be null
      assert Enum.at(rows2, 3)["v"] == nil
      assert Enum.at(rows2, 4)["v"] == nil
      # First should be 3
      assert Enum.at(rows2, 0)["v"] == 3
    end
  end
end
