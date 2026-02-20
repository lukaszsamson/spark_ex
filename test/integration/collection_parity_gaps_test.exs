defmodule SparkEx.Integration.CollectionParityGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Empty-result semantics for head/first ──

  describe "empty-result head/first" do
    test "head on empty DataFrame returns empty list", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS id WHERE 1 = 0")
      assert {:ok, []} = DataFrame.head(df, 5)
    end

    test "head(1) returns single row", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, [row]} = DataFrame.head(df, 1)
      assert is_map(row)
    end

    test "first on empty DataFrame returns nil or error", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS id WHERE 1 = 0")
      result = DataFrame.first(df)

      case result do
        {:ok, nil} -> assert true
        {:error, _} -> assert true
        {:ok, row} when is_map(row) -> flunk("expected nil or error for empty DF, got: #{inspect(row)}")
      end
    end

    test "take(0) returns empty list", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, []} = DataFrame.take(df, 0)
    end
  end

  # ── Timestamp collection parity ──

  describe "timestamp collection parity" do
    test "collects timestamp values with correct type", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          TIMESTAMP '2024-06-15 10:30:45' AS ts,
          TIMESTAMP '2024-06-15 10:30:45.123456' AS ts_micro,
          DATE '2024-06-15' AS dt
        """)

      assert {:ok, [row]} = DataFrame.collect(df)

      # Timestamp should come back as DateTime or NaiveDateTime
      ts = row["ts"]
      assert ts != nil
      # Verify at least date component is present
      assert is_struct(ts) or is_binary(ts)

      # Date should come back as Date
      dt = row["dt"]
      assert dt == ~D[2024-06-15]
    end

    test "collects timestamp_ntz values", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST('2024-06-15 10:30:45' AS TIMESTAMP_NTZ) AS ts_ntz
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["ts_ntz"] != nil
    end

    test "collects current_timestamp()", %{session: session} do
      df = SparkEx.sql(session, "SELECT current_timestamp() AS now")
      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["now"] != nil
    end
  end

  # ── Binary payload collection parity ──

  describe "binary payload collection" do
    test "collects binary data", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST('hello' AS BINARY) AS bin
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert is_binary(row["bin"])
    end

    test "collects empty binary", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST('' AS BINARY) AS bin
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["bin"] == "" or row["bin"] == <<>>
    end

    test "collects binary with special bytes", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT X'DEADBEEF' AS hex_bin
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert is_binary(row["hex_bin"])
      assert byte_size(row["hex_bin"]) == 4
    end
  end

  # ── Null collection semantics ──

  describe "null collection semantics" do
    test "collects all-null row", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST(NULL AS INT) AS a, CAST(NULL AS STRING) AS b, CAST(NULL AS DOUBLE) AS c
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["a"] == nil
      assert row["b"] == nil
      assert row["c"] == nil
    end

    test "collects mixed null and non-null", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES
          (1, 'a', 1.0),
          (CAST(NULL AS INT), 'b', CAST(NULL AS DOUBLE)),
          (3, CAST(NULL AS STRING), 3.0)
        AS t(i, s, d)
        ORDER BY i NULLS FIRST
        """)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3

      null_row = Enum.find(rows, &is_nil(&1["i"]))
      assert null_row["s"] == "b"
      assert null_row["d"] == nil
    end
  end

  # ── Large result collection ──

  describe "large result collection" do
    test "collect handles many rows", %{session: session} do
      df = SparkEx.range(session, 10_000)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 10_000
    end

    test "collect handles wide rows", %{session: session} do
      # Generate a query with many columns
      cols = Enum.map_join(1..50, ", ", fn i -> "#{i} AS c#{i}" end)
      df = SparkEx.sql(session, "SELECT #{cols}")

      assert {:ok, [row]} = DataFrame.collect(df)
      assert map_size(row) == 50
      assert row["c1"] == 1
      assert row["c50"] == 50
    end
  end
end
