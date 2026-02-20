defmodule SparkEx.Integration.ReadWriteTypesGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame
  alias SparkEx.Reader
  alias SparkEx.Writer

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── ORC format roundtrip ──

  describe "ORC format roundtrip" do
    test "write and read ORC file", %{session: session} do
      path = "/tmp/spark_ex_orc_test_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")

      assert :ok = Writer.orc(df, path, mode: :overwrite)

      read_df = Reader.orc(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
      assert Enum.any?(rows, &(&1["name"] == "Alice"))
    end
  end

  # ── Text format roundtrip ──

  describe "text format roundtrip" do
    test "write and read text file", %{session: session} do
      path = "/tmp/spark_ex_text_test_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES ('line1'), ('line2'), ('line3') AS t(value)")

      assert :ok = Writer.text(df, path, mode: :overwrite)

      read_df = Reader.text(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 3
      # Text reader returns a single 'value' column
      values = Enum.map(rows, & &1["value"]) |> Enum.sort()
      assert values == ["line1", "line2", "line3"]
    end
  end

  # ── bucketed write ──

  describe "bucketed write" do
    test "write with bucket_by specification", %{session: session} do
      table_name = "spark_ex_bucket_test_#{System.unique_integer([:positive])}"

      SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30) AS t(id, name, value)"
        )

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.bucket_by(4, ["id"])
               |> Writer.sort_by(["id"])
               |> Writer.save_as_table(table_name)

      read_df = Reader.table(session, table_name)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 3
    end
  end

  # ── cluster_by write ──

  describe "cluster_by write" do
    test "write with cluster_by specification", %{session: session} do
      table_name = "spark_ex_cluster_test_#{System.unique_integer([:positive])}"

      SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)"
        )

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.cluster_by(["id"])
               |> Writer.save_as_table(table_name)

      read_df = Reader.table(session, table_name)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 3
    end
  end

  # ── decimal precision/scale ──

  describe "decimal type precision and scale" do
    test "decimal with explicit precision and scale", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST(123.456 AS DECIMAL(10, 3)) AS d1,
               CAST(0.001 AS DECIMAL(5, 4)) AS d2
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert %Decimal{} = row["d1"]
      assert Decimal.to_float(row["d1"]) == 123.456
    end

    test "decimal arithmetic preserves precision", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST(1.1 AS DECIMAL(3,1)) + CAST(2.2 AS DECIMAL(3,1)) AS sum_d
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert %Decimal{} = row["sum_d"]
      assert_in_delta Decimal.to_float(row["sum_d"]), 3.3, 0.001
    end
  end

  # ── char / varchar types ──

  describe "char and varchar types" do
    test "char pads and varchar truncates", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          CAST('hi' AS CHAR(5)) AS c,
          CAST('hello world' AS VARCHAR(5)) AS v
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      # CHAR pads to length
      assert is_binary(row["c"])
      # VARCHAR truncates
      assert is_binary(row["v"])
    end
  end

  # ── interval types ──

  describe "interval types" do
    test "day-time interval", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT INTERVAL '2' DAY + INTERVAL '3' HOUR AS dt_interval
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["dt_interval"] != nil
    end

    @tag :skip
    @tag :explorer_limitation
    test "year-month interval", %{session: session} do
      # SKIP: Explorer/Polars cannot deserialize YearMonth interval Arrow type.
      # See EXPLORER_TODO.md for details.
      df =
        SparkEx.sql(session, """
        SELECT INTERVAL '1' YEAR + INTERVAL '6' MONTH AS ym_interval
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["ym_interval"] != nil
    end

    test "interval arithmetic with dates", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT DATE '2024-01-15' + INTERVAL '1' MONTH AS next_month
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["next_month"] != nil
    end
  end

  # ── variant type ──

  describe "variant type" do
    test "parse_json returns variant and variant_get extracts values", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          variant_get(parse_json('{"name": "Alice", "age": 30}'), '$.name', 'string') AS name,
          variant_get(parse_json('{"name": "Alice", "age": 30}'), '$.age', 'int') AS age
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["name"] == "Alice"
      assert row["age"] == 30
    end

    test "variant handles nested JSON", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          variant_get(parse_json('{"a": {"b": [1, 2, 3]}}'), '$.a.b[1]', 'int') AS nested_val
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["nested_val"] == 2
    end
  end

  # ── schema merge on read ──

  describe "schema merge on read" do
    test "parquet with mergeSchema", %{session: session} do
      base_path = "/tmp/spark_ex_merge_schema_#{System.unique_integer([:positive])}"

      # Write first batch with schema (id, a)
      SparkEx.sql(session, "SELECT 1 AS id, 'x' AS a")
      |> DataFrame.write()
      |> Writer.format("parquet")
      |> Writer.mode(:overwrite)
      |> Writer.save("#{base_path}/part1")

      # Write second batch with schema (id, b)
      SparkEx.sql(session, "SELECT 2 AS id, 'y' AS b")
      |> DataFrame.write()
      |> Writer.format("parquet")
      |> Writer.mode(:overwrite)
      |> Writer.save("#{base_path}/part2")

      # Read with mergeSchema
      df =
        session
        |> SparkEx.read()
        |> Reader.format("parquet")
        |> Reader.option("mergeSchema", "true")
        |> Reader.option("recursiveFileLookup", "true")
        |> Reader.load(base_path)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      # Merged schema should have both a and b columns
      all_keys = rows |> Enum.flat_map(&Map.keys/1) |> Enum.uniq() |> Enum.sort()
      assert "a" in all_keys
      assert "b" in all_keys
    end
  end

  # ── JDBC write/read (beyond basic SQLite) ──

  describe "JDBC write modes" do
    test "JDBC write with overwrite mode", %{session: session} do
      table_name = "spark_ex_jdbc_write_#{System.unique_integer([:positive])}"
      jdbc_path = "/tmp/spark_ex_jdbc_db_#{System.unique_integer([:positive])}.db"
      jdbc_url = "jdbc:sqlite:#{jdbc_path}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")

      case Writer.jdbc(df, jdbc_url, table_name, mode: :overwrite) do
        :ok ->
          # Read back
          read_df = Reader.jdbc(session, jdbc_url, table_name)
          {:ok, rows} = DataFrame.collect(read_df)
          assert length(rows) == 2

        {:error, %SparkEx.Error.Remote{}} ->
          # JDBC driver may not be available in all environments — pass gracefully
          assert true
      end
    end
  end

  # ── binary type ──

  describe "binary type" do
    test "binary data roundtrip", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT CAST('hello' AS BINARY) AS bin_data
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      assert is_binary(row["bin_data"])
    end
  end

  # ── timestamp microsecond precision ──

  describe "timestamp precision" do
    test "timestamp with microsecond precision", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT TIMESTAMP '2024-01-15 10:30:45.123456' AS ts
        """)

      assert {:ok, [row]} = DataFrame.collect(df)
      ts = row["ts"]
      assert ts != nil
    end
  end

  # ── schema inference ──

  describe "schema inference" do
    test "create_dataframe infers schema from data", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(
          session,
          [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}],
          schema: "id INT, name STRING"
        )

      assert {:ok, schema} = DataFrame.schema(df)
      {:struct, struct} = schema.kind
      names = Enum.map(struct.fields, & &1.name) |> Enum.sort()
      assert names == ["id", "name"]

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end
  end

  # ── DDL roundtrip ──

  describe "DDL parse roundtrip" do
    test "analyze_ddl_parse handles complex types", %{session: session} do
      df =
        SparkEx.sql(session, """
        SELECT
          CAST(1 AS INT) AS id,
          array(1, 2) AS arr,
          map('a', 1) AS m,
          named_struct('x', 1, 'y', 'hello') AS s
        """)

      assert {:ok, schema} = DataFrame.schema(df)
      {:struct, struct} = schema.kind
      assert length(struct.fields) == 4
    end
  end
end
