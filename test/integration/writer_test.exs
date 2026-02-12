defmodule SparkEx.Integration.WriterTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame
  alias SparkEx.Writer
  alias SparkEx.Reader

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "write parquet roundtrip" do
    test "write and read back parquet file", %{session: session} do
      # Create source data via SQL
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")

      # Write to a temp path
      path = "/tmp/spark_ex_writer_test_#{System.unique_integer([:positive])}"

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.save(path)

      # Read back and verify
      read_df = Reader.parquet(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
      assert Enum.any?(rows, fn row -> row["name"] == "Alice" end)
      assert Enum.any?(rows, fn row -> row["id"] == 2 end)
    end

    test "write parquet convenience function", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM VALUES (10, 20), (30, 40) AS t(a, b)")
      path = "/tmp/spark_ex_parquet_conv_#{System.unique_integer([:positive])}"

      assert :ok = Writer.parquet(df, path, mode: :overwrite)

      read_df = Reader.parquet(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end
  end

  describe "write CSV roundtrip" do
    test "write and read back CSV file", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")
      path = "/tmp/spark_ex_csv_test_#{System.unique_integer([:positive])}"

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("csv")
               |> Writer.mode(:overwrite)
               |> Writer.option("header", "true")
               |> Writer.save(path)

      read_df = Reader.csv(session, path, header: true, infer_schema: true)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "write CSV convenience function", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM VALUES ('hello'), ('world') AS t(msg)")
      path = "/tmp/spark_ex_csv_conv_#{System.unique_integer([:positive])}"

      assert :ok = Writer.csv(df, path, mode: :overwrite, header: true)

      read_df = Reader.csv(session, path, header: true, infer_schema: true)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end
  end

  describe "write JSON roundtrip" do
    test "write and read back JSON file", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(x, y)")
      path = "/tmp/spark_ex_json_test_#{System.unique_integer([:positive])}"

      assert :ok = Writer.json(df, path, mode: :overwrite)

      read_df = Reader.json(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end
  end

  describe "write with transform pipeline" do
    test "read -> filter -> write -> read back", %{session: session} do
      import SparkEx.Functions, only: [col: 1, lit: 1]

      # Create source data
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50) AS t(id, value)"
        )

      # Filter
      filtered = DataFrame.filter(df, col("value") |> SparkEx.Column.gt(lit(25)))

      # Write
      path = "/tmp/spark_ex_pipeline_test_#{System.unique_integer([:positive])}"

      assert :ok =
               filtered
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.save(path)

      # Read back
      read_df = Reader.parquet(session, path)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 3
      assert Enum.all?(rows, fn row -> row["value"] > 25 end)
    end
  end

  describe "save modes" do
    test "overwrite replaces existing data", %{session: session} do
      path = "/tmp/spark_ex_mode_test_#{System.unique_integer([:positive])}"

      # Write first batch
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(x)")
      assert :ok = Writer.parquet(df1, path, mode: :overwrite)

      # Overwrite with second batch
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (10), (20), (30) AS t(x)")
      assert :ok = Writer.parquet(df2, path, mode: :overwrite)

      # Read back — should be the second batch
      {:ok, rows} = Reader.parquet(session, path) |> DataFrame.collect()
      assert length(rows) == 3
    end

    test "append adds to existing data", %{session: session} do
      path = "/tmp/spark_ex_append_test_#{System.unique_integer([:positive])}"

      # Write first batch
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(x)")
      assert :ok = Writer.parquet(df1, path, mode: :overwrite)

      # Append second batch
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (3), (4) AS t(x)")
      assert :ok = Writer.parquet(df2, path, mode: :append)

      # Read back — should be all 4 rows
      {:ok, rows} = Reader.parquet(session, path) |> DataFrame.collect()
      assert length(rows) == 4
    end

    test "error_if_exists fails on existing path", %{session: session} do
      path = "/tmp/spark_ex_error_test_#{System.unique_integer([:positive])}"

      # Write first time
      df = SparkEx.sql(session, "SELECT 1 AS x")
      assert :ok = Writer.parquet(df, path, mode: :overwrite)

      # Second time with error_if_exists should fail
      assert {:error, _} = Writer.parquet(df, path, mode: :error_if_exists)
    end

    test "ignore silently skips on existing path", %{session: session} do
      path = "/tmp/spark_ex_ignore_test_#{System.unique_integer([:positive])}"

      # Write first time
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1) AS t(x)")
      assert :ok = Writer.parquet(df1, path, mode: :overwrite)

      # Second time with ignore — should succeed but not change data
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (99) AS t(x)")
      assert :ok = Writer.parquet(df2, path, mode: :ignore)

      # Read back — should still be original data
      {:ok, rows} = Reader.parquet(session, path) |> DataFrame.collect()
      assert length(rows) == 1
      assert hd(rows)["x"] == 1
    end
  end

  describe "save_as_table" do
    test "writes data to a table and reads back", %{session: session} do
      table_name = "spark_ex_test_table_#{System.unique_integer([:positive])}"

      SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.save_as_table(table_name)

      # Read back via table reader
      read_df = Reader.table(session, table_name)
      {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end
  end

  describe "insert_into semantics" do
    test "default insert_into appends rows", %{session: session} do
      table_name = "spark_ex_insert_default_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      df = SparkEx.sql(session, "SELECT * FROM VALUES ('a', 1), ('b', 2) AS t(c1, c2)")

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.save_as_table(table_name)

      assert :ok = df |> DataFrame.write() |> Writer.insert_into(table_name)
      assert {:ok, 4} = SparkEx.sql(session, "SELECT * FROM #{table_name}") |> DataFrame.count()
    end

    test "insert_into supports explicit overwrite boolean", %{session: session} do
      table_name = "spark_ex_insert_overwrite_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      seed_df = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(x)")
      new_df = SparkEx.sql(session, "SELECT * FROM VALUES (10), (20) AS t(x)")

      assert :ok =
               seed_df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.save_as_table(table_name)

      assert :ok = new_df |> DataFrame.write() |> Writer.insert_into(table_name, true)
      assert {:ok, 2} = SparkEx.sql(session, "SELECT * FROM #{table_name}") |> DataFrame.count()
    end
  end

  describe "partition_by" do
    test "writes partitioned parquet", %{session: session} do
      path = "/tmp/spark_ex_partition_test_#{System.unique_integer([:positive])}"

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30) AS t(id, group_col, value)"
        )

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.format("parquet")
               |> Writer.mode(:overwrite)
               |> Writer.partition_by(["group_col"])
               |> Writer.save(path)

      # Read back
      {:ok, rows} = Reader.parquet(session, path) |> DataFrame.collect()
      assert length(rows) == 3
    end
  end

  describe "reader builder options" do
    test "read.format/schema/options/load roundtrip", %{session: session} do
      path = "/tmp/spark_ex_reader_builder_#{System.unique_integer([:positive])}.csv"
      File.write!(path, "id|name\n1|Alice\n2|Bob\n")
      on_exit(fn -> File.rm(path) end)

      df =
        session
        |> SparkEx.read()
        |> Reader.format("csv")
        |> Reader.schema("id INT, name STRING")
        |> Reader.option("header", true)
        |> Reader.options(%{"sep" => "|"})
        |> Reader.load(path)

      {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.any?(rows, fn row -> row["name"] == "Alice" end)
    end
  end
end
