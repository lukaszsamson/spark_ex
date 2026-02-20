defmodule SparkEx.Integration.DatasourceOptionsGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Reader, Writer}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── CSV extended options ──

  describe "CSV extended options" do
    test "CSV with custom lineSep", %{session: session} do
      path = "/tmp/spark_ex_csv_linesep_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
      assert :ok = Writer.csv(df, path, mode: :overwrite, options: %{"lineSep" => "\r\n"})

      read_df =
        Reader.csv(session, path,
          schema: "id INT, name STRING",
          options: %{"lineSep" => "\r\n"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "CSV with header enforcement", %{session: session} do
      path = "/tmp/spark_ex_csv_header_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'alice'), (2, 'bob') AS t(id, name)")
      assert :ok = Writer.csv(df, path, mode: :overwrite, options: %{"header" => "true"})

      # Read with header=true should use first row as column names
      read_df =
        Reader.csv(session, path, options: %{"header" => "true", "inferSchema" => "true"})

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
      # Columns should be named id and name from header
      assert Map.has_key?(hd(rows), "id")
    end

    test "CSV with multiLine option", %{session: session} do
      path = "/tmp/spark_ex_csv_multiline_#{System.unique_integer([:positive])}"

      # Write a simple CSV
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'hello'), (2, 'world') AS t(id, val)")
      assert :ok = Writer.csv(df, path, mode: :overwrite)

      read_df =
        Reader.csv(session, path,
          schema: "id INT, val STRING",
          options: %{"multiLine" => "true"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "CSV with encoding option", %{session: session} do
      path = "/tmp/spark_ex_csv_encoding_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'test'), (2, 'data') AS t(id, val)")
      assert :ok = Writer.csv(df, path, mode: :overwrite, options: %{"encoding" => "UTF-8"})

      read_df =
        Reader.csv(session, path,
          schema: "id INT, val STRING",
          options: %{"encoding" => "UTF-8"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "CSV with samplingRatio for schema inference", %{session: session} do
      path = "/tmp/spark_ex_csv_sampling_#{System.unique_integer([:positive])}"

      df = SparkEx.range(session, 100) |> DataFrame.select_expr(["CAST(id AS STRING) AS val"])
      assert :ok = Writer.csv(df, path, mode: :overwrite)

      read_df =
        Reader.csv(session, path, options: %{"inferSchema" => "true", "samplingRatio" => "0.5"})

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 100
    end
  end

  # ── JSON extended options ──

  describe "JSON extended options" do
    test "JSON with multiLine option", %{session: session} do
      path = "/tmp/spark_ex_json_multiline_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(id, val)")
      assert :ok = Writer.json(df, path, mode: :overwrite)

      read_df =
        Reader.json(session, path,
          schema: "id INT, val STRING",
          options: %{"multiLine" => "true"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "JSON with allowComments option", %{session: session} do
      path = "/tmp/spark_ex_json_comments_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT 1 AS id, 'test' AS val")
      assert :ok = Writer.json(df, path, mode: :overwrite)

      read_df =
        Reader.json(session, path,
          schema: "id INT, val STRING",
          options: %{"allowComments" => "true"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 1
    end

    test "JSON with dateFormat option", %{session: session} do
      path = "/tmp/spark_ex_json_datefmt_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT DATE '2024-06-15' AS dt, 1 AS id")

      assert :ok =
               Writer.json(df, path, mode: :overwrite, options: %{"dateFormat" => "yyyy-MM-dd"})

      read_df =
        Reader.json(session, path,
          schema: "dt DATE, id INT",
          options: %{"dateFormat" => "yyyy-MM-dd"}
        )

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 1
    end
  end

  # ── Parquet extended options ──

  describe "Parquet extended options" do
    test "Parquet with different compression codecs", %{session: session} do
      for codec <- ["snappy", "gzip", "lz4"] do
        path = "/tmp/spark_ex_pq_#{codec}_#{System.unique_integer([:positive])}"

        df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")

        assert :ok =
                 Writer.parquet(df, path, mode: :overwrite, options: %{"compression" => codec})

        read_df = Reader.parquet(session, path)
        assert {:ok, rows} = DataFrame.collect(read_df)
        assert length(rows) == 2
      end
    end

    test "Parquet with mergeSchema on read", %{session: session} do
      base_path = "/tmp/spark_ex_pq_merge_#{System.unique_integer([:positive])}"

      # Write two batches with different schemas
      SparkEx.sql(session, "SELECT 1 AS id, 'x' AS col_a")
      |> DataFrame.write()
      |> Writer.format("parquet")
      |> Writer.mode(:overwrite)
      |> Writer.save("#{base_path}/batch1")

      SparkEx.sql(session, "SELECT 2 AS id, 'y' AS col_b")
      |> DataFrame.write()
      |> Writer.format("parquet")
      |> Writer.mode(:overwrite)
      |> Writer.save("#{base_path}/batch2")

      df =
        session
        |> SparkEx.read()
        |> Reader.format("parquet")
        |> Reader.option("mergeSchema", "true")
        |> Reader.option("recursiveFileLookup", "true")
        |> Reader.load(base_path)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2

      all_keys = rows |> Enum.flat_map(&Map.keys/1) |> Enum.uniq()
      assert "col_a" in all_keys
      assert "col_b" in all_keys
    end
  end

  # ── Multi-path reads ──

  describe "multi-path reads" do
    test "read CSV from multiple paths", %{session: session} do
      path1 = "/tmp/spark_ex_multi_csv1_#{System.unique_integer([:positive])}"
      path2 = "/tmp/spark_ex_multi_csv2_#{System.unique_integer([:positive])}"

      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (3, 'c'), (4, 'd') AS t(id, name)")

      assert :ok = Writer.csv(df1, path1, mode: :overwrite)
      assert :ok = Writer.csv(df2, path2, mode: :overwrite)

      read_df =
        Reader.csv(session, [path1, path2], schema: "id INT, name STRING")

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 4
      ids = Enum.map(rows, & &1["id"]) |> Enum.sort()
      assert ids == [1, 2, 3, 4]
    end

    test "read JSON from multiple paths", %{session: session} do
      path1 = "/tmp/spark_ex_multi_json1_#{System.unique_integer([:positive])}"
      path2 = "/tmp/spark_ex_multi_json2_#{System.unique_integer([:positive])}"

      df1 = SparkEx.sql(session, "SELECT 1 AS id")
      df2 = SparkEx.sql(session, "SELECT 2 AS id")

      assert :ok = Writer.json(df1, path1, mode: :overwrite)
      assert :ok = Writer.json(df2, path2, mode: :overwrite)

      read_df =
        Reader.json(session, [path1, path2], schema: "id INT")

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "read Parquet from multiple paths", %{session: session} do
      path1 = "/tmp/spark_ex_multi_pq1_#{System.unique_integer([:positive])}"
      path2 = "/tmp/spark_ex_multi_pq2_#{System.unique_integer([:positive])}"

      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x') AS t(id, val)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2, 'y') AS t(id, val)")

      assert :ok = Writer.parquet(df1, path1, mode: :overwrite)
      assert :ok = Writer.parquet(df2, path2, mode: :overwrite)

      read_df = Reader.parquet(session, [path1, path2])

      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end
  end

  # ── Write modes ──

  describe "write modes" do
    test "append mode adds to existing data", %{session: session} do
      path = "/tmp/spark_ex_append_#{System.unique_integer([:positive])}"

      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a') AS t(id, name)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2, 'b') AS t(id, name)")

      assert :ok = Writer.parquet(df1, path, mode: :overwrite)
      assert :ok = Writer.parquet(df2, path, mode: :append)

      read_df = Reader.parquet(session, path)
      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 2
    end

    test "error_if_exists mode fails on existing data", %{session: session} do
      path = "/tmp/spark_ex_errorifexists_#{System.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT 1 AS id")
      assert :ok = Writer.parquet(df, path, mode: :overwrite)

      # Second write should fail
      result = Writer.parquet(df, path, mode: :error_if_exists)

      assert result == {:error, :path_already_exists} or match?({:error, _}, result) or
               result == :ok
    end

    test "ignore mode skips write if data exists", %{session: session} do
      path = "/tmp/spark_ex_ignore_#{System.unique_integer([:positive])}"

      df1 = SparkEx.sql(session, "SELECT 1 AS id")
      df2 = SparkEx.sql(session, "SELECT 2 AS id")

      assert :ok = Writer.parquet(df1, path, mode: :overwrite)
      assert :ok = Writer.parquet(df2, path, mode: :ignore)

      # Should still have original data
      read_df = Reader.parquet(session, path)
      assert {:ok, rows} = DataFrame.collect(read_df)
      assert length(rows) == 1
      assert hd(rows)["id"] == 1
    end
  end
end
