defmodule SparkEx.Integration.WriterV2Test do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame
  alias SparkEx.WriterV2

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)
    :ok = SparkEx.config_set(session, [{"spark.sql.sources.useV1SourceList", ""}])

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "DataFrameWriterV2 actions" do
    test "create, append, and replace table data", %{session: session} do
      table_name = "spark_ex_v2_basic_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, value)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (3, 'c') AS t(id, value)")
      df3 = SparkEx.sql(session, "SELECT * FROM VALUES (10, 'z') AS t(id, value)")

      assert :ok =
               df1
               |> DataFrame.write_v2(table_name)
               |> WriterV2.using("parquet")
               |> WriterV2.table_property("owner", "spark_ex")
               |> WriterV2.create()

      assert {:ok, 2} = SparkEx.sql(session, "SELECT * FROM #{table_name}") |> DataFrame.count()

      case df2 |> DataFrame.write_v2(table_name) |> WriterV2.append() do
        :ok ->
          assert {:ok, 3} =
                   SparkEx.sql(session, "SELECT * FROM #{table_name}") |> DataFrame.count()

        {:error, %SparkEx.Error.Remote{message: message}} ->
          if message =~ "Cannot write into v1 table" do
            assert message =~ "Cannot write into v1 table"
          else
            flunk("unexpected WriterV2.append error: #{inspect(message)}")
          end

        other ->
          flunk("unexpected append result: #{inspect(other)}")
      end

      case df3 |> DataFrame.write_v2(table_name) |> WriterV2.replace() do
        :ok ->
          assert {:ok, 1} =
                   SparkEx.sql(session, "SELECT * FROM #{table_name}") |> DataFrame.count()

        {:error, %SparkEx.Error.Remote{message: message}} ->
          if message =~ "Cannot write into v1 table" or
               message =~ "does not support REPLACE TABLE AS SELECT" do
            assert is_binary(message)
          else
            flunk("unexpected WriterV2.replace error: #{inspect(message)}")
          end

        other ->
          flunk("unexpected replace result: #{inspect(other)}")
      end
    end

    test "overwrite with condition", %{session: session} do
      import SparkEx.Functions, only: [col: 1, lit: 1]

      table_name = "spark_ex_v2_overwrite_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      seed_df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, value)")
      overwrite_df = SparkEx.sql(session, "SELECT * FROM VALUES (2, 'bb') AS t(id, value)")

      assert :ok =
               seed_df
               |> DataFrame.write_v2(table_name)
               |> WriterV2.using("parquet")
               |> WriterV2.create()

      case overwrite_df
           |> DataFrame.write_v2(table_name)
           |> WriterV2.overwrite(col("id") |> SparkEx.Column.eq(lit(2))) do
        :ok ->
          {:ok, rows} =
            SparkEx.sql(session, "SELECT * FROM #{table_name} ORDER BY id")
            |> DataFrame.collect()

          assert length(rows) == 2
          assert Enum.find(rows, fn row -> row["id"] == 2 end)["value"] == "bb"

        {:error, %SparkEx.Error.Remote{message: message}} ->
          if message =~ "Cannot write into v1 table" do
            assert message =~ "Cannot write into v1 table"
          else
            flunk("unexpected WriterV2.overwrite error: #{inspect(message)}")
          end

        other ->
          flunk("unexpected overwrite result: #{inspect(other)}")
      end
    end

    test "overwrite_partitions on partitioned table", %{session: session} do
      table_name = "spark_ex_v2_partitions_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        SparkEx.sql(session, "DROP TABLE IF EXISTS #{table_name}") |> DataFrame.collect()
      end)

      initial_df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'A'), (2, 'B'), (3, 'A') AS t(id, part)"
        )

      update_df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (10, 'A') AS t(id, part)"
        )

      assert :ok =
               initial_df
               |> DataFrame.write_v2(table_name)
               |> WriterV2.using("parquet")
               |> WriterV2.partitioned_by(["part"])
               |> WriterV2.create()

      case update_df |> DataFrame.write_v2(table_name) |> WriterV2.overwrite_partitions() do
        :ok ->
          {:ok, rows} =
            SparkEx.sql(session, "SELECT * FROM #{table_name} ORDER BY part, id")
            |> DataFrame.collect()

          assert Enum.count(rows, &(&1["part"] == "A")) == 1
          assert Enum.count(rows, &(&1["part"] == "B")) == 1
          assert Enum.any?(rows, fn row -> row["id"] == 10 and row["part"] == "A" end)

        {:error, %SparkEx.Error.Remote{message: message}} ->
          if message =~ "Cannot write into v1 table" do
            assert message =~ "Cannot write into v1 table"
          else
            flunk("unexpected WriterV2.overwrite_partitions error: #{inspect(message)}")
          end

        other ->
          flunk("unexpected overwrite_partitions result: #{inspect(other)}")
      end
    end
  end
end
