defmodule SparkEx.Integration.M13.MergeTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.{DataFrame, Column, MergeIntoWriter, Writer}
  import SparkEx.Functions, only: [col: 1]

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "merge_into" do
    test "merge with update_all and insert_all", %{session: session} do
      suffix = System.unique_integer([:positive])
      target_table = "merge_target_#{suffix}"
      target_path = "/tmp/spark_ex_merge_#{suffix}"

      # Create target table as a Delta-like parquet (use temp view backed by parquet)
      target_df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (1, 'Alice', 100), (2, 'Bob', 200) AS t(id, name, salary)
        """)

      # Write target as parquet and register as table
      :ok =
        target_df
        |> DataFrame.write()
        |> Writer.format("parquet")
        |> Writer.mode(:overwrite)
        |> Writer.save(target_path)

      # For merge to work, we need a Delta table. Spark's MERGE INTO requires
      # a Delta/Iceberg table. Let's test the builder API and command encoding
      # without executing (since we don't have Delta Lake in the test cluster).
      # Instead, verify the builder produces correct struct.

      source_df =
        SparkEx.sql(session, """
        SELECT * FROM VALUES (2, 'Bobby', 250), (3, 'Charlie', 300) AS t(id, name, salary)
        """)

      merge_writer =
        source_df
        |> DataFrame.merge_into(target_table)
        |> MergeIntoWriter.on(col("source.id") |> Column.eq(col("target.id")))
        |> MergeIntoWriter.when_matched_update_all()
        |> MergeIntoWriter.when_not_matched_insert_all()

      # Verify the builder struct is constructed correctly
      assert merge_writer.target_table == target_table
      assert merge_writer.condition != nil
      assert length(merge_writer.match_actions) == 1
      assert length(merge_writer.not_matched_actions) == 1

      [{action_type, _cond, _assigns}] = merge_writer.match_actions
      assert action_type == :update_star

      [{action_type2, _cond2, _assigns2}] = merge_writer.not_matched_actions
      assert action_type2 == :insert_star
    end

    test "merge builder with conditional delete and schema evolution", %{session: session} do
      source_df = SparkEx.sql(session, "SELECT 1 AS id, 'test' AS name")

      merge_writer =
        source_df
        |> DataFrame.merge_into("some_table")
        |> MergeIntoWriter.on(col("source.id") |> Column.eq(col("target.id")))
        |> MergeIntoWriter.when_matched_delete(
          col("target.name")
          |> Column.eq(col("source.name"))
        )
        |> MergeIntoWriter.when_not_matched_insert_all()
        |> MergeIntoWriter.when_not_matched_by_source_delete()
        |> MergeIntoWriter.with_schema_evolution()

      assert merge_writer.schema_evolution == true
      assert length(merge_writer.match_actions) == 1
      assert length(merge_writer.not_matched_actions) == 1
      assert length(merge_writer.not_matched_by_source_actions) == 1
    end
  end
end
