defmodule SparkEx.Integration.CatalogRefreshRecoverTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Catalog, DataFrame, Reader, Writer}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "refresh_by_path and recover_partitions", %{session: session} do
    path = "/tmp/spark_ex_refresh_#{System.unique_integer([:positive])}"
    table = "refresh_tbl_#{System.unique_integer([:positive])}"

    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS value")
    assert :ok = Writer.parquet(df, path, mode: :overwrite)

    external = Reader.parquet(session, path)
    assert {:ok, _} = DataFrame.collect(external)

    assert {:ok, _} =
             SparkEx.sql(
               session,
               """
               CREATE TABLE #{table} (id INT, value STRING)
               USING parquet
               PARTITIONED BY (id)
               LOCATION '#{path}'
               """
             )
             |> DataFrame.collect()

    assert :ok = Catalog.refresh_by_path(session, path)
    assert :ok = Catalog.recover_partitions(session, table)
    assert :ok = Catalog.drop_table(session, table, if_exists: true, purge: true)
  end

  test "create_table with options", %{session: session} do
    table = "create_tbl_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             SparkEx.sql(
               session,
               """
               CREATE TABLE #{table} (id INT, value STRING)
               USING parquet
               TBLPROPERTIES ('parquet.compression'='gzip')
               """
             )
             |> DataFrame.collect()

    assert :ok = Catalog.drop_table(session, table, if_exists: true, purge: true)
  end
end
