defmodule SparkEx.Integration.Spark42P1CatalogTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Catalog, DataFrame, Session}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: Session.stop(session)
    end)

    %{session: session}
  end

  test "SQL fallbacks work on the supported 4.1 server with quoted names", %{session: session} do
    name = "p1_catalog_fallback_#{System.unique_integer([:positive])}"

    SparkEx.sql(session, "CREATE OR REPLACE TEMP VIEW `#{name}` AS SELECT 1 AS id")
    |> DataFrame.collect()

    assert {:ok, views} = Catalog.list_views(session, nil, name)
    assert Enum.any?(views, &(&1.name == name and &1.table_type == "VIEW"))
    assert :ok = Catalog.drop_view(session, name, if_exists: true)
    assert {:ok, false} = Catalog.table_exists?(session, name)
  end

  @tag min_spark: "4.2"
  test "native catalog DDL, introspection, partitions and actions preserve flags", %{
    session: session
  } do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    database = "p1_catalog_#{suffix}"
    table = "#{database}.partitioned"
    table_ref = "`#{database}`.`partitioned`"
    view = "#{database}.view_#{suffix}"
    view_ref = "`#{database}`.`view_#{suffix}`"

    on_exit(fn ->
      if Process.alive?(session) do
        SparkEx.sql(session, "DROP DATABASE IF EXISTS `#{database}` CASCADE")
        |> DataFrame.collect()
      end
    end)

    assert :ok =
             Catalog.create_database(session, database,
               backend: :catalog,
               if_not_exists: true,
               properties: %{owner: "spark_ex"}
             )

    assert :ok =
             SparkEx.sql(
               session,
               "CREATE TABLE #{table_ref} (id INT, ds STRING) USING parquet PARTITIONED BY (ds) TBLPROPERTIES ('p1'='yes')"
             )
             |> collect_void()

    assert :ok =
             SparkEx.sql(session, "INSERT INTO #{table_ref} VALUES (1, 'a'), (2, 'b')")
             |> collect_void()

    assert {:ok,
            [
              %Catalog.TablePartition{partition: "ds=a"},
              %Catalog.TablePartition{partition: "ds=b"}
            ]} =
             Catalog.list_partitions(session, table, backend: :catalog)

    assert {:ok, ddl} = Catalog.get_create_table_string(session, table, backend: :catalog)
    assert ddl =~ "CREATE TABLE"
    assert ddl =~ "partitioned"

    assert {:ok, properties} = Catalog.get_table_properties(session, table, backend: :catalog)
    assert is_map(properties)
    assert properties["p1"] == "yes"

    assert :ok = Catalog.analyze_table(session, table, backend: :catalog, no_scan: true)
    assert :ok = Catalog.truncate_table(session, table, backend: :catalog)

    assert {:ok, [%{"count(1)" => 0}]} =
             SparkEx.sql(session, "SELECT count(1) FROM #{table_ref}") |> DataFrame.collect()

    assert :ok =
             SparkEx.sql(session, "CREATE VIEW #{view_ref} AS SELECT 1 AS id") |> collect_void()

    assert {:ok, views} = Catalog.list_views(session, database, "view_*", backend: :catalog)
    assert Enum.any?(views, &(&1.name == "view_#{suffix}" and &1.table_type == "VIEW"))
    assert :ok = Catalog.drop_view(session, view, backend: :catalog, if_exists: true)

    assert :ok =
             Catalog.drop_table(session, table, backend: :catalog, if_exists: true, purge: true)

    assert :ok =
             Catalog.drop_database(session, database,
               backend: :catalog,
               if_exists: true,
               cascade: true
             )
  end

  defp collect_void(df) do
    case DataFrame.collect(df) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end
end
