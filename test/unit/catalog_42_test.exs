defmodule SparkEx.Catalog42Test do
  use ExUnit.Case, async: true

  alias SparkEx.Catalog
  alias SparkEx.Connect.PlanEncoder

  describe "Spark 4.2 catalog relation encoding" do
    test "preserves action flags and database properties" do
      assert_catalog({:drop_table, "db.t", true, true}, :drop_table, Spark.Connect.DropTable,
        table_name: "db.t",
        if_exists: true,
        purge: true
      )

      assert_catalog({:drop_view, "db.v", true}, :drop_view, Spark.Connect.DropView,
        view_name: "db.v",
        if_exists: true
      )

      assert_catalog(
        {:create_database, "db", true, %{"owner" => "analytics"}},
        :create_database,
        Spark.Connect.CreateDatabase,
        db_name: "db",
        if_not_exists: true,
        properties: %{"owner" => "analytics"}
      )

      assert_catalog(
        {:drop_database, "db", true, true},
        :drop_database,
        Spark.Connect.DropDatabase,
        db_name: "db",
        if_exists: true,
        cascade: true
      )

      assert_catalog(
        {:get_create_table_string, "db.t", true},
        :get_create_table_string,
        Spark.Connect.GetCreateTableString,
        table_name: "db.t",
        as_serde: true
      )

      assert_catalog({:analyze_table, "db.t", true}, :analyze_table, Spark.Connect.AnalyzeTable,
        table_name: "db.t",
        no_scan: true
      )
    end

    test "encodes lookup and mutation relations" do
      assert_catalog({:list_partitions, "db.t"}, :list_partitions, Spark.Connect.ListPartitions,
        table_name: "db.t"
      )

      assert_catalog({:list_views, nil, "v*"}, :list_views, Spark.Connect.ListViews,
        db_name: nil,
        pattern: "v*"
      )

      assert_catalog(
        {:get_table_properties, "db.t"},
        :get_table_properties,
        Spark.Connect.GetTableProperties,
        table_name: "db.t"
      )

      assert_catalog({:truncate_table, "db.t"}, :truncate_table, Spark.Connect.TruncateTable,
        table_name: "db.t"
      )
    end
  end

  describe "SQL-compatible catalog defaults" do
    test "keeps database options available to the SQL backend" do
      assert Catalog.build_create_database_sql("db",
               if_not_exists: true,
               comment: "data",
               location: "/warehouse/db",
               properties: %{owner: "analytics"}
             ) ==
               "CREATE DATABASE IF NOT EXISTS `db` COMMENT 'data' LOCATION '/warehouse/db' WITH DBPROPERTIES ('owner'='analytics')"
    end

    test "retains TablePartition's single partition field" do
      assert %Catalog.TablePartition{partition: "ds=2026-09-06"} =
               %Catalog.TablePartition{partition: "ds=2026-09-06"}
    end

    test "rejects unknown and invalid backend options before executing a plan" do
      assert_raise ArgumentError, ~r/unsupported catalog options/, fn ->
        Catalog.drop_view(self(), "v", unexpected: true)
      end

      assert_raise ArgumentError, ~r/expected :backend to be :sql or :catalog/, fn ->
        Catalog.drop_view(self(), "v", backend: :probe)
      end
    end
  end

  defp assert_catalog(plan, tag, module, expected) do
    {relation, _counter} = PlanEncoder.encode_relation({:catalog, plan}, 0)
    assert {:catalog, catalog} = relation.rel_type
    assert {^tag, message} = catalog.cat_type
    assert ^module = message.__struct__

    Enum.each(expected, fn {field, value} -> assert Map.fetch!(message, field) == value end)
  end
end
