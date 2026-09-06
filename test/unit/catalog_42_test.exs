defmodule SparkEx.Catalog42Test do
  use ExUnit.Case, async: true

  alias SparkEx.Catalog
  alias SparkEx.Connect.PlanEncoder

  defmodule RowsSession do
    use GenServer

    def start_link(rows), do: GenServer.start_link(__MODULE__, rows)
    @impl true
    def init(rows), do: {:ok, rows}
    @impl true
    def handle_call({:execute_collect, _, _}, _from, rows), do: {:reply, {:ok, rows}, rows}
  end

  test "scalar catalog results reject ambiguous columns instead of selecting a map value" do
    session = start_supervised!({RowsSession, [%{"extra" => "wrong", "value" => "ddl"}]})

    for backend <- [:sql, :catalog] do
      assert {:error, {:unexpected_columns, _}} =
               Catalog.get_create_table_string(session, "t", backend: backend)

      assert {:error, {:unexpected_columns, _}} =
               Catalog.list_partitions(session, "t", backend: backend)
    end
  end

  test "single-column catalog results retain their value regardless of the column label" do
    session = start_supervised!({RowsSession, [%{"server_column" => "ds=one"}]})
    assert {:ok, "ds=one"} = Catalog.get_create_table_string(session, "t")

    assert {:ok, [%Catalog.TablePartition{partition: "ds=one"}]} =
             Catalog.list_partitions(session, "t")
  end

  test "views require a recognized name and preserve SQL and native row shapes" do
    for row <- [
          %{"name" => "native", "namespace" => ["db"]},
          %{"viewName" => "sql", "namespace" => "db", "isTemporary" => true},
          %{"view_name" => "alias", "namespace" => "db"}
        ] do
      {:ok, session} = RowsSession.start_link([row])

      assert {:ok, [%Catalog.Table{name: name, namespace: ["db"]}]} =
               Catalog.list_views(session, "db")

      assert name in ["native", "sql", "alias"]
      GenServer.stop(session)
    end

    session = start_supervised!({RowsSession, [%{"renamed" => "v", "namespace" => "db"}]})
    assert {:error, {:unexpected_columns, _}} = Catalog.list_views(session, "db")
  end

  test "table properties decode named key/value columns and reject unknown pairs" do
    session =
      start_supervised!(
        {RowsSession, [%{"value" => "v", "key" => "k"}, %{"Value" => "V", "Key" => "K"}]}
      )

    assert {:ok, %{"k" => "v", "K" => "V"}} = Catalog.get_table_properties(session, "t")
    GenServer.stop(session)

    {:ok, unknown} = RowsSession.start_link([%{"a_value" => "v", "z_key" => "k"}])
    assert {:error, {:unexpected_columns, _}} = Catalog.get_table_properties(unknown, "t")
    GenServer.stop(unknown)
  end

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
