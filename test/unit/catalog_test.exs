defmodule SparkEx.CatalogTest do
  use ExUnit.Case, async: true

  alias SparkEx.Catalog

  defmodule FakeCatalogSession do
    use GenServer

    def start_link(test_pid, current_db, databases \\ ["analytics"], current_catalog \\ "spark_catalog") do
      GenServer.start_link(__MODULE__, {test_pid, current_db, databases, current_catalog})
    end

    @impl true
    def init({test_pid, current_db, databases, current_catalog}),
      do:
        {:ok,
         %{
            test_pid: test_pid,
            current_db: current_db,
            databases: databases,
            current_catalog: current_catalog
          }}

    @impl true
    def handle_call({:execute_collect, {:catalog, {:current_catalog}}, _opts}, _from, state) do
      {:reply, {:ok, [%{"current_catalog" => state.current_catalog}]}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:get_table, table_name, db_name}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:get_table_called, table_name, db_name})
      short_name = table_name |> String.split(".") |> List.last()

      row = %{
        "name" => short_name,
        "catalog" => "sfx",
        "namespace" => ["sfx_dev_warehouse"],
        "description" => nil,
        "tableType" => "MANAGED",
        "isTemporary" => false
      }

      {:reply, {:ok, [row]}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:list_columns, table_name, db_name}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:list_columns_called, table_name, db_name})

      row = %{
        "name" => "id",
        "description" => nil,
        "dataType" => "int",
        "nullable" => true,
        "isPartition" => false,
        "isBucket" => false,
        "isCluster" => false
      }

      {:reply, {:ok, [row]}, state}
    end

    def handle_call({:execute_collect, {:catalog, {:current_database}}, _opts}, _from, state) do
      {:reply, {:ok, [%{"current_database" => state.current_db}]}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:list_tables, db_name, pattern}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:list_tables_called, db_name, pattern})
      {:reply, {:ok, []}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:table_exists, table_name, db_name}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:table_exists_called, table_name, db_name})
      {:reply, {:ok, [%{"table_exists" => false}]}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:list_functions, db_name, pattern}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:list_functions_called, db_name, pattern})
      {:reply, {:ok, []}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:function_exists, function_name, db_name}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:function_exists_called, function_name, db_name})
      {:reply, {:ok, [%{"function_exists" => true}]}, state}
    end

    def handle_call({:analyze_ddl_parse, ddl}, _from, state) do
      send(state.test_pid, {:analyze_ddl_parse_called, ddl})

      parsed =
        %Spark.Connect.DataType{
          kind: {:struct, %Spark.Connect.DataType.Struct{fields: []}}
        }

      {:reply, {:ok, parsed}, state}
    end

    def handle_call(
          {:execute_collect,
           {:catalog, {:create_table, _table_name, _path, _source, _description, schema, _options}},
           _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:create_table_called, schema})
      {:reply, {:ok, []}, state}
    end

    def handle_call(
          {:execute_collect, {:catalog, {:list_databases, _pattern}}, _opts},
          _from,
          state
        ) do
      rows = Enum.map(state.databases, fn db_name -> %{"name" => db_name} end)
      {:reply, {:ok, rows}, state}
    end
  end

  test "list_tables/3 resolves nil db_name from current_database" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "analytics")

    assert {:ok, []} = Catalog.list_tables(session, nil, "emp*")
    assert_receive {:list_tables_called, "analytics", "emp*"}
  end

  test "list_functions/3 resolves nil db_name from current_database" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "analytics")

    assert {:ok, []} = Catalog.list_functions(session, nil, "abs*")
    assert_receive {:list_functions_called, "analytics", "abs*"}
  end

  test "list_tables/3 falls back to first database when current_database is empty" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "", ["warehouse", "sandbox"])

    assert {:ok, []} = Catalog.list_tables(session, nil, nil)
    assert_receive {:list_tables_called, "warehouse", nil}
  end

  test "list_functions/3 falls back to first database when current_database is empty" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "", ["warehouse", "sandbox"])

    assert {:ok, []} = Catalog.list_functions(session, nil, nil)
    assert_receive {:list_functions_called, "warehouse", nil}
  end

  test "table_exists?/3 matches direct TableExists RPC behavior" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "warehouse", ["warehouse"])

    assert {:ok, false} = Catalog.table_exists?(session, "orders", "warehouse")
    assert_receive {:table_exists_called, "orders", "warehouse"}
    refute_receive {:list_tables_called, "warehouse", "orders"}
  end

  test "get_table/3 qualifies db_name with current catalog" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "sfx_dev_warehouse", ["sfx_dev_warehouse"], "sfx")

    assert {:ok, %Catalog.Table{name: "orders"}} =
             Catalog.get_table(session, "orders", "sfx_dev_warehouse")

    assert_receive {:get_table_called, "sfx.sfx_dev_warehouse.orders", nil}
  end

  test "list_columns/3 qualifies db_name with current catalog" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "sfx_dev_warehouse", ["sfx_dev_warehouse"], "sfx")

    assert {:ok, [%Catalog.ColumnInfo{name: "id"}]} =
             Catalog.list_columns(session, "orders", "sfx_dev_warehouse")

    assert_receive {:list_columns_called, "sfx.sfx_dev_warehouse.orders", nil}
  end

  test "function_exists?/3 passes provided db_name to catalog RPC" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "sfx_dev_warehouse", ["sfx_dev_warehouse"], "sfx")

    assert {:ok, true} = Catalog.function_exists?(session, "concat", "sfx_dev_warehouse")
    assert_receive {:function_exists_called, "concat", "sfx_dev_warehouse"}
  end

  test "build_drop_table_sql quotes dotted identifiers by parts" do
    sql = Catalog.build_drop_table_sql("sfx.sfx_dev_warehouse.my_table", if_exists: true, purge: true)
    assert sql == "DROP TABLE IF EXISTS `sfx`.`sfx_dev_warehouse`.`my_table` PURGE"
  end

  test "build_alter_table_sql quotes dotted identifiers by parts" do
    sql =
      Catalog.build_alter_table_sql("sfx.sfx_dev_warehouse.old_name",
        rename_to: "sfx.sfx_dev_warehouse.new_name"
      )

    assert sql ==
             "ALTER TABLE `sfx`.`sfx_dev_warehouse`.`old_name` RENAME TO `sfx`.`sfx_dev_warehouse`.`new_name`"
  end

  test "create_table/3 parses DDL schema into DataType before encode" do
    {:ok, session} = FakeCatalogSession.start_link(self(), "analytics")

    assert {:ok, _df} = Catalog.create_table(session, "my_table", schema: "id INT, name STRING")
    assert_receive {:analyze_ddl_parse_called, "id INT, name STRING"}
    assert_receive {:create_table_called, %Spark.Connect.DataType{}}
  end
end
