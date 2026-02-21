defmodule SparkEx.CatalogTest do
  use ExUnit.Case, async: true

  alias SparkEx.Catalog

  defmodule FakeCatalogSession do
    use GenServer

    def start_link(test_pid, current_db) do
      GenServer.start_link(__MODULE__, {test_pid, current_db})
    end

    @impl true
    def init({test_pid, current_db}), do: {:ok, %{test_pid: test_pid, current_db: current_db}}

    @impl true
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
          {:execute_collect, {:catalog, {:list_functions, db_name, pattern}}, _opts},
          _from,
          state
        ) do
      send(state.test_pid, {:list_functions_called, db_name, pattern})
      {:reply, {:ok, []}, state}
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
end
