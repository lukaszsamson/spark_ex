defmodule SparkEx.Integration.M12.CatalogTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.Catalog

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Catalog Management ──

  describe "catalog management" do
    test "current_catalog returns a string", %{session: session} do
      assert {:ok, catalog} = Catalog.current_catalog(session)
      assert is_binary(catalog)
      assert catalog == "spark_catalog"
    end

    test "list_catalogs returns at least spark_catalog", %{session: session} do
      assert {:ok, catalogs} = Catalog.list_catalogs(session)
      assert is_list(catalogs)
      names = Enum.map(catalogs, & &1.name)
      assert "spark_catalog" in names
    end
  end

  # ── Database Management ──

  describe "database management" do
    test "current_database returns default", %{session: session} do
      assert {:ok, db} = Catalog.current_database(session)
      assert db == "default"
    end

    test "list_databases returns at least default", %{session: session} do
      assert {:ok, databases} = Catalog.list_databases(session)
      assert is_list(databases)
      names = Enum.map(databases, & &1.name)
      assert "default" in names

      # Check struct fields
      default_db = Enum.find(databases, &(&1.name == "default"))
      assert %Catalog.Database{} = default_db
      assert is_binary(default_db.location_uri)
    end

    test "get_database returns database info", %{session: session} do
      assert {:ok, db} = Catalog.get_database(session, "default")
      assert %Catalog.Database{} = db
      assert db.name == "default"
      assert is_binary(db.location_uri)
    end

    test "database_exists? returns true for default", %{session: session} do
      assert {:ok, true} = Catalog.database_exists?(session, "default")
    end

    test "database_exists? returns false for non-existent", %{session: session} do
      assert {:ok, false} = Catalog.database_exists?(session, "nonexistent_db_12345")
    end

    test "set_current_database changes and restores", %{session: session} do
      # Ensure we start at default
      assert {:ok, "default"} = Catalog.current_database(session)

      # Set to default again (safe operation)
      assert :ok = Catalog.set_current_database(session, "default")
      assert {:ok, "default"} = Catalog.current_database(session)
    end
  end

  # ── Table Operations ──

  describe "table operations" do
    test "list_tables returns a list", %{session: session} do
      assert {:ok, tables} = Catalog.list_tables(session)
      assert is_list(tables)
    end

    test "table lifecycle: create temp view → exists → get → columns → drop", %{
      session: session
    } do
      view_name = "m12_test_table_#{:erlang.unique_integer([:positive])}"

      # Create a temp view
      df =
        SparkEx.sql(session, """
        SELECT 1 AS id, 'Alice' AS name, 25 AS age
        UNION ALL
        SELECT 2 AS id, 'Bob' AS name, 30 AS age
        """)

      SparkEx.DataFrame.create_temp_view(df, view_name)

      # table_exists? should be true
      assert {:ok, true} = Catalog.table_exists?(session, view_name)

      # get_table should return table info
      assert {:ok, table} = Catalog.get_table(session, view_name)
      assert %Catalog.Table{} = table
      assert table.name == view_name
      assert table.is_temporary == true

      # list_tables should include our view
      assert {:ok, tables} = Catalog.list_tables(session)
      names = Enum.map(tables, & &1.name)
      assert view_name in names

      # list_columns should return column info
      assert {:ok, columns} = Catalog.list_columns(session, view_name)
      assert is_list(columns)
      col_names = Enum.map(columns, & &1.name)
      assert "id" in col_names
      assert "name" in col_names
      assert "age" in col_names

      # Check column struct fields
      id_col = Enum.find(columns, &(&1.name == "id"))
      assert %Catalog.ColumnInfo{} = id_col
      assert is_binary(id_col.data_type)

      # Drop the temp view
      assert {:ok, true} = Catalog.drop_temp_view(session, view_name)

      # table_exists? should be false now
      assert {:ok, false} = Catalog.table_exists?(session, view_name)
    end

    test "table_exists? returns false for non-existent", %{session: session} do
      assert {:ok, false} = Catalog.table_exists?(session, "nonexistent_table_12345")
    end

    test "drop_temp_view returns false for non-existent", %{session: session} do
      assert {:ok, false} = Catalog.drop_temp_view(session, "nonexistent_view_12345")
    end
  end

  # ── Function Operations ──

  describe "function operations" do
    test "list_functions returns built-in functions", %{session: session} do
      assert {:ok, functions} = Catalog.list_functions(session)
      assert is_list(functions)
      assert length(functions) > 0

      names = Enum.map(functions, & &1.name)
      assert "abs" in names
    end

    test "function_exists? returns true for abs", %{session: session} do
      assert {:ok, true} = Catalog.function_exists?(session, "abs")
    end

    test "function_exists? returns false for non-existent", %{session: session} do
      assert {:ok, false} = Catalog.function_exists?(session, "nonexistent_function_12345")
    end

    test "get_function returns function info", %{session: session} do
      assert {:ok, func} = Catalog.get_function(session, "abs")
      assert %Catalog.Function{} = func
      assert func.name == "abs"
      assert is_binary(func.class_name)
    end
  end

  # ── Caching ──

  describe "caching" do
    test "cache lifecycle: cache → is_cached → uncache → is_cached", %{session: session} do
      view_name = "m12_cache_test_#{:erlang.unique_integer([:positive])}"

      # Create a temp view to cache
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(x)")
      SparkEx.DataFrame.create_temp_view(df, view_name)

      # Initially not cached
      assert {:ok, false} = Catalog.is_cached?(session, view_name)

      # Cache it
      assert :ok = Catalog.cache_table(session, view_name)

      # Now it should be cached
      assert {:ok, true} = Catalog.is_cached?(session, view_name)

      # Uncache it
      assert :ok = Catalog.uncache_table(session, view_name)

      # Should no longer be cached
      assert {:ok, false} = Catalog.is_cached?(session, view_name)

      # Clean up
      Catalog.drop_temp_view(session, view_name)
    end

    test "clear_cache works", %{session: session} do
      view_name = "m12_clear_cache_test_#{:erlang.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(x)")
      SparkEx.DataFrame.create_temp_view(df, view_name)
      Catalog.cache_table(session, view_name)

      assert {:ok, true} = Catalog.is_cached?(session, view_name)

      assert :ok = Catalog.clear_cache(session)

      assert {:ok, false} = Catalog.is_cached?(session, view_name)

      # Clean up
      Catalog.drop_temp_view(session, view_name)
    end
  end

  # ── Drop Global Temp View ──

  describe "global temp views" do
    test "drop_global_temp_view returns false for non-existent", %{session: session} do
      assert {:ok, false} =
               Catalog.drop_global_temp_view(session, "nonexistent_global_view_12345")
    end
  end

  # ── Refresh ──

  describe "refresh operations" do
    test "refresh_table on a temp view", %{session: session} do
      view_name = "m12_refresh_test_#{:erlang.unique_integer([:positive])}"

      df = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(x)")
      SparkEx.DataFrame.create_temp_view(df, view_name)

      # Cache it first so refresh has something to do
      Catalog.cache_table(session, view_name)
      assert :ok = Catalog.refresh_table(session, view_name)

      # Clean up
      Catalog.uncache_table(session, view_name)
      Catalog.drop_temp_view(session, view_name)
    end
  end
end
