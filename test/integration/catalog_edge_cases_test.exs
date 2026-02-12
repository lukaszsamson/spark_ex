defmodule SparkEx.Integration.CatalogEdgeCasesTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Catalog, DataFrame}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "drop non-empty database requires cascade", %{session: session} do
    db_name = "spark_ex_db_#{System.unique_integer([:positive])}"
    table_name = "table_#{System.unique_integer([:positive])}"
    qualified = "#{db_name}.#{table_name}"

    assert :ok = Catalog.create_database(session, db_name)

    assert {:ok, _} =
             SparkEx.sql(session, "CREATE TABLE #{qualified} (id INT) USING parquet")
             |> DataFrame.collect()

    assert {:error, %SparkEx.Error.Remote{}} = Catalog.drop_database(session, db_name)
    assert :ok = Catalog.drop_database(session, db_name, cascade: true)
  end

  test "list tables and columns in a non-default namespace", %{session: session} do
    db_name = "spark_ex_ns_#{System.unique_integer([:positive])}"
    table_name = "tbl_#{System.unique_integer([:positive])}"
    qualified = "#{db_name}.#{table_name}"

    assert :ok = Catalog.create_database(session, db_name)

    assert {:ok, _} =
             SparkEx.sql(session, "CREATE TABLE #{qualified} (id INT, val STRING) USING parquet")
             |> DataFrame.collect()

    assert {:ok, tables} = Catalog.list_tables(session, db_name)
    assert Enum.any?(tables, fn table -> table.name == table_name end)

    assert {:ok, cols} = Catalog.list_columns(session, table_name, db_name)
    col_names = Enum.map(cols, & &1.name)
    assert "id" in col_names
    assert "val" in col_names

    assert :ok = Catalog.drop_database(session, db_name, cascade: true)
  end
end
