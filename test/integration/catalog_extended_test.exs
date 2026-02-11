defmodule SparkEx.Integration.CatalogExtendedTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Catalog

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "list_tables returns temp view", %{session: session} do
    view_name = "cat_list_tables_#{System.unique_integer([:positive])}"

    SparkEx.sql(session, "SELECT 1 AS id")
    |> SparkEx.DataFrame.create_or_replace_temp_view(view_name)

    assert {:ok, tables} = Catalog.list_tables(session)
    assert Enum.any?(tables, fn table -> table.name == view_name end)
  end

  test "list_columns returns schema for view", %{session: session} do
    view_name = "cat_list_columns_#{System.unique_integer([:positive])}"

    SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")
    |> SparkEx.DataFrame.create_or_replace_temp_view(view_name)

    assert {:ok, cols} = Catalog.list_columns(session, view_name)
    names = Enum.map(cols, & &1.name)
    assert "id" in names
    assert "name" in names
  end
end
