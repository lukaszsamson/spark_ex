defmodule SparkEx.Integration.DataframePropertiesTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "columns/dtypes reflect schema", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")

    assert {:ok, columns} = DataFrame.columns(df)
    assert columns == ["id", "name"]

    assert {:ok, dtypes} = DataFrame.dtypes(df)
    assert {"id", "INT"} in dtypes
    assert {"name", "STRING"} in dtypes
  end

  test "columns/dtypes after select preserve order", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")
    projected = DataFrame.select(df, ["name", "id"])

    assert {:ok, columns} = DataFrame.columns(projected)
    assert columns == ["name", "id"]

    assert {:ok, dtypes} = DataFrame.dtypes(projected)
    assert dtypes == [{"name", "STRING"}, {"id", "INT"}]
  end
end
