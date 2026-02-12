defmodule SparkEx.Integration.SchemaPreservationTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Functions}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "schema/columns/dtypes stable across repeated calls", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")

    assert {:ok, schema1} = DataFrame.schema(df)
    assert {:ok, schema2} = DataFrame.schema(df)
    assert schema1 == schema2

    assert {:ok, cols1} = DataFrame.columns(df)
    assert {:ok, cols2} = DataFrame.columns(df)
    assert cols1 == cols2

    assert {:ok, dtypes1} = DataFrame.dtypes(df)
    assert {:ok, dtypes2} = DataFrame.dtypes(df)
    assert dtypes1 == dtypes2
  end

  test "schema and dtypes preserved after select/with_columns", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")

    projected = DataFrame.select(df, ["name", "id"])

    with_cols =
      DataFrame.with_columns(projected, %{
        "name_upper" => Functions.upper(Functions.col("name"))
      })

    assert {:ok, cols} = DataFrame.columns(with_cols)
    assert cols == ["name", "id", "name_upper"]

    assert {:ok, dtypes} = DataFrame.dtypes(with_cols)
    assert dtypes == [{"name", "STRING"}, {"id", "INT"}, {"name_upper", "STRING"}]
  end

  test "schema/columns/dtypes preserved after filter and limit", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")

    filtered =
      DataFrame.filter(df, SparkEx.Column.gt(Functions.col("id"), Functions.lit(0)))
      |> DataFrame.limit(1)

    assert {:ok, cols} = DataFrame.columns(filtered)
    assert cols == ["id", "name"]

    assert {:ok, dtypes} = DataFrame.dtypes(filtered)
    assert dtypes == [{"id", "INT"}, {"name", "STRING"}]
  end

  test "schema preserved after join and aggregation", %{session: session} do
    left = SparkEx.sql(session, "SELECT 1 AS id, 'a' AS name")
    right = SparkEx.sql(session, "SELECT 1 AS id, 'b' AS tag")

    joined = DataFrame.join(left, right, ["id"], :inner)

    assert {:ok, cols} = DataFrame.columns(joined)
    assert "id" in cols

    grouped = joined |> DataFrame.group_by(["id"]) |> SparkEx.GroupedData.count()
    assert {:ok, dtypes} = DataFrame.dtypes(grouped)
    assert {"id", "INT"} in dtypes
  end
end
