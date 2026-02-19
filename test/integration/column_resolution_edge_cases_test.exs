defmodule SparkEx.Integration.ColumnResolutionEdgeCasesTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "ambiguous column reference returns structured error", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT id FROM (SELECT 1 AS id) a JOIN (SELECT 2 AS id) b ON a.id = b.id"
      )

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.DataFrame.collect(df)

    assert error.error_class in [
             "AMBIGUOUS_REFERENCE",
             "AMBIGUOUS_COLUMN_REFERENCE"
           ]
  end

  test "missing nested field returns structured error", %{session: session} do
    df = SparkEx.sql(session, "SELECT s.c FROM (SELECT named_struct('a', 1) AS s) t")

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.DataFrame.collect(df)

    assert error.error_class in [
             "FIELD_NOT_FOUND",
             "UNRESOLVED_COLUMN.WITH_SUGGESTION"
           ]
  end

  test "struct star expands fields", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT s.* FROM (SELECT named_struct('a', 1, 'b', 2) AS s) t"
      )

    assert {:ok, [row]} = SparkEx.DataFrame.collect(df)
    assert Map.keys(row) |> Enum.sort() == ["a", "b"]
    assert row["a"] == 1
    assert row["b"] == 2
  end

  test "ambiguous unqualified column in where returns structured error", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT * FROM (SELECT 1 AS id) a JOIN (SELECT 2 AS id) b ON a.id = b.id WHERE id = 1"
      )

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.DataFrame.collect(df)

    assert error.error_class in [
             "AMBIGUOUS_REFERENCE",
             "AMBIGUOUS_COLUMN_REFERENCE"
           ]
  end

  test "missing column in order by returns structured error", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM (SELECT 1 AS id) t ORDER BY missing")

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.DataFrame.collect(df)

    assert error.error_class in [
             "UNRESOLVED_COLUMN.WITH_SUGGESTION",
             "UNRESOLVED_COLUMN",
             "COLUMN_NOT_FOUND"
           ]
  end
end
