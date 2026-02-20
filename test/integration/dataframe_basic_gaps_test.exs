defmodule SparkEx.Integration.DataframeBasicGapsTest do
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

  test "is_empty returns true for empty dataframe", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id WHERE 1 = 0")
    assert {:ok, true} = DataFrame.is_empty(df)
  end

  test "is_empty returns false for non-empty dataframe", %{session: session} do
    df = SparkEx.range(session, 1)
    assert {:ok, false} = DataFrame.is_empty(df)
  end

  test "cache/unpersist roundtrip succeeds", %{session: session} do
    df = SparkEx.range(session, 5)
    assert %DataFrame{} = DataFrame.cache(df)
    assert %DataFrame{} = DataFrame.unpersist(df)
  end

  test "self join returns expected results", %{session: session} do
    df =
      SparkEx.sql(
        session,
        """
        SELECT l.id AS id, l.tag AS left_tag, r.tag AS right_tag
        FROM (SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, tag)) l
        JOIN (SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, tag)) r
        ON l.id = r.id
        """
      )

    assert {:ok, rows} = DataFrame.collect(df)
    assert length(rows) == 2
    assert Enum.any?(rows, &(&1["id"] == 1 and &1["left_tag"] == "a" and &1["right_tag"] == "a"))
  end
end
