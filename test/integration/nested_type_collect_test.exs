defmodule SparkEx.Integration.NestedTypeCollectTest do
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

  test "collect returns nested struct/array/map values", %{session: session} do
    df =
      SparkEx.sql(
        session,
        """
        SELECT
          named_struct('id', 1, 'name', 'alpha') AS st,
          array(1, 2, 3) AS arr,
          map('a', 1, 'b', 2) AS mp
        """
      )

    assert {:ok, [row]} = DataFrame.collect(df)

    assert row["st"]["id"] == 1
    assert row["st"]["name"] == "alpha"
    assert row["arr"] == [1, 2, 3]

    mp =
      row["mp"]
      |> Enum.into(%{}, fn %{"key" => key, "value" => value} -> {key, value} end)

    assert mp["a"] == 1
    assert mp["b"] == 2
  end

  test "collect casts nested values", %{session: session} do
    df =
      SparkEx.sql(
        session,
        """
        SELECT
          CAST(named_struct('n', '42') AS STRUCT<n: INT>) AS st,
          CAST(array('1', '2') AS ARRAY<INT>) AS arr,
          CAST(map('a', '3') AS MAP<STRING, INT>) AS mp
        """
      )

    assert {:ok, [row]} = DataFrame.collect(df)
    assert row["st"]["n"] == 42
    assert row["arr"] == [1, 2]

    mp =
      row["mp"]
      |> Enum.into(%{}, fn %{"key" => key, "value" => value} -> {key, value} end)

    assert mp["a"] == 3
  end
end
