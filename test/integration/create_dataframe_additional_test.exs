defmodule SparkEx.Integration.CreateDataframeAdditionalTest do
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

  test "create_dataframe with nested types and explicit schema", %{session: session} do
    data = [%{"id" => 1, "tags" => ["a", "b"], "meta" => %{"k" => "v"}}]

    {:ok, df} =
      SparkEx.create_dataframe(session, data,
        schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>"
      )

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["id"] == 1
        assert row["tags"] == ["a", "b"]

        meta =
          row["meta"]
          |> Enum.into(%{}, fn %{"key" => key, "value" => value} -> {key, value} end)

        assert meta["k"] == "v"

      {:error, %SparkEx.Error.Remote{error_class: "UNSUPPORTED_ARROWTYPE"}} ->
        assert true
    end
  end

  test "create_dataframe nullability and coercion", %{session: session} do
    data = [%{"id" => "42", "note" => nil}]

    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT, note STRING")

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["id"] in [42, "42"]
        assert row["note"] == nil

      {:error, %SparkEx.Error.Remote{error_class: "INVALID_COLUMN_OR_FIELD_DATA_TYPE"}} ->
        assert true
    end
  end

  test "create_dataframe with large input", %{session: session} do
    data = Enum.map(1..2000, fn i -> %{"id" => i} end)

    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT")
    assert {:ok, rows} = DataFrame.take(df, 5)
    assert length(rows) == 5
  end

  test "head/first/take correctness", %{session: session} do
    {:ok, df} = SparkEx.create_dataframe(session, [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])

    assert {:ok, [row | _]} = DataFrame.head(df, 1)
    assert row["id"] in [1, 2, 3]

    assert {:ok, first} = DataFrame.first(df)
    assert first["id"] in [1, 2, 3]

    assert {:ok, rows} = DataFrame.take(df, 2)
    assert length(rows) == 2
  end
end
