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

    assert {:ok, [row]} = DataFrame.collect(df)
    assert row["id"] == 1
    assert row["tags"] == ["a", "b"]

    meta =
      row["meta"]
      |> Enum.into(%{}, fn %{"key" => key, "value" => value} -> {key, value} end)

    assert meta["k"] == "v"
  end

  test "create_dataframe nullability and coercion", %{session: session} do
    data = [%{"id" => "42", "note" => nil}]

    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT, note STRING")

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["id"] == 42
        assert row["note"] == nil

      {:error, %SparkEx.Error.Remote{} = error} ->
        if error.error_class == "INVALID_COLUMN_OR_FIELD_DATA_TYPE" and
             error.message_parameters["expectedType"] == "\"INT\"" and
             error.sql_state == "42000" do
          assert error.message_parameters["type"] == "\"STRING\""
        else
          flunk("unexpected coercion error: #{inspect(error)}")
        end
    end
  end

  test "create_dataframe with large input", %{session: session} do
    data = Enum.map(1..2000, fn i -> %{"id" => i} end)

    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT")
    assert {:ok, rows} = DataFrame.take(df, 5)
    assert length(rows) == 5
  end

  test "create_dataframe with explicit schema ignores map key order", %{session: session} do
    rows = [%{"active" => true, "id" => 1, "name" => "Alice"}]

    {:ok, df} =
      SparkEx.create_dataframe(session, rows, schema: "id INT, name STRING, active BOOLEAN")

    assert {:ok, [row]} = DataFrame.collect(df)
    assert row["id"] == 1
    assert row["name"] == "Alice"
    assert row["active"] == true
  end

  test "create_dataframe with MAP schema supports varying key sets", %{session: session} do
    rows = [
      %{"id" => 1, "meta" => %{"k1" => "v1", "k2" => "v2"}},
      %{"id" => 2, "meta" => %{"k3" => "v3"}}
    ]

    {:ok, df} =
      SparkEx.create_dataframe(session, rows, schema: "id INT, meta MAP<STRING, STRING>")

    assert {:ok, data} = DataFrame.collect(df)
    assert length(data) == 2
  end

  test "create_dataframe infers nested struct from list of maps", %{session: session} do
    rows = [
      %{"id" => 1, "info" => %{"name" => "Alice", "age" => 30}},
      %{"id" => 2, "info" => %{"name" => "Bob", "age" => 25}}
    ]

    {:ok, df} = SparkEx.create_dataframe(session, rows)
    assert {:ok, data} = DataFrame.collect(df)
    assert length(data) == 2
    assert Enum.any?(data, &(&1["info"]["name"] == "Alice" and &1["info"]["age"] == 30))
  end

  test "create_dataframe infers array from Elixir lists", %{session: session} do
    rows = [
      %{"id" => 1, "tags" => ["eng", "lead"]},
      %{"id" => 2, "tags" => ["sales"]}
    ]

    {:ok, df} = SparkEx.create_dataframe(session, rows)
    assert {:ok, data} = DataFrame.collect(df)
    assert Enum.find(data, &(&1["id"] == 1))["tags"] == ["eng", "lead"]
  end

  test "create_dataframe can disable local Arrow normalization", %{session: session} do
    data = [%{"id" => 1, "tags" => ["a", "b"], "meta" => %{"k" => "v"}}]

    {:ok, df} =
      SparkEx.create_dataframe(session, data,
        schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>",
        normalize_local_relation_arrow: false
      )

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["id"] == 1
        assert row["tags"] == ["a", "b"]

      {:error, %SparkEx.Error.Remote{} = error} ->
        assert error.error_class == "UNSUPPORTED_ARROWTYPE"
        assert error.message_parameters["typeName"] == "LargeList"
    end
  end

  test "head/first/take correctness", %{session: session} do
    {:ok, df} = SparkEx.create_dataframe(session, [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])

    assert {:ok, [row]} = DataFrame.head(df, 1)
    assert row["id"] == 1

    assert {:ok, first} = DataFrame.first(df)
    assert first["id"] == 1

    assert {:ok, rows} = DataFrame.take(df, 2)
    assert Enum.map(rows, & &1["id"]) == [1, 2]
  end

  test "create_dataframe infers heterogeneous nested map keys without crashing", %{
    session: session
  } do
    rows = [
      %{"id" => 1, "nested" => %{"level1" => %{"a" => 10, "b" => 20}}},
      %{"id" => 2, "nested" => %{"level1" => %{"c" => 30}}}
    ]

    assert {:ok, df} = SparkEx.create_dataframe(session, rows)
    assert {:ok, data} = DataFrame.collect(df)
    assert length(data) == 2

    second = Enum.find(data, &(&1["id"] == 2))
    assert get_in(second, ["nested", "level1", "c"]) == 30
  end

  test "create_dataframe supports map schemas with integer keys", %{session: session} do
    rows = [
      %{"id" => 1, "scores" => %{1 => 100, 2 => 95}},
      %{"id" => 2, "scores" => %{1 => 80, 3 => 70}}
    ]

    assert {:ok, df} =
             SparkEx.create_dataframe(session, rows, schema: "id INT, scores MAP<INT, INT>")

    assert {:ok, data} = DataFrame.collect(df)
    assert length(data) == 2

    first_scores =
      data
      |> Enum.find(&(&1["id"] == 1))
      |> Map.fetch!("scores")
      |> Enum.into(%{}, fn %{"key" => key, "value" => value} -> {key, value} end)

    assert first_scores[1] == 100
    assert first_scores[2] == 95
  end

  test "create_dataframe preserves nested struct map content via json fallback", %{
    session: session
  } do
    rows = [%{"id" => 1, "profile" => %{"tags" => ["a", "b"], "attrs" => %{"x" => "1"}}}]

    assert {:ok, df} =
             SparkEx.create_dataframe(session, rows,
               schema: "id INT, profile STRUCT<tags: ARRAY<STRING>, attrs: MAP<STRING, STRING>>"
             )

    assert {:ok, [%{"profile" => profile_json}]} =
             DataFrame.select(df, ["profile"]) |> DataFrame.collect()

    assert is_binary(profile_json)
    assert profile_json =~ "\"attrs\":{\"x\":\"1\"}"
  end

  test "lit map with array values no longer fails", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT 1 AS id")
      |> DataFrame.with_column(
        "data",
        SparkEx.Functions.lit(%{"scores" => [1, 2, 3], "tags" => ["a", "b"]})
      )

    assert {:ok, [%{"data" => data}]} = DataFrame.select(df, ["data"]) |> DataFrame.collect()

    by_key = Map.new(data, fn %{"key" => key, "value" => value} -> {key, value} end)
    assert by_key["tags"] == ["a", "b"]
    assert by_key["scores"] == ["1", "2", "3"]
  end

  test "lit nested map keeps inner map payload", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT 1 AS id")
      |> DataFrame.with_column(
        "profile",
        SparkEx.Functions.lit(%{"name" => "Alice", "addr" => %{"city" => "NYC"}})
      )

    assert {:ok, [%{"profile" => profile}]} = DataFrame.select(df, ["profile"]) |> DataFrame.collect()
    by_key = Map.new(profile, fn %{"key" => key, "value" => value} -> {key, value} end)

    assert by_key["name"] == "Alice"
    assert by_key["addr"] == "{\"city\":\"NYC\"}"
  end

  test "create_dataframe complex dtypes preserve declared schema", %{session: session} do
    rows = [%{"id" => 1, "tags" => ["a"], "meta" => %{"k" => "v"}, "info" => %{"name" => "test"}}]

    assert {:ok, df} =
             SparkEx.create_dataframe(session, rows,
               schema: "id INT, tags ARRAY<STRING>, meta MAP<STRING, STRING>, info STRUCT<name: STRING>"
             )

    assert {:ok, dtypes} = DataFrame.dtypes(df)
    assert {"tags", "ARRAY<STRING>"} in dtypes
    assert {"meta", "MAP<STRING, STRING>"} in dtypes
    assert {"info", "STRUCT<name: STRING>"} in dtypes
  end

  test "create_dataframe supports binary payloads", %{session: session} do
    assert {:ok, df} =
             SparkEx.create_dataframe(session, [%{"id" => 1, "data" => <<0xDE, 0xAD, 0xBE, 0xEF>>}],
               schema: "id INT, data BINARY"
             )

    assert {:ok, [%{"data" => data}]} = DataFrame.select(df, ["data"]) |> DataFrame.collect()
    assert data == <<0xDE, 0xAD, 0xBE, 0xEF>>
  end

  test "joining two create_dataframe results works", %{session: session} do
    assert {:ok, left} =
             SparkEx.create_dataframe(session, [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}],
               schema: "id INT, name STRING"
             )

    assert {:ok, right} =
             SparkEx.create_dataframe(session, [%{"id" => 1, "score" => 95.5}, %{"id" => 2, "score" => 87.3}],
               schema: "id INT, score DOUBLE"
             )

    assert {:ok, rows} = DataFrame.join(left, right, ["id"]) |> DataFrame.collect()
    assert Enum.find(rows, &(&1["id"] == 1))["score"] == 95.5
  end

  test "to_explorer keeps complex columns as native containers", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT array(1,2,3) AS arr, map('a',1,'b',2) AS m, named_struct('name','alice','age',30) AS st"
      )

    assert {:ok, explorer_df} = DataFrame.to_explorer(df)
    dtypes = Explorer.DataFrame.dtypes(explorer_df)

    assert dtypes["arr"] != :string
    assert dtypes["m"] != :string
    assert dtypes["st"] != :string
  end

  test "lit nested arrays preserves inner values", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT 1 AS id")
      |> DataFrame.with_column("nested", SparkEx.Functions.lit([[1, 2], [3, 4]]))

    assert {:ok, [%{"nested" => nested}]} =
             DataFrame.select(df, ["nested"]) |> DataFrame.collect()

    assert nested == [[1, 2], [3, 4]]
  end
end
