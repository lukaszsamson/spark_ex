defmodule SparkEx.Integration.Wave4CollectPathsTest do
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

  describe "T-33 to_explorer preflight" do
    test "duplicate column names decode instead of panicking", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS a, 2 AS a")

      assert {:ok, edf} = DataFrame.to_explorer(df)
      assert Explorer.DataFrame.n_rows(edf) == 1
      assert length(Explorer.DataFrame.names(edf)) == 2

      values =
        edf
        |> Explorer.DataFrame.names()
        |> Enum.map(fn name ->
          edf |> Explorer.DataFrame.pull(name) |> Explorer.Series.to_list()
        end)

      assert values == [[1], [2]]
    end

    test "nested map columns fall back to a JSON/string projection", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT named_struct('x', 1) AS st, map('a', map('b', 1)) AS mp"
        )

      assert {:ok, edf} = DataFrame.to_explorer(df)
      assert Explorer.DataFrame.n_rows(edf) == 1
    end
  end

  describe "T-33 to_explorer keeps native containers" do
    test "top-level struct/map/array columns are not JSON-projected", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT array(1,2,3) AS arr, map('a',1) AS m, named_struct('x', 1) AS st"
        )

      assert {:ok, edf} = DataFrame.to_explorer(df)
      dtypes = Explorer.DataFrame.dtypes(edf)

      assert dtypes["arr"] == {:list, {:s, 32}}
      assert {:list, {:struct, _}} = dtypes["m"]
      assert {:struct, _} = dtypes["st"]
    end
  end

  describe "T-33 to_local_iterator preflight" do
    test "map_format: :map is honoured on JSON-projected plans", %{session: session} do
      df = SparkEx.sql(session, "SELECT named_struct('x', 1) AS st, map('a', 1) AS mp")

      assert {:ok, stream} = DataFrame.to_local_iterator(df, map_format: :map)
      assert [{:ok, row}] = Enum.to_list(stream)
      assert row["mp"] == %{"a" => 1}
      assert row["st"] == %{"x" => 1}

      assert {:ok, stream} = DataFrame.to_local_iterator(df)
      assert [{:ok, row}] = Enum.to_list(stream)
      assert row["mp"] == [%{"key" => "a", "value" => 1}]
    end

    test "JSON-projected DECIMAL(38,6) keeps all digits and DOUBLE stays a float",
         %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT named_struct('d', CAST('123456789012345678.123456' AS DECIMAL(38,6)), " <>
            "'f', CAST(1.5 AS DOUBLE)) AS st, map('a', 1) AS mp"
        )

      assert {:ok, stream} = DataFrame.to_local_iterator(df)
      assert [{:ok, %{"st" => %{"d" => d, "f" => f}}}] = Enum.to_list(stream)
      assert Decimal.equal?(d, Decimal.new("123456789012345678.123456"))
      assert Decimal.to_string(d, :normal) == "123456789012345678.123456"
      assert f === 1.5

      assert {:ok, [%{"st" => %{"d" => d2}}]} = DataFrame.collect(df)
      assert Decimal.to_string(d2, :normal) == "123456789012345678.123456"
    end

    test "duplicate column names stream instead of failing", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS a, 2 AS a")

      assert {:ok, stream} = DataFrame.to_local_iterator(df)
      rows = Enum.to_list(stream)

      assert [{:ok, row}] = rows
      # The preflight renamed the duplicate column, so both values survive.
      assert Enum.count(row) == 2
      assert Enum.sort(Enum.map(row, fn {_k, v} -> v end)) == [1, 2]
    end

    test "JSON-projected complex columns decode per batch", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT named_struct('d', DATE'2024-01-02') AS st, map('a', map('b', 1)) AS mp"
        )

      assert {:ok, stream} = DataFrame.to_local_iterator(df)
      assert [{:ok, row}] = Enum.to_list(stream)
      assert %{"d" => ~D[2024-01-02]} = row["st"]
      assert [%{"key" => "a", "value" => [%{"key" => "b", "value" => 1}]}] = row["mp"]
    end

    test "is lazy: the stream is not consumed until enumerated", %{session: session} do
      df = SparkEx.sql(session, "SELECT id FROM range(5)")

      assert {:ok, stream} = DataFrame.to_local_iterator(df)
      refute is_list(stream)

      assert stream |> Enum.take(2) |> length() == 2
    end
  end

  describe "T-32 JSON fallback scalar coercion" do
    test "nested decimal/timestamp/date/binary decode to Elixir terms", %{session: session} do
      # A top-level STRUCT alongside a MAP column forces the JSON projection
      # fallback (schema_has_struct_and_map?), so the nested scalars arrive as
      # JSON and must be coerced back.
      df =
        SparkEx.sql(
          session,
          """
          SELECT named_struct(
                   'dec', CAST(1.25 AS DECIMAL(10,2)),
                   'ts', CAST('2024-01-02 03:04:05' AS TIMESTAMP),
                   'ntz', CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ),
                   'd', DATE'2024-01-02',
                   'b', CAST('hi' AS BINARY),
                   'txt', 'plain'
                 ) AS st,
                 map('a', 1) AS mp
          """
        )

      assert {:ok, [row]} = DataFrame.collect(df, map_format: :map)
      st = row["st"]

      assert Decimal.equal?(st["dec"], Decimal.new("1.25"))
      assert %DateTime{} = st["ts"]
      assert %NaiveDateTime{} = st["ntz"]
      assert NaiveDateTime.to_date(st["ntz"]) == ~D[2024-01-02]
      assert st["d"] == ~D[2024-01-02]
      assert st["b"] == "hi"
      assert st["txt"] == "plain"
    end
  end
end
