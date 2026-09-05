defmodule SparkEx.Integration.Wave6Spark35Test do
  @moduledoc """
  Wave 6 integration coverage.

  T-34: Spark 3.5 servers reject 4.x-only relations (LateralJoin, AsOfJoin,
  Transpose) with "Expected Relation to be set, but is empty." wherever they
  appear in the plan. The session rewrites them at any depth, so these tests
  deliberately put the unsupported node *under* a select / filter / count /
  schema and must pass unchanged on both 3.5 (fallback path) and 4.x (native).

  T-64: `create_dataframe/3` reads the server's localRelation configs and
  caches payloads at or above `spark.sql.session.localRelationCacheThreshold`
  without any explicit `:cache_threshold` option.
  """
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp lateral(session) do
    left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
    right = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(rid, tag)")

    DataFrame.lateral_join(left, right, Column.eq(Functions.col("id"), Functions.col("rid")))
  end

  describe "T-34: lateral join below other operators" do
    test "under select", %{session: session} do
      df =
        lateral(session)
        |> DataFrame.select(["id", "tag"])
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, &{&1["id"], &1["tag"]}) == [{1, "a"}, {2, "b"}]
    end

    test "under filter", %{session: session} do
      df =
        lateral(session)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(1)))
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, &{&1["id"], &1["rid"]}) == [{2, 2}]
    end

    test "count and schema under select", %{session: session} do
      df = lateral(session) |> DataFrame.select(["id"])

      assert {:ok, 2} = DataFrame.count(df)

      assert {:ok, %Spark.Connect.DataType{kind: {:struct, %{fields: fields}}}} =
               DataFrame.schema(df)

      assert Enum.map(fields, & &1.name) == ["id"]
    end
  end

  describe "as-of join below other operators" do
    # Rows where a plain equi-join and a backward as-of join disagree: id 2 has
    # two right candidates (19 and 25); the as-of match for t1=20 is 19 only.
    test "keeps as-of semantics on 4.x and refuses clearly on 3.5", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id_l, t1)")

      right =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 9), (2, 19), (2, 25), (3, 29) AS t(id_r, t2)"
        )

      df =
        DataFrame.as_of_join(left, right, Functions.col("t1"), Functions.col("t2"),
          on: Column.eq(Functions.col("id_l"), Functions.col("id_r"))
        )
        |> DataFrame.select(["id_l", "t1", "t2"])
        |> DataFrame.filter(Column.gte(Functions.col("id_l"), Functions.lit(2)))
        |> DataFrame.order_by(["id_l"])

      {:ok, version} = SparkEx.spark_version(session)

      if String.starts_with?(version, "3.") do
        assert {:error, {:unsupported_on_server, :as_of_join, _}} = DataFrame.collect(df)
        assert {:error, {:unsupported_on_server, :as_of_join, _}} = DataFrame.count(df)
        assert {:error, {:unsupported_on_server, :as_of_join, _}} = DataFrame.schema(df)
      else
        assert {:ok, rows} = DataFrame.collect(df)
        assert Enum.map(rows, &{&1["id_l"], &1["t1"], &1["t2"]}) == [{2, 20, 19}, {3, 30, 29}]
        assert {:ok, 2} = DataFrame.count(df)
      end
    end
  end

  describe "T-34: transpose below other operators" do
    test "under select", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT * FROM VALUES ('a', 1), ('b', 2) AS t(k, v)")
        |> DataFrame.transpose(index_column: "k")
        |> DataFrame.select(["a", "b"])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert {row["a"], row["b"]} == {1, 2}
      assert {:ok, 1} = DataFrame.count(df)
    end
  end

  describe "T-64: server-driven local relation cache threshold" do
    @describetag min_spark: "4.1"

    test "payload above the server threshold is cached without options", %{session: session} do
      {:ok, [{_, threshold}]} =
        SparkEx.config_get_option(session, ["spark.sql.session.localRelationCacheThreshold"])

      threshold = String.to_integer(threshold)

      # Keep the test bounded: a cluster with an unusually large threshold
      # cannot be exercised cheaply, so the case degrades to a no-op.
      if threshold > 64 * 1024 * 1024 do
        :ok
      else
        assert_threshold_cached(session, threshold)
      end
    end

    defp assert_threshold_cached(session, threshold) do
      # ~40 bytes/row of string payload -> comfortably above the threshold.
      n = div(threshold, 32) + 1000

      data =
        Explorer.DataFrame.new(%{
          "id" => Enum.to_list(1..n),
          "payload" => Enum.map(1..n, &(String.duplicate("x", 30) <> Integer.to_string(&1)))
        })

      {:ok, df} = SparkEx.create_dataframe(session, data)

      assert {:chunked_cached_local_relation, hashes, _schema_hash} =
               SparkEx.Test.PlanHelpers.unwrap(df.plan)

      assert hashes != []
      assert {:ok, ^n} = DataFrame.count(df)

      assert {:ok, rows} =
               df
               |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(n - 3)))
               |> DataFrame.order_by(["id"])
               |> DataFrame.collect()

      assert Enum.map(rows, & &1["id"]) == [n - 2, n - 1, n]
      assert List.last(rows)["payload"] == String.duplicate("x", 30) <> Integer.to_string(n)
    end

    test "a payload exactly at the threshold is cached (>= boundary)", %{session: session} do
      data = Explorer.DataFrame.new(%{"id" => [1, 2, 3]})
      {:ok, ipc} = Explorer.DataFrame.dump_ipc_stream(data)

      {:ok, at} = SparkEx.create_dataframe(session, data, cache_threshold: byte_size(ipc))
      assert {:chunked_cached_local_relation, _, _} = SparkEx.Test.PlanHelpers.unwrap(at.plan)

      {:ok, below} = SparkEx.create_dataframe(session, data, cache_threshold: byte_size(ipc) + 1)
      assert {:local_relation, _, _} = SparkEx.Test.PlanHelpers.unwrap(below.plan)

      assert {:ok, rows} = at |> DataFrame.order_by(["id"]) |> DataFrame.collect()
      assert Enum.map(rows, & &1["id"]) == [1, 2, 3]
    end

    test "row cap splits the payload into several cached chunks", %{session: session} do
      data = Explorer.DataFrame.new(%{"id" => Enum.to_list(1..10)})

      {:ok, df} =
        SparkEx.create_dataframe(session, data, cache_threshold: 0, cache_chunk_rows: 4)

      assert {:chunked_cached_local_relation, hashes, _} =
               SparkEx.Test.PlanHelpers.unwrap(df.plan)

      assert length(hashes) == 3
      assert {:ok, 10} = DataFrame.count(df)
    end
  end
end
