defmodule SparkEx.Integration.CreateDataFrameTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "create_dataframe from Explorer.DataFrame" do
    test "creates DataFrame from small Explorer.DataFrame and collects", %{session: session} do
      explorer_df =
        Explorer.DataFrame.new(%{
          "id" => [1, 2, 3],
          "name" => ["Alice", "Bob", "Charlie"]
        })

      assert {:ok, df} = SparkEx.create_dataframe(session, explorer_df)
      assert %DataFrame{} = df

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      ordered = Enum.sort_by(rows, & &1["id"])
      assert Enum.map(ordered, & &1["id"]) == [1, 2, 3]
      assert Enum.map(ordered, & &1["name"]) == ["Alice", "Bob", "Charlie"]
    end

    test "creates DataFrame and applies filter", %{session: session} do
      import SparkEx.Functions, only: [col: 1, lit: 1]

      explorer_df =
        Explorer.DataFrame.new(%{
          "id" => [1, 2, 3, 4, 5],
          "value" => [10, 20, 30, 40, 50]
        })

      {:ok, df} = SparkEx.create_dataframe(session, explorer_df)

      filtered =
        df
        |> DataFrame.filter(col("value") |> SparkEx.Column.gt(lit(25)))

      assert {:ok, rows} = DataFrame.collect(filtered)
      assert length(rows) == 3
      assert Enum.all?(rows, fn row -> row["value"] > 25 end)
    end

    test "creates DataFrame and applies select", %{session: session} do
      import SparkEx.Functions, only: [col: 1]

      explorer_df =
        Explorer.DataFrame.new(%{
          "id" => [1, 2],
          "name" => ["a", "b"],
          "extra" => [true, false]
        })

      {:ok, df} = SparkEx.create_dataframe(session, explorer_df)

      selected = DataFrame.select(df, [col("id"), col("name")])
      assert {:ok, rows} = DataFrame.collect(selected)
      assert length(rows) == 2
      assert Map.keys(hd(rows)) |> Enum.sort() == ["id", "name"]
    end

    test "creates DataFrame and gets schema", %{session: session} do
      explorer_df =
        Explorer.DataFrame.new(%{
          "id" => [1, 2],
          "score" => [1.5, 2.5],
          "flag" => [true, false]
        })

      {:ok, df} = SparkEx.create_dataframe(session, explorer_df)
      assert {:ok, schema} = DataFrame.schema(df)
      assert is_struct(schema)
    end

    test "creates DataFrame and gets count", %{session: session} do
      explorer_df = Explorer.DataFrame.new(%{"x" => Enum.to_list(1..100)})

      {:ok, df} = SparkEx.create_dataframe(session, explorer_df)
      assert {:ok, 100} = DataFrame.count(df)
    end

    test "creates DataFrame with explicit schema", %{session: session} do
      explorer_df = Explorer.DataFrame.new(%{"id" => [1, 2], "name" => ["a", "b"]})

      {:ok, df} = SparkEx.create_dataframe(session, explorer_df, schema: "id INT, name STRING")
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end

    test "creates DataFrame from list column without LargeList remote failure", %{
      session: session
    } do
      explorer_df =
        Explorer.DataFrame.new(%{
          "id" => [1, 2],
          "tags" => [["a", "b"], ["c"]]
        })

      {:ok, df} =
        SparkEx.create_dataframe(session, explorer_df, schema: "id INT, tags ARRAY<STRING>")

      assert {:ok, rows} = DataFrame.collect(df)
      ordered = Enum.sort_by(rows, & &1["id"])
      assert Enum.map(ordered, & &1["tags"]) == [["a", "b"], ["c"]]
    end
  end

  describe "create_dataframe from list of maps" do
    test "creates DataFrame from list of maps", %{session: session} do
      data = [
        %{"id" => 1, "name" => "Alice"},
        %{"id" => 2, "name" => "Bob"}
      ]

      assert {:ok, df} = SparkEx.create_dataframe(session, data)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end

    test "creates DataFrame from list of maps with schema", %{session: session} do
      data = [%{"id" => 1, "value" => 100}]

      {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT, value INT")
      {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 1
      assert hd(rows)["id"] == 1
    end

    test "includes keys that appear after first row", %{session: session} do
      data = [
        %{"id" => 1},
        %{"id" => 2, "name" => "Bob"}
      ]

      {:ok, df} = SparkEx.create_dataframe(session, data)
      {:ok, rows} = DataFrame.collect(df)

      assert length(rows) == 2
      assert Enum.all?(rows, fn row -> Map.has_key?(row, "id") and Map.has_key?(row, "name") end)
      assert Enum.find(rows, fn row -> row["id"] == 1 end)["name"] == nil
      assert Enum.find(rows, fn row -> row["id"] == 2 end)["name"] == "Bob"
    end

    test "returns error for empty list without schema", %{session: session} do
      assert {:error, {:invalid_data, _}} = SparkEx.create_dataframe(session, [])
    end
  end

  describe "create_dataframe from column map" do
    test "creates DataFrame from column-oriented map", %{session: session} do
      data = %{"id" => [1, 2, 3], "name" => ["a", "b", "c"]}

      {:ok, df} = SparkEx.create_dataframe(session, data)
      {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
    end

    test "creates DataFrame from column map with arrays", %{session: session} do
      data = %{"id" => [1, 2], "tags" => [["x"], ["y", "z"]]}

      {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT, tags ARRAY<STRING>")
      {:ok, rows} = DataFrame.collect(df)
      ordered = Enum.sort_by(rows, & &1["id"])
      assert Enum.map(ordered, & &1["tags"]) == [["x"], ["y", "z"]]
    end
  end

  describe "join with local DataFrame" do
    test "joins local DataFrame with SQL DataFrame", %{session: session} do
      # Create local employees
      employees =
        Explorer.DataFrame.new(%{
          "id" => [1, 2, 3],
          "name" => ["Alice", "Bob", "Charlie"],
          "dept_id" => [10, 20, 10]
        })

      {:ok, emp_df} = SparkEx.create_dataframe(session, employees)

      # Create departments from SQL
      dept_df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (10, 'Engineering'), (20, 'HR') AS t(dept_id, dept_name)"
        )

      # Join
      joined = DataFrame.join(emp_df, dept_df, ["dept_id"], :inner)
      {:ok, rows} = DataFrame.collect(joined)

      assert length(rows) == 3

      assert Enum.any?(rows, fn row ->
               row["name"] == "Alice" and row["dept_name"] == "Engineering"
             end)
    end
  end

  describe "cached (large) local DataFrame" do
    @describetag min_spark: "4.1"
    test "creates DataFrame from data exceeding cache threshold", %{session: session} do
      # Create data that exceeds a tiny cache threshold
      n = 1000

      data =
        Explorer.DataFrame.new(%{"id" => Enum.to_list(1..n), "val" => Enum.map(1..n, &(&1 * 10))})

      # Use a very small cache threshold to force the cached path
      {:ok, df} = SparkEx.create_dataframe(session, data, cache_threshold: 100)

      {:ok, count} = DataFrame.count(df)
      assert count == n
    end

    test "cached DataFrame supports filter", %{session: session} do
      import SparkEx.Functions, only: [col: 1, lit: 1]

      n = 500
      data = Explorer.DataFrame.new(%{"x" => Enum.to_list(1..n)})

      {:ok, df} = SparkEx.create_dataframe(session, data, cache_threshold: 100)
      filtered = DataFrame.filter(df, col("x") |> SparkEx.Column.gt(lit(400)))

      {:ok, rows} = DataFrame.collect(filtered)
      assert length(rows) == 100
    end

    test "list payload works when cache_threshold is low", %{session: session} do
      data =
        Explorer.DataFrame.new(%{
          "id" => Enum.to_list(1..200),
          "tags" => Enum.map(1..200, fn i -> [Integer.to_string(i)] end)
        })

      {:ok, df} =
        SparkEx.create_dataframe(session, data,
          schema: "id INT, tags ARRAY<STRING>",
          cache_threshold: 100
        )

      {:ok, count} = DataFrame.count(df)
      assert count == 200
    end

    test "reuses already cached artifacts for identical payload", %{session: session} do
      handler_id = "create-df-cache-dedup-#{System.unique_integer([:positive])}"
      parent = self()
      ref = make_ref()

      :ok =
        :telemetry.attach_many(
          handler_id,
          [[:spark_ex, :rpc, :start]],
          fn event, _measurements, metadata, {pid, marker} ->
            if metadata[:rpc] == :add_artifacts do
              send(pid, {:telemetry, marker, event, metadata[:rpc]})
            end
          end,
          {parent, ref}
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      n = 2_000

      data =
        Explorer.DataFrame.new(%{
          "id" => Enum.to_list(1..n),
          "payload" =>
            Enum.map(1..n, fn i -> String.duplicate("x", 40) <> Integer.to_string(i) end)
        })

      {:ok, df1} = SparkEx.create_dataframe(session, data, cache_threshold: 100)
      {:ok, _} = DataFrame.count(df1)
      first_upload_calls = drain_add_artifacts_rpc_starts(ref)
      assert first_upload_calls >= 1

      {:ok, df2} = SparkEx.create_dataframe(session, data, cache_threshold: 100)
      {:ok, _} = DataFrame.count(df2)
      second_upload_calls = drain_add_artifacts_rpc_starts(ref)
      assert second_upload_calls == 0
    end
  end

  describe "chunked upload path" do
    @describetag min_spark: "4.1"
    test "exercises chunked artifact upload for large payloads", %{session: session} do
      # Create enough data to trigger chunked artifact upload (> 32KB chunks)
      n = 5000

      data =
        Explorer.DataFrame.new(%{
          "id" => Enum.to_list(1..n),
          "payload" =>
            Enum.map(1..n, fn i -> String.duplicate("x", 50) <> Integer.to_string(i) end)
        })

      # Force cache path with low threshold
      {:ok, df} = SparkEx.create_dataframe(session, data, cache_threshold: 100)

      {:ok, count} = DataFrame.count(df)
      assert count == n

      {:ok, rows} = DataFrame.take(df, 5)
      assert length(rows) == 5
    end
  end

  defp drain_add_artifacts_rpc_starts(ref, count \\ 0) do
    receive do
      {:telemetry, ^ref, [:spark_ex, :rpc, :start], :add_artifacts} ->
        drain_add_artifacts_rpc_starts(ref, count + 1)
    after
      150 -> count
    end
  end
end
