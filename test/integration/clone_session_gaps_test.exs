defmodule SparkEx.Integration.CloneSessionGapsTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.1"

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

  describe "clone with custom target session ID" do
    test "custom UUID is accepted", %{session: session} do
      # Establish server-side session before cloning
      {:ok, _} = SparkEx.spark_version(session)

      custom_id = SparkEx.Internal.UUID.generate_v4()
      {:ok, clone} = SparkEx.clone_session(session, custom_id)
      Process.unlink(clone)

      on_exit(fn ->
        if Process.alive?(clone), do: SparkEx.Session.stop(clone)
      end)

      state = SparkEx.Session.get_state(clone)
      assert state.session_id == custom_id

      # Clone should be functional
      df = SparkEx.range(clone, 3)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
    end
  end

  describe "clone with invalid UUID format" do
    test "invalid UUID is rejected or coerced", %{session: session} do
      # Establish server-side session before cloning
      {:ok, _} = SparkEx.spark_version(session)

      result = SparkEx.clone_session(session, "not-a-uuid")

      case result do
        {:error, _} ->
          # Expected: invalid format rejected
          assert true

        {:ok, clone} ->
          # Some implementations accept any string; verify it's still functional
          Process.unlink(clone)

          on_exit(fn ->
            if Process.alive?(clone), do: SparkEx.Session.stop(clone)
          end)

          df = SparkEx.range(clone, 1)
          assert {:ok, _} = DataFrame.collect(df)
      end
    end
  end

  describe "temp view cloning" do
    test "temp views from parent are visible in clone", %{session: session} do
      # Create temp view in parent session
      SparkEx.sql(
        session,
        "CREATE OR REPLACE TEMPORARY VIEW clone_test_view AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)"
      )
      |> DataFrame.collect()

      # Clone the session
      {:ok, clone} = SparkEx.clone_session(session)
      Process.unlink(clone)

      on_exit(fn ->
        if Process.alive?(clone), do: SparkEx.Session.stop(clone)
      end)

      # Temp view should be visible in clone
      df = SparkEx.sql(clone, "SELECT * FROM clone_test_view ORDER BY id")
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.map(rows, & &1["id"]) == [1, 2]
    end
  end

  describe "temp view independence after clone" do
    test "new temp view in clone does not affect parent", %{session: session} do
      # Establish server-side session before cloning
      {:ok, _} = SparkEx.spark_version(session)

      {:ok, clone} = SparkEx.clone_session(session)
      Process.unlink(clone)

      on_exit(fn ->
        if Process.alive?(clone), do: SparkEx.Session.stop(clone)
      end)

      # Create a temp view only in the clone
      SparkEx.sql(
        clone,
        "CREATE OR REPLACE TEMPORARY VIEW clone_only_view AS SELECT 42 AS answer"
      )
      |> DataFrame.collect()

      # Should be accessible from the clone
      df = SparkEx.sql(clone, "SELECT * FROM clone_only_view")
      assert {:ok, [%{"answer" => 42}]} = DataFrame.collect(df)

      # Should NOT be accessible from the parent
      result =
        SparkEx.sql(session, "SELECT * FROM clone_only_view")
        |> DataFrame.collect()

      assert {:error, _} = result
    end

    test "dropping temp view in clone does not affect parent", %{session: session} do
      # Create temp view in parent
      SparkEx.sql(
        session,
        "CREATE OR REPLACE TEMPORARY VIEW shared_view AS SELECT 1 AS v"
      )
      |> DataFrame.collect()

      # Clone and verify view is accessible
      {:ok, clone} = SparkEx.clone_session(session)
      Process.unlink(clone)

      on_exit(fn ->
        if Process.alive?(clone), do: SparkEx.Session.stop(clone)
      end)

      # Drop view in clone
      SparkEx.sql(clone, "DROP VIEW IF EXISTS shared_view") |> DataFrame.collect()

      # Parent should still have the view
      df = SparkEx.sql(session, "SELECT * FROM shared_view")
      assert {:ok, [%{"v" => 1}]} = DataFrame.collect(df)
    end
  end

  describe "config independence after clone" do
    test "config change in clone does not affect parent", %{session: session} do
      SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "100"}])

      {:ok, clone} = SparkEx.clone_session(session)
      Process.unlink(clone)

      on_exit(fn ->
        if Process.alive?(clone), do: SparkEx.Session.stop(clone)
      end)

      # Verify both have the same initial config
      assert {:ok, [{"spark.sql.shuffle.partitions", "100"}]} =
               SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])

      assert {:ok, [{"spark.sql.shuffle.partitions", "100"}]} =
               SparkEx.config_get(clone, ["spark.sql.shuffle.partitions"])

      # Change clone config
      SparkEx.config_set(clone, [{"spark.sql.shuffle.partitions", "50"}])

      # Parent should be unaffected
      assert {:ok, [{"spark.sql.shuffle.partitions", "100"}]} =
               SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])

      assert {:ok, [{"spark.sql.shuffle.partitions", "50"}]} =
               SparkEx.config_get(clone, ["spark.sql.shuffle.partitions"])
    end
  end
end
