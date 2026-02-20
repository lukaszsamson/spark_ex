defmodule SparkEx.Integration.JobCancellationTagGapsTest do
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

  # ── Tag API lifecycle ──

  describe "tag API lifecycle" do
    test "add_tag and get_tags", %{session: session} do
      SparkEx.clear_tags(session)

      :ok = SparkEx.add_tag(session, "tag-alpha")
      tags = SparkEx.get_tags(session)
      assert "tag-alpha" in tags
    end

    test "add multiple tags", %{session: session} do
      SparkEx.clear_tags(session)

      :ok = SparkEx.add_tag(session, "tag-1")
      :ok = SparkEx.add_tag(session, "tag-2")
      :ok = SparkEx.add_tag(session, "tag-3")

      tags = SparkEx.get_tags(session)
      assert "tag-1" in tags
      assert "tag-2" in tags
      assert "tag-3" in tags
    end

    test "remove_tag removes specific tag", %{session: session} do
      SparkEx.clear_tags(session)

      :ok = SparkEx.add_tag(session, "keep-me")
      :ok = SparkEx.add_tag(session, "remove-me")
      :ok = SparkEx.remove_tag(session, "remove-me")

      tags = SparkEx.get_tags(session)
      assert "keep-me" in tags
      refute "remove-me" in tags
    end

    test "clear_tags removes all tags", %{session: session} do
      :ok = SparkEx.add_tag(session, "a")
      :ok = SparkEx.add_tag(session, "b")
      :ok = SparkEx.clear_tags(session)

      tags = SparkEx.get_tags(session)
      assert tags == []
    end

    test "get_tags returns empty list initially", %{session: _session} do
      {:ok, fresh_session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(fresh_session)

      on_exit(fn ->
        if Process.alive?(fresh_session), do: SparkEx.Session.stop(fresh_session)
      end)

      tags = SparkEx.get_tags(fresh_session)
      assert tags == []
    end

    test "duplicate add_tag is idempotent or accumulates", %{session: session} do
      SparkEx.clear_tags(session)

      :ok = SparkEx.add_tag(session, "dup")
      :ok = SparkEx.add_tag(session, "dup")

      tags = SparkEx.get_tags(session)
      # Should have at least one "dup" tag
      assert "dup" in tags
    end
  end

  # ── Tag with operations ──

  describe "tag with operations" do
    test "tagged operations execute successfully", %{session: session} do
      SparkEx.clear_tags(session)
      :ok = SparkEx.add_tag(session, "batch-job")

      df = SparkEx.range(session, 10)
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 10

      SparkEx.clear_tags(session)
    end

    test "tags do not affect query results", %{session: session} do
      SparkEx.clear_tags(session)

      # Collect without tags
      df1 = SparkEx.sql(session, "SELECT 42 AS answer")
      {:ok, [row1]} = DataFrame.collect(df1)

      # Collect with tags
      :ok = SparkEx.add_tag(session, "tagged-query")
      df2 = SparkEx.sql(session, "SELECT 42 AS answer")
      {:ok, [row2]} = DataFrame.collect(df2)

      assert row1["answer"] == row2["answer"]

      SparkEx.clear_tags(session)
    end
  end

  # ── Interrupt semantics ──

  describe "interrupt semantics" do
    test "interrupt_all on idle session returns empty list", %{session: session} do
      assert {:ok, []} = SparkEx.interrupt_all(session)
    end

    test "interrupt_tag on nonexistent tag returns empty list", %{session: session} do
      assert {:ok, []} = SparkEx.interrupt_tag(session, "nonexistent-tag-#{System.unique_integer([:positive])}")
    end

    test "interrupt_operation on nonexistent operation returns empty list", %{session: session} do
      assert {:ok, []} = SparkEx.interrupt_operation(session, "nonexistent-op-#{System.unique_integer([:positive])}")
    end

    test "concurrent queries on same session complete independently", %{session: session} do
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            df = SparkEx.sql(session, "SELECT #{i} AS val")
            DataFrame.collect(df)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, fn {:ok, [_]} -> true; _ -> false end)

      values =
        results
        |> Enum.map(fn {:ok, [row]} -> row["val"] end)
        |> Enum.sort()

      assert values == Enum.to_list(1..10)
    end
  end

  # ── Tag isolation between sessions ──

  describe "tag isolation between sessions" do
    test "tags in one session don't leak to another", %{session: session} do
      {:ok, session2} = SparkEx.connect(url: @spark_remote)
      Process.unlink(session2)

      on_exit(fn ->
        if Process.alive?(session2), do: SparkEx.Session.stop(session2)
      end)

      SparkEx.clear_tags(session)
      SparkEx.clear_tags(session2)

      :ok = SparkEx.add_tag(session, "session1-only")

      tags1 = SparkEx.get_tags(session)
      tags2 = SparkEx.get_tags(session2)

      assert "session1-only" in tags1
      refute "session1-only" in tags2
    end
  end
end
