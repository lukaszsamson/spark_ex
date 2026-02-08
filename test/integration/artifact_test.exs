defmodule SparkEx.Integration.ArtifactTest do
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

  describe "artifact_status" do
    test "reports non-existent artifacts as not existing", %{session: session} do
      assert {:ok, statuses} =
               SparkEx.artifact_status(session, ["nonexistent-artifact.jar"])

      assert is_map(statuses)
      assert statuses["nonexistent-artifact.jar"] == false
    end

    test "handles multiple artifact names", %{session: session} do
      names = ["file1.jar", "file2.jar", "file3.jar"]
      assert {:ok, statuses} = SparkEx.artifact_status(session, names)

      assert map_size(statuses) == 3

      Enum.each(names, fn name ->
        assert Map.has_key?(statuses, name)
        assert is_boolean(statuses[name])
      end)
    end

    test "handles empty names list", %{session: session} do
      assert {:ok, statuses} = SparkEx.artifact_status(session, [])
      assert statuses == %{}
    end
  end

  describe "add_artifacts" do
    test "uploads a small artifact", %{session: session} do
      data = "test artifact content for M7"
      artifact_name = "test-artifact-#{System.unique_integer([:positive])}.txt"

      assert {:ok, summaries} = SparkEx.add_artifacts(session, [{artifact_name, data}])
      assert length(summaries) == 1
      {name, crc_ok} = hd(summaries)
      assert name == artifact_name
      assert crc_ok == true
    end

    test "uploads multiple artifacts", %{session: session} do
      artifacts =
        for i <- 1..3 do
          name = "multi-artifact-#{i}-#{System.unique_integer([:positive])}.txt"
          {name, "content for artifact #{i}"}
        end

      assert {:ok, summaries} = SparkEx.add_artifacts(session, artifacts)
      assert length(summaries) == 3

      Enum.each(summaries, fn {_name, crc_ok} ->
        assert crc_ok == true
      end)
    end

    test "uploaded artifact can be queried via status API", %{session: session} do
      artifact_name = "status-check-#{System.unique_integer([:positive])}.txt"
      data = "checking existence"

      assert {:ok, _summaries} = SparkEx.add_artifacts(session, [{artifact_name, data}])
      assert {:ok, statuses} = SparkEx.artifact_status(session, [artifact_name])
      assert is_boolean(statuses[artifact_name])
    end

    test "uploads chunked artifact payloads larger than chunk threshold", %{session: session} do
      artifact_name = "large-artifact-#{System.unique_integer([:positive])}.bin"
      data = :binary.copy(<<1, 2, 3, 4>>, 20_000)

      assert byte_size(data) > 32 * 1024
      assert {:ok, summaries} = SparkEx.add_artifacts(session, [{artifact_name, data}])
      assert [{^artifact_name, true}] = summaries
      assert {:ok, statuses} = SparkEx.artifact_status(session, [artifact_name])
      assert is_boolean(statuses[artifact_name])
    end
  end
end
