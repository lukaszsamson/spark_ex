defmodule SparkEx.Integration.ArtifactsExtendedTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  test "add_files uploads and reports status" do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    tmp_path =
      Path.join(System.tmp_dir!(), "spark_ex_artifact_#{System.unique_integer([:positive])}.txt")

    File.write!(tmp_path, "artifact content")

    {:ok, _} = SparkEx.Artifacts.add_files(session, [tmp_path])

    name = Path.basename(tmp_path)
    assert {:ok, statuses} = SparkEx.artifact_status(session, ["files/#{name}"])
    assert statuses == %{"files/#{name}" => false}
  end
end
