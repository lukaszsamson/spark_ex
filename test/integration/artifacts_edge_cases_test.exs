defmodule SparkEx.Integration.ArtifactsEdgeCasesTest do
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

  test "add_files reports missing path error", %{session: session} do
    missing = "/tmp/does_not_exist_#{System.unique_integer([:positive])}"
    assert {:error, _} = SparkEx.Artifacts.add_files(session, [missing])
  end

  test "add_archives accepts zip artifacts", %{session: session} do
    tmp_dir = "/tmp/spark_ex_artifacts_#{System.unique_integer([:positive])}"
    File.mkdir_p!(tmp_dir)

    file_path = Path.join(tmp_dir, "data.txt")
    File.write!(file_path, "hello")

    archive_path = Path.join(tmp_dir, "data.zip")

    {:ok, {_, zip_bin}} =
      :zip.create(String.to_charlist(archive_path), [String.to_charlist(file_path)], [:memory])

    File.write!(archive_path, zip_bin)

    assert {:ok, _} = SparkEx.Artifacts.add_archives(session, [archive_path])
  end

  test "create_dataframe uses chunked cache for large payloads", %{session: session} do
    data = Enum.map(1..6000, fn i -> %{"id" => i} end)
    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT", cache_threshold: 1)

    assert match?({:chunked_cached_local_relation, _, _}, df.plan) or match?({:sql, _, _}, df.plan)
  end

  test "chunked cache upload stores artifacts", %{session: session} do
    data = Enum.map(1..6000, fn i -> %{"id" => i} end)
    {:ok, df} = SparkEx.create_dataframe(session, data, schema: "id INT", cache_threshold: 1)

    assert {:ok, _} = SparkEx.DataFrame.collect(df)
  end

  test "duplicate artifact upload returns statuses", %{session: session} do
    tmp_dir = "/tmp/spark_ex_artifacts_dup_#{System.unique_integer([:positive])}"
    File.mkdir_p!(tmp_dir)

    file_path = Path.join(tmp_dir, "dup.txt")
    File.write!(file_path, "dup")

    assert {:ok, first} = SparkEx.Artifacts.add_files(session, [file_path])
    assert {:ok, second} = SparkEx.Artifacts.add_files(session, [file_path])

    assert [{name, true}] = first
    assert [{^name, true}] = second
    assert {:ok, statuses} = SparkEx.artifact_status(session, [name])
    assert statuses == %{name => false}
  end

  test "uploading files with same basename reports duplicates", %{session: session} do
    tmp_dir = "/tmp/spark_ex_artifacts_dupname_#{System.unique_integer([:positive])}"
    dir_a = Path.join(tmp_dir, "a")
    dir_b = Path.join(tmp_dir, "b")
    File.mkdir_p!(dir_a)
    File.mkdir_p!(dir_b)

    file_a = Path.join(dir_a, "dup.txt")
    file_b = Path.join(dir_b, "dup.txt")

    File.write!(file_a, "dup")
    File.write!(file_b, "dup")

    assert {:ok, first} = SparkEx.Artifacts.add_files(session, [file_a])
    assert {:ok, second} = SparkEx.Artifacts.add_files(session, [file_b])

    assert [{name, true}] = first
    assert [{^name, true}] = second
    assert name == "files/dup.txt"
    assert {:ok, statuses} = SparkEx.artifact_status(session, [name])
    assert statuses == %{name => false}
  end
end
