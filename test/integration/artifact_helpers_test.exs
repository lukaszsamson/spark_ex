defmodule SparkEx.Integration.ArtifactHelpersTest do
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

  describe "add_jars/2" do
    test "uploads JAR artifacts with jars/ prefix", %{session: session} do
      jar_name = "test-jar-#{System.unique_integer([:positive])}.jar"
      data = "fake jar content"

      assert {:ok, summaries} = SparkEx.add_jars(session, [{jar_name, data}])
      assert [{prefixed_name, true}] = summaries
      assert prefixed_name == "jars/#{jar_name}"
    end
  end

  describe "add_files/2" do
    test "uploads file artifacts with files/ prefix", %{session: session} do
      file_name = "test-file-#{System.unique_integer([:positive])}.txt"
      data = "file content"

      assert {:ok, summaries} = SparkEx.add_files(session, [{file_name, data}])
      assert [{prefixed_name, true}] = summaries
      assert prefixed_name == "files/#{file_name}"
    end
  end

  describe "add_archives/2" do
    @describetag min_spark: "4.1"
    test "uploads archive artifacts with archives/ prefix", %{session: session} do
      archive_name = "test-archive-#{System.unique_integer([:positive])}.tar.gz"
      data = "fake archive content"

      assert {:ok, summaries} = SparkEx.add_archives(session, [{archive_name, data}])
      assert [{prefixed_name, true}] = summaries
      assert prefixed_name == "archives/#{archive_name}"
    end
  end

  describe "copy_from_local_to_fs/3" do
    @tag min_spark: "4.0"
    test "uploads a local file to the server destination path", %{session: session} do
      src_path =
        Path.join(System.tmp_dir!(), "spark_ex_src_#{System.unique_integer([:positive])}.txt")

      dest_path =
        Path.join(System.tmp_dir!(), "spark_ex_dest_#{System.unique_integer([:positive])}.txt")

      File.write!(src_path, "hello from local file")

      on_exit(fn ->
        File.rm(src_path)
        File.rm(dest_path)
      end)

      result = SparkEx.copy_from_local_to_fs(session, src_path, dest_path)

      case result do
        :ok ->
          assert File.read!(dest_path) == "hello from local file"

        {:error, %SparkEx.Error.Remote{message: message}} ->
          if message =~ "not supported" do
            assert message =~ "not supported"
          else
            flunk("unexpected copy_from_local_to_fs error: #{inspect(message)}")
          end
      end
    end

    test "returns error for non-existent local file", %{session: session} do
      assert {:error, {:file_read_error, _, :enoent}} =
               SparkEx.copy_from_local_to_fs(
                 session,
                 "/nonexistent/path.txt",
                 Path.join(System.tmp_dir!(), "spark_ex_dest_missing.txt")
               )
    end

    test "returns error for non-absolute destination path", %{session: session} do
      src_path =
        Path.join(System.tmp_dir!(), "spark_ex_src_rel_#{System.unique_integer([:positive])}.txt")

      File.write!(src_path, "hello")
      on_exit(fn -> File.rm(src_path) end)

      assert {:error, {:invalid_destination_path, _}} =
               SparkEx.copy_from_local_to_fs(session, src_path, "relative/path.txt")
    end

    test "returns error for destination path with URI scheme", %{session: session} do
      src_path =
        Path.join(
          System.tmp_dir!(),
          "spark_ex_src_scheme_#{System.unique_integer([:positive])}.txt"
        )

      File.write!(src_path, "hello")
      on_exit(fn -> File.rm(src_path) end)

      assert {:error, {:invalid_destination_path, _}} =
               SparkEx.copy_from_local_to_fs(session, src_path, "file:///tmp/target.txt")
    end
  end
end
