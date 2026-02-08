defmodule SparkEx.Integration.SparkVersionTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  describe "AnalyzePlan.SparkVersion" do
    test "fetches Spark version from a running server" do
      {:ok, session} = SparkEx.connect(url: @spark_remote)

      assert {:ok, version} = SparkEx.spark_version(session)
      assert is_binary(version)
      assert version =~ ~r/^\d+\.\d+/

      SparkEx.Session.stop(session)
    end
  end
end
