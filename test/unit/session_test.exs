defmodule SparkEx.SessionTest do
  use ExUnit.Case, async: true

  describe "connect/1" do
    test "rejects invalid URI scheme before attempting connection" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "http://bad")
    end

    test "rejects URI without host" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "sc://")
    end

    test "rejects missing scheme" do
      assert {:error, {:invalid_uri, _}} = SparkEx.connect(url: "localhost:15002")
    end
  end
end
