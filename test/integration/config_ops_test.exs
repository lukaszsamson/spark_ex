defmodule SparkEx.Integration.ConfigOpsTest do
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

  describe "config_get_with_default" do
    test "returns configured value when set", %{session: session} do
      :ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "42"}])

      assert {:ok, [{key, val}]} =
               SparkEx.config_get_with_default(session, [
                 {"spark.sql.shuffle.partitions", "default"}
               ])

      assert key == "spark.sql.shuffle.partitions"
      assert val == "42"
    end

    test "returns default value when key is not set", %{session: session} do
      assert {:ok, [{key, val}]} =
               SparkEx.config_get_with_default(session, [
                 {"spark.nonexistent.test.key.m7", "my-default"}
               ])

      assert key == "spark.nonexistent.test.key.m7"
      assert val == "my-default"
    end
  end

  describe "config_get_option" do
    test "returns value for existing key", %{session: session} do
      :ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "50"}])

      assert {:ok, [{key, val}]} =
               SparkEx.config_get_option(session, ["spark.sql.shuffle.partitions"])

      assert key == "spark.sql.shuffle.partitions"
      assert val == "50"
    end

    test "returns nil for unset key", %{session: session} do
      assert {:ok, [{key, val}]} =
               SparkEx.config_get_option(session, ["spark.nonexistent.option.test.m7"])

      assert key == "spark.nonexistent.option.test.m7"
      assert is_nil(val) or val == ""
    end
  end

  describe "config_get_all" do
    test "returns all config with prefix filter", %{session: session} do
      :ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "10"}])

      assert {:ok, pairs} = SparkEx.config_get_all(session, "spark.sql.shuffle")
      assert is_list(pairs)
      assert length(pairs) >= 1

      assert Enum.any?(pairs, fn {k, _v} -> k == "spark.sql.shuffle.partitions" end)
    end

    test "returns configs without prefix", %{session: session} do
      assert {:ok, pairs} = SparkEx.config_get_all(session)
      assert is_list(pairs)
      assert length(pairs) > 0
    end
  end

  describe "config_unset" do
    test "unsets a previously set config key", %{session: session} do
      :ok = SparkEx.config_set(session, [{"spark.test.unset.key.m7", "value"}])
      assert {:ok, [{"spark.test.unset.key.m7", "value"}]} = SparkEx.config_get(session, ["spark.test.unset.key.m7"])

      :ok = SparkEx.config_unset(session, ["spark.test.unset.key.m7"])

      # After unset, get_option should return nil/empty
      assert {:ok, [{_, val}]} = SparkEx.config_get_option(session, ["spark.test.unset.key.m7"])
      assert is_nil(val) or val == ""
    end
  end

  describe "config_is_modifiable" do
    test "checks modifiability of known keys", %{session: session} do
      assert {:ok, pairs} =
               SparkEx.config_is_modifiable(session, ["spark.sql.shuffle.partitions"])

      assert is_list(pairs)
      assert length(pairs) == 1
      {key, val} = hd(pairs)
      assert key == "spark.sql.shuffle.partitions"
      assert val in ["true", "false"]
    end
  end
end
