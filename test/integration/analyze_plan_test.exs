defmodule SparkEx.Integration.AnalyzePlanTest do
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

  describe "analyze_tree_string" do
    test "returns tree-string for a SQL query", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1 AS n")
      assert {:ok, str} = DataFrame.tree_string(df)
      assert is_binary(str)
      assert String.length(str) > 0
    end

    test "returns tree-string for a range", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, str} = DataFrame.tree_string(df)
      assert is_binary(str)
    end
  end

  describe "analyze_is_local" do
    test "range is not local", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, false} = DataFrame.is_local(df)
    end
  end

  describe "analyze_is_streaming" do
    test "range is not streaming", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, false} = DataFrame.is_streaming(df)
    end

    test "SQL query is not streaming", %{session: session} do
      df = SparkEx.sql(session, "SELECT 1")
      assert {:ok, false} = DataFrame.is_streaming(df)
    end
  end

  describe "analyze_input_files" do
    test "returns empty list for non-file sources", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, files} = DataFrame.input_files(df)
      assert is_list(files)
    end
  end

  describe "analyze_ddl_parse" do
    test "parses simple DDL string", %{session: session} do
      assert {:ok, parsed} = SparkEx.Session.analyze_ddl_parse(session, "id INT, name STRING")
      assert parsed != nil
    end

    test "parses struct DDL", %{session: session} do
      assert {:ok, parsed} =
               SparkEx.Session.analyze_ddl_parse(
                 session,
                 "id BIGINT, data STRUCT<name: STRING, age: INT>"
               )

      assert parsed != nil
    end
  end

  describe "analyze_json_to_ddl" do
    test "converts JSON schema to DDL", %{session: session} do
      json = ~s({"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}}]})

      assert {:ok, ddl} = SparkEx.Session.analyze_json_to_ddl(session, json)
      assert is_binary(ddl)
      assert String.length(ddl) > 0
    end
  end

  describe "analyze_same_semantics" do
    test "same plan has same semantics", %{session: session} do
      df1 = SparkEx.range(session, 10)
      df2 = SparkEx.range(session, 10)
      assert {:ok, true} = DataFrame.same_semantics(df1, df2)
    end

    test "different plans have different semantics", %{session: session} do
      df1 = SparkEx.range(session, 10)
      df2 = SparkEx.range(session, 20)
      assert {:ok, false} = DataFrame.same_semantics(df1, df2)
    end
  end

  describe "analyze_semantic_hash" do
    test "returns integer hash", %{session: session} do
      df = SparkEx.range(session, 10)
      assert {:ok, hash} = DataFrame.semantic_hash(df)
      assert is_integer(hash)
    end

    test "same plans have same hash", %{session: session} do
      df1 = SparkEx.range(session, 10)
      df2 = SparkEx.range(session, 10)
      assert {:ok, hash1} = DataFrame.semantic_hash(df1)
      assert {:ok, hash2} = DataFrame.semantic_hash(df2)
      assert hash1 == hash2
    end
  end

  describe "persist/unpersist/storage_level" do
    test "persist and unpersist a DataFrame", %{session: session} do
      df = SparkEx.range(session, 10)
      assert :ok = DataFrame.persist(df)
      assert :ok = DataFrame.unpersist(df)
    end

    test "persist with custom storage level", %{session: session} do
      df = SparkEx.range(session, 10)

      storage_level = %Spark.Connect.StorageLevel{
        use_disk: true,
        use_memory: true,
        deserialized: true,
        replication: 1
      }

      assert :ok = DataFrame.persist(df, storage_level: storage_level)
      assert {:ok, sl} = DataFrame.storage_level(df)
      assert sl.use_disk == true
      assert sl.use_memory == true
      assert :ok = DataFrame.unpersist(df)
    end

    test "get storage level of persisted DataFrame", %{session: session} do
      df = SparkEx.range(session, 10)
      assert :ok = DataFrame.persist(df)

      assert {:ok, sl} = DataFrame.storage_level(df)
      assert %Spark.Connect.StorageLevel{} = sl
      assert sl.use_memory == true

      assert :ok = DataFrame.unpersist(df)
    end
  end
end
