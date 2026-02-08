defmodule SparkEx.Unit.AnalyzePlanTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    AnalyzePlanRequest,
    AnalyzePlanResponse,
    Plan,
    StorageLevel
  }

  describe "AnalyzePlan request building" do
    setup do
      session = %SparkEx.Session{
        channel: nil,
        session_id: "test-session-123",
        server_side_session_id: "server-side-456",
        user_id: "test_user",
        client_type: "elixir/test"
      }

      plan = %Plan{op_type: {:root, %Spark.Connect.Relation{}}}

      %{session: session, plan: plan}
    end

    test "TreeString request has plan and optional level" do
      msg = %AnalyzePlanRequest.TreeString{plan: %Plan{}, level: 3}
      assert msg.plan == %Plan{}
      assert msg.level == 3

      msg_no_level = %AnalyzePlanRequest.TreeString{plan: %Plan{}}
      assert is_nil(msg_no_level.level)
    end

    test "IsLocal request has plan" do
      msg = %AnalyzePlanRequest.IsLocal{plan: %Plan{}}
      assert msg.plan == %Plan{}
    end

    test "IsStreaming request has plan" do
      msg = %AnalyzePlanRequest.IsStreaming{plan: %Plan{}}
      assert msg.plan == %Plan{}
    end

    test "InputFiles request has plan" do
      msg = %AnalyzePlanRequest.InputFiles{plan: %Plan{}}
      assert msg.plan == %Plan{}
    end

    test "DDLParse request has ddl_string" do
      msg = %AnalyzePlanRequest.DDLParse{ddl_string: "id INT, name STRING"}
      assert msg.ddl_string == "id INT, name STRING"
    end

    test "JsonToDDL request has json_string" do
      json = ~s({"type":"struct","fields":[{"name":"id","type":"integer"}]})
      msg = %AnalyzePlanRequest.JsonToDDL{json_string: json}
      assert msg.json_string == json
    end

    test "SameSemantics request has target_plan and other_plan" do
      p1 = %Plan{op_type: {:root, %Spark.Connect.Relation{}}}
      p2 = %Plan{op_type: {:root, %Spark.Connect.Relation{}}}
      msg = %AnalyzePlanRequest.SameSemantics{target_plan: p1, other_plan: p2}
      assert msg.target_plan == p1
      assert msg.other_plan == p2
    end

    test "SemanticHash request has plan" do
      msg = %AnalyzePlanRequest.SemanticHash{plan: %Plan{}}
      assert msg.plan == %Plan{}
    end

    test "Persist request has relation and optional storage_level" do
      relation = %Spark.Connect.Relation{}

      storage_level = %StorageLevel{
        use_disk: true,
        use_memory: true,
        use_off_heap: false,
        deserialized: false,
        replication: 1
      }

      msg = %AnalyzePlanRequest.Persist{relation: relation, storage_level: storage_level}
      assert msg.relation == relation
      assert msg.storage_level.use_disk == true
      assert msg.storage_level.replication == 1

      msg_no_level = %AnalyzePlanRequest.Persist{relation: relation}
      assert msg_no_level.storage_level == nil
    end

    test "Unpersist request has relation and optional blocking" do
      relation = %Spark.Connect.Relation{}
      msg = %AnalyzePlanRequest.Unpersist{relation: relation, blocking: true}
      assert msg.relation == relation
      assert msg.blocking == true

      msg_default = %AnalyzePlanRequest.Unpersist{relation: relation}
      assert is_nil(msg_default.blocking)
    end

    test "GetStorageLevel request has relation" do
      relation = %Spark.Connect.Relation{}
      msg = %AnalyzePlanRequest.GetStorageLevel{relation: relation}
      assert msg.relation == relation
    end
  end

  describe "AnalyzePlan response parsing" do
    test "TreeString response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        server_side_session_id: "ss",
        result: {:tree_string, %AnalyzePlanResponse.TreeString{tree_string: "== Physical Plan ==\nRange(0,100,1)"}}
      }

      assert {:tree_string, %{tree_string: str}} = resp.result
      assert String.starts_with?(str, "== Physical Plan ==")
    end

    test "IsLocal response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:is_local, %AnalyzePlanResponse.IsLocal{is_local: false}}
      }

      assert {:is_local, %{is_local: false}} = resp.result
    end

    test "IsStreaming response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:is_streaming, %AnalyzePlanResponse.IsStreaming{is_streaming: false}}
      }

      assert {:is_streaming, %{is_streaming: false}} = resp.result
    end

    test "InputFiles response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:input_files, %AnalyzePlanResponse.InputFiles{files: ["/path/to/file.parquet"]}}
      }

      assert {:input_files, %{files: ["/path/to/file.parquet"]}} = resp.result
    end

    test "DDLParse response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:ddl_parse, %AnalyzePlanResponse.DDLParse{parsed: %Spark.Connect.DataType{}}}
      }

      assert {:ddl_parse, %{parsed: %Spark.Connect.DataType{}}} = resp.result
    end

    test "JsonToDDL response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:json_to_ddl, %AnalyzePlanResponse.JsonToDDL{ddl_string: "STRUCT<id: INT>"}}
      }

      assert {:json_to_ddl, %{ddl_string: "STRUCT<id: INT>"}} = resp.result
    end

    test "SameSemantics response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:same_semantics, %AnalyzePlanResponse.SameSemantics{result: true}}
      }

      assert {:same_semantics, %{result: true}} = resp.result
    end

    test "SemanticHash response" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:semantic_hash, %AnalyzePlanResponse.SemanticHash{result: 42}}
      }

      assert {:semantic_hash, %{result: 42}} = resp.result
    end

    test "Persist response is empty" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:persist, %AnalyzePlanResponse.Persist{}}
      }

      assert {:persist, _} = resp.result
    end

    test "Unpersist response is empty" do
      resp = %AnalyzePlanResponse{
        session_id: "s",
        result: {:unpersist, %AnalyzePlanResponse.Unpersist{}}
      }

      assert {:unpersist, _} = resp.result
    end

    test "GetStorageLevel response" do
      storage_level = %StorageLevel{
        use_disk: false,
        use_memory: true,
        use_off_heap: false,
        deserialized: true,
        replication: 1
      }

      resp = %AnalyzePlanResponse{
        session_id: "s",
        result:
          {:get_storage_level,
           %AnalyzePlanResponse.GetStorageLevel{storage_level: storage_level}}
      }

      assert {:get_storage_level, %{storage_level: sl}} = resp.result
      assert sl.use_memory == true
      assert sl.deserialized == true
    end
  end

  describe "StorageLevel struct" do
    test "all fields default to falsy/zero" do
      sl = %StorageLevel{}
      assert sl.use_disk == false
      assert sl.use_memory == false
      assert sl.use_off_heap == false
      assert sl.deserialized == false
      assert sl.replication == 0
    end

    test "MEMORY_ONLY equivalent" do
      sl = %StorageLevel{use_memory: true, deserialized: true, replication: 1}
      assert sl.use_memory == true
      assert sl.deserialized == true
      assert sl.use_disk == false
    end

    test "DISK_ONLY equivalent" do
      sl = %StorageLevel{use_disk: true, replication: 1}
      assert sl.use_disk == true
      assert sl.use_memory == false
    end

    test "MEMORY_AND_DISK equivalent" do
      sl = %StorageLevel{use_disk: true, use_memory: true, deserialized: true, replication: 1}
      assert sl.use_disk == true
      assert sl.use_memory == true
    end
  end
end
