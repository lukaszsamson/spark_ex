defmodule SparkEx.Unit.ConfigOpsTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    ConfigRequest,
    ConfigResponse,
    KeyValue
  }

  describe "Config request building" do
    test "GetWithDefault has key-value pairs" do
      pairs = [%KeyValue{key: "spark.some.key", value: "default-val"}]
      msg = %ConfigRequest.GetWithDefault{pairs: pairs}
      assert length(msg.pairs) == 1
      assert hd(msg.pairs).key == "spark.some.key"
      assert hd(msg.pairs).value == "default-val"
    end

    test "GetOption has keys" do
      msg = %ConfigRequest.GetOption{keys: ["spark.key1", "spark.key2"]}
      assert msg.keys == ["spark.key1", "spark.key2"]
    end

    test "GetAll has optional prefix" do
      msg = %ConfigRequest.GetAll{prefix: "spark.sql."}
      assert msg.prefix == "spark.sql."

      msg_no_prefix = %ConfigRequest.GetAll{}
      assert is_nil(msg_no_prefix.prefix)
    end

    test "Unset has keys" do
      msg = %ConfigRequest.Unset{keys: ["spark.key.to.unset"]}
      assert msg.keys == ["spark.key.to.unset"]
    end

    test "IsModifiable has keys" do
      msg = %ConfigRequest.IsModifiable{keys: ["spark.sql.shuffle.partitions"]}
      assert msg.keys == ["spark.sql.shuffle.partitions"]
    end
  end

  describe "Config operation oneof variants" do
    test "get_with_default operation" do
      op = %ConfigRequest.Operation{
        op_type: {:get_with_default, %ConfigRequest.GetWithDefault{pairs: []}}
      }

      assert {:get_with_default, _} = op.op_type
    end

    test "get_option operation" do
      op = %ConfigRequest.Operation{
        op_type: {:get_option, %ConfigRequest.GetOption{keys: ["k"]}}
      }

      assert {:get_option, %ConfigRequest.GetOption{keys: ["k"]}} = op.op_type
    end

    test "get_all operation" do
      op = %ConfigRequest.Operation{
        op_type: {:get_all, %ConfigRequest.GetAll{prefix: "spark."}}
      }

      assert {:get_all, %ConfigRequest.GetAll{prefix: "spark."}} = op.op_type
    end

    test "unset operation" do
      op = %ConfigRequest.Operation{
        op_type: {:unset, %ConfigRequest.Unset{keys: ["k"]}}
      }

      assert {:unset, %ConfigRequest.Unset{keys: ["k"]}} = op.op_type
    end

    test "is_modifiable operation" do
      op = %ConfigRequest.Operation{
        op_type: {:is_modifiable, %ConfigRequest.IsModifiable{keys: ["k"]}}
      }

      assert {:is_modifiable, %ConfigRequest.IsModifiable{keys: ["k"]}} = op.op_type
    end
  end

  describe "ConfigResponse structure" do
    test "pairs and warnings fields" do
      resp = %ConfigResponse{
        session_id: "s",
        server_side_session_id: "ss",
        pairs: [%KeyValue{key: "k1", value: "v1"}, %KeyValue{key: "k2", value: "v2"}],
        warnings: ["deprecated config"]
      }

      assert length(resp.pairs) == 2
      assert length(resp.warnings) == 1
      assert resp.server_side_session_id == "ss"
    end
  end
end
