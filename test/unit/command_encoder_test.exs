defmodule SparkEx.Unit.CommandEncoderTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    Command,
    CommonInlineUserDefinedDataSource,
    CreateDataFrameViewCommand,
    Plan,
    Relation,
    PythonDataSource,
    WriteOperation,
    WriteOperationV2
  }

  alias SparkEx.Connect.CommandEncoder

  describe "encode/2 for create_dataframe_view" do
    test "encodes temp view command" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:create_dataframe_view, plan, "my_view", false, false},
          0
        )

      assert %Command{command_type: {:create_dataframe_view, view_cmd}} = command
      assert %CreateDataFrameViewCommand{} = view_cmd
      assert view_cmd.name == "my_view"
      assert view_cmd.is_global == false
      assert view_cmd.replace == false
      assert %Relation{} = view_cmd.input
    end

    test "encodes global replace view command" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:create_dataframe_view, plan, "global_view", true, true},
          0
        )

      assert %Command{command_type: {:create_dataframe_view, view_cmd}} = command
      assert view_cmd.is_global == true
      assert view_cmd.replace == true
    end

    test "increments plan counter" do
      plan = {:sql, "SELECT 1", nil}

      {_proto, counter} =
        CommandEncoder.encode(
          {:create_dataframe_view, plan, "v", false, true},
          5
        )

      assert counter > 5
    end
  end

  describe "encode/2 for write_operation" do
    test "encodes write to path" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan, [path: "/tmp/output", format: "parquet", mode: :overwrite]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert %WriteOperation{} = write_op
      assert write_op.source == "parquet"
      assert write_op.mode == :SAVE_MODE_OVERWRITE
      assert {:path, "/tmp/output"} = write_op.save_type
    end

    test "encodes write to table (save_as_table)" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan, [table: "my_db.my_table", mode: :append]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert {:table, save_table} = write_op.save_type
      assert save_table.table_name == "my_db.my_table"
      assert save_table.save_method == :TABLE_SAVE_METHOD_SAVE_AS_TABLE
      assert write_op.mode == :SAVE_MODE_APPEND
    end

    test "encodes insert_into" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan, [insert_into: "my_table", mode: :append]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert {:table, save_table} = write_op.save_type
      assert save_table.save_method == :TABLE_SAVE_METHOD_INSERT_INTO
    end

    test "encodes save modes" do
      plan = {:sql, "SELECT 1", nil}

      for {mode, expected} <- [
            {:append, :SAVE_MODE_APPEND},
            {:overwrite, :SAVE_MODE_OVERWRITE},
            {:error_if_exists, :SAVE_MODE_ERROR_IF_EXISTS},
            {:ignore, :SAVE_MODE_IGNORE}
          ] do
        {%Plan{op_type: {:command, command}}, _counter} =
          CommandEncoder.encode(
            {:write_operation, plan, [path: "/tmp/out", mode: mode]},
            0
          )

        assert %Command{command_type: {:write_operation, write_op}} = command
        assert write_op.mode == expected
      end
    end

    test "encodes partition_by, sort_by, cluster_by" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan,
           [
             path: "/tmp/out",
             mode: :overwrite,
             partition_by: ["year", "month"],
             sort_by: ["id"],
             cluster_by: ["region"]
           ]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert write_op.partitioning_columns == ["year", "month"]
      assert write_op.sort_column_names == ["id"]
      assert write_op.clustering_columns == ["region"]
    end

    test "encodes bucket_by" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan,
           [path: "/tmp/out", mode: :overwrite, bucket_by: {10, ["id", "name"]}]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert write_op.bucket_by.num_buckets == 10
      assert write_op.bucket_by.bucket_column_names == ["id", "name"]
    end

    test "encodes options" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation, plan,
           [path: "/tmp/out", mode: :overwrite, options: %{"compression" => "snappy"}]},
          0
        )

      assert %Command{command_type: {:write_operation, write_op}} = command
      assert write_op.options == %{"compression" => "snappy"}
    end

    test "raises on invalid save mode" do
      plan = {:sql, "SELECT 1", nil}

      assert_raise ArgumentError, ~r/invalid save mode/, fn ->
        CommandEncoder.encode(
          {:write_operation, plan, [path: "/tmp/out", mode: :invalid]},
          0
        )
      end
    end
  end

  describe "encode/2 for write_operation_v2" do
    test "encodes V2 create" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation_v2, plan, "my_table", [mode: :create, provider: "parquet"]},
          0
        )

      assert %Command{command_type: {:write_operation_v2, write_v2}} = command
      assert %WriteOperationV2{} = write_v2
      assert write_v2.table_name == "my_table"
      assert write_v2.mode == :MODE_CREATE
      assert write_v2.provider == "parquet"
    end

    test "encodes V2 modes" do
      plan = {:sql, "SELECT 1", nil}

      for {mode, expected} <- [
            {:create, :MODE_CREATE},
            {:overwrite, :MODE_OVERWRITE},
            {:overwrite_partitions, :MODE_OVERWRITE_PARTITIONS},
            {:append, :MODE_APPEND},
            {:replace, :MODE_REPLACE},
            {:create_or_replace, :MODE_CREATE_OR_REPLACE}
          ] do
        {%Plan{op_type: {:command, command}}, _counter} =
          CommandEncoder.encode(
            {:write_operation_v2, plan, "t", [mode: mode]},
            0
          )

        assert %Command{command_type: {:write_operation_v2, write_v2}} = command
        assert write_v2.mode == expected
      end
    end

    test "encodes V2 options and table properties" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation_v2, plan, "t",
           [
             mode: :create,
             options: %{"opt1" => "val1"},
             table_properties: %{"desc" => "My table"}
           ]},
          0
        )

      assert %Command{command_type: {:write_operation_v2, write_v2}} = command
      assert write_v2.options == %{"opt1" => "val1"}
      assert write_v2.table_properties == %{"desc" => "My table"}
    end

    test "encodes V2 clustering columns" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation_v2, plan, "t", [mode: :create, cluster_by: ["region"]]},
          0
        )

      assert %Command{command_type: {:write_operation_v2, write_v2}} = command
      assert write_v2.clustering_columns == ["region"]
    end

    test "encodes V2 overwrite_condition" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:write_operation_v2, plan, "t",
           [mode: :overwrite, overwrite_condition: {:col, "date"}]},
          0
        )

      assert %Command{command_type: {:write_operation_v2, write_v2}} = command
      assert write_v2.overwrite_condition != nil
    end
  end

  describe "encode/2 for register_data_source" do
    test "encodes data source registration" do
      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode(
          {:register_data_source, "my_source", <<1, 2, 3>>, "3.11"},
          0
        )

      assert %Command{command_type: {:register_data_source, ds}} = command
      assert %CommonInlineUserDefinedDataSource{name: "my_source"} = ds

      assert {:python_data_source, %PythonDataSource{command: <<1, 2, 3>>, python_ver: "3.11"}} =
               ds.data_source
    end
  end

  describe "encode/2 for checkpoint" do
    test "encodes checkpoint command" do
      plan = {:sql, "SELECT 1", nil}

      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode({:checkpoint, plan, false, true, nil}, 0)

      assert %Command{command_type: {:checkpoint_command, checkpoint}} = command
      assert %Spark.Connect.CheckpointCommand{} = checkpoint
      assert checkpoint.local == false
      assert checkpoint.eager == true
      assert checkpoint.storage_level == nil
    end
  end

  describe "encode/2 for remove_cached_remote_relation" do
    test "encodes remove cached remote relation command" do
      {%Plan{op_type: {:command, command}}, _counter} =
        CommandEncoder.encode({:remove_cached_remote_relation, "rel-9"}, 0)

      assert %Command{command_type: {:remove_cached_remote_relation_command, remove}} = command
      assert %Spark.Connect.RemoveCachedRemoteRelationCommand{} = remove
      assert %Spark.Connect.CachedRemoteRelation{relation_id: "rel-9"} = remove.relation
    end
  end
end
