defmodule Spark.Connect.StreamingQueryEventType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.StreamingQueryEventType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:QUERY_PROGRESS_UNSPECIFIED, 0)
  field(:QUERY_PROGRESS_EVENT, 1)
  field(:QUERY_TERMINATED_EVENT, 2)
  field(:QUERY_IDLE_EVENT, 3)
end

defmodule Spark.Connect.WriteOperation.SaveMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.WriteOperation.SaveMode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SAVE_MODE_UNSPECIFIED, 0)
  field(:SAVE_MODE_APPEND, 1)
  field(:SAVE_MODE_OVERWRITE, 2)
  field(:SAVE_MODE_ERROR_IF_EXISTS, 3)
  field(:SAVE_MODE_IGNORE, 4)
end

defmodule Spark.Connect.WriteOperation.SaveTable.TableSaveMethod do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.WriteOperation.SaveTable.TableSaveMethod",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:TABLE_SAVE_METHOD_UNSPECIFIED, 0)
  field(:TABLE_SAVE_METHOD_SAVE_AS_TABLE, 1)
  field(:TABLE_SAVE_METHOD_INSERT_INTO, 2)
end

defmodule Spark.Connect.WriteOperationV2.Mode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.WriteOperationV2.Mode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:MODE_UNSPECIFIED, 0)
  field(:MODE_CREATE, 1)
  field(:MODE_OVERWRITE, 2)
  field(:MODE_OVERWRITE_PARTITIONS, 3)
  field(:MODE_APPEND, 4)
  field(:MODE_REPLACE, 5)
  field(:MODE_CREATE_OR_REPLACE, 6)
end

defmodule Spark.Connect.Command do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Command",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command_type, 0)

  field(:register_function, 1,
    type: Spark.Connect.CommonInlineUserDefinedFunction,
    json_name: "registerFunction",
    oneof: 0
  )

  field(:write_operation, 2,
    type: Spark.Connect.WriteOperation,
    json_name: "writeOperation",
    oneof: 0
  )

  field(:create_dataframe_view, 3,
    type: Spark.Connect.CreateDataFrameViewCommand,
    json_name: "createDataframeView",
    oneof: 0
  )

  field(:write_operation_v2, 4,
    type: Spark.Connect.WriteOperationV2,
    json_name: "writeOperationV2",
    oneof: 0
  )

  field(:sql_command, 5, type: Spark.Connect.SqlCommand, json_name: "sqlCommand", oneof: 0)

  field(:write_stream_operation_start, 6,
    type: Spark.Connect.WriteStreamOperationStart,
    json_name: "writeStreamOperationStart",
    oneof: 0
  )

  field(:streaming_query_command, 7,
    type: Spark.Connect.StreamingQueryCommand,
    json_name: "streamingQueryCommand",
    oneof: 0
  )

  field(:get_resources_command, 8,
    type: Spark.Connect.GetResourcesCommand,
    json_name: "getResourcesCommand",
    oneof: 0
  )

  field(:streaming_query_manager_command, 9,
    type: Spark.Connect.StreamingQueryManagerCommand,
    json_name: "streamingQueryManagerCommand",
    oneof: 0
  )

  field(:register_table_function, 10,
    type: Spark.Connect.CommonInlineUserDefinedTableFunction,
    json_name: "registerTableFunction",
    oneof: 0
  )

  field(:streaming_query_listener_bus_command, 11,
    type: Spark.Connect.StreamingQueryListenerBusCommand,
    json_name: "streamingQueryListenerBusCommand",
    oneof: 0
  )

  field(:register_data_source, 12,
    type: Spark.Connect.CommonInlineUserDefinedDataSource,
    json_name: "registerDataSource",
    oneof: 0
  )

  field(:create_resource_profile_command, 13,
    type: Spark.Connect.CreateResourceProfileCommand,
    json_name: "createResourceProfileCommand",
    oneof: 0
  )

  field(:checkpoint_command, 14,
    type: Spark.Connect.CheckpointCommand,
    json_name: "checkpointCommand",
    oneof: 0
  )

  field(:remove_cached_remote_relation_command, 15,
    type: Spark.Connect.RemoveCachedRemoteRelationCommand,
    json_name: "removeCachedRemoteRelationCommand",
    oneof: 0
  )

  field(:merge_into_table_command, 16,
    type: Spark.Connect.MergeIntoTableCommand,
    json_name: "mergeIntoTableCommand",
    oneof: 0
  )

  field(:ml_command, 17, type: Spark.Connect.MlCommand, json_name: "mlCommand", oneof: 0)

  field(:execute_external_command, 18,
    type: Spark.Connect.ExecuteExternalCommand,
    json_name: "executeExternalCommand",
    oneof: 0
  )

  field(:pipeline_command, 19,
    type: Spark.Connect.PipelineCommand,
    json_name: "pipelineCommand",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
end

defmodule Spark.Connect.SqlCommand.ArgsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SqlCommand.ArgsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.SqlCommand.NamedArgumentsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SqlCommand.NamedArgumentsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.SqlCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SqlCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:sql, 1, type: :string, deprecated: true)

  field(:args, 2,
    repeated: true,
    type: Spark.Connect.SqlCommand.ArgsEntry,
    map: true,
    deprecated: true
  )

  field(:pos_args, 3,
    repeated: true,
    type: Spark.Connect.Expression.Literal,
    json_name: "posArgs",
    deprecated: true
  )

  field(:named_arguments, 4,
    repeated: true,
    type: Spark.Connect.SqlCommand.NamedArgumentsEntry,
    json_name: "namedArguments",
    map: true,
    deprecated: true
  )

  field(:pos_arguments, 5,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "posArguments",
    deprecated: true
  )

  field(:input, 6, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.CreateDataFrameViewCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateDataFrameViewCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:name, 2, type: :string)
  field(:is_global, 3, type: :bool, json_name: "isGlobal")
  field(:replace, 4, type: :bool)
end

defmodule Spark.Connect.WriteOperation.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperation.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.WriteOperation.SaveTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperation.SaveTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")

  field(:save_method, 2,
    type: Spark.Connect.WriteOperation.SaveTable.TableSaveMethod,
    json_name: "saveMethod",
    enum: true
  )
end

defmodule Spark.Connect.WriteOperation.BucketBy do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperation.BucketBy",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:bucket_column_names, 1, repeated: true, type: :string, json_name: "bucketColumnNames")
  field(:num_buckets, 2, type: :int32, json_name: "numBuckets")
end

defmodule Spark.Connect.WriteOperation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:save_type, 0)

  field(:input, 1, type: Spark.Connect.Relation)
  field(:source, 2, proto3_optional: true, type: :string)
  field(:path, 3, type: :string, oneof: 0)
  field(:table, 4, type: Spark.Connect.WriteOperation.SaveTable, oneof: 0)
  field(:mode, 5, type: Spark.Connect.WriteOperation.SaveMode, enum: true)
  field(:sort_column_names, 6, repeated: true, type: :string, json_name: "sortColumnNames")
  field(:partitioning_columns, 7, repeated: true, type: :string, json_name: "partitioningColumns")
  field(:bucket_by, 8, type: Spark.Connect.WriteOperation.BucketBy, json_name: "bucketBy")
  field(:options, 9, repeated: true, type: Spark.Connect.WriteOperation.OptionsEntry, map: true)
  field(:clustering_columns, 10, repeated: true, type: :string, json_name: "clusteringColumns")
end

defmodule Spark.Connect.WriteOperationV2.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperationV2.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.WriteOperationV2.TablePropertiesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperationV2.TablePropertiesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.WriteOperationV2 do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteOperationV2",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:table_name, 2, type: :string, json_name: "tableName")
  field(:provider, 3, proto3_optional: true, type: :string)

  field(:partitioning_columns, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "partitioningColumns"
  )

  field(:options, 5, repeated: true, type: Spark.Connect.WriteOperationV2.OptionsEntry, map: true)

  field(:table_properties, 6,
    repeated: true,
    type: Spark.Connect.WriteOperationV2.TablePropertiesEntry,
    json_name: "tableProperties",
    map: true
  )

  field(:mode, 7, type: Spark.Connect.WriteOperationV2.Mode, enum: true)
  field(:overwrite_condition, 8, type: Spark.Connect.Expression, json_name: "overwriteCondition")
  field(:clustering_columns, 9, repeated: true, type: :string, json_name: "clusteringColumns")
end

defmodule Spark.Connect.WriteStreamOperationStart.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteStreamOperationStart.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.WriteStreamOperationStart do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteStreamOperationStart",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:trigger, 0)

  oneof(:sink_destination, 1)

  field(:input, 1, type: Spark.Connect.Relation)
  field(:format, 2, type: :string)

  field(:options, 3,
    repeated: true,
    type: Spark.Connect.WriteStreamOperationStart.OptionsEntry,
    map: true
  )

  field(:partitioning_column_names, 4,
    repeated: true,
    type: :string,
    json_name: "partitioningColumnNames"
  )

  field(:processing_time_interval, 5,
    type: :string,
    json_name: "processingTimeInterval",
    oneof: 0
  )

  field(:available_now, 6, type: :bool, json_name: "availableNow", oneof: 0)
  field(:once, 7, type: :bool, oneof: 0)

  field(:continuous_checkpoint_interval, 8,
    type: :string,
    json_name: "continuousCheckpointInterval",
    oneof: 0
  )

  field(:output_mode, 9, type: :string, json_name: "outputMode")
  field(:query_name, 10, type: :string, json_name: "queryName")
  field(:path, 11, type: :string, oneof: 1)
  field(:table_name, 12, type: :string, json_name: "tableName", oneof: 1)

  field(:foreach_writer, 13,
    type: Spark.Connect.StreamingForeachFunction,
    json_name: "foreachWriter"
  )

  field(:foreach_batch, 14,
    type: Spark.Connect.StreamingForeachFunction,
    json_name: "foreachBatch"
  )

  field(:clustering_column_names, 15,
    repeated: true,
    type: :string,
    json_name: "clusteringColumnNames"
  )
end

defmodule Spark.Connect.StreamingForeachFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingForeachFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:function, 0)

  field(:python_function, 1, type: Spark.Connect.PythonUDF, json_name: "pythonFunction", oneof: 0)

  field(:scala_function, 2,
    type: Spark.Connect.ScalarScalaUDF,
    json_name: "scalaFunction",
    oneof: 0
  )
end

defmodule Spark.Connect.WriteStreamOperationStartResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WriteStreamOperationStartResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:query_id, 1, type: Spark.Connect.StreamingQueryInstanceId, json_name: "queryId")
  field(:name, 2, type: :string)

  field(:query_started_event_json, 3,
    proto3_optional: true,
    type: :string,
    json_name: "queryStartedEventJson"
  )
end

defmodule Spark.Connect.StreamingQueryInstanceId do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryInstanceId",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:id, 1, type: :string)
  field(:run_id, 2, type: :string, json_name: "runId")
end

defmodule Spark.Connect.StreamingQueryCommand.ExplainCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommand.ExplainCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:extended, 1, type: :bool)
end

defmodule Spark.Connect.StreamingQueryCommand.AwaitTerminationCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommand.AwaitTerminationCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:timeout_ms, 2, proto3_optional: true, type: :int64, json_name: "timeoutMs")
end

defmodule Spark.Connect.StreamingQueryCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command, 0)

  field(:query_id, 1, type: Spark.Connect.StreamingQueryInstanceId, json_name: "queryId")
  field(:status, 2, type: :bool, oneof: 0)
  field(:last_progress, 3, type: :bool, json_name: "lastProgress", oneof: 0)
  field(:recent_progress, 4, type: :bool, json_name: "recentProgress", oneof: 0)
  field(:stop, 5, type: :bool, oneof: 0)
  field(:process_all_available, 6, type: :bool, json_name: "processAllAvailable", oneof: 0)
  field(:explain, 7, type: Spark.Connect.StreamingQueryCommand.ExplainCommand, oneof: 0)
  field(:exception, 8, type: :bool, oneof: 0)

  field(:await_termination, 9,
    type: Spark.Connect.StreamingQueryCommand.AwaitTerminationCommand,
    json_name: "awaitTermination",
    oneof: 0
  )
end

defmodule Spark.Connect.StreamingQueryCommandResult.StatusResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult.StatusResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:status_message, 1, type: :string, json_name: "statusMessage")
  field(:is_data_available, 2, type: :bool, json_name: "isDataAvailable")
  field(:is_trigger_active, 3, type: :bool, json_name: "isTriggerActive")
  field(:is_active, 4, type: :bool, json_name: "isActive")
end

defmodule Spark.Connect.StreamingQueryCommandResult.RecentProgressResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult.RecentProgressResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:recent_progress_json, 5, repeated: true, type: :string, json_name: "recentProgressJson")
end

defmodule Spark.Connect.StreamingQueryCommandResult.ExplainResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult.ExplainResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:result, 1, type: :string)
end

defmodule Spark.Connect.StreamingQueryCommandResult.ExceptionResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult.ExceptionResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:exception_message, 1,
    proto3_optional: true,
    type: :string,
    json_name: "exceptionMessage"
  )

  field(:error_class, 2, proto3_optional: true, type: :string, json_name: "errorClass")
  field(:stack_trace, 3, proto3_optional: true, type: :string, json_name: "stackTrace")
end

defmodule Spark.Connect.StreamingQueryCommandResult.AwaitTerminationResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult.AwaitTerminationResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:terminated, 1, type: :bool)
end

defmodule Spark.Connect.StreamingQueryCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:result_type, 0)

  field(:query_id, 1, type: Spark.Connect.StreamingQueryInstanceId, json_name: "queryId")
  field(:status, 2, type: Spark.Connect.StreamingQueryCommandResult.StatusResult, oneof: 0)

  field(:recent_progress, 3,
    type: Spark.Connect.StreamingQueryCommandResult.RecentProgressResult,
    json_name: "recentProgress",
    oneof: 0
  )

  field(:explain, 4, type: Spark.Connect.StreamingQueryCommandResult.ExplainResult, oneof: 0)
  field(:exception, 5, type: Spark.Connect.StreamingQueryCommandResult.ExceptionResult, oneof: 0)

  field(:await_termination, 6,
    type: Spark.Connect.StreamingQueryCommandResult.AwaitTerminationResult,
    json_name: "awaitTermination",
    oneof: 0
  )
end

defmodule Spark.Connect.StreamingQueryManagerCommand.AwaitAnyTerminationCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommand.AwaitAnyTerminationCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:timeout_ms, 1, proto3_optional: true, type: :int64, json_name: "timeoutMs")
end

defmodule Spark.Connect.StreamingQueryManagerCommand.StreamingQueryListenerCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommand.StreamingQueryListenerCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:listener_payload, 1, type: :bytes, json_name: "listenerPayload")

  field(:python_listener_payload, 2,
    proto3_optional: true,
    type: Spark.Connect.PythonUDF,
    json_name: "pythonListenerPayload"
  )

  field(:id, 3, type: :string)
end

defmodule Spark.Connect.StreamingQueryManagerCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command, 0)

  field(:active, 1, type: :bool, oneof: 0)
  field(:get_query, 2, type: :string, json_name: "getQuery", oneof: 0)

  field(:await_any_termination, 3,
    type: Spark.Connect.StreamingQueryManagerCommand.AwaitAnyTerminationCommand,
    json_name: "awaitAnyTermination",
    oneof: 0
  )

  field(:reset_terminated, 4, type: :bool, json_name: "resetTerminated", oneof: 0)

  field(:add_listener, 5,
    type: Spark.Connect.StreamingQueryManagerCommand.StreamingQueryListenerCommand,
    json_name: "addListener",
    oneof: 0
  )

  field(:remove_listener, 6,
    type: Spark.Connect.StreamingQueryManagerCommand.StreamingQueryListenerCommand,
    json_name: "removeListener",
    oneof: 0
  )

  field(:list_listeners, 7, type: :bool, json_name: "listListeners", oneof: 0)
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult.ActiveResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommandResult.ActiveResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:active_queries, 1,
    repeated: true,
    type: Spark.Connect.StreamingQueryManagerCommandResult.StreamingQueryInstance,
    json_name: "activeQueries"
  )
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult.StreamingQueryInstance do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommandResult.StreamingQueryInstance",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:id, 1, type: Spark.Connect.StreamingQueryInstanceId)
  field(:name, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult.AwaitAnyTerminationResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommandResult.AwaitAnyTerminationResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:terminated, 1, type: :bool)
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult.StreamingQueryListenerInstance do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommandResult.StreamingQueryListenerInstance",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:listener_payload, 1, type: :bytes, json_name: "listenerPayload")
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult.ListStreamingQueryListenerResult do
  @moduledoc false

  use Protobuf,
    full_name:
      "spark.connect.StreamingQueryManagerCommandResult.ListStreamingQueryListenerResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:listener_ids, 1, repeated: true, type: :string, json_name: "listenerIds")
end

defmodule Spark.Connect.StreamingQueryManagerCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryManagerCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:result_type, 0)

  field(:active, 1, type: Spark.Connect.StreamingQueryManagerCommandResult.ActiveResult, oneof: 0)

  field(:query, 2,
    type: Spark.Connect.StreamingQueryManagerCommandResult.StreamingQueryInstance,
    oneof: 0
  )

  field(:await_any_termination, 3,
    type: Spark.Connect.StreamingQueryManagerCommandResult.AwaitAnyTerminationResult,
    json_name: "awaitAnyTermination",
    oneof: 0
  )

  field(:reset_terminated, 4, type: :bool, json_name: "resetTerminated", oneof: 0)
  field(:add_listener, 5, type: :bool, json_name: "addListener", oneof: 0)
  field(:remove_listener, 6, type: :bool, json_name: "removeListener", oneof: 0)

  field(:list_listeners, 7,
    type: Spark.Connect.StreamingQueryManagerCommandResult.ListStreamingQueryListenerResult,
    json_name: "listListeners",
    oneof: 0
  )
end

defmodule Spark.Connect.StreamingQueryListenerBusCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryListenerBusCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command, 0)

  field(:add_listener_bus_listener, 1, type: :bool, json_name: "addListenerBusListener", oneof: 0)

  field(:remove_listener_bus_listener, 2,
    type: :bool,
    json_name: "removeListenerBusListener",
    oneof: 0
  )
end

defmodule Spark.Connect.StreamingQueryListenerEvent do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryListenerEvent",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:event_json, 1, type: :string, json_name: "eventJson")

  field(:event_type, 2,
    type: Spark.Connect.StreamingQueryEventType,
    json_name: "eventType",
    enum: true
  )
end

defmodule Spark.Connect.StreamingQueryListenerEventsResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StreamingQueryListenerEventsResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:events, 1, repeated: true, type: Spark.Connect.StreamingQueryListenerEvent)

  field(:listener_bus_listener_added, 2,
    proto3_optional: true,
    type: :bool,
    json_name: "listenerBusListenerAdded"
  )
end

defmodule Spark.Connect.GetResourcesCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetResourcesCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.GetResourcesCommandResult.ResourcesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetResourcesCommandResult.ResourcesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.ResourceInformation)
end

defmodule Spark.Connect.GetResourcesCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetResourcesCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:resources, 1,
    repeated: true,
    type: Spark.Connect.GetResourcesCommandResult.ResourcesEntry,
    map: true
  )
end

defmodule Spark.Connect.CreateResourceProfileCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateResourceProfileCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:profile, 1, type: Spark.Connect.ResourceProfile)
end

defmodule Spark.Connect.CreateResourceProfileCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateResourceProfileCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:profile_id, 1, type: :int32, json_name: "profileId")
end

defmodule Spark.Connect.RemoveCachedRemoteRelationCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RemoveCachedRemoteRelationCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.CachedRemoteRelation)
end

defmodule Spark.Connect.CheckpointCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CheckpointCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.Relation)
  field(:local, 2, type: :bool)
  field(:eager, 3, type: :bool)

  field(:storage_level, 4,
    proto3_optional: true,
    type: Spark.Connect.StorageLevel,
    json_name: "storageLevel"
  )
end

defmodule Spark.Connect.MergeIntoTableCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MergeIntoTableCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:target_table_name, 1, type: :string, json_name: "targetTableName")
  field(:source_table_plan, 2, type: Spark.Connect.Relation, json_name: "sourceTablePlan")
  field(:merge_condition, 3, type: Spark.Connect.Expression, json_name: "mergeCondition")

  field(:match_actions, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "matchActions"
  )

  field(:not_matched_actions, 5,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "notMatchedActions"
  )

  field(:not_matched_by_source_actions, 6,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "notMatchedBySourceActions"
  )

  field(:with_schema_evolution, 7, type: :bool, json_name: "withSchemaEvolution")
end

defmodule Spark.Connect.ExecuteExternalCommand.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecuteExternalCommand.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.ExecuteExternalCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecuteExternalCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:runner, 1, type: :string)
  field(:command, 2, type: :string)

  field(:options, 3,
    repeated: true,
    type: Spark.Connect.ExecuteExternalCommand.OptionsEntry,
    map: true
  )
end
