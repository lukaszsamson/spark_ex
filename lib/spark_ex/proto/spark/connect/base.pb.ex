defmodule Spark.Connect.CompressionCodec do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.CompressionCodec",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:COMPRESSION_CODEC_UNSPECIFIED, 0)
  field(:COMPRESSION_CODEC_ZSTD, 1)
end

defmodule Spark.Connect.Plan.CompressedOperation.OpType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Plan.CompressedOperation.OpType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:OP_TYPE_UNSPECIFIED, 0)
  field(:OP_TYPE_RELATION, 1)
  field(:OP_TYPE_COMMAND, 2)
end

defmodule Spark.Connect.AnalyzePlanRequest.Explain.ExplainMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.AnalyzePlanRequest.Explain.ExplainMode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:EXPLAIN_MODE_UNSPECIFIED, 0)
  field(:EXPLAIN_MODE_SIMPLE, 1)
  field(:EXPLAIN_MODE_EXTENDED, 2)
  field(:EXPLAIN_MODE_CODEGEN, 3)
  field(:EXPLAIN_MODE_COST, 4)
  field(:EXPLAIN_MODE_FORMATTED, 5)
end

defmodule Spark.Connect.InterruptRequest.InterruptType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.InterruptRequest.InterruptType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:INTERRUPT_TYPE_UNSPECIFIED, 0)
  field(:INTERRUPT_TYPE_ALL, 1)
  field(:INTERRUPT_TYPE_TAG, 2)
  field(:INTERRUPT_TYPE_OPERATION_ID, 3)
end

defmodule Spark.Connect.FetchErrorDetailsResponse.QueryContext.ContextType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.FetchErrorDetailsResponse.QueryContext.ContextType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SQL, 0)
  field(:DATAFRAME, 1)
end

defmodule Spark.Connect.Plan.CompressedOperation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Plan.CompressedOperation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:data, 1, type: :bytes)

  field(:op_type, 2,
    type: Spark.Connect.Plan.CompressedOperation.OpType,
    json_name: "opType",
    enum: true
  )

  field(:compression_codec, 3,
    type: Spark.Connect.CompressionCodec,
    json_name: "compressionCodec",
    enum: true
  )
end

defmodule Spark.Connect.Plan do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Plan",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:op_type, 0)

  field(:root, 1, type: Spark.Connect.Relation, oneof: 0)
  field(:command, 2, type: Spark.Connect.Command, oneof: 0)

  field(:compressed_operation, 3,
    type: Spark.Connect.Plan.CompressedOperation,
    json_name: "compressedOperation",
    oneof: 0
  )
end

defmodule Spark.Connect.UserContext do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.UserContext",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:user_id, 1, type: :string, json_name: "userId")
  field(:user_name, 2, type: :string, json_name: "userName")
  field(:extensions, 999, repeated: true, type: Google.Protobuf.Any)
end

defmodule Spark.Connect.AnalyzePlanRequest.Schema do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.Schema",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
end

defmodule Spark.Connect.AnalyzePlanRequest.Explain do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.Explain",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)

  field(:explain_mode, 2,
    type: Spark.Connect.AnalyzePlanRequest.Explain.ExplainMode,
    json_name: "explainMode",
    enum: true
  )
end

defmodule Spark.Connect.AnalyzePlanRequest.TreeString do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.TreeString",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
  field(:level, 2, proto3_optional: true, type: :int32)
end

defmodule Spark.Connect.AnalyzePlanRequest.IsLocal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.IsLocal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
end

defmodule Spark.Connect.AnalyzePlanRequest.IsStreaming do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.IsStreaming",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
end

defmodule Spark.Connect.AnalyzePlanRequest.InputFiles do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.InputFiles",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
end

defmodule Spark.Connect.AnalyzePlanRequest.SparkVersion do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.SparkVersion",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.AnalyzePlanRequest.DDLParse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.DDLParse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ddl_string, 1, type: :string, json_name: "ddlString")
end

defmodule Spark.Connect.AnalyzePlanRequest.SameSemantics do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.SameSemantics",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:target_plan, 1, type: Spark.Connect.Plan, json_name: "targetPlan")
  field(:other_plan, 2, type: Spark.Connect.Plan, json_name: "otherPlan")
end

defmodule Spark.Connect.AnalyzePlanRequest.SemanticHash do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.SemanticHash",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan, 1, type: Spark.Connect.Plan)
end

defmodule Spark.Connect.AnalyzePlanRequest.Persist do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.Persist",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.Relation)

  field(:storage_level, 2,
    proto3_optional: true,
    type: Spark.Connect.StorageLevel,
    json_name: "storageLevel"
  )
end

defmodule Spark.Connect.AnalyzePlanRequest.Unpersist do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.Unpersist",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.Relation)
  field(:blocking, 2, proto3_optional: true, type: :bool)
end

defmodule Spark.Connect.AnalyzePlanRequest.GetStorageLevel do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.GetStorageLevel",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.AnalyzePlanRequest.JsonToDDL do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest.JsonToDDL",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:json_string, 1, type: :string, json_name: "jsonString")
end

defmodule Spark.Connect.AnalyzePlanRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:analyze, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 17,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:client_type, 3, proto3_optional: true, type: :string, json_name: "clientType")
  field(:schema, 4, type: Spark.Connect.AnalyzePlanRequest.Schema, oneof: 0)
  field(:explain, 5, type: Spark.Connect.AnalyzePlanRequest.Explain, oneof: 0)

  field(:tree_string, 6,
    type: Spark.Connect.AnalyzePlanRequest.TreeString,
    json_name: "treeString",
    oneof: 0
  )

  field(:is_local, 7,
    type: Spark.Connect.AnalyzePlanRequest.IsLocal,
    json_name: "isLocal",
    oneof: 0
  )

  field(:is_streaming, 8,
    type: Spark.Connect.AnalyzePlanRequest.IsStreaming,
    json_name: "isStreaming",
    oneof: 0
  )

  field(:input_files, 9,
    type: Spark.Connect.AnalyzePlanRequest.InputFiles,
    json_name: "inputFiles",
    oneof: 0
  )

  field(:spark_version, 10,
    type: Spark.Connect.AnalyzePlanRequest.SparkVersion,
    json_name: "sparkVersion",
    oneof: 0
  )

  field(:ddl_parse, 11,
    type: Spark.Connect.AnalyzePlanRequest.DDLParse,
    json_name: "ddlParse",
    oneof: 0
  )

  field(:same_semantics, 12,
    type: Spark.Connect.AnalyzePlanRequest.SameSemantics,
    json_name: "sameSemantics",
    oneof: 0
  )

  field(:semantic_hash, 13,
    type: Spark.Connect.AnalyzePlanRequest.SemanticHash,
    json_name: "semanticHash",
    oneof: 0
  )

  field(:persist, 14, type: Spark.Connect.AnalyzePlanRequest.Persist, oneof: 0)
  field(:unpersist, 15, type: Spark.Connect.AnalyzePlanRequest.Unpersist, oneof: 0)

  field(:get_storage_level, 16,
    type: Spark.Connect.AnalyzePlanRequest.GetStorageLevel,
    json_name: "getStorageLevel",
    oneof: 0
  )

  field(:json_to_ddl, 18,
    type: Spark.Connect.AnalyzePlanRequest.JsonToDDL,
    json_name: "jsonToDdl",
    oneof: 0
  )
end

defmodule Spark.Connect.AnalyzePlanResponse.Schema do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.Schema",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:schema, 1, type: Spark.Connect.DataType)
end

defmodule Spark.Connect.AnalyzePlanResponse.Explain do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.Explain",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:explain_string, 1, type: :string, json_name: "explainString")
end

defmodule Spark.Connect.AnalyzePlanResponse.TreeString do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.TreeString",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:tree_string, 1, type: :string, json_name: "treeString")
end

defmodule Spark.Connect.AnalyzePlanResponse.IsLocal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.IsLocal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:is_local, 1, type: :bool, json_name: "isLocal")
end

defmodule Spark.Connect.AnalyzePlanResponse.IsStreaming do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.IsStreaming",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:is_streaming, 1, type: :bool, json_name: "isStreaming")
end

defmodule Spark.Connect.AnalyzePlanResponse.InputFiles do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.InputFiles",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:files, 1, repeated: true, type: :string)
end

defmodule Spark.Connect.AnalyzePlanResponse.SparkVersion do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.SparkVersion",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:version, 1, type: :string)
end

defmodule Spark.Connect.AnalyzePlanResponse.DDLParse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.DDLParse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:parsed, 1, type: Spark.Connect.DataType)
end

defmodule Spark.Connect.AnalyzePlanResponse.SameSemantics do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.SameSemantics",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:result, 1, type: :bool)
end

defmodule Spark.Connect.AnalyzePlanResponse.SemanticHash do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.SemanticHash",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:result, 1, type: :int32)
end

defmodule Spark.Connect.AnalyzePlanResponse.Persist do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.Persist",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.AnalyzePlanResponse.Unpersist do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.Unpersist",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.AnalyzePlanResponse.GetStorageLevel do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.GetStorageLevel",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:storage_level, 1, type: Spark.Connect.StorageLevel, json_name: "storageLevel")
end

defmodule Spark.Connect.AnalyzePlanResponse.JsonToDDL do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse.JsonToDDL",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ddl_string, 1, type: :string, json_name: "ddlString")
end

defmodule Spark.Connect.AnalyzePlanResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AnalyzePlanResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:result, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 15, type: :string, json_name: "serverSideSessionId")
  field(:schema, 2, type: Spark.Connect.AnalyzePlanResponse.Schema, oneof: 0)
  field(:explain, 3, type: Spark.Connect.AnalyzePlanResponse.Explain, oneof: 0)

  field(:tree_string, 4,
    type: Spark.Connect.AnalyzePlanResponse.TreeString,
    json_name: "treeString",
    oneof: 0
  )

  field(:is_local, 5,
    type: Spark.Connect.AnalyzePlanResponse.IsLocal,
    json_name: "isLocal",
    oneof: 0
  )

  field(:is_streaming, 6,
    type: Spark.Connect.AnalyzePlanResponse.IsStreaming,
    json_name: "isStreaming",
    oneof: 0
  )

  field(:input_files, 7,
    type: Spark.Connect.AnalyzePlanResponse.InputFiles,
    json_name: "inputFiles",
    oneof: 0
  )

  field(:spark_version, 8,
    type: Spark.Connect.AnalyzePlanResponse.SparkVersion,
    json_name: "sparkVersion",
    oneof: 0
  )

  field(:ddl_parse, 9,
    type: Spark.Connect.AnalyzePlanResponse.DDLParse,
    json_name: "ddlParse",
    oneof: 0
  )

  field(:same_semantics, 10,
    type: Spark.Connect.AnalyzePlanResponse.SameSemantics,
    json_name: "sameSemantics",
    oneof: 0
  )

  field(:semantic_hash, 11,
    type: Spark.Connect.AnalyzePlanResponse.SemanticHash,
    json_name: "semanticHash",
    oneof: 0
  )

  field(:persist, 12, type: Spark.Connect.AnalyzePlanResponse.Persist, oneof: 0)
  field(:unpersist, 13, type: Spark.Connect.AnalyzePlanResponse.Unpersist, oneof: 0)

  field(:get_storage_level, 14,
    type: Spark.Connect.AnalyzePlanResponse.GetStorageLevel,
    json_name: "getStorageLevel",
    oneof: 0
  )

  field(:json_to_ddl, 16,
    type: Spark.Connect.AnalyzePlanResponse.JsonToDDL,
    json_name: "jsonToDdl",
    oneof: 0
  )
end

defmodule Spark.Connect.ExecutePlanRequest.RequestOption do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanRequest.RequestOption",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:request_option, 0)

  field(:reattach_options, 1,
    type: Spark.Connect.ReattachOptions,
    json_name: "reattachOptions",
    oneof: 0
  )

  field(:result_chunking_options, 2,
    type: Spark.Connect.ResultChunkingOptions,
    json_name: "resultChunkingOptions",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
end

defmodule Spark.Connect.ExecutePlanRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 8,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:operation_id, 6, proto3_optional: true, type: :string, json_name: "operationId")
  field(:plan, 3, type: Spark.Connect.Plan)
  field(:client_type, 4, proto3_optional: true, type: :string, json_name: "clientType")

  field(:request_options, 5,
    repeated: true,
    type: Spark.Connect.ExecutePlanRequest.RequestOption,
    json_name: "requestOptions"
  )

  field(:tags, 7, repeated: true, type: :string)
end

defmodule Spark.Connect.ExecutePlanResponse.SqlCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.SqlCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.ExecutePlanResponse.ArrowBatch do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.ArrowBatch",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:row_count, 1, type: :int64, json_name: "rowCount")
  field(:data, 2, type: :bytes)
  field(:start_offset, 3, proto3_optional: true, type: :int64, json_name: "startOffset")
  field(:chunk_index, 4, proto3_optional: true, type: :int64, json_name: "chunkIndex")

  field(:num_chunks_in_batch, 5,
    proto3_optional: true,
    type: :int64,
    json_name: "numChunksInBatch"
  )
end

defmodule Spark.Connect.ExecutePlanResponse.Metrics.MetricObject.ExecutionMetricsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.Metrics.MetricObject.ExecutionMetricsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.ExecutePlanResponse.Metrics.MetricValue)
end

defmodule Spark.Connect.ExecutePlanResponse.Metrics.MetricObject do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.Metrics.MetricObject",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:plan_id, 2, type: :int64, json_name: "planId")
  field(:parent, 3, type: :int64)

  field(:execution_metrics, 4,
    repeated: true,
    type: Spark.Connect.ExecutePlanResponse.Metrics.MetricObject.ExecutionMetricsEntry,
    json_name: "executionMetrics",
    map: true
  )
end

defmodule Spark.Connect.ExecutePlanResponse.Metrics.MetricValue do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.Metrics.MetricValue",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:value, 2, type: :int64)
  field(:metric_type, 3, type: :string, json_name: "metricType")
end

defmodule Spark.Connect.ExecutePlanResponse.Metrics do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.Metrics",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:metrics, 1, repeated: true, type: Spark.Connect.ExecutePlanResponse.Metrics.MetricObject)
end

defmodule Spark.Connect.ExecutePlanResponse.ObservedMetrics do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.ObservedMetrics",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:values, 2, repeated: true, type: Spark.Connect.Expression.Literal)
  field(:keys, 3, repeated: true, type: :string)
  field(:plan_id, 4, type: :int64, json_name: "planId")
end

defmodule Spark.Connect.ExecutePlanResponse.ResultComplete do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.ResultComplete",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.ExecutePlanResponse.ExecutionProgress.StageInfo do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.ExecutionProgress.StageInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:stage_id, 1, type: :int64, json_name: "stageId")
  field(:num_tasks, 2, type: :int64, json_name: "numTasks")
  field(:num_completed_tasks, 3, type: :int64, json_name: "numCompletedTasks")
  field(:input_bytes_read, 4, type: :int64, json_name: "inputBytesRead")
  field(:done, 5, type: :bool)
end

defmodule Spark.Connect.ExecutePlanResponse.ExecutionProgress do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse.ExecutionProgress",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:stages, 1,
    repeated: true,
    type: Spark.Connect.ExecutePlanResponse.ExecutionProgress.StageInfo
  )

  field(:num_inflight_tasks, 2, type: :int64, json_name: "numInflightTasks")
end

defmodule Spark.Connect.ExecutePlanResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutePlanResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:response_type, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 15, type: :string, json_name: "serverSideSessionId")
  field(:operation_id, 12, type: :string, json_name: "operationId")
  field(:response_id, 13, type: :string, json_name: "responseId")

  field(:arrow_batch, 2,
    type: Spark.Connect.ExecutePlanResponse.ArrowBatch,
    json_name: "arrowBatch",
    oneof: 0
  )

  field(:sql_command_result, 5,
    type: Spark.Connect.ExecutePlanResponse.SqlCommandResult,
    json_name: "sqlCommandResult",
    oneof: 0
  )

  field(:write_stream_operation_start_result, 8,
    type: Spark.Connect.WriteStreamOperationStartResult,
    json_name: "writeStreamOperationStartResult",
    oneof: 0
  )

  field(:streaming_query_command_result, 9,
    type: Spark.Connect.StreamingQueryCommandResult,
    json_name: "streamingQueryCommandResult",
    oneof: 0
  )

  field(:get_resources_command_result, 10,
    type: Spark.Connect.GetResourcesCommandResult,
    json_name: "getResourcesCommandResult",
    oneof: 0
  )

  field(:streaming_query_manager_command_result, 11,
    type: Spark.Connect.StreamingQueryManagerCommandResult,
    json_name: "streamingQueryManagerCommandResult",
    oneof: 0
  )

  field(:streaming_query_listener_events_result, 16,
    type: Spark.Connect.StreamingQueryListenerEventsResult,
    json_name: "streamingQueryListenerEventsResult",
    oneof: 0
  )

  field(:result_complete, 14,
    type: Spark.Connect.ExecutePlanResponse.ResultComplete,
    json_name: "resultComplete",
    oneof: 0
  )

  field(:create_resource_profile_command_result, 17,
    type: Spark.Connect.CreateResourceProfileCommandResult,
    json_name: "createResourceProfileCommandResult",
    oneof: 0
  )

  field(:execution_progress, 18,
    type: Spark.Connect.ExecutePlanResponse.ExecutionProgress,
    json_name: "executionProgress",
    oneof: 0
  )

  field(:checkpoint_command_result, 19,
    type: Spark.Connect.CheckpointCommandResult,
    json_name: "checkpointCommandResult",
    oneof: 0
  )

  field(:ml_command_result, 20,
    type: Spark.Connect.MlCommandResult,
    json_name: "mlCommandResult",
    oneof: 0
  )

  field(:pipeline_event_result, 21,
    type: Spark.Connect.PipelineEventResult,
    json_name: "pipelineEventResult",
    oneof: 0
  )

  field(:pipeline_command_result, 22,
    type: Spark.Connect.PipelineCommandResult,
    json_name: "pipelineCommandResult",
    oneof: 0
  )

  field(:pipeline_query_function_execution_signal, 23,
    type: Spark.Connect.PipelineQueryFunctionExecutionSignal,
    json_name: "pipelineQueryFunctionExecutionSignal",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
  field(:metrics, 4, type: Spark.Connect.ExecutePlanResponse.Metrics)

  field(:observed_metrics, 6,
    repeated: true,
    type: Spark.Connect.ExecutePlanResponse.ObservedMetrics,
    json_name: "observedMetrics"
  )

  field(:schema, 7, type: Spark.Connect.DataType)
end

defmodule Spark.Connect.KeyValue do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.KeyValue",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest.Operation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.Operation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:op_type, 0)

  field(:set, 1, type: Spark.Connect.ConfigRequest.Set, oneof: 0)
  field(:get, 2, type: Spark.Connect.ConfigRequest.Get, oneof: 0)

  field(:get_with_default, 3,
    type: Spark.Connect.ConfigRequest.GetWithDefault,
    json_name: "getWithDefault",
    oneof: 0
  )

  field(:get_option, 4,
    type: Spark.Connect.ConfigRequest.GetOption,
    json_name: "getOption",
    oneof: 0
  )

  field(:get_all, 5, type: Spark.Connect.ConfigRequest.GetAll, json_name: "getAll", oneof: 0)
  field(:unset, 6, type: Spark.Connect.ConfigRequest.Unset, oneof: 0)

  field(:is_modifiable, 7,
    type: Spark.Connect.ConfigRequest.IsModifiable,
    json_name: "isModifiable",
    oneof: 0
  )
end

defmodule Spark.Connect.ConfigRequest.Set do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.Set",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:pairs, 1, repeated: true, type: Spark.Connect.KeyValue)
  field(:silent, 2, proto3_optional: true, type: :bool)
end

defmodule Spark.Connect.ConfigRequest.Get do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.Get",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:keys, 1, repeated: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest.GetWithDefault do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.GetWithDefault",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:pairs, 1, repeated: true, type: Spark.Connect.KeyValue)
end

defmodule Spark.Connect.ConfigRequest.GetOption do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.GetOption",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:keys, 1, repeated: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest.GetAll do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.GetAll",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:prefix, 1, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest.Unset do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.Unset",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:keys, 1, repeated: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest.IsModifiable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest.IsModifiable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:keys, 1, repeated: true, type: :string)
end

defmodule Spark.Connect.ConfigRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 8,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:operation, 3, type: Spark.Connect.ConfigRequest.Operation)
  field(:client_type, 4, proto3_optional: true, type: :string, json_name: "clientType")
end

defmodule Spark.Connect.ConfigResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ConfigResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 4, type: :string, json_name: "serverSideSessionId")
  field(:pairs, 2, repeated: true, type: Spark.Connect.KeyValue)
  field(:warnings, 3, repeated: true, type: :string)
end

defmodule Spark.Connect.AddArtifactsRequest.ArtifactChunk do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsRequest.ArtifactChunk",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:data, 1, type: :bytes)
  field(:crc, 2, type: :int64)
end

defmodule Spark.Connect.AddArtifactsRequest.SingleChunkArtifact do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsRequest.SingleChunkArtifact",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:data, 2, type: Spark.Connect.AddArtifactsRequest.ArtifactChunk)
end

defmodule Spark.Connect.AddArtifactsRequest.Batch do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsRequest.Batch",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:artifacts, 1,
    repeated: true,
    type: Spark.Connect.AddArtifactsRequest.SingleChunkArtifact
  )
end

defmodule Spark.Connect.AddArtifactsRequest.BeginChunkedArtifact do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsRequest.BeginChunkedArtifact",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:total_bytes, 2, type: :int64, json_name: "totalBytes")
  field(:num_chunks, 3, type: :int64, json_name: "numChunks")

  field(:initial_chunk, 4,
    type: Spark.Connect.AddArtifactsRequest.ArtifactChunk,
    json_name: "initialChunk"
  )
end

defmodule Spark.Connect.AddArtifactsRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:payload, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")

  field(:client_observed_server_side_session_id, 7,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:client_type, 6, proto3_optional: true, type: :string, json_name: "clientType")
  field(:batch, 3, type: Spark.Connect.AddArtifactsRequest.Batch, oneof: 0)

  field(:begin_chunk, 4,
    type: Spark.Connect.AddArtifactsRequest.BeginChunkedArtifact,
    json_name: "beginChunk",
    oneof: 0
  )

  field(:chunk, 5, type: Spark.Connect.AddArtifactsRequest.ArtifactChunk, oneof: 0)
end

defmodule Spark.Connect.AddArtifactsResponse.ArtifactSummary do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsResponse.ArtifactSummary",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:is_crc_successful, 2, type: :bool, json_name: "isCrcSuccessful")
end

defmodule Spark.Connect.AddArtifactsResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AddArtifactsResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 2, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 3, type: :string, json_name: "serverSideSessionId")
  field(:artifacts, 1, repeated: true, type: Spark.Connect.AddArtifactsResponse.ArtifactSummary)
end

defmodule Spark.Connect.ArtifactStatusesRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ArtifactStatusesRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 5,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:client_type, 3, proto3_optional: true, type: :string, json_name: "clientType")
  field(:names, 4, repeated: true, type: :string)
end

defmodule Spark.Connect.ArtifactStatusesResponse.StatusesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ArtifactStatusesResponse.StatusesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.ArtifactStatusesResponse.ArtifactStatus)
end

defmodule Spark.Connect.ArtifactStatusesResponse.ArtifactStatus do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ArtifactStatusesResponse.ArtifactStatus",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:exists, 1, type: :bool)
end

defmodule Spark.Connect.ArtifactStatusesResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ArtifactStatusesResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 2, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 3, type: :string, json_name: "serverSideSessionId")

  field(:statuses, 1,
    repeated: true,
    type: Spark.Connect.ArtifactStatusesResponse.StatusesEntry,
    map: true
  )
end

defmodule Spark.Connect.InterruptRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.InterruptRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:interrupt, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 7,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:client_type, 3, proto3_optional: true, type: :string, json_name: "clientType")

  field(:interrupt_type, 4,
    type: Spark.Connect.InterruptRequest.InterruptType,
    json_name: "interruptType",
    enum: true
  )

  field(:operation_tag, 5, type: :string, json_name: "operationTag", oneof: 0)
  field(:operation_id, 6, type: :string, json_name: "operationId", oneof: 0)
end

defmodule Spark.Connect.InterruptResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.InterruptResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 3, type: :string, json_name: "serverSideSessionId")
  field(:interrupted_ids, 2, repeated: true, type: :string, json_name: "interruptedIds")
end

defmodule Spark.Connect.ReattachOptions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReattachOptions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:reattachable, 1, type: :bool)
end

defmodule Spark.Connect.ResultChunkingOptions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResultChunkingOptions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:allow_arrow_batch_chunking, 1, type: :bool, json_name: "allowArrowBatchChunking")

  field(:preferred_arrow_chunk_size, 2,
    proto3_optional: true,
    type: :int64,
    json_name: "preferredArrowChunkSize"
  )
end

defmodule Spark.Connect.ReattachExecuteRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReattachExecuteRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 6,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:operation_id, 3, type: :string, json_name: "operationId")
  field(:client_type, 4, proto3_optional: true, type: :string, json_name: "clientType")
  field(:last_response_id, 5, proto3_optional: true, type: :string, json_name: "lastResponseId")
end

defmodule Spark.Connect.ReleaseExecuteRequest.ReleaseAll do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseExecuteRequest.ReleaseAll",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.ReleaseExecuteRequest.ReleaseUntil do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseExecuteRequest.ReleaseUntil",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:response_id, 1, type: :string, json_name: "responseId")
end

defmodule Spark.Connect.ReleaseExecuteRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseExecuteRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:release, 0)

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 7,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:operation_id, 3, type: :string, json_name: "operationId")
  field(:client_type, 4, proto3_optional: true, type: :string, json_name: "clientType")

  field(:release_all, 5,
    type: Spark.Connect.ReleaseExecuteRequest.ReleaseAll,
    json_name: "releaseAll",
    oneof: 0
  )

  field(:release_until, 6,
    type: Spark.Connect.ReleaseExecuteRequest.ReleaseUntil,
    json_name: "releaseUntil",
    oneof: 0
  )
end

defmodule Spark.Connect.ReleaseExecuteResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseExecuteResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 3, type: :string, json_name: "serverSideSessionId")
  field(:operation_id, 2, proto3_optional: true, type: :string, json_name: "operationId")
end

defmodule Spark.Connect.ReleaseSessionRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseSessionRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:client_type, 3, proto3_optional: true, type: :string, json_name: "clientType")
  field(:allow_reconnect, 4, type: :bool, json_name: "allowReconnect")
end

defmodule Spark.Connect.ReleaseSessionResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ReleaseSessionResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 2, type: :string, json_name: "serverSideSessionId")
end

defmodule Spark.Connect.FetchErrorDetailsRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 5,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:error_id, 3, type: :string, json_name: "errorId")
  field(:client_type, 4, proto3_optional: true, type: :string, json_name: "clientType")
end

defmodule Spark.Connect.FetchErrorDetailsResponse.StackTraceElement do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.StackTraceElement",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:declaring_class, 1, type: :string, json_name: "declaringClass")
  field(:method_name, 2, type: :string, json_name: "methodName")
  field(:file_name, 3, proto3_optional: true, type: :string, json_name: "fileName")
  field(:line_number, 4, type: :int32, json_name: "lineNumber")
end

defmodule Spark.Connect.FetchErrorDetailsResponse.QueryContext do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.QueryContext",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:context_type, 10,
    type: Spark.Connect.FetchErrorDetailsResponse.QueryContext.ContextType,
    json_name: "contextType",
    enum: true
  )

  field(:object_type, 1, type: :string, json_name: "objectType")
  field(:object_name, 2, type: :string, json_name: "objectName")
  field(:start_index, 3, type: :int32, json_name: "startIndex")
  field(:stop_index, 4, type: :int32, json_name: "stopIndex")
  field(:fragment, 5, type: :string)
  field(:call_site, 6, type: :string, json_name: "callSite")
  field(:summary, 7, type: :string)
end

defmodule Spark.Connect.FetchErrorDetailsResponse.SparkThrowable.MessageParametersEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.SparkThrowable.MessageParametersEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.FetchErrorDetailsResponse.SparkThrowable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.SparkThrowable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:error_class, 1, proto3_optional: true, type: :string, json_name: "errorClass")

  field(:message_parameters, 2,
    repeated: true,
    type: Spark.Connect.FetchErrorDetailsResponse.SparkThrowable.MessageParametersEntry,
    json_name: "messageParameters",
    map: true
  )

  field(:query_contexts, 3,
    repeated: true,
    type: Spark.Connect.FetchErrorDetailsResponse.QueryContext,
    json_name: "queryContexts"
  )

  field(:sql_state, 4, proto3_optional: true, type: :string, json_name: "sqlState")

  field(:breaking_change_info, 5,
    proto3_optional: true,
    type: Spark.Connect.FetchErrorDetailsResponse.BreakingChangeInfo,
    json_name: "breakingChangeInfo"
  )
end

defmodule Spark.Connect.FetchErrorDetailsResponse.BreakingChangeInfo do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.BreakingChangeInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:migration_message, 1, repeated: true, type: :string, json_name: "migrationMessage")

  field(:mitigation_config, 2,
    proto3_optional: true,
    type: Spark.Connect.FetchErrorDetailsResponse.MitigationConfig,
    json_name: "mitigationConfig"
  )

  field(:needs_audit, 3, proto3_optional: true, type: :bool, json_name: "needsAudit")
end

defmodule Spark.Connect.FetchErrorDetailsResponse.MitigationConfig do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.MitigationConfig",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.FetchErrorDetailsResponse.Error do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse.Error",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:error_type_hierarchy, 1, repeated: true, type: :string, json_name: "errorTypeHierarchy")
  field(:message, 2, type: :string)

  field(:stack_trace, 3,
    repeated: true,
    type: Spark.Connect.FetchErrorDetailsResponse.StackTraceElement,
    json_name: "stackTrace"
  )

  field(:cause_idx, 4, proto3_optional: true, type: :int32, json_name: "causeIdx")

  field(:spark_throwable, 5,
    proto3_optional: true,
    type: Spark.Connect.FetchErrorDetailsResponse.SparkThrowable,
    json_name: "sparkThrowable"
  )
end

defmodule Spark.Connect.FetchErrorDetailsResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FetchErrorDetailsResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:server_side_session_id, 3, type: :string, json_name: "serverSideSessionId")
  field(:session_id, 4, type: :string, json_name: "sessionId")
  field(:root_error_idx, 1, proto3_optional: true, type: :int32, json_name: "rootErrorIdx")
  field(:errors, 2, repeated: true, type: Spark.Connect.FetchErrorDetailsResponse.Error)
end

defmodule Spark.Connect.CheckpointCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CheckpointCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, type: Spark.Connect.CachedRemoteRelation)
end

defmodule Spark.Connect.CloneSessionRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CloneSessionRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")

  field(:client_observed_server_side_session_id, 5,
    proto3_optional: true,
    type: :string,
    json_name: "clientObservedServerSideSessionId"
  )

  field(:user_context, 2, type: Spark.Connect.UserContext, json_name: "userContext")
  field(:client_type, 3, proto3_optional: true, type: :string, json_name: "clientType")
  field(:new_session_id, 4, proto3_optional: true, type: :string, json_name: "newSessionId")
end

defmodule Spark.Connect.CloneSessionResponse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CloneSessionResponse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:session_id, 1, type: :string, json_name: "sessionId")
  field(:server_side_session_id, 2, type: :string, json_name: "serverSideSessionId")
  field(:new_session_id, 3, type: :string, json_name: "newSessionId")
  field(:new_server_side_session_id, 4, type: :string, json_name: "newServerSideSessionId")
end

defmodule Spark.Connect.SparkConnectService.Service do
  @moduledoc false

  use GRPC.Service, name: "spark.connect.SparkConnectService", protoc_gen_elixir_version: "0.16.0"

  rpc(:ExecutePlan, Spark.Connect.ExecutePlanRequest, stream(Spark.Connect.ExecutePlanResponse))

  rpc(:AnalyzePlan, Spark.Connect.AnalyzePlanRequest, Spark.Connect.AnalyzePlanResponse)

  rpc(:Config, Spark.Connect.ConfigRequest, Spark.Connect.ConfigResponse)

  rpc(
    :AddArtifacts,
    stream(Spark.Connect.AddArtifactsRequest),
    Spark.Connect.AddArtifactsResponse
  )

  rpc(
    :ArtifactStatus,
    Spark.Connect.ArtifactStatusesRequest,
    Spark.Connect.ArtifactStatusesResponse
  )

  rpc(:Interrupt, Spark.Connect.InterruptRequest, Spark.Connect.InterruptResponse)

  rpc(
    :ReattachExecute,
    Spark.Connect.ReattachExecuteRequest,
    stream(Spark.Connect.ExecutePlanResponse)
  )

  rpc(:ReleaseExecute, Spark.Connect.ReleaseExecuteRequest, Spark.Connect.ReleaseExecuteResponse)

  rpc(:ReleaseSession, Spark.Connect.ReleaseSessionRequest, Spark.Connect.ReleaseSessionResponse)

  rpc(
    :FetchErrorDetails,
    Spark.Connect.FetchErrorDetailsRequest,
    Spark.Connect.FetchErrorDetailsResponse
  )

  rpc(:CloneSession, Spark.Connect.CloneSessionRequest, Spark.Connect.CloneSessionResponse)
end

defmodule Spark.Connect.SparkConnectService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spark.Connect.SparkConnectService.Service
end
