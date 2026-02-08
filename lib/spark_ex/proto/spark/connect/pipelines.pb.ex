defmodule Spark.Connect.OutputType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.OutputType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:OUTPUT_TYPE_UNSPECIFIED, 0)
  field(:MATERIALIZED_VIEW, 1)
  field(:TABLE, 2)
  field(:TEMPORARY_VIEW, 3)
  field(:SINK, 4)
end

defmodule Spark.Connect.PipelineCommand.CreateDataflowGraph.SqlConfEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.CreateDataflowGraph.SqlConfEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.PipelineCommand.CreateDataflowGraph do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.CreateDataflowGraph",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:default_catalog, 1, proto3_optional: true, type: :string, json_name: "defaultCatalog")
  field(:default_database, 2, proto3_optional: true, type: :string, json_name: "defaultDatabase")

  field(:sql_conf, 5,
    repeated: true,
    type: Spark.Connect.PipelineCommand.CreateDataflowGraph.SqlConfEntry,
    json_name: "sqlConf",
    map: true
  )
end

defmodule Spark.Connect.PipelineCommand.DropDataflowGraph do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DropDataflowGraph",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
end

defmodule Spark.Connect.PipelineCommand.DefineOutput.TableDetails.TablePropertiesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineOutput.TableDetails.TablePropertiesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.PipelineCommand.DefineOutput.TableDetails do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineOutput.TableDetails",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:schema, 0)

  field(:table_properties, 1,
    repeated: true,
    type: Spark.Connect.PipelineCommand.DefineOutput.TableDetails.TablePropertiesEntry,
    json_name: "tableProperties",
    map: true
  )

  field(:partition_cols, 2, repeated: true, type: :string, json_name: "partitionCols")
  field(:format, 3, proto3_optional: true, type: :string)
  field(:schema_data_type, 4, type: Spark.Connect.DataType, json_name: "schemaDataType", oneof: 0)
  field(:schema_string, 5, type: :string, json_name: "schemaString", oneof: 0)
  field(:clustering_columns, 6, repeated: true, type: :string, json_name: "clusteringColumns")
end

defmodule Spark.Connect.PipelineCommand.DefineOutput.SinkDetails.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineOutput.SinkDetails.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.PipelineCommand.DefineOutput.SinkDetails do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineOutput.SinkDetails",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:options, 1,
    repeated: true,
    type: Spark.Connect.PipelineCommand.DefineOutput.SinkDetails.OptionsEntry,
    map: true
  )

  field(:format, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.PipelineCommand.DefineOutput do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineOutput",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:details, 0)

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:output_name, 2, proto3_optional: true, type: :string, json_name: "outputName")

  field(:output_type, 3,
    proto3_optional: true,
    type: Spark.Connect.OutputType,
    json_name: "outputType",
    enum: true
  )

  field(:comment, 4, proto3_optional: true, type: :string)

  field(:source_code_location, 5,
    proto3_optional: true,
    type: Spark.Connect.SourceCodeLocation,
    json_name: "sourceCodeLocation"
  )

  field(:table_details, 6,
    type: Spark.Connect.PipelineCommand.DefineOutput.TableDetails,
    json_name: "tableDetails",
    oneof: 0
  )

  field(:sink_details, 7,
    type: Spark.Connect.PipelineCommand.DefineOutput.SinkDetails,
    json_name: "sinkDetails",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
end

defmodule Spark.Connect.PipelineCommand.DefineFlow.SqlConfEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineFlow.SqlConfEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.PipelineCommand.DefineFlow.WriteRelationFlowDetails do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineFlow.WriteRelationFlowDetails",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation, 1, proto3_optional: true, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.PipelineCommand.DefineFlow.Response do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineFlow.Response",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:flow_name, 1, proto3_optional: true, type: :string, json_name: "flowName")
end

defmodule Spark.Connect.PipelineCommand.DefineFlow do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineFlow",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:details, 0)

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:flow_name, 2, proto3_optional: true, type: :string, json_name: "flowName")

  field(:target_dataset_name, 3,
    proto3_optional: true,
    type: :string,
    json_name: "targetDatasetName"
  )

  field(:sql_conf, 4,
    repeated: true,
    type: Spark.Connect.PipelineCommand.DefineFlow.SqlConfEntry,
    json_name: "sqlConf",
    map: true
  )

  field(:client_id, 5, proto3_optional: true, type: :string, json_name: "clientId")

  field(:source_code_location, 6,
    proto3_optional: true,
    type: Spark.Connect.SourceCodeLocation,
    json_name: "sourceCodeLocation"
  )

  field(:relation_flow_details, 7,
    type: Spark.Connect.PipelineCommand.DefineFlow.WriteRelationFlowDetails,
    json_name: "relationFlowDetails",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
  field(:once, 8, proto3_optional: true, type: :bool)
end

defmodule Spark.Connect.PipelineCommand.StartRun do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.StartRun",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")

  field(:full_refresh_selection, 2,
    repeated: true,
    type: :string,
    json_name: "fullRefreshSelection"
  )

  field(:full_refresh_all, 3, proto3_optional: true, type: :bool, json_name: "fullRefreshAll")
  field(:refresh_selection, 4, repeated: true, type: :string, json_name: "refreshSelection")
  field(:dry, 5, proto3_optional: true, type: :bool)
  field(:storage, 6, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.PipelineCommand.DefineSqlGraphElements do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineSqlGraphElements",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:sql_file_path, 2, proto3_optional: true, type: :string, json_name: "sqlFilePath")
  field(:sql_text, 3, proto3_optional: true, type: :string, json_name: "sqlText")
end

defmodule Spark.Connect.PipelineCommand.GetQueryFunctionExecutionSignalStream do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.GetQueryFunctionExecutionSignalStream",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:client_id, 2, proto3_optional: true, type: :string, json_name: "clientId")
end

defmodule Spark.Connect.PipelineCommand.DefineFlowQueryFunctionResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand.DefineFlowQueryFunctionResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:flow_name, 1, proto3_optional: true, type: :string, json_name: "flowName")
  field(:dataflow_graph_id, 2, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:relation, 3, proto3_optional: true, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.PipelineCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command_type, 0)

  field(:create_dataflow_graph, 1,
    type: Spark.Connect.PipelineCommand.CreateDataflowGraph,
    json_name: "createDataflowGraph",
    oneof: 0
  )

  field(:define_output, 2,
    type: Spark.Connect.PipelineCommand.DefineOutput,
    json_name: "defineOutput",
    oneof: 0
  )

  field(:define_flow, 3,
    type: Spark.Connect.PipelineCommand.DefineFlow,
    json_name: "defineFlow",
    oneof: 0
  )

  field(:drop_dataflow_graph, 4,
    type: Spark.Connect.PipelineCommand.DropDataflowGraph,
    json_name: "dropDataflowGraph",
    oneof: 0
  )

  field(:start_run, 5,
    type: Spark.Connect.PipelineCommand.StartRun,
    json_name: "startRun",
    oneof: 0
  )

  field(:define_sql_graph_elements, 6,
    type: Spark.Connect.PipelineCommand.DefineSqlGraphElements,
    json_name: "defineSqlGraphElements",
    oneof: 0
  )

  field(:get_query_function_execution_signal_stream, 7,
    type: Spark.Connect.PipelineCommand.GetQueryFunctionExecutionSignalStream,
    json_name: "getQueryFunctionExecutionSignalStream",
    oneof: 0
  )

  field(:define_flow_query_function_result, 8,
    type: Spark.Connect.PipelineCommand.DefineFlowQueryFunctionResult,
    json_name: "defineFlowQueryFunctionResult",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
end

defmodule Spark.Connect.PipelineCommandResult.CreateDataflowGraphResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommandResult.CreateDataflowGraphResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
end

defmodule Spark.Connect.PipelineCommandResult.DefineOutputResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommandResult.DefineOutputResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:resolved_identifier, 1,
    proto3_optional: true,
    type: Spark.Connect.ResolvedIdentifier,
    json_name: "resolvedIdentifier"
  )
end

defmodule Spark.Connect.PipelineCommandResult.DefineFlowResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommandResult.DefineFlowResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:resolved_identifier, 1,
    proto3_optional: true,
    type: Spark.Connect.ResolvedIdentifier,
    json_name: "resolvedIdentifier"
  )
end

defmodule Spark.Connect.PipelineCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:result_type, 0)

  field(:create_dataflow_graph_result, 1,
    type: Spark.Connect.PipelineCommandResult.CreateDataflowGraphResult,
    json_name: "createDataflowGraphResult",
    oneof: 0
  )

  field(:define_output_result, 2,
    type: Spark.Connect.PipelineCommandResult.DefineOutputResult,
    json_name: "defineOutputResult",
    oneof: 0
  )

  field(:define_flow_result, 3,
    type: Spark.Connect.PipelineCommandResult.DefineFlowResult,
    json_name: "defineFlowResult",
    oneof: 0
  )
end

defmodule Spark.Connect.PipelineEventResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineEventResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:event, 1, type: Spark.Connect.PipelineEvent)
end

defmodule Spark.Connect.PipelineEvent do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineEvent",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:timestamp, 1, type: Google.Protobuf.Timestamp)
  field(:message, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.SourceCodeLocation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SourceCodeLocation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:file_name, 1, proto3_optional: true, type: :string, json_name: "fileName")
  field(:line_number, 2, proto3_optional: true, type: :int32, json_name: "lineNumber")
  field(:definition_path, 3, proto3_optional: true, type: :string, json_name: "definitionPath")
  field(:extension, 999, repeated: true, type: Google.Protobuf.Any)
end

defmodule Spark.Connect.PipelineQueryFunctionExecutionSignal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineQueryFunctionExecutionSignal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:flow_names, 1, repeated: true, type: :string, json_name: "flowNames")
end

defmodule Spark.Connect.PipelineAnalysisContext do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PipelineAnalysisContext",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataflow_graph_id, 1, proto3_optional: true, type: :string, json_name: "dataflowGraphId")
  field(:definition_path, 2, proto3_optional: true, type: :string, json_name: "definitionPath")
  field(:flow_name, 3, proto3_optional: true, type: :string, json_name: "flowName")
  field(:extension, 999, repeated: true, type: Google.Protobuf.Any)
end
