defmodule Spark.Connect.Join.JoinType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Join.JoinType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:JOIN_TYPE_UNSPECIFIED, 0)
  field(:JOIN_TYPE_INNER, 1)
  field(:JOIN_TYPE_FULL_OUTER, 2)
  field(:JOIN_TYPE_LEFT_OUTER, 3)
  field(:JOIN_TYPE_RIGHT_OUTER, 4)
  field(:JOIN_TYPE_LEFT_ANTI, 5)
  field(:JOIN_TYPE_LEFT_SEMI, 6)
  field(:JOIN_TYPE_CROSS, 7)
end

defmodule Spark.Connect.SetOperation.SetOpType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.SetOperation.SetOpType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SET_OP_TYPE_UNSPECIFIED, 0)
  field(:SET_OP_TYPE_INTERSECT, 1)
  field(:SET_OP_TYPE_UNION, 2)
  field(:SET_OP_TYPE_EXCEPT, 3)
end

defmodule Spark.Connect.Aggregate.GroupType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Aggregate.GroupType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:GROUP_TYPE_UNSPECIFIED, 0)
  field(:GROUP_TYPE_GROUPBY, 1)
  field(:GROUP_TYPE_ROLLUP, 2)
  field(:GROUP_TYPE_CUBE, 3)
  field(:GROUP_TYPE_PIVOT, 4)
  field(:GROUP_TYPE_GROUPING_SETS, 5)
end

defmodule Spark.Connect.Parse.ParseFormat do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Parse.ParseFormat",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:PARSE_FORMAT_UNSPECIFIED, 0)
  field(:PARSE_FORMAT_CSV, 1)
  field(:PARSE_FORMAT_JSON, 2)
end

defmodule Spark.Connect.Relation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Relation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:rel_type, 0)

  field(:common, 1, type: Spark.Connect.RelationCommon)
  field(:read, 2, type: Spark.Connect.Read, oneof: 0)
  field(:project, 3, type: Spark.Connect.Project, oneof: 0)
  field(:filter, 4, type: Spark.Connect.Filter, oneof: 0)
  field(:join, 5, type: Spark.Connect.Join, oneof: 0)
  field(:set_op, 6, type: Spark.Connect.SetOperation, json_name: "setOp", oneof: 0)
  field(:sort, 7, type: Spark.Connect.Sort, oneof: 0)
  field(:limit, 8, type: Spark.Connect.Limit, oneof: 0)
  field(:aggregate, 9, type: Spark.Connect.Aggregate, oneof: 0)
  field(:sql, 10, type: Spark.Connect.SQL, oneof: 0)

  field(:local_relation, 11,
    type: Spark.Connect.LocalRelation,
    json_name: "localRelation",
    oneof: 0
  )

  field(:sample, 12, type: Spark.Connect.Sample, oneof: 0)
  field(:offset, 13, type: Spark.Connect.Offset, oneof: 0)
  field(:deduplicate, 14, type: Spark.Connect.Deduplicate, oneof: 0)
  field(:range, 15, type: Spark.Connect.Range, oneof: 0)

  field(:subquery_alias, 16,
    type: Spark.Connect.SubqueryAlias,
    json_name: "subqueryAlias",
    oneof: 0
  )

  field(:repartition, 17, type: Spark.Connect.Repartition, oneof: 0)
  field(:to_df, 18, type: Spark.Connect.ToDF, json_name: "toDf", oneof: 0)

  field(:with_columns_renamed, 19,
    type: Spark.Connect.WithColumnsRenamed,
    json_name: "withColumnsRenamed",
    oneof: 0
  )

  field(:show_string, 20, type: Spark.Connect.ShowString, json_name: "showString", oneof: 0)
  field(:drop, 21, type: Spark.Connect.Drop, oneof: 0)
  field(:tail, 22, type: Spark.Connect.Tail, oneof: 0)
  field(:with_columns, 23, type: Spark.Connect.WithColumns, json_name: "withColumns", oneof: 0)
  field(:hint, 24, type: Spark.Connect.Hint, oneof: 0)
  field(:unpivot, 25, type: Spark.Connect.Unpivot, oneof: 0)
  field(:to_schema, 26, type: Spark.Connect.ToSchema, json_name: "toSchema", oneof: 0)

  field(:repartition_by_expression, 27,
    type: Spark.Connect.RepartitionByExpression,
    json_name: "repartitionByExpression",
    oneof: 0
  )

  field(:map_partitions, 28,
    type: Spark.Connect.MapPartitions,
    json_name: "mapPartitions",
    oneof: 0
  )

  field(:collect_metrics, 29,
    type: Spark.Connect.CollectMetrics,
    json_name: "collectMetrics",
    oneof: 0
  )

  field(:parse, 30, type: Spark.Connect.Parse, oneof: 0)
  field(:group_map, 31, type: Spark.Connect.GroupMap, json_name: "groupMap", oneof: 0)
  field(:co_group_map, 32, type: Spark.Connect.CoGroupMap, json_name: "coGroupMap", oneof: 0)

  field(:with_watermark, 33,
    type: Spark.Connect.WithWatermark,
    json_name: "withWatermark",
    oneof: 0
  )

  field(:apply_in_pandas_with_state, 34,
    type: Spark.Connect.ApplyInPandasWithState,
    json_name: "applyInPandasWithState",
    oneof: 0
  )

  field(:html_string, 35, type: Spark.Connect.HtmlString, json_name: "htmlString", oneof: 0)

  field(:cached_local_relation, 36,
    type: Spark.Connect.CachedLocalRelation,
    json_name: "cachedLocalRelation",
    oneof: 0
  )

  field(:cached_remote_relation, 37,
    type: Spark.Connect.CachedRemoteRelation,
    json_name: "cachedRemoteRelation",
    oneof: 0
  )

  field(:common_inline_user_defined_table_function, 38,
    type: Spark.Connect.CommonInlineUserDefinedTableFunction,
    json_name: "commonInlineUserDefinedTableFunction",
    oneof: 0
  )

  field(:as_of_join, 39, type: Spark.Connect.AsOfJoin, json_name: "asOfJoin", oneof: 0)

  field(:common_inline_user_defined_data_source, 40,
    type: Spark.Connect.CommonInlineUserDefinedDataSource,
    json_name: "commonInlineUserDefinedDataSource",
    oneof: 0
  )

  field(:with_relations, 41,
    type: Spark.Connect.WithRelations,
    json_name: "withRelations",
    oneof: 0
  )

  field(:transpose, 42, type: Spark.Connect.Transpose, oneof: 0)

  field(:unresolved_table_valued_function, 43,
    type: Spark.Connect.UnresolvedTableValuedFunction,
    json_name: "unresolvedTableValuedFunction",
    oneof: 0
  )

  field(:lateral_join, 44, type: Spark.Connect.LateralJoin, json_name: "lateralJoin", oneof: 0)

  field(:chunked_cached_local_relation, 45,
    type: Spark.Connect.ChunkedCachedLocalRelation,
    json_name: "chunkedCachedLocalRelation",
    oneof: 0
  )

  field(:fill_na, 90, type: Spark.Connect.NAFill, json_name: "fillNa", oneof: 0)
  field(:drop_na, 91, type: Spark.Connect.NADrop, json_name: "dropNa", oneof: 0)
  field(:replace, 92, type: Spark.Connect.NAReplace, oneof: 0)
  field(:summary, 100, type: Spark.Connect.StatSummary, oneof: 0)
  field(:crosstab, 101, type: Spark.Connect.StatCrosstab, oneof: 0)
  field(:describe, 102, type: Spark.Connect.StatDescribe, oneof: 0)
  field(:cov, 103, type: Spark.Connect.StatCov, oneof: 0)
  field(:corr, 104, type: Spark.Connect.StatCorr, oneof: 0)

  field(:approx_quantile, 105,
    type: Spark.Connect.StatApproxQuantile,
    json_name: "approxQuantile",
    oneof: 0
  )

  field(:freq_items, 106, type: Spark.Connect.StatFreqItems, json_name: "freqItems", oneof: 0)
  field(:sample_by, 107, type: Spark.Connect.StatSampleBy, json_name: "sampleBy", oneof: 0)
  field(:catalog, 200, type: Spark.Connect.Catalog, oneof: 0)
  field(:ml_relation, 300, type: Spark.Connect.MlRelation, json_name: "mlRelation", oneof: 0)
  field(:extension, 998, type: Google.Protobuf.Any, oneof: 0)
  field(:unknown, 999, type: Spark.Connect.Unknown, oneof: 0)
end

defmodule Spark.Connect.MlRelation.Transform do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlRelation.Transform",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:operator, 0)

  field(:obj_ref, 1, type: Spark.Connect.ObjectRef, json_name: "objRef", oneof: 0)
  field(:transformer, 2, type: Spark.Connect.MlOperator, oneof: 0)
  field(:input, 3, type: Spark.Connect.Relation)
  field(:params, 4, type: Spark.Connect.MlParams)
end

defmodule Spark.Connect.MlRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:ml_type, 0)

  field(:transform, 1, type: Spark.Connect.MlRelation.Transform, oneof: 0)
  field(:fetch, 2, type: Spark.Connect.Fetch, oneof: 0)

  field(:model_summary_dataset, 3,
    proto3_optional: true,
    type: Spark.Connect.Relation,
    json_name: "modelSummaryDataset"
  )
end

defmodule Spark.Connect.Fetch.Method.Args do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Fetch.Method.Args",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:args_type, 0)

  field(:param, 1, type: Spark.Connect.Expression.Literal, oneof: 0)
  field(:input, 2, type: Spark.Connect.Relation, oneof: 0)
end

defmodule Spark.Connect.Fetch.Method do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Fetch.Method",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:method, 1, type: :string)
  field(:args, 2, repeated: true, type: Spark.Connect.Fetch.Method.Args)
end

defmodule Spark.Connect.Fetch do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Fetch",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:obj_ref, 1, type: Spark.Connect.ObjectRef, json_name: "objRef")
  field(:methods, 2, repeated: true, type: Spark.Connect.Fetch.Method)
end

defmodule Spark.Connect.Unknown do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Unknown",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.RelationCommon do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RelationCommon",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:source_info, 1, type: :string, json_name: "sourceInfo", deprecated: true)
  field(:plan_id, 2, proto3_optional: true, type: :int64, json_name: "planId")
  field(:origin, 3, type: Spark.Connect.Origin)
end

defmodule Spark.Connect.SQL.ArgsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SQL.ArgsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.SQL.NamedArgumentsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SQL.NamedArgumentsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.SQL do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SQL",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:query, 1, type: :string)
  field(:args, 2, repeated: true, type: Spark.Connect.SQL.ArgsEntry, map: true, deprecated: true)

  field(:pos_args, 3,
    repeated: true,
    type: Spark.Connect.Expression.Literal,
    json_name: "posArgs",
    deprecated: true
  )

  field(:named_arguments, 4,
    repeated: true,
    type: Spark.Connect.SQL.NamedArgumentsEntry,
    json_name: "namedArguments",
    map: true
  )

  field(:pos_arguments, 5,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "posArguments"
  )
end

defmodule Spark.Connect.WithRelations do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithRelations",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:root, 1, type: Spark.Connect.Relation)
  field(:references, 2, repeated: true, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.Read.NamedTable.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Read.NamedTable.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.Read.NamedTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Read.NamedTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:unparsed_identifier, 1, type: :string, json_name: "unparsedIdentifier")
  field(:options, 2, repeated: true, type: Spark.Connect.Read.NamedTable.OptionsEntry, map: true)
end

defmodule Spark.Connect.Read.DataSource.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Read.DataSource.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.Read.DataSource do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Read.DataSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:format, 1, proto3_optional: true, type: :string)
  field(:schema, 2, proto3_optional: true, type: :string)
  field(:options, 3, repeated: true, type: Spark.Connect.Read.DataSource.OptionsEntry, map: true)
  field(:paths, 4, repeated: true, type: :string)
  field(:predicates, 5, repeated: true, type: :string)
end

defmodule Spark.Connect.Read do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Read",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:read_type, 0)

  field(:named_table, 1, type: Spark.Connect.Read.NamedTable, json_name: "namedTable", oneof: 0)
  field(:data_source, 2, type: Spark.Connect.Read.DataSource, json_name: "dataSource", oneof: 0)
  field(:is_streaming, 3, type: :bool, json_name: "isStreaming")
end

defmodule Spark.Connect.Project do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Project",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:expressions, 3, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Filter do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Filter",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:condition, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Join.JoinDataType do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Join.JoinDataType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:is_left_struct, 1, type: :bool, json_name: "isLeftStruct")
  field(:is_right_struct, 2, type: :bool, json_name: "isRightStruct")
end

defmodule Spark.Connect.Join do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Join",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:left, 1, type: Spark.Connect.Relation)
  field(:right, 2, type: Spark.Connect.Relation)
  field(:join_condition, 3, type: Spark.Connect.Expression, json_name: "joinCondition")
  field(:join_type, 4, type: Spark.Connect.Join.JoinType, json_name: "joinType", enum: true)
  field(:using_columns, 5, repeated: true, type: :string, json_name: "usingColumns")

  field(:join_data_type, 6,
    proto3_optional: true,
    type: Spark.Connect.Join.JoinDataType,
    json_name: "joinDataType"
  )
end

defmodule Spark.Connect.SetOperation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SetOperation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:left_input, 1, type: Spark.Connect.Relation, json_name: "leftInput")
  field(:right_input, 2, type: Spark.Connect.Relation, json_name: "rightInput")

  field(:set_op_type, 3,
    type: Spark.Connect.SetOperation.SetOpType,
    json_name: "setOpType",
    enum: true
  )

  field(:is_all, 4, proto3_optional: true, type: :bool, json_name: "isAll")
  field(:by_name, 5, proto3_optional: true, type: :bool, json_name: "byName")

  field(:allow_missing_columns, 6,
    proto3_optional: true,
    type: :bool,
    json_name: "allowMissingColumns"
  )
end

defmodule Spark.Connect.Limit do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Limit",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:limit, 2, type: :int32)
end

defmodule Spark.Connect.Offset do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Offset",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:offset, 2, type: :int32)
end

defmodule Spark.Connect.Tail do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Tail",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:limit, 2, type: :int32)
end

defmodule Spark.Connect.Aggregate.Pivot do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Aggregate.Pivot",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:col, 1, type: Spark.Connect.Expression)
  field(:values, 2, repeated: true, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.Aggregate.GroupingSets do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Aggregate.GroupingSets",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:grouping_set, 1,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "groupingSet"
  )
end

defmodule Spark.Connect.Aggregate do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Aggregate",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:group_type, 2,
    type: Spark.Connect.Aggregate.GroupType,
    json_name: "groupType",
    enum: true
  )

  field(:grouping_expressions, 3,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "groupingExpressions"
  )

  field(:aggregate_expressions, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "aggregateExpressions"
  )

  field(:pivot, 5, type: Spark.Connect.Aggregate.Pivot)

  field(:grouping_sets, 6,
    repeated: true,
    type: Spark.Connect.Aggregate.GroupingSets,
    json_name: "groupingSets"
  )
end

defmodule Spark.Connect.Sort do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Sort",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:order, 2, repeated: true, type: Spark.Connect.Expression.SortOrder)
  field(:is_global, 3, proto3_optional: true, type: :bool, json_name: "isGlobal")
end

defmodule Spark.Connect.Drop do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Drop",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:columns, 2, repeated: true, type: Spark.Connect.Expression)
  field(:column_names, 3, repeated: true, type: :string, json_name: "columnNames")
end

defmodule Spark.Connect.Deduplicate do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Deduplicate",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:column_names, 2, repeated: true, type: :string, json_name: "columnNames")

  field(:all_columns_as_keys, 3,
    proto3_optional: true,
    type: :bool,
    json_name: "allColumnsAsKeys"
  )

  field(:within_watermark, 4, proto3_optional: true, type: :bool, json_name: "withinWatermark")
end

defmodule Spark.Connect.LocalRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.LocalRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:data, 1, proto3_optional: true, type: :bytes)
  field(:schema, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.CachedLocalRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CachedLocalRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:hash, 3, type: :string)
end

defmodule Spark.Connect.ChunkedCachedLocalRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ChunkedCachedLocalRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:dataHashes, 1, repeated: true, type: :string)
  field(:schemaHash, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.CachedRemoteRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CachedRemoteRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:relation_id, 1, type: :string, json_name: "relationId")
end

defmodule Spark.Connect.Sample do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Sample",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:lower_bound, 2, type: :double, json_name: "lowerBound")
  field(:upper_bound, 3, type: :double, json_name: "upperBound")
  field(:with_replacement, 4, proto3_optional: true, type: :bool, json_name: "withReplacement")
  field(:seed, 5, proto3_optional: true, type: :int64)
  field(:deterministic_order, 6, type: :bool, json_name: "deterministicOrder")
end

defmodule Spark.Connect.Range do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Range",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:start, 1, proto3_optional: true, type: :int64)
  field(:end, 2, type: :int64)
  field(:step, 3, type: :int64)
  field(:num_partitions, 4, proto3_optional: true, type: :int32, json_name: "numPartitions")
end

defmodule Spark.Connect.SubqueryAlias do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SubqueryAlias",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:alias, 2, type: :string)
  field(:qualifier, 3, repeated: true, type: :string)
end

defmodule Spark.Connect.Repartition do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Repartition",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:num_partitions, 2, type: :int32, json_name: "numPartitions")
  field(:shuffle, 3, proto3_optional: true, type: :bool)
end

defmodule Spark.Connect.ShowString do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ShowString",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:num_rows, 2, type: :int32, json_name: "numRows")
  field(:truncate, 3, type: :int32)
  field(:vertical, 4, type: :bool)
end

defmodule Spark.Connect.HtmlString do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.HtmlString",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:num_rows, 2, type: :int32, json_name: "numRows")
  field(:truncate, 3, type: :int32)
end

defmodule Spark.Connect.StatSummary do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatSummary",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:statistics, 2, repeated: true, type: :string)
end

defmodule Spark.Connect.StatDescribe do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatDescribe",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
end

defmodule Spark.Connect.StatCrosstab do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatCrosstab",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:col1, 2, type: :string)
  field(:col2, 3, type: :string)
end

defmodule Spark.Connect.StatCov do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatCov",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:col1, 2, type: :string)
  field(:col2, 3, type: :string)
end

defmodule Spark.Connect.StatCorr do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatCorr",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:col1, 2, type: :string)
  field(:col2, 3, type: :string)
  field(:method, 4, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.StatApproxQuantile do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatApproxQuantile",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
  field(:probabilities, 3, repeated: true, type: :double)
  field(:relative_error, 4, type: :double, json_name: "relativeError")
end

defmodule Spark.Connect.StatFreqItems do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatFreqItems",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
  field(:support, 3, proto3_optional: true, type: :double)
end

defmodule Spark.Connect.StatSampleBy.Fraction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatSampleBy.Fraction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:stratum, 1, type: Spark.Connect.Expression.Literal)
  field(:fraction, 2, type: :double)
end

defmodule Spark.Connect.StatSampleBy do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StatSampleBy",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:col, 2, type: Spark.Connect.Expression)
  field(:fractions, 3, repeated: true, type: Spark.Connect.StatSampleBy.Fraction)
  field(:seed, 5, proto3_optional: true, type: :int64)
end

defmodule Spark.Connect.NAFill do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.NAFill",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
  field(:values, 3, repeated: true, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.NADrop do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.NADrop",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
  field(:min_non_nulls, 3, proto3_optional: true, type: :int32, json_name: "minNonNulls")
end

defmodule Spark.Connect.NAReplace.Replacement do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.NAReplace.Replacement",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:old_value, 1, type: Spark.Connect.Expression.Literal, json_name: "oldValue")
  field(:new_value, 2, type: Spark.Connect.Expression.Literal, json_name: "newValue")
end

defmodule Spark.Connect.NAReplace do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.NAReplace",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:cols, 2, repeated: true, type: :string)
  field(:replacements, 3, repeated: true, type: Spark.Connect.NAReplace.Replacement)
end

defmodule Spark.Connect.ToDF do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ToDF",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:column_names, 2, repeated: true, type: :string, json_name: "columnNames")
end

defmodule Spark.Connect.WithColumnsRenamed.RenameColumnsMapEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithColumnsRenamed.RenameColumnsMapEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.WithColumnsRenamed.Rename do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithColumnsRenamed.Rename",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:col_name, 1, type: :string, json_name: "colName")
  field(:new_col_name, 2, type: :string, json_name: "newColName")
end

defmodule Spark.Connect.WithColumnsRenamed do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithColumnsRenamed",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:rename_columns_map, 2,
    repeated: true,
    type: Spark.Connect.WithColumnsRenamed.RenameColumnsMapEntry,
    json_name: "renameColumnsMap",
    map: true,
    deprecated: true
  )

  field(:renames, 3, repeated: true, type: Spark.Connect.WithColumnsRenamed.Rename)
end

defmodule Spark.Connect.WithColumns do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithColumns",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:aliases, 2, repeated: true, type: Spark.Connect.Expression.Alias)
end

defmodule Spark.Connect.WithWatermark do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.WithWatermark",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:event_time, 2, type: :string, json_name: "eventTime")
  field(:delay_threshold, 3, type: :string, json_name: "delayThreshold")
end

defmodule Spark.Connect.Hint do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Hint",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:name, 2, type: :string)
  field(:parameters, 3, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Unpivot.Values do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Unpivot.Values",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Unpivot do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Unpivot",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:ids, 2, repeated: true, type: Spark.Connect.Expression)
  field(:values, 3, proto3_optional: true, type: Spark.Connect.Unpivot.Values)
  field(:variable_column_name, 4, type: :string, json_name: "variableColumnName")
  field(:value_column_name, 5, type: :string, json_name: "valueColumnName")
end

defmodule Spark.Connect.Transpose do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Transpose",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:index_columns, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "indexColumns"
  )
end

defmodule Spark.Connect.UnresolvedTableValuedFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.UnresolvedTableValuedFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:arguments, 2, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.ToSchema do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ToSchema",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:schema, 2, type: Spark.Connect.DataType)
end

defmodule Spark.Connect.RepartitionByExpression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RepartitionByExpression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:partition_exprs, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "partitionExprs"
  )

  field(:num_partitions, 3, proto3_optional: true, type: :int32, json_name: "numPartitions")
end

defmodule Spark.Connect.MapPartitions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MapPartitions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:func, 2, type: Spark.Connect.CommonInlineUserDefinedFunction)
  field(:is_barrier, 3, proto3_optional: true, type: :bool, json_name: "isBarrier")
  field(:profile_id, 4, proto3_optional: true, type: :int32, json_name: "profileId")
end

defmodule Spark.Connect.GroupMap do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GroupMap",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:grouping_expressions, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "groupingExpressions"
  )

  field(:func, 3, type: Spark.Connect.CommonInlineUserDefinedFunction)

  field(:sorting_expressions, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "sortingExpressions"
  )

  field(:initial_input, 5, type: Spark.Connect.Relation, json_name: "initialInput")

  field(:initial_grouping_expressions, 6,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "initialGroupingExpressions"
  )

  field(:is_map_groups_with_state, 7,
    proto3_optional: true,
    type: :bool,
    json_name: "isMapGroupsWithState"
  )

  field(:output_mode, 8, proto3_optional: true, type: :string, json_name: "outputMode")
  field(:timeout_conf, 9, proto3_optional: true, type: :string, json_name: "timeoutConf")

  field(:state_schema, 10,
    proto3_optional: true,
    type: Spark.Connect.DataType,
    json_name: "stateSchema"
  )

  field(:transform_with_state_info, 11,
    proto3_optional: true,
    type: Spark.Connect.TransformWithStateInfo,
    json_name: "transformWithStateInfo"
  )
end

defmodule Spark.Connect.TransformWithStateInfo do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.TransformWithStateInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:time_mode, 1, type: :string, json_name: "timeMode")

  field(:event_time_column_name, 2,
    proto3_optional: true,
    type: :string,
    json_name: "eventTimeColumnName"
  )

  field(:output_schema, 3,
    proto3_optional: true,
    type: Spark.Connect.DataType,
    json_name: "outputSchema"
  )
end

defmodule Spark.Connect.CoGroupMap do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CoGroupMap",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:input_grouping_expressions, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "inputGroupingExpressions"
  )

  field(:other, 3, type: Spark.Connect.Relation)

  field(:other_grouping_expressions, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "otherGroupingExpressions"
  )

  field(:func, 5, type: Spark.Connect.CommonInlineUserDefinedFunction)

  field(:input_sorting_expressions, 6,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "inputSortingExpressions"
  )

  field(:other_sorting_expressions, 7,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "otherSortingExpressions"
  )
end

defmodule Spark.Connect.ApplyInPandasWithState do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ApplyInPandasWithState",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)

  field(:grouping_expressions, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "groupingExpressions"
  )

  field(:func, 3, type: Spark.Connect.CommonInlineUserDefinedFunction)
  field(:output_schema, 4, type: :string, json_name: "outputSchema")
  field(:state_schema, 5, type: :string, json_name: "stateSchema")
  field(:output_mode, 6, type: :string, json_name: "outputMode")
  field(:timeout_conf, 7, type: :string, json_name: "timeoutConf")
end

defmodule Spark.Connect.CommonInlineUserDefinedTableFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CommonInlineUserDefinedTableFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:function, 0)

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:deterministic, 2, type: :bool)
  field(:arguments, 3, repeated: true, type: Spark.Connect.Expression)
  field(:python_udtf, 4, type: Spark.Connect.PythonUDTF, json_name: "pythonUdtf", oneof: 0)
end

defmodule Spark.Connect.PythonUDTF do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PythonUDTF",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:return_type, 1,
    proto3_optional: true,
    type: Spark.Connect.DataType,
    json_name: "returnType"
  )

  field(:eval_type, 2, type: :int32, json_name: "evalType")
  field(:command, 3, type: :bytes)
  field(:python_ver, 4, type: :string, json_name: "pythonVer")
end

defmodule Spark.Connect.CommonInlineUserDefinedDataSource do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CommonInlineUserDefinedDataSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:data_source, 0)

  field(:name, 1, type: :string)

  field(:python_data_source, 2,
    type: Spark.Connect.PythonDataSource,
    json_name: "pythonDataSource",
    oneof: 0
  )
end

defmodule Spark.Connect.PythonDataSource do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PythonDataSource",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:command, 1, type: :bytes)
  field(:python_ver, 2, type: :string, json_name: "pythonVer")
end

defmodule Spark.Connect.CollectMetrics do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CollectMetrics",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:name, 2, type: :string)
  field(:metrics, 3, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Parse.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Parse.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.Parse do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Parse",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:format, 2, type: Spark.Connect.Parse.ParseFormat, enum: true)
  field(:schema, 3, proto3_optional: true, type: Spark.Connect.DataType)
  field(:options, 4, repeated: true, type: Spark.Connect.Parse.OptionsEntry, map: true)
end

defmodule Spark.Connect.AsOfJoin do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.AsOfJoin",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:left, 1, type: Spark.Connect.Relation)
  field(:right, 2, type: Spark.Connect.Relation)
  field(:left_as_of, 3, type: Spark.Connect.Expression, json_name: "leftAsOf")
  field(:right_as_of, 4, type: Spark.Connect.Expression, json_name: "rightAsOf")
  field(:join_expr, 5, type: Spark.Connect.Expression, json_name: "joinExpr")
  field(:using_columns, 6, repeated: true, type: :string, json_name: "usingColumns")
  field(:join_type, 7, type: :string, json_name: "joinType")
  field(:tolerance, 8, type: Spark.Connect.Expression)
  field(:allow_exact_matches, 9, type: :bool, json_name: "allowExactMatches")
  field(:direction, 10, type: :string)
end

defmodule Spark.Connect.LateralJoin do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.LateralJoin",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:left, 1, type: Spark.Connect.Relation)
  field(:right, 2, type: Spark.Connect.Relation)
  field(:join_condition, 3, type: Spark.Connect.Expression, json_name: "joinCondition")
  field(:join_type, 4, type: Spark.Connect.Join.JoinType, json_name: "joinType", enum: true)
end
