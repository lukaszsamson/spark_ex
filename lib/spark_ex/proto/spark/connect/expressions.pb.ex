defmodule Spark.Connect.Expression.Window.WindowFrame.FrameType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Expression.Window.WindowFrame.FrameType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:FRAME_TYPE_UNDEFINED, 0)
  field(:FRAME_TYPE_ROW, 1)
  field(:FRAME_TYPE_RANGE, 2)
end

defmodule Spark.Connect.Expression.SortOrder.SortDirection do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Expression.SortOrder.SortDirection",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SORT_DIRECTION_UNSPECIFIED, 0)
  field(:SORT_DIRECTION_ASCENDING, 1)
  field(:SORT_DIRECTION_DESCENDING, 2)
end

defmodule Spark.Connect.Expression.SortOrder.NullOrdering do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Expression.SortOrder.NullOrdering",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SORT_NULLS_UNSPECIFIED, 0)
  field(:SORT_NULLS_FIRST, 1)
  field(:SORT_NULLS_LAST, 2)
end

defmodule Spark.Connect.Expression.Cast.EvalMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.Expression.Cast.EvalMode",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:EVAL_MODE_UNSPECIFIED, 0)
  field(:EVAL_MODE_LEGACY, 1)
  field(:EVAL_MODE_ANSI, 2)
  field(:EVAL_MODE_TRY, 3)
end

defmodule Spark.Connect.MergeAction.ActionType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.MergeAction.ActionType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:ACTION_TYPE_INVALID, 0)
  field(:ACTION_TYPE_DELETE, 1)
  field(:ACTION_TYPE_INSERT, 2)
  field(:ACTION_TYPE_INSERT_STAR, 3)
  field(:ACTION_TYPE_UPDATE, 4)
  field(:ACTION_TYPE_UPDATE_STAR, 5)
end

defmodule Spark.Connect.SubqueryExpression.SubqueryType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.SubqueryExpression.SubqueryType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:SUBQUERY_TYPE_UNKNOWN, 0)
  field(:SUBQUERY_TYPE_SCALAR, 1)
  field(:SUBQUERY_TYPE_EXISTS, 2)
  field(:SUBQUERY_TYPE_TABLE_ARG, 3)
  field(:SUBQUERY_TYPE_IN, 4)
end

defmodule Spark.Connect.Expression.Window.WindowFrame.FrameBoundary do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Window.WindowFrame.FrameBoundary",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:boundary, 0)

  field(:current_row, 1, type: :bool, json_name: "currentRow", oneof: 0)
  field(:unbounded, 2, type: :bool, oneof: 0)
  field(:value, 3, type: Spark.Connect.Expression, oneof: 0)
end

defmodule Spark.Connect.Expression.Window.WindowFrame do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Window.WindowFrame",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:frame_type, 1,
    type: Spark.Connect.Expression.Window.WindowFrame.FrameType,
    json_name: "frameType",
    enum: true
  )

  field(:lower, 2, type: Spark.Connect.Expression.Window.WindowFrame.FrameBoundary)
  field(:upper, 3, type: Spark.Connect.Expression.Window.WindowFrame.FrameBoundary)
end

defmodule Spark.Connect.Expression.Window do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Window",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:window_function, 1, type: Spark.Connect.Expression, json_name: "windowFunction")

  field(:partition_spec, 2,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "partitionSpec"
  )

  field(:order_spec, 3,
    repeated: true,
    type: Spark.Connect.Expression.SortOrder,
    json_name: "orderSpec"
  )

  field(:frame_spec, 4, type: Spark.Connect.Expression.Window.WindowFrame, json_name: "frameSpec")
end

defmodule Spark.Connect.Expression.SortOrder do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.SortOrder",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:child, 1, type: Spark.Connect.Expression)
  field(:direction, 2, type: Spark.Connect.Expression.SortOrder.SortDirection, enum: true)

  field(:null_ordering, 3,
    type: Spark.Connect.Expression.SortOrder.NullOrdering,
    json_name: "nullOrdering",
    enum: true
  )
end

defmodule Spark.Connect.Expression.DirectShufflePartitionID do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.DirectShufflePartitionID",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:child, 1, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Expression.Cast do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Cast",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:cast_to_type, 0)

  field(:expr, 1, type: Spark.Connect.Expression)
  field(:type, 2, type: Spark.Connect.DataType, oneof: 0)
  field(:type_str, 3, type: :string, json_name: "typeStr", oneof: 0)

  field(:eval_mode, 4,
    type: Spark.Connect.Expression.Cast.EvalMode,
    json_name: "evalMode",
    enum: true
  )
end

defmodule Spark.Connect.Expression.Literal.Decimal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.Decimal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:value, 1, type: :string)
  field(:precision, 2, proto3_optional: true, type: :int32)
  field(:scale, 3, proto3_optional: true, type: :int32)
end

defmodule Spark.Connect.Expression.Literal.CalendarInterval do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.CalendarInterval",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:months, 1, type: :int32)
  field(:days, 2, type: :int32)
  field(:microseconds, 3, type: :int64)
end

defmodule Spark.Connect.Expression.Literal.Array do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.Array",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:element_type, 1,
    type: Spark.Connect.DataType,
    json_name: "elementType",
    deprecated: true
  )

  field(:elements, 2, repeated: true, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.Expression.Literal.Map do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.Map",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key_type, 1, type: Spark.Connect.DataType, json_name: "keyType", deprecated: true)
  field(:value_type, 2, type: Spark.Connect.DataType, json_name: "valueType", deprecated: true)
  field(:keys, 3, repeated: true, type: Spark.Connect.Expression.Literal)
  field(:values, 4, repeated: true, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.Expression.Literal.Struct do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.Struct",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:struct_type, 1, type: Spark.Connect.DataType, json_name: "structType", deprecated: true)
  field(:elements, 2, repeated: true, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.Expression.Literal.SpecializedArray do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.SpecializedArray",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:value_type, 0)

  field(:bools, 1, type: Spark.Connect.Bools, oneof: 0)
  field(:ints, 2, type: Spark.Connect.Ints, oneof: 0)
  field(:longs, 3, type: Spark.Connect.Longs, oneof: 0)
  field(:floats, 4, type: Spark.Connect.Floats, oneof: 0)
  field(:doubles, 5, type: Spark.Connect.Doubles, oneof: 0)
  field(:strings, 6, type: Spark.Connect.Strings, oneof: 0)
end

defmodule Spark.Connect.Expression.Literal.Time do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal.Time",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:nano, 1, type: :int64)
  field(:precision, 2, proto3_optional: true, type: :int32)
end

defmodule Spark.Connect.Expression.Literal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Literal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:literal_type, 0)

  field(:null, 1, type: Spark.Connect.DataType, oneof: 0)
  field(:binary, 2, type: :bytes, oneof: 0)
  field(:boolean, 3, type: :bool, oneof: 0)
  field(:byte, 4, type: :int32, oneof: 0)
  field(:short, 5, type: :int32, oneof: 0)
  field(:integer, 6, type: :int32, oneof: 0)
  field(:long, 7, type: :int64, oneof: 0)
  field(:float, 10, type: :float, oneof: 0)
  field(:double, 11, type: :double, oneof: 0)
  field(:decimal, 12, type: Spark.Connect.Expression.Literal.Decimal, oneof: 0)
  field(:string, 13, type: :string, oneof: 0)
  field(:date, 16, type: :int32, oneof: 0)
  field(:timestamp, 17, type: :int64, oneof: 0)
  field(:timestamp_ntz, 18, type: :int64, json_name: "timestampNtz", oneof: 0)

  field(:calendar_interval, 19,
    type: Spark.Connect.Expression.Literal.CalendarInterval,
    json_name: "calendarInterval",
    oneof: 0
  )

  field(:year_month_interval, 20, type: :int32, json_name: "yearMonthInterval", oneof: 0)
  field(:day_time_interval, 21, type: :int64, json_name: "dayTimeInterval", oneof: 0)
  field(:array, 22, type: Spark.Connect.Expression.Literal.Array, oneof: 0)
  field(:map, 23, type: Spark.Connect.Expression.Literal.Map, oneof: 0)
  field(:struct, 24, type: Spark.Connect.Expression.Literal.Struct, oneof: 0)

  field(:specialized_array, 25,
    type: Spark.Connect.Expression.Literal.SpecializedArray,
    json_name: "specializedArray",
    oneof: 0
  )

  field(:time, 26, type: Spark.Connect.Expression.Literal.Time, oneof: 0)
  field(:data_type, 100, type: Spark.Connect.DataType, json_name: "dataType")
end

defmodule Spark.Connect.Expression.UnresolvedAttribute do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedAttribute",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:unparsed_identifier, 1, type: :string, json_name: "unparsedIdentifier")
  field(:plan_id, 2, proto3_optional: true, type: :int64, json_name: "planId")
  field(:is_metadata_column, 3, proto3_optional: true, type: :bool, json_name: "isMetadataColumn")
end

defmodule Spark.Connect.Expression.UnresolvedFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:arguments, 2, repeated: true, type: Spark.Connect.Expression)
  field(:is_distinct, 3, type: :bool, json_name: "isDistinct")
  field(:is_user_defined_function, 4, type: :bool, json_name: "isUserDefinedFunction")
  field(:is_internal, 5, proto3_optional: true, type: :bool, json_name: "isInternal")
end

defmodule Spark.Connect.Expression.ExpressionString do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.ExpressionString",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:expression, 1, type: :string)
end

defmodule Spark.Connect.Expression.UnresolvedStar do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedStar",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:unparsed_target, 1, proto3_optional: true, type: :string, json_name: "unparsedTarget")
  field(:plan_id, 2, proto3_optional: true, type: :int64, json_name: "planId")
end

defmodule Spark.Connect.Expression.UnresolvedRegex do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedRegex",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:col_name, 1, type: :string, json_name: "colName")
  field(:plan_id, 2, proto3_optional: true, type: :int64, json_name: "planId")
end

defmodule Spark.Connect.Expression.UnresolvedExtractValue do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedExtractValue",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:child, 1, type: Spark.Connect.Expression)
  field(:extraction, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.Expression.UpdateFields do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UpdateFields",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:struct_expression, 1, type: Spark.Connect.Expression, json_name: "structExpression")
  field(:field_name, 2, type: :string, json_name: "fieldName")
  field(:value_expression, 3, type: Spark.Connect.Expression, json_name: "valueExpression")
end

defmodule Spark.Connect.Expression.Alias do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.Alias",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:expr, 1, type: Spark.Connect.Expression)
  field(:name, 2, repeated: true, type: :string)
  field(:metadata, 3, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.Expression.LambdaFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.LambdaFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function, 1, type: Spark.Connect.Expression)

  field(:arguments, 2,
    repeated: true,
    type: Spark.Connect.Expression.UnresolvedNamedLambdaVariable
  )
end

defmodule Spark.Connect.Expression.UnresolvedNamedLambdaVariable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression.UnresolvedNamedLambdaVariable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name_parts, 1, repeated: true, type: :string, json_name: "nameParts")
end

defmodule Spark.Connect.Expression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Expression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:expr_type, 0)

  field(:common, 18, type: Spark.Connect.ExpressionCommon)
  field(:literal, 1, type: Spark.Connect.Expression.Literal, oneof: 0)

  field(:unresolved_attribute, 2,
    type: Spark.Connect.Expression.UnresolvedAttribute,
    json_name: "unresolvedAttribute",
    oneof: 0
  )

  field(:unresolved_function, 3,
    type: Spark.Connect.Expression.UnresolvedFunction,
    json_name: "unresolvedFunction",
    oneof: 0
  )

  field(:expression_string, 4,
    type: Spark.Connect.Expression.ExpressionString,
    json_name: "expressionString",
    oneof: 0
  )

  field(:unresolved_star, 5,
    type: Spark.Connect.Expression.UnresolvedStar,
    json_name: "unresolvedStar",
    oneof: 0
  )

  field(:alias, 6, type: Spark.Connect.Expression.Alias, oneof: 0)
  field(:cast, 7, type: Spark.Connect.Expression.Cast, oneof: 0)

  field(:unresolved_regex, 8,
    type: Spark.Connect.Expression.UnresolvedRegex,
    json_name: "unresolvedRegex",
    oneof: 0
  )

  field(:sort_order, 9,
    type: Spark.Connect.Expression.SortOrder,
    json_name: "sortOrder",
    oneof: 0
  )

  field(:lambda_function, 10,
    type: Spark.Connect.Expression.LambdaFunction,
    json_name: "lambdaFunction",
    oneof: 0
  )

  field(:window, 11, type: Spark.Connect.Expression.Window, oneof: 0)

  field(:unresolved_extract_value, 12,
    type: Spark.Connect.Expression.UnresolvedExtractValue,
    json_name: "unresolvedExtractValue",
    oneof: 0
  )

  field(:update_fields, 13,
    type: Spark.Connect.Expression.UpdateFields,
    json_name: "updateFields",
    oneof: 0
  )

  field(:unresolved_named_lambda_variable, 14,
    type: Spark.Connect.Expression.UnresolvedNamedLambdaVariable,
    json_name: "unresolvedNamedLambdaVariable",
    oneof: 0
  )

  field(:common_inline_user_defined_function, 15,
    type: Spark.Connect.CommonInlineUserDefinedFunction,
    json_name: "commonInlineUserDefinedFunction",
    oneof: 0
  )

  field(:call_function, 16, type: Spark.Connect.CallFunction, json_name: "callFunction", oneof: 0)

  field(:named_argument_expression, 17,
    type: Spark.Connect.NamedArgumentExpression,
    json_name: "namedArgumentExpression",
    oneof: 0
  )

  field(:merge_action, 19, type: Spark.Connect.MergeAction, json_name: "mergeAction", oneof: 0)

  field(:typed_aggregate_expression, 20,
    type: Spark.Connect.TypedAggregateExpression,
    json_name: "typedAggregateExpression",
    oneof: 0
  )

  field(:subquery_expression, 21,
    type: Spark.Connect.SubqueryExpression,
    json_name: "subqueryExpression",
    oneof: 0
  )

  field(:direct_shuffle_partition_id, 22,
    type: Spark.Connect.Expression.DirectShufflePartitionID,
    json_name: "directShufflePartitionId",
    oneof: 0
  )

  field(:extension, 999, type: Google.Protobuf.Any, oneof: 0)
end

defmodule Spark.Connect.ExpressionCommon do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExpressionCommon",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:origin, 1, type: Spark.Connect.Origin)
end

defmodule Spark.Connect.CommonInlineUserDefinedFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CommonInlineUserDefinedFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:function, 0)

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:deterministic, 2, type: :bool)
  field(:arguments, 3, repeated: true, type: Spark.Connect.Expression)
  field(:python_udf, 4, type: Spark.Connect.PythonUDF, json_name: "pythonUdf", oneof: 0)

  field(:scalar_scala_udf, 5,
    type: Spark.Connect.ScalarScalaUDF,
    json_name: "scalarScalaUdf",
    oneof: 0
  )

  field(:java_udf, 6, type: Spark.Connect.JavaUDF, json_name: "javaUdf", oneof: 0)
  field(:is_distinct, 7, type: :bool, json_name: "isDistinct")
end

defmodule Spark.Connect.PythonUDF do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PythonUDF",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:output_type, 1, type: Spark.Connect.DataType, json_name: "outputType")
  field(:eval_type, 2, type: :int32, json_name: "evalType")
  field(:command, 3, type: :bytes)
  field(:python_ver, 4, type: :string, json_name: "pythonVer")
  field(:additional_includes, 5, repeated: true, type: :string, json_name: "additionalIncludes")
end

defmodule Spark.Connect.ScalarScalaUDF do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ScalarScalaUDF",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:payload, 1, type: :bytes)
  field(:inputTypes, 2, repeated: true, type: Spark.Connect.DataType)
  field(:outputType, 3, type: Spark.Connect.DataType)
  field(:nullable, 4, type: :bool)
  field(:aggregate, 5, type: :bool)
end

defmodule Spark.Connect.JavaUDF do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.JavaUDF",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:class_name, 1, type: :string, json_name: "className")

  field(:output_type, 2,
    proto3_optional: true,
    type: Spark.Connect.DataType,
    json_name: "outputType"
  )

  field(:aggregate, 3, type: :bool)
end

defmodule Spark.Connect.TypedAggregateExpression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.TypedAggregateExpression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:scalar_scala_udf, 1, type: Spark.Connect.ScalarScalaUDF, json_name: "scalarScalaUdf")
end

defmodule Spark.Connect.CallFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CallFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:arguments, 2, repeated: true, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.NamedArgumentExpression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.NamedArgumentExpression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.MergeAction.Assignment do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MergeAction.Assignment",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: Spark.Connect.Expression)
  field(:value, 2, type: Spark.Connect.Expression)
end

defmodule Spark.Connect.MergeAction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MergeAction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:action_type, 1,
    type: Spark.Connect.MergeAction.ActionType,
    json_name: "actionType",
    enum: true
  )

  field(:condition, 2, proto3_optional: true, type: Spark.Connect.Expression)
  field(:assignments, 3, repeated: true, type: Spark.Connect.MergeAction.Assignment)
end

defmodule Spark.Connect.SubqueryExpression.TableArgOptions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SubqueryExpression.TableArgOptions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:partition_spec, 1,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "partitionSpec"
  )

  field(:order_spec, 2,
    repeated: true,
    type: Spark.Connect.Expression.SortOrder,
    json_name: "orderSpec"
  )

  field(:with_single_partition, 3,
    proto3_optional: true,
    type: :bool,
    json_name: "withSinglePartition"
  )
end

defmodule Spark.Connect.SubqueryExpression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SubqueryExpression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:plan_id, 1, type: :int64, json_name: "planId")

  field(:subquery_type, 2,
    type: Spark.Connect.SubqueryExpression.SubqueryType,
    json_name: "subqueryType",
    enum: true
  )

  field(:table_arg_options, 3,
    proto3_optional: true,
    type: Spark.Connect.SubqueryExpression.TableArgOptions,
    json_name: "tableArgOptions"
  )

  field(:in_subquery_values, 4,
    repeated: true,
    type: Spark.Connect.Expression,
    json_name: "inSubqueryValues"
  )
end
