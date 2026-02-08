defmodule Spark.Connect.MlCommand.Fit do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Fit",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:estimator, 1, type: Spark.Connect.MlOperator)
  field(:params, 2, proto3_optional: true, type: Spark.Connect.MlParams)
  field(:dataset, 3, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.MlCommand.Delete do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Delete",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:obj_refs, 1, repeated: true, type: Spark.Connect.ObjectRef, json_name: "objRefs")
  field(:evict_only, 2, proto3_optional: true, type: :bool, json_name: "evictOnly")
end

defmodule Spark.Connect.MlCommand.CleanCache do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.CleanCache",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.MlCommand.GetCacheInfo do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.GetCacheInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.MlCommand.Write.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Write.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.MlCommand.Write do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Write",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:operator, 1, type: Spark.Connect.MlOperator, oneof: 0)
  field(:obj_ref, 2, type: Spark.Connect.ObjectRef, json_name: "objRef", oneof: 0)
  field(:params, 3, proto3_optional: true, type: Spark.Connect.MlParams)
  field(:path, 4, type: :string)
  field(:should_overwrite, 5, proto3_optional: true, type: :bool, json_name: "shouldOverwrite")
  field(:options, 6, repeated: true, type: Spark.Connect.MlCommand.Write.OptionsEntry, map: true)
end

defmodule Spark.Connect.MlCommand.Read do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Read",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:operator, 1, type: Spark.Connect.MlOperator)
  field(:path, 2, type: :string)
end

defmodule Spark.Connect.MlCommand.Evaluate do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.Evaluate",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:evaluator, 1, type: Spark.Connect.MlOperator)
  field(:params, 2, proto3_optional: true, type: Spark.Connect.MlParams)
  field(:dataset, 3, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.MlCommand.CreateSummary do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.CreateSummary",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:model_ref, 1, type: Spark.Connect.ObjectRef, json_name: "modelRef")
  field(:dataset, 2, type: Spark.Connect.Relation)
end

defmodule Spark.Connect.MlCommand.GetModelSize do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand.GetModelSize",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:model_ref, 1, type: Spark.Connect.ObjectRef, json_name: "modelRef")
end

defmodule Spark.Connect.MlCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:command, 0)

  field(:fit, 1, type: Spark.Connect.MlCommand.Fit, oneof: 0)
  field(:fetch, 2, type: Spark.Connect.Fetch, oneof: 0)
  field(:delete, 3, type: Spark.Connect.MlCommand.Delete, oneof: 0)
  field(:write, 4, type: Spark.Connect.MlCommand.Write, oneof: 0)
  field(:read, 5, type: Spark.Connect.MlCommand.Read, oneof: 0)
  field(:evaluate, 6, type: Spark.Connect.MlCommand.Evaluate, oneof: 0)

  field(:clean_cache, 7,
    type: Spark.Connect.MlCommand.CleanCache,
    json_name: "cleanCache",
    oneof: 0
  )

  field(:get_cache_info, 8,
    type: Spark.Connect.MlCommand.GetCacheInfo,
    json_name: "getCacheInfo",
    oneof: 0
  )

  field(:create_summary, 9,
    type: Spark.Connect.MlCommand.CreateSummary,
    json_name: "createSummary",
    oneof: 0
  )

  field(:get_model_size, 10,
    type: Spark.Connect.MlCommand.GetModelSize,
    json_name: "getModelSize",
    oneof: 0
  )
end

defmodule Spark.Connect.MlCommandResult.MlOperatorInfo do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommandResult.MlOperatorInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:type, 0)

  field(:obj_ref, 1, type: Spark.Connect.ObjectRef, json_name: "objRef", oneof: 0)
  field(:name, 2, type: :string, oneof: 0)
  field(:uid, 3, proto3_optional: true, type: :string)
  field(:params, 4, proto3_optional: true, type: Spark.Connect.MlParams)
  field(:warning_message, 5, proto3_optional: true, type: :string, json_name: "warningMessage")
end

defmodule Spark.Connect.MlCommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlCommandResult",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:result_type, 0)

  field(:param, 1, type: Spark.Connect.Expression.Literal, oneof: 0)
  field(:summary, 2, type: :string, oneof: 0)

  field(:operator_info, 3,
    type: Spark.Connect.MlCommandResult.MlOperatorInfo,
    json_name: "operatorInfo",
    oneof: 0
  )
end
