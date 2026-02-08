defmodule Spark.Connect.MlOperator.OperatorType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "spark.connect.MlOperator.OperatorType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:OPERATOR_TYPE_UNSPECIFIED, 0)
  field(:OPERATOR_TYPE_ESTIMATOR, 1)
  field(:OPERATOR_TYPE_TRANSFORMER, 2)
  field(:OPERATOR_TYPE_EVALUATOR, 3)
  field(:OPERATOR_TYPE_MODEL, 4)
end

defmodule Spark.Connect.MlParams.ParamsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlParams.ParamsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.Expression.Literal)
end

defmodule Spark.Connect.MlParams do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlParams",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:params, 1, repeated: true, type: Spark.Connect.MlParams.ParamsEntry, map: true)
end

defmodule Spark.Connect.MlOperator do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.MlOperator",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:uid, 2, type: :string)
  field(:type, 3, type: Spark.Connect.MlOperator.OperatorType, enum: true)
end

defmodule Spark.Connect.ObjectRef do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ObjectRef",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:id, 1, type: :string)
end
