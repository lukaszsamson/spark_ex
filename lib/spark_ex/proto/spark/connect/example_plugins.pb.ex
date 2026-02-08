defmodule Spark.Connect.ExamplePluginRelation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExamplePluginRelation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:input, 1, type: Spark.Connect.Relation)
  field(:custom_field, 2, type: :string, json_name: "customField")
end

defmodule Spark.Connect.ExamplePluginExpression do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExamplePluginExpression",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:child, 1, type: Spark.Connect.Expression)
  field(:custom_field, 2, type: :string, json_name: "customField")
end

defmodule Spark.Connect.ExamplePluginCommand do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExamplePluginCommand",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:custom_field, 1, type: :string, json_name: "customField")
end
