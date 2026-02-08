defmodule Spark.Connect.StorageLevel do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StorageLevel",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:use_disk, 1, type: :bool, json_name: "useDisk")
  field(:use_memory, 2, type: :bool, json_name: "useMemory")
  field(:use_off_heap, 3, type: :bool, json_name: "useOffHeap")
  field(:deserialized, 4, type: :bool)
  field(:replication, 5, type: :int32)
end

defmodule Spark.Connect.ResourceInformation do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResourceInformation",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:addresses, 2, repeated: true, type: :string)
end

defmodule Spark.Connect.ExecutorResourceRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ExecutorResourceRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:resource_name, 1, type: :string, json_name: "resourceName")
  field(:amount, 2, type: :int64)
  field(:discovery_script, 3, proto3_optional: true, type: :string, json_name: "discoveryScript")
  field(:vendor, 4, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.TaskResourceRequest do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.TaskResourceRequest",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:resource_name, 1, type: :string, json_name: "resourceName")
  field(:amount, 2, type: :double)
end

defmodule Spark.Connect.ResourceProfile.ExecutorResourcesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResourceProfile.ExecutorResourcesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.ExecutorResourceRequest)
end

defmodule Spark.Connect.ResourceProfile.TaskResourcesEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResourceProfile.TaskResourcesEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Spark.Connect.TaskResourceRequest)
end

defmodule Spark.Connect.ResourceProfile do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResourceProfile",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:executor_resources, 1,
    repeated: true,
    type: Spark.Connect.ResourceProfile.ExecutorResourcesEntry,
    json_name: "executorResources",
    map: true
  )

  field(:task_resources, 2,
    repeated: true,
    type: Spark.Connect.ResourceProfile.TaskResourcesEntry,
    json_name: "taskResources",
    map: true
  )
end

defmodule Spark.Connect.Origin do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Origin",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:function, 0)

  field(:python_origin, 1, type: Spark.Connect.PythonOrigin, json_name: "pythonOrigin", oneof: 0)
  field(:jvm_origin, 2, type: Spark.Connect.JvmOrigin, json_name: "jvmOrigin", oneof: 0)
end

defmodule Spark.Connect.PythonOrigin do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.PythonOrigin",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:fragment, 1, type: :string)
  field(:call_site, 2, type: :string, json_name: "callSite")
end

defmodule Spark.Connect.JvmOrigin do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.JvmOrigin",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:line, 1, proto3_optional: true, type: :int32)
  field(:start_position, 2, proto3_optional: true, type: :int32, json_name: "startPosition")
  field(:start_index, 3, proto3_optional: true, type: :int32, json_name: "startIndex")
  field(:stop_index, 4, proto3_optional: true, type: :int32, json_name: "stopIndex")
  field(:sql_text, 5, proto3_optional: true, type: :string, json_name: "sqlText")
  field(:object_type, 6, proto3_optional: true, type: :string, json_name: "objectType")
  field(:object_name, 7, proto3_optional: true, type: :string, json_name: "objectName")

  field(:stack_trace, 8,
    repeated: true,
    type: Spark.Connect.StackTraceElement,
    json_name: "stackTrace"
  )
end

defmodule Spark.Connect.StackTraceElement do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.StackTraceElement",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:class_loader_name, 1, proto3_optional: true, type: :string, json_name: "classLoaderName")
  field(:module_name, 2, proto3_optional: true, type: :string, json_name: "moduleName")
  field(:module_version, 3, proto3_optional: true, type: :string, json_name: "moduleVersion")
  field(:declaring_class, 4, type: :string, json_name: "declaringClass")
  field(:method_name, 5, type: :string, json_name: "methodName")
  field(:file_name, 6, proto3_optional: true, type: :string, json_name: "fileName")
  field(:line_number, 7, type: :int32, json_name: "lineNumber")
end

defmodule Spark.Connect.ResolvedIdentifier do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ResolvedIdentifier",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:catalog_name, 1, type: :string, json_name: "catalogName")
  field(:namespace, 2, repeated: true, type: :string)
  field(:table_name, 3, type: :string, json_name: "tableName")
end

defmodule Spark.Connect.Bools do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Bools",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :bool)
end

defmodule Spark.Connect.Ints do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Ints",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :int32)
end

defmodule Spark.Connect.Longs do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Longs",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :int64)
end

defmodule Spark.Connect.Floats do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Floats",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :float)
end

defmodule Spark.Connect.Doubles do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Doubles",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :double)
end

defmodule Spark.Connect.Strings do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Strings",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:values, 1, repeated: true, type: :string)
end
