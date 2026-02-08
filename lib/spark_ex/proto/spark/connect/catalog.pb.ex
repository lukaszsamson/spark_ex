defmodule Spark.Connect.Catalog do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.Catalog",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:cat_type, 0)

  field(:current_database, 1,
    type: Spark.Connect.CurrentDatabase,
    json_name: "currentDatabase",
    oneof: 0
  )

  field(:set_current_database, 2,
    type: Spark.Connect.SetCurrentDatabase,
    json_name: "setCurrentDatabase",
    oneof: 0
  )

  field(:list_databases, 3,
    type: Spark.Connect.ListDatabases,
    json_name: "listDatabases",
    oneof: 0
  )

  field(:list_tables, 4, type: Spark.Connect.ListTables, json_name: "listTables", oneof: 0)

  field(:list_functions, 5,
    type: Spark.Connect.ListFunctions,
    json_name: "listFunctions",
    oneof: 0
  )

  field(:list_columns, 6, type: Spark.Connect.ListColumns, json_name: "listColumns", oneof: 0)
  field(:get_database, 7, type: Spark.Connect.GetDatabase, json_name: "getDatabase", oneof: 0)
  field(:get_table, 8, type: Spark.Connect.GetTable, json_name: "getTable", oneof: 0)
  field(:get_function, 9, type: Spark.Connect.GetFunction, json_name: "getFunction", oneof: 0)

  field(:database_exists, 10,
    type: Spark.Connect.DatabaseExists,
    json_name: "databaseExists",
    oneof: 0
  )

  field(:table_exists, 11, type: Spark.Connect.TableExists, json_name: "tableExists", oneof: 0)

  field(:function_exists, 12,
    type: Spark.Connect.FunctionExists,
    json_name: "functionExists",
    oneof: 0
  )

  field(:create_external_table, 13,
    type: Spark.Connect.CreateExternalTable,
    json_name: "createExternalTable",
    oneof: 0
  )

  field(:create_table, 14, type: Spark.Connect.CreateTable, json_name: "createTable", oneof: 0)

  field(:drop_temp_view, 15,
    type: Spark.Connect.DropTempView,
    json_name: "dropTempView",
    oneof: 0
  )

  field(:drop_global_temp_view, 16,
    type: Spark.Connect.DropGlobalTempView,
    json_name: "dropGlobalTempView",
    oneof: 0
  )

  field(:recover_partitions, 17,
    type: Spark.Connect.RecoverPartitions,
    json_name: "recoverPartitions",
    oneof: 0
  )

  field(:is_cached, 18, type: Spark.Connect.IsCached, json_name: "isCached", oneof: 0)
  field(:cache_table, 19, type: Spark.Connect.CacheTable, json_name: "cacheTable", oneof: 0)
  field(:uncache_table, 20, type: Spark.Connect.UncacheTable, json_name: "uncacheTable", oneof: 0)
  field(:clear_cache, 21, type: Spark.Connect.ClearCache, json_name: "clearCache", oneof: 0)
  field(:refresh_table, 22, type: Spark.Connect.RefreshTable, json_name: "refreshTable", oneof: 0)

  field(:refresh_by_path, 23,
    type: Spark.Connect.RefreshByPath,
    json_name: "refreshByPath",
    oneof: 0
  )

  field(:current_catalog, 24,
    type: Spark.Connect.CurrentCatalog,
    json_name: "currentCatalog",
    oneof: 0
  )

  field(:set_current_catalog, 25,
    type: Spark.Connect.SetCurrentCatalog,
    json_name: "setCurrentCatalog",
    oneof: 0
  )

  field(:list_catalogs, 26, type: Spark.Connect.ListCatalogs, json_name: "listCatalogs", oneof: 0)
end

defmodule Spark.Connect.CurrentDatabase do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CurrentDatabase",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.SetCurrentDatabase do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SetCurrentDatabase",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:db_name, 1, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.ListDatabases do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ListDatabases",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:pattern, 1, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.ListTables do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ListTables",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:db_name, 1, proto3_optional: true, type: :string, json_name: "dbName")
  field(:pattern, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.ListFunctions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ListFunctions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:db_name, 1, proto3_optional: true, type: :string, json_name: "dbName")
  field(:pattern, 2, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.ListColumns do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ListColumns",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
  field(:db_name, 2, proto3_optional: true, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.GetDatabase do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetDatabase",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:db_name, 1, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.GetTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
  field(:db_name, 2, proto3_optional: true, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.GetFunction do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.GetFunction",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:db_name, 2, proto3_optional: true, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.DatabaseExists do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DatabaseExists",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:db_name, 1, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.TableExists do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.TableExists",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
  field(:db_name, 2, proto3_optional: true, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.FunctionExists do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.FunctionExists",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:function_name, 1, type: :string, json_name: "functionName")
  field(:db_name, 2, proto3_optional: true, type: :string, json_name: "dbName")
end

defmodule Spark.Connect.CreateExternalTable.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateExternalTable.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.CreateExternalTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateExternalTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
  field(:path, 2, proto3_optional: true, type: :string)
  field(:source, 3, proto3_optional: true, type: :string)
  field(:schema, 4, proto3_optional: true, type: Spark.Connect.DataType)

  field(:options, 5,
    repeated: true,
    type: Spark.Connect.CreateExternalTable.OptionsEntry,
    map: true
  )
end

defmodule Spark.Connect.CreateTable.OptionsEntry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateTable.OptionsEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spark.Connect.CreateTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CreateTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
  field(:path, 2, proto3_optional: true, type: :string)
  field(:source, 3, proto3_optional: true, type: :string)
  field(:description, 4, proto3_optional: true, type: :string)
  field(:schema, 5, proto3_optional: true, type: Spark.Connect.DataType)
  field(:options, 6, repeated: true, type: Spark.Connect.CreateTable.OptionsEntry, map: true)
end

defmodule Spark.Connect.DropTempView do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DropTempView",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:view_name, 1, type: :string, json_name: "viewName")
end

defmodule Spark.Connect.DropGlobalTempView do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DropGlobalTempView",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:view_name, 1, type: :string, json_name: "viewName")
end

defmodule Spark.Connect.RecoverPartitions do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RecoverPartitions",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
end

defmodule Spark.Connect.IsCached do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.IsCached",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
end

defmodule Spark.Connect.CacheTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CacheTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")

  field(:storage_level, 2,
    proto3_optional: true,
    type: Spark.Connect.StorageLevel,
    json_name: "storageLevel"
  )
end

defmodule Spark.Connect.UncacheTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.UncacheTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
end

defmodule Spark.Connect.ClearCache do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ClearCache",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.RefreshTable do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RefreshTable",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:table_name, 1, type: :string, json_name: "tableName")
end

defmodule Spark.Connect.RefreshByPath do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.RefreshByPath",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:path, 1, type: :string)
end

defmodule Spark.Connect.CurrentCatalog do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.CurrentCatalog",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3
end

defmodule Spark.Connect.SetCurrentCatalog do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.SetCurrentCatalog",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:catalog_name, 1, type: :string, json_name: "catalogName")
end

defmodule Spark.Connect.ListCatalogs do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.ListCatalogs",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:pattern, 1, proto3_optional: true, type: :string)
end
