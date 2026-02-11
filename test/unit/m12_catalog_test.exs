defmodule SparkEx.M12.CatalogPlanEncoderTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.PlanEncoder

  # ── Catalog Management ──

  describe "encode_relation for catalog operations" do
    test "encodes current_catalog" do
      plan = {:catalog, {:current_catalog}}
      {relation, counter} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:current_catalog, %Spark.Connect.CurrentCatalog{}} = catalog.cat_type
      assert counter == 1
    end

    test "encodes set_current_catalog" do
      plan = {:catalog, {:set_current_catalog, "my_catalog"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:set_current_catalog, msg} = catalog.cat_type
      assert msg.catalog_name == "my_catalog"
    end

    test "encodes list_catalogs with pattern" do
      plan = {:catalog, {:list_catalogs, "spark*"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_catalogs, msg} = catalog.cat_type
      assert msg.pattern == "spark*"
    end

    test "encodes list_catalogs with nil pattern" do
      plan = {:catalog, {:list_catalogs, nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_catalogs, msg} = catalog.cat_type
      assert msg.pattern == nil
    end
  end

  # ── Database Management ──

  describe "encode_relation for database operations" do
    test "encodes current_database" do
      plan = {:catalog, {:current_database}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:current_database, %Spark.Connect.CurrentDatabase{}} = catalog.cat_type
    end

    test "encodes set_current_database" do
      plan = {:catalog, {:set_current_database, "my_db"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:set_current_database, msg} = catalog.cat_type
      assert msg.db_name == "my_db"
    end

    test "encodes list_databases with pattern" do
      plan = {:catalog, {:list_databases, "test*"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_databases, msg} = catalog.cat_type
      assert msg.pattern == "test*"
    end

    test "encodes get_database" do
      plan = {:catalog, {:get_database, "default"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:get_database, msg} = catalog.cat_type
      assert msg.db_name == "default"
    end

    test "encodes database_exists" do
      plan = {:catalog, {:database_exists, "default"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:database_exists, msg} = catalog.cat_type
      assert msg.db_name == "default"
    end
  end

  # ── Table Management ──

  describe "encode_relation for table operations" do
    test "encodes list_tables" do
      plan = {:catalog, {:list_tables, "default", "test*"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_tables, msg} = catalog.cat_type
      assert msg.db_name == "default"
      assert msg.pattern == "test*"
    end

    test "encodes get_table" do
      plan = {:catalog, {:get_table, "my_table", nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:get_table, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
      assert msg.db_name == nil
    end

    test "encodes table_exists" do
      plan = {:catalog, {:table_exists, "my_table", "my_db"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:table_exists, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
      assert msg.db_name == "my_db"
    end

    test "encodes list_columns" do
      plan = {:catalog, {:list_columns, "my_table", "my_db"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_columns, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
      assert msg.db_name == "my_db"
    end
  end

  # ── Function Management ──

  describe "encode_relation for function operations" do
    test "encodes list_functions" do
      plan = {:catalog, {:list_functions, nil, "abs*"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:list_functions, msg} = catalog.cat_type
      assert msg.db_name == nil
      assert msg.pattern == "abs*"
    end

    test "encodes get_function" do
      plan = {:catalog, {:get_function, "abs", nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:get_function, msg} = catalog.cat_type
      assert msg.function_name == "abs"
      assert msg.db_name == nil
    end

    test "encodes function_exists" do
      plan = {:catalog, {:function_exists, "abs", nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:function_exists, msg} = catalog.cat_type
      assert msg.function_name == "abs"
    end
  end

  # ── Temp Views ──

  describe "encode_relation for temp view operations" do
    test "encodes drop_temp_view" do
      plan = {:catalog, {:drop_temp_view, "my_view"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:drop_temp_view, msg} = catalog.cat_type
      assert msg.view_name == "my_view"
    end

    test "encodes drop_global_temp_view" do
      plan = {:catalog, {:drop_global_temp_view, "my_view"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:drop_global_temp_view, msg} = catalog.cat_type
      assert msg.view_name == "my_view"
    end
  end

  # ── Caching ──

  describe "encode_relation for cache operations" do
    test "encodes is_cached" do
      plan = {:catalog, {:is_cached, "my_table"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:is_cached, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
    end

    test "encodes cache_table" do
      plan = {:catalog, {:cache_table, "my_table", nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:cache_table, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
      assert msg.storage_level == nil
    end

    test "encodes uncache_table" do
      plan = {:catalog, {:uncache_table, "my_table"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:uncache_table, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
    end

    test "encodes clear_cache" do
      plan = {:catalog, {:clear_cache}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:clear_cache, %Spark.Connect.ClearCache{}} = catalog.cat_type
    end
  end

  # ── Refresh / Recovery ──

  describe "encode_relation for refresh operations" do
    test "encodes refresh_table" do
      plan = {:catalog, {:refresh_table, "my_table"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:refresh_table, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
    end

    test "encodes refresh_by_path" do
      plan = {:catalog, {:refresh_by_path, "/data/my_table"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:refresh_by_path, msg} = catalog.cat_type
      assert msg.path == "/data/my_table"
    end

    test "encodes recover_partitions" do
      plan = {:catalog, {:recover_partitions, "my_table"}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:recover_partitions, msg} = catalog.cat_type
      assert msg.table_name == "my_table"
    end
  end

  # ── Table Creation ──

  describe "encode_relation for table creation" do
    test "encodes create_table" do
      plan =
        {:catalog,
         {:create_table, "new_table", "/data/path", "parquet", "A test table", nil,
          %{"key" => "val"}}}

      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:create_table, msg} = catalog.cat_type
      assert msg.table_name == "new_table"
      assert msg.path == "/data/path"
      assert msg.source == "parquet"
      assert msg.description == "A test table"
      assert msg.options == %{"key" => "val"}
    end

    test "encodes create_table with nil options defaults to empty map" do
      plan = {:catalog, {:create_table, "t", nil, nil, nil, nil, nil}}
      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:create_table, msg} = catalog.cat_type
      assert msg.options == %{}
    end

    test "encodes create_external_table" do
      plan =
        {:catalog,
         {:create_external_table, "ext_table", "/data/ext", "csv", nil, %{"sep" => ","}}}

      {relation, _} = PlanEncoder.encode_relation(plan, 0)

      assert {:catalog, catalog} = relation.rel_type
      assert {:create_external_table, msg} = catalog.cat_type
      assert msg.table_name == "ext_table"
      assert msg.path == "/data/ext"
      assert msg.source == "csv"
      assert msg.options == %{"sep" => ","}
    end
  end

  # ── Counter management ──

  describe "counter management" do
    test "catalog plans use exactly 1 plan_id" do
      plan = {:catalog, {:current_database}}
      {_relation, counter} = PlanEncoder.encode_relation(plan, 0)
      assert counter == 1
    end

    test "counter starts from given value" do
      plan = {:catalog, {:list_tables, nil, nil}}
      {_relation, counter} = PlanEncoder.encode_relation(plan, 10)
      assert counter == 11
    end
  end

  # ── Struct tests ──

  describe "Catalog structs" do
    test "CatalogMetadata struct" do
      meta = %SparkEx.Catalog.CatalogMetadata{name: "spark_catalog", description: nil}
      assert meta.name == "spark_catalog"
      assert meta.description == nil
    end

    test "Database struct" do
      db = %SparkEx.Catalog.Database{
        name: "default",
        catalog: "spark_catalog",
        description: "Default database",
        location_uri: "file:/tmp/spark-warehouse"
      }

      assert db.name == "default"
      assert db.catalog == "spark_catalog"
      assert db.location_uri == "file:/tmp/spark-warehouse"
    end

    test "Table struct" do
      table = %SparkEx.Catalog.Table{
        name: "my_table",
        catalog: "spark_catalog",
        namespace: ["default"],
        description: nil,
        table_type: "MANAGED",
        is_temporary: false
      }

      assert table.name == "my_table"
      assert table.table_type == "MANAGED"
      assert table.is_temporary == false
    end

    test "Function struct" do
      func = %SparkEx.Catalog.Function{
        name: "abs",
        catalog: nil,
        namespace: nil,
        description: nil,
        class_name: "org.apache.spark.sql.catalyst.expressions.Abs",
        is_temporary: false
      }

      assert func.name == "abs"
      assert func.class_name == "org.apache.spark.sql.catalyst.expressions.Abs"
    end

    test "ColumnInfo struct" do
      col = %SparkEx.Catalog.ColumnInfo{
        name: "id",
        description: nil,
        data_type: "bigint",
        nullable: true,
        is_partition: false,
        is_bucket: false,
        is_cluster: false
      }

      assert col.name == "id"
      assert col.data_type == "bigint"
      assert col.nullable == true
    end
  end

  describe "DDL SQL builders" do
    test "build_create_database_sql supports IF NOT EXISTS and LOCATION" do
      sql =
        SparkEx.Catalog.build_create_database_sql("db1",
          if_not_exists: true,
          location: "/tmp/db"
        )

      assert sql == "CREATE DATABASE IF NOT EXISTS db1 LOCATION '/tmp/db'"
    end

    test "build_drop_database_sql supports IF EXISTS and CASCADE" do
      sql =
        SparkEx.Catalog.build_drop_database_sql("db1",
          if_exists: true,
          cascade: true
        )

      assert sql == "DROP DATABASE IF EXISTS CASCADE db1"
    end

    test "build_alter_database_sql supports set_location" do
      sql = SparkEx.Catalog.build_alter_database_sql("db1", set_location: "/tmp/db")
      assert sql == "ALTER DATABASE db1 SET LOCATION '/tmp/db'"
    end

    test "build_alter_database_sql supports set_properties" do
      sql = SparkEx.Catalog.build_alter_database_sql("db1", set_properties: %{"k" => "v"})
      assert sql == "ALTER DATABASE db1 SET DBPROPERTIES ('k'='v')"
    end

    test "build_drop_table_sql supports IF EXISTS and PURGE" do
      sql = SparkEx.Catalog.build_drop_table_sql("t1", if_exists: true, purge: true)
      assert sql == "DROP TABLE IF EXISTS PURGE t1"
    end

    test "build_alter_table_sql supports rename_to" do
      sql = SparkEx.Catalog.build_alter_table_sql("t1", rename_to: "t2")
      assert sql == "ALTER TABLE t1 RENAME TO t2"
    end

    test "build_alter_table_sql supports set_properties" do
      sql = SparkEx.Catalog.build_alter_table_sql("t1", set_properties: %{"k" => "v"})
      assert sql == "ALTER TABLE t1 SET TBLPROPERTIES ('k'='v')"
    end

    test "build_create_function_sql supports temporary + using jar" do
      sql =
        SparkEx.Catalog.build_create_function_sql("f1", "com.example.F",
          temporary: true,
          using_jar: "/tmp/f.jar"
        )

      assert sql ==
               "CREATE TEMPORARY FUNCTION f1 AS 'com.example.F' USING JAR '/tmp/f.jar'"
    end

    test "build_drop_function_sql supports temporary and IF EXISTS" do
      sql = SparkEx.Catalog.build_drop_function_sql("f1", temporary: true, if_exists: true)
      assert sql == "DROP TEMPORARY IF EXISTS FUNCTION f1"
    end
  end
end
