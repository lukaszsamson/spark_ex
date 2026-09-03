defmodule SparkEx.Wave3CatalogTest do
  use ExUnit.Case, async: true

  alias SparkEx.Catalog

  describe "build_create_database_sql/2" do
    test "quotes dotted database names per component" do
      sql = Catalog.build_create_database_sql("cat.db", [])

      assert sql == "CREATE DATABASE `cat`.`db`"
    end

    test "quotes dotted database names with if_not_exists and clauses" do
      sql =
        Catalog.build_create_database_sql("cat.db",
          if_not_exists: true,
          comment: "hello",
          location: "/tmp/x"
        )

      assert sql ==
               "CREATE DATABASE IF NOT EXISTS `cat`.`db` COMMENT 'hello' LOCATION '/tmp/x'"
    end
  end

  describe "build_drop_database_sql/2" do
    test "quotes dotted database names per component" do
      sql = Catalog.build_drop_database_sql("cat.db", if_exists: true, cascade: true)

      assert sql == "DROP DATABASE IF EXISTS `cat`.`db` CASCADE"
    end
  end

  describe "build_alter_database_sql/2" do
    test "quotes dotted database names per component when setting location" do
      sql = Catalog.build_alter_database_sql("cat.db", set_location: "/tmp/y")

      assert sql == "ALTER DATABASE `cat`.`db` SET LOCATION '/tmp/y'"
    end

    test "accepts a map of properties" do
      sql = Catalog.build_alter_database_sql("db", set_properties: %{"a" => "b"})

      assert sql == "ALTER DATABASE `db` SET DBPROPERTIES ('a'='b')"
    end

    test "accepts a keyword list of properties" do
      sql = Catalog.build_alter_database_sql("db", set_properties: [a: "b"])

      assert sql == "ALTER DATABASE `db` SET DBPROPERTIES ('a'='b')"
    end

    test "raises when both set_location and set_properties are given" do
      assert_raise ArgumentError,
                   "alter_database supports only one of :set_location or :set_properties",
                   fn ->
                     Catalog.build_alter_database_sql("db",
                       set_location: "/tmp/y",
                       set_properties: [a: "b"]
                     )
                   end
    end
  end

  describe "build_alter_table_sql/2" do
    test "accepts a map of properties" do
      sql = Catalog.build_alter_table_sql("t", set_properties: %{"a" => "b"})

      assert sql == "ALTER TABLE `t` SET TBLPROPERTIES ('a'='b')"
    end

    test "accepts a keyword list of properties" do
      sql = Catalog.build_alter_table_sql("t", set_properties: [a: "b"])

      assert sql == "ALTER TABLE `t` SET TBLPROPERTIES ('a'='b')"
    end

    test "raises when both rename_to and set_properties are given" do
      assert_raise ArgumentError,
                   "alter_table supports only one of :rename_to or :set_properties",
                   fn ->
                     Catalog.build_alter_table_sql("t",
                       rename_to: "t2",
                       set_properties: [a: "b"]
                     )
                   end
    end
  end
end
