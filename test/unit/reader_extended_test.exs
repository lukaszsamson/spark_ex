defmodule SparkEx.Unit.ReaderExtendedTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.Reader
  alias SparkEx.DataFrame

  @session :fake_session

  describe "text/3" do
    test "creates DataFrame with text format" do
      df = Reader.text(@session, "/data/lines.txt")
      assert %DataFrame{} = df
      assert {:read_data_source, "text", ["/data/lines.txt"], nil, %{}} = unwrap_plan(df)
    end
  end

  describe "orc/3" do
    test "creates DataFrame with orc format" do
      df = Reader.orc(@session, "/data/events.orc")
      assert {:read_data_source, "orc", ["/data/events.orc"], nil, %{}} = unwrap_plan(df)
    end

    test "accepts schema option" do
      df = Reader.orc(@session, "/data/events.orc", schema: "id INT, name STRING")

      assert {:read_data_source, "orc", ["/data/events.orc"], "id INT, name STRING", %{}} =
               unwrap_plan(df)
    end
  end

  describe "avro/3" do
    test "creates DataFrame with avro format" do
      df = Reader.avro(@session, "/data/events.avro")
      assert {:read_data_source, "avro", ["/data/events.avro"], nil, %{}} = unwrap_plan(df)
    end
  end

  describe "xml/3" do
    test "creates DataFrame with xml format" do
      df = Reader.xml(@session, "/data/events.xml")
      assert {:read_data_source, "xml", ["/data/events.xml"], nil, %{}} = unwrap_plan(df)
    end
  end

  describe "binary_file/3" do
    test "creates DataFrame with binaryFile format" do
      df = Reader.binary_file(@session, "/data/files")
      assert {:read_data_source, "binaryFile", ["/data/files"], nil, %{}} = unwrap_plan(df)
    end
  end

  describe "jdbc/3" do
    test "creates DataFrame with jdbc format and options" do
      df = Reader.jdbc(@session, "jdbc:postgresql://host/db", "public.users")

      assert {:read_data_source, "jdbc", [], nil,
              %{"url" => "jdbc:postgresql://host/db", "dbtable" => "public.users"}} =
               unwrap_plan(df)
    end

    test "passes explicit options" do
      df =
        Reader.jdbc(@session, "jdbc:postgresql://host/db", "public.users",
          options: %{"user" => "alice", "password" => "secret"}
        )

      assert {:read_data_source, "jdbc", [], nil,
              %{
                "url" => "jdbc:postgresql://host/db",
                "dbtable" => "public.users",
                "user" => "alice",
                "password" => "secret"
              }} = unwrap_plan(df)
    end

    test "passes predicates for JDBC pushdown" do
      df =
        Reader.jdbc(@session, "jdbc:postgresql://host/db", "public.users",
          options: %{"user" => "alice"},
          predicates: ["id >= 10", "id < 20"]
        )

      assert {:read_data_source, "jdbc", [], nil,
              %{
                "url" => "jdbc:postgresql://host/db",
                "dbtable" => "public.users",
                "user" => "alice"
              }, ["id >= 10", "id < 20"]} = unwrap_plan(df)
    end
  end

  describe "load/4" do
    test "creates DataFrame with generic format" do
      df = Reader.load(@session, "avro", "/data/events.avro")

      assert {:read_data_source, "avro", ["/data/events.avro"], nil, %{}} = unwrap_plan(df)
    end

    test "accepts nil paths for formats like jdbc" do
      df =
        Reader.load(@session, "jdbc",
          options: %{"url" => "jdbc:mysql://host/db", "dbtable" => "users"}
        )

      assert {:read_data_source, "jdbc", [], nil,
              %{"url" => "jdbc:mysql://host/db", "dbtable" => "users"}} = unwrap_plan(df)
    end

    test "accepts schema option" do
      df = Reader.load(@session, "csv", "/data/file.csv", schema: "id INT")

      assert {:read_data_source, "csv", ["/data/file.csv"], "id INT", %{}} = unwrap_plan(df)
    end

    test "accepts list of paths" do
      df = Reader.load(@session, "parquet", ["/a.parquet", "/b.parquet"])

      assert {:read_data_source, "parquet", ["/a.parquet", "/b.parquet"], nil, %{}} =
               unwrap_plan(df)
    end

    test "stringifies primitive option values" do
      df =
        Reader.load(@session, "csv", "/data/file.csv",
          options: %{"header" => true, "maxColumns" => 50, "samplingRatio" => 0.25}
        )

      assert {:read_data_source, "csv", ["/data/file.csv"], nil,
              %{
                "header" => "true",
                "maxColumns" => "50",
                "samplingRatio" => "0.25"
              }} = unwrap_plan(df)
    end
  end
end
