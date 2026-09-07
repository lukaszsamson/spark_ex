defmodule SparkEx.ReaderTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.Reader

  describe "table/2" do
    test "creates read_named_table plan" do
      df = Reader.table(self(), "my_db.my_table")
      assert {:read_named_table, "my_db.my_table", %{}} = unwrap_plan(df)
      assert df.session == self()
    end

    test "passes options" do
      df = Reader.table(self(), "my_table", options: %{"key" => "val"})
      assert {:read_named_table, "my_table", %{"key" => "val"}} = unwrap_plan(df)
    end

    test "merges top-level options with nested options" do
      df = Reader.table(self(), "my_table", streaming: false, options: %{"key" => "val"})

      assert {:read_named_table, "my_table", %{"streaming" => "false", "key" => "val"}} =
               unwrap_plan(df)
    end
  end

  describe "builder API" do
    test "SparkEx.read/1 returns a reader builder" do
      reader = SparkEx.read(self())
      assert %Reader{} = reader
      assert reader.session == self()
    end

    test "builds a stateful read plan with format/schema/options/load" do
      df =
        self()
        |> SparkEx.read()
        |> Reader.format("csv")
        |> Reader.schema("id INT, name STRING")
        |> Reader.option("header", true)
        |> Reader.options(%{"inferSchema" => false, "maxColumns" => 200})
        |> Reader.load("/tmp/data.csv")

      assert {:read_data_source, "csv", ["/tmp/data.csv"], "id INT, name STRING",
              %{"header" => "true", "inferSchema" => "false", "maxColumns" => "200"}} =
               unwrap_plan(df)
    end

    test "builder table/2 carries options" do
      df =
        self()
        |> SparkEx.read()
        |> Reader.option("streaming", false)
        |> Reader.table("my_db.my_table")

      assert {:read_named_table, "my_db.my_table", %{"streaming" => "false"}} = unwrap_plan(df)
    end

    test "Reader.schema/2 accepts Spark.Connect.DataType" do
      schema = %Spark.Connect.DataType{
        kind:
          {:struct,
           %Spark.Connect.DataType.Struct{
             fields: [
               %Spark.Connect.DataType.StructField{
                 name: "id",
                 data_type: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}},
                 nullable: false,
                 metadata: ~s({"comment":"pk"})
               }
             ]
           }}
      }

      reader = self() |> SparkEx.read() |> Reader.schema(schema)
      assert reader.schema == schema
    end

    test "csv/3 accepts a builder and pins the format" do
      df = self() |> SparkEx.read() |> Reader.csv("/tmp/x.csv")

      assert {:read_data_source, "csv", ["/tmp/x.csv"], nil, %{}} = unwrap_plan(df)
    end

    test "json/3 and xml/3 accept a builder and a list of paths" do
      json_df = self() |> SparkEx.read() |> Reader.json(["/a.json", "/b.json"])
      assert {:read_data_source, "json", ["/a.json", "/b.json"], nil, %{}} = unwrap_plan(json_df)

      xml_df = self() |> SparkEx.read() |> Reader.xml("/a.xml")
      assert {:read_data_source, "xml", ["/a.xml"], nil, %{}} = unwrap_plan(xml_df)
    end

    test "builder schema and options propagate, call-time options win" do
      reader =
        self()
        |> SparkEx.read()
        |> Reader.format("parquet")
        |> Reader.schema("id INT, name STRING")
        |> Reader.options(%{"mode" => "PERMISSIVE", "maxColumns" => 200})

      assert {:read_data_source, "csv", ["/tmp/x.csv"], "id INT, name STRING",
              %{"mode" => "FAILFAST", "maxColumns" => "200"}} =
               unwrap_plan(Reader.csv(reader, "/tmp/x.csv", options: %{"mode" => "FAILFAST"}))

      assert {:read_data_source, "json", ["/a.json"], "id INT, name STRING",
              %{"mode" => "PERMISSIVE", "maxColumns" => "200"}} =
               unwrap_plan(Reader.json(reader, "/a.json"))

      assert {:read_data_source, "xml", ["/a.xml"], "a STRING",
              %{"mode" => "PERMISSIVE", "maxColumns" => "200"}} =
               unwrap_plan(Reader.xml(reader, "/a.xml", schema: "a STRING"))
    end

    test "csv/3 with a builder normalizes :header and :sep aliases" do
      reader = self() |> SparkEx.read() |> Reader.option("quote", "'")

      assert {:read_data_source, "csv", ["/tmp/x.csv"], nil,
              %{"header" => "true", "sep" => ";", "quote" => "'"}} =
               unwrap_plan(Reader.csv(reader, "/tmp/x.csv", header: true, separator: ";"))

      assert {:read_data_source, "csv", ["/tmp/x.csv"], nil, %{"sep" => "|", "quote" => "'"}} =
               unwrap_plan(Reader.csv(reader, "/tmp/x.csv", sep: "|"))

      assert_raise ArgumentError, ~r/conflicting :sep and :separator/, fn ->
        Reader.csv(reader, "/tmp/x.csv", sep: "|", separator: ";")
      end
    end
  end

  describe "parquet/2" do
    test "creates read_data_source with parquet format" do
      df = Reader.parquet(self(), "/data/file.parquet")

      assert {:read_data_source, "parquet", ["/data/file.parquet"], nil, %{}} = unwrap_plan(df)
    end

    test "accepts list of paths" do
      paths = ["/data/part1.parquet", "/data/part2.parquet"]
      df = Reader.parquet(self(), paths)
      assert {:read_data_source, "parquet", ^paths, nil, %{}} = unwrap_plan(df)
    end

    test "accepts schema option" do
      df = Reader.parquet(self(), "/data/file.parquet", schema: "id INT, name STRING")

      assert {:read_data_source, "parquet", _, "id INT, name STRING", %{}} = unwrap_plan(df)
    end

    test "accepts SparkEx.Types struct schema option as JSON" do
      schema =
        SparkEx.Types.struct_type([
          SparkEx.Types.struct_field("id", :integer,
            nullable: false,
            metadata: %{"comment" => "pk"}
          )
        ])

      df = Reader.parquet(self(), "/data/file.parquet", schema: schema)

      assert {:read_data_source, "parquet", _, encoded_schema, %{}} = unwrap_plan(df)

      decoded = Jason.decode!(encoded_schema)
      assert decoded["type"] == "struct"
      assert Enum.at(decoded["fields"], 0)["nullable"] == false
      assert Enum.at(decoded["fields"], 0)["metadata"]["comment"] == "pk"
    end
  end

  describe "csv/2" do
    test "creates read_data_source with csv format" do
      df = Reader.csv(self(), "/data/file.csv")
      assert {:read_data_source, "csv", ["/data/file.csv"], nil, %{}} = unwrap_plan(df)
    end

    test "translates header and infer_schema options" do
      df = Reader.csv(self(), "/data/file.csv", header: true, infer_schema: true)

      assert {:read_data_source, "csv", _, nil, %{"header" => "true", "inferSchema" => "true"}} =
               unwrap_plan(df)
    end

    test "translates separator option" do
      df = Reader.csv(self(), "/data/file.csv", separator: "|")

      assert {:read_data_source, "csv", _, nil, %{"sep" => "|"}} = unwrap_plan(df)
    end

    test "raises when a convenience option is also given inside :options" do
      assert_raise ArgumentError, ~r/multiple values for keyword argument\(s\) "header"/, fn ->
        Reader.csv(self(), "/data/file.csv", header: true, options: %{"header" => "false"})
      end
    end

    test "raises when :separator and :sep disagree" do
      assert_raise ArgumentError, ~r/conflicting :sep and :separator/, fn ->
        Reader.csv(self(), "/data/file.csv", separator: "|", sep: ",")
      end
    end

    test "accepts :separator and :sep when they agree (T-22)" do
      df = Reader.csv(self(), "/data/file.csv", separator: "|", sep: "|")
      assert {:read_data_source, "csv", _, nil, %{"sep" => "|"}} = unwrap_plan(df)
    end

    test "accepts a keyword list for :options (T-22)" do
      df = Reader.csv(self(), "/data/file.csv", header: true, options: [multi_line: true])

      assert {:read_data_source, "csv", _, nil, %{"header" => "true", "multiLine" => "true"}} =
               unwrap_plan(df)
    end
  end

  describe "option collisions (FAB-3)" do
    test "generic source raises when a top-level option is also in :options" do
      assert_raise ArgumentError, ~r/multiple values for keyword argument/, fn ->
        Reader.json(self(), "/data/file.json",
          multi_line: true,
          options: %{"multiLine" => "false"}
        )
      end
    end
  end

  describe "json/2" do
    test "creates read_data_source with json format" do
      df = Reader.json(self(), "/data/file.json")
      assert {:read_data_source, "json", ["/data/file.json"], nil, %{}} = unwrap_plan(df)
    end

    test "merges top-level options with nested options" do
      df =
        Reader.json(self(), "/data/file.json",
          multi_line: true,
          options: %{"mode" => "PERMISSIVE"}
        )

      assert {:read_data_source, "json", ["/data/file.json"], nil,
              %{"multiLine" => "true", "mode" => "PERMISSIVE"}} = unwrap_plan(df)
    end
  end

  describe "builder + path for parquet/text/orc/avro" do
    test "routes a %Reader{} with a path through the builder and pins the format" do
      for {fun, format} <- [
            {&Reader.parquet/3, "parquet"},
            {&Reader.text/3, "text"},
            {&Reader.orc/3, "orc"},
            {&Reader.avro/3, "avro"}
          ] do
        df =
          self()
          |> SparkEx.read()
          |> Reader.format("csv")
          |> Reader.schema("id INT")
          |> Reader.option("mode", "PERMISSIVE")
          |> fun.("/data/in", options: %{"mode" => "FAILFAST"})

        assert {:read_data_source, ^format, ["/data/in"], "id INT", %{"mode" => "FAILFAST"}} =
                 unwrap_plan(df)

        assert df.session == self()
      end
    end

    test "accepts a list of paths from a builder" do
      df = self() |> SparkEx.read() |> Reader.parquet(["/a", "/b"])
      assert {:read_data_source, "parquet", ["/a", "/b"], nil, %{}} = unwrap_plan(df)
    end
  end
end
