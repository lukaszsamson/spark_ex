defmodule SparkEx.ReaderTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.Reader

  describe "table/2" do
    test "creates read_named_table plan" do
      df = Reader.table(self(), "my_db.my_table")
      assert %DataFrame{plan: {:read_named_table, "my_db.my_table", %{}}} = df
      assert df.session == self()
    end

    test "passes options" do
      df = Reader.table(self(), "my_table", options: %{"key" => "val"})
      assert %DataFrame{plan: {:read_named_table, "my_table", %{"key" => "val"}}} = df
    end

    test "merges top-level options with nested options" do
      df = Reader.table(self(), "my_table", streaming: false, options: %{"key" => "val"})

      assert %DataFrame{
               plan: {:read_named_table, "my_table", %{"streaming" => "false", "key" => "val"}}
             } = df
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

      assert %DataFrame{
               plan:
                 {:read_data_source, "csv", ["/tmp/data.csv"], "id INT, name STRING",
                  %{"header" => "true", "inferSchema" => "false", "maxColumns" => "200"}}
             } = df
    end

    test "builder table/2 carries options" do
      df =
        self()
        |> SparkEx.read()
        |> Reader.option("streaming", false)
        |> Reader.table("my_db.my_table")

      assert %DataFrame{
               plan: {:read_named_table, "my_db.my_table", %{"streaming" => "false"}}
             } = df
    end
  end

  describe "parquet/2" do
    test "creates read_data_source with parquet format" do
      df = Reader.parquet(self(), "/data/file.parquet")

      assert %DataFrame{
               plan: {:read_data_source, "parquet", ["/data/file.parquet"], nil, %{}}
             } = df
    end

    test "accepts list of paths" do
      paths = ["/data/part1.parquet", "/data/part2.parquet"]
      df = Reader.parquet(self(), paths)
      assert %DataFrame{plan: {:read_data_source, "parquet", ^paths, nil, %{}}} = df
    end

    test "accepts schema option" do
      df = Reader.parquet(self(), "/data/file.parquet", schema: "id INT, name STRING")

      assert %DataFrame{
               plan: {:read_data_source, "parquet", _, "id INT, name STRING", %{}}
             } = df
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

      assert %DataFrame{
               plan: {:read_data_source, "parquet", _, encoded_schema, %{}}
             } = df

      decoded = Jason.decode!(encoded_schema)
      assert decoded["type"] == "struct"
      assert Enum.at(decoded["fields"], 0)["nullable"] == false
      assert Enum.at(decoded["fields"], 0)["metadata"]["comment"] == "pk"
    end
  end

  describe "csv/2" do
    test "creates read_data_source with csv format" do
      df = Reader.csv(self(), "/data/file.csv")
      assert %DataFrame{plan: {:read_data_source, "csv", ["/data/file.csv"], nil, %{}}} = df
    end

    test "translates header and infer_schema options" do
      df = Reader.csv(self(), "/data/file.csv", header: true, infer_schema: true)

      assert %DataFrame{
               plan:
                 {:read_data_source, "csv", _, nil,
                  %{"header" => "true", "inferSchema" => "true"}}
             } = df
    end

    test "translates separator option" do
      df = Reader.csv(self(), "/data/file.csv", separator: "|")

      assert %DataFrame{
               plan: {:read_data_source, "csv", _, nil, %{"sep" => "|"}}
             } = df
    end
  end

  describe "json/2" do
    test "creates read_data_source with json format" do
      df = Reader.json(self(), "/data/file.json")
      assert %DataFrame{plan: {:read_data_source, "json", ["/data/file.json"], nil, %{}}} = df
    end

    test "merges top-level options with nested options" do
      df =
        Reader.json(self(), "/data/file.json",
          multi_line: true,
          options: %{"mode" => "PERMISSIVE"}
        )

      assert %DataFrame{
               plan:
                 {:read_data_source, "json", ["/data/file.json"], nil,
                  %{"multi_line" => "true", "mode" => "PERMISSIVE"}}
             } = df
    end
  end
end
