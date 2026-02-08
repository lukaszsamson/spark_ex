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
  end
end
