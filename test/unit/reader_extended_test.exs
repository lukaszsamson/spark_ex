defmodule SparkEx.Unit.ReaderExtendedTest do
  use ExUnit.Case, async: true

  alias SparkEx.Reader
  alias SparkEx.DataFrame

  @session :fake_session

  describe "text/3" do
    test "creates DataFrame with text format" do
      df = Reader.text(@session, "/data/lines.txt")
      assert %DataFrame{plan: {:read_data_source, "text", ["/data/lines.txt"], nil, %{}}} = df
    end
  end

  describe "orc/3" do
    test "creates DataFrame with orc format" do
      df = Reader.orc(@session, "/data/events.orc")
      assert %DataFrame{plan: {:read_data_source, "orc", ["/data/events.orc"], nil, %{}}} = df
    end

    test "accepts schema option" do
      df = Reader.orc(@session, "/data/events.orc", schema: "id INT, name STRING")

      assert %DataFrame{
               plan: {:read_data_source, "orc", ["/data/events.orc"], "id INT, name STRING", %{}}
             } = df
    end
  end

  describe "load/4" do
    test "creates DataFrame with generic format" do
      df = Reader.load(@session, "avro", "/data/events.avro")

      assert %DataFrame{plan: {:read_data_source, "avro", ["/data/events.avro"], nil, %{}}} = df
    end

    test "accepts nil paths for formats like jdbc" do
      df =
        Reader.load(@session, "jdbc",
          options: %{"url" => "jdbc:mysql://host/db", "dbtable" => "users"}
        )

      assert %DataFrame{
               plan:
                 {:read_data_source, "jdbc", [], nil,
                  %{"url" => "jdbc:mysql://host/db", "dbtable" => "users"}}
             } = df
    end

    test "accepts schema option" do
      df = Reader.load(@session, "csv", "/data/file.csv", schema: "id INT")

      assert %DataFrame{plan: {:read_data_source, "csv", ["/data/file.csv"], "id INT", %{}}} = df
    end

    test "accepts list of paths" do
      df = Reader.load(@session, "parquet", ["/a.parquet", "/b.parquet"])

      assert %DataFrame{
               plan: {:read_data_source, "parquet", ["/a.parquet", "/b.parquet"], nil, %{}}
             } = df
    end

    test "stringifies primitive option values" do
      df =
        Reader.load(@session, "csv", "/data/file.csv",
          options: %{"header" => true, "maxColumns" => 50, "samplingRatio" => 0.25}
        )

      assert %DataFrame{
               plan:
                 {:read_data_source, "csv", ["/data/file.csv"], nil,
                  %{
                    "header" => "true",
                    "maxColumns" => "50",
                    "samplingRatio" => "0.25"
                  }}
             } = df
    end
  end
end
