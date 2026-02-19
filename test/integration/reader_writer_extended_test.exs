defmodule SparkEx.Integration.ReaderWriterExtendedTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Reader, Writer}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "parquet roundtrip via Writer/Reader", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
    path = "/tmp/spark_ex_it_parquet_#{System.unique_integer([:positive])}"

    assert :ok = Writer.parquet(df, path, mode: :overwrite)

    read_df = Reader.parquet(session, path)
    assert {:ok, rows} = DataFrame.collect(read_df)
    assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2]
  end

  test "json roundtrip via Writer/Reader", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(id, tag)")
    path = "/tmp/spark_ex_it_json_#{System.unique_integer([:positive])}"

    assert :ok = Writer.json(df, path, mode: :overwrite)

    read_df = Reader.json(session, path)
    assert {:ok, rows} = DataFrame.collect(read_df)
    assert Enum.sort(Enum.map(rows, & &1["tag"])) == ["x", "y"]
  end

  test "xml roundtrip via Writer/Reader", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(id, tag)")
    path = "/tmp/spark_ex_it_xml_#{System.unique_integer([:positive])}"

    assert :ok = Writer.xml(df, path, mode: :overwrite, options: %{"rowTag" => "row"})

    read_df = Reader.xml(session, path, options: %{"rowTag" => "row"})
    assert {:ok, rows} = DataFrame.collect(read_df)
    assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2]
  end

  test "avro roundtrip via Writer/Reader", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(id, tag)")
    path = "/tmp/spark_ex_it_avro_#{System.unique_integer([:positive])}"

    case Writer.avro(df, path, mode: :overwrite) do
      :ok ->
        read_df = Reader.avro(session, path)
        assert {:ok, rows} = DataFrame.collect(read_df)
        assert Enum.map(rows, & &1["tag"]) == ["x", "y"]

      {:error, %SparkEx.Error.Remote{message: msg}} ->
        if is_binary(msg) and String.contains?(msg, "data source: avro") do
          assert msg =~ "data source: avro"
        else
          flunk("unexpected avro error: #{inspect(msg)}")
        end
    end
  end
end
