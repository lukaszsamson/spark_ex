defmodule SparkEx.Integration.DataSourceOptionsTest do
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

  test "csv options for separator, quote, escape, and null handling", %{session: session} do
    df =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES (1, 'a|b', NULL), (2, 'c', 'x') AS t(id, name, note)"
      )

    path = "/tmp/spark_ex_csv_opts_#{System.unique_integer([:positive])}"

    assert :ok =
             Writer.csv(df, path,
               mode: :overwrite,
               options: %{
                 "sep" => "|",
                 "quote" => "\"",
                 "escape" => "\\",
                 "nullValue" => "NULL"
               }
             )

    read_df =
      Reader.csv(session, path,
        schema: "id INT, name STRING, note STRING",
        options: %{"sep" => "|", "quote" => "\"", "escape" => "\\", "nullValue" => "NULL"}
      )

    assert {:ok, rows} = DataFrame.collect(read_df)
    assert Enum.map(rows, & &1["id"]) == [1, 2]
    assert Enum.find(rows, &(&1["id"] == 1))["note"] == nil
  end

  test "json options for primitives as string", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, true AS flag")
    path = "/tmp/spark_ex_json_opts_#{System.unique_integer([:positive])}"

    assert :ok = Writer.json(df, path, mode: :overwrite)

    read_df =
      Reader.json(session, path,
        schema: "id STRING, flag STRING",
        options: %{"primitivesAsString" => "true"}
      )

    assert {:ok, [row]} = DataFrame.collect(read_df)
    assert row["id"] == "1"
    assert row["flag"] == "true"
  end

  test "parquet compression option", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
    path = "/tmp/spark_ex_parquet_comp_#{System.unique_integer([:positive])}"

    assert :ok = Writer.parquet(df, path, mode: :overwrite, options: %{"compression" => "gzip"})

    read_df = Reader.parquet(session, path)
    assert {:ok, rows} = DataFrame.collect(read_df)
    assert Enum.map(rows, & &1["id"]) |> Enum.sort() == [1, 2]
  end
end
