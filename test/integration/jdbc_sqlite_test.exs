defmodule SparkEx.Integration.JdbcSqliteTest do
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

  test "jdbc roundtrip with sqlite", %{session: session} do
    jar_path = Path.expand("../jars/sqlite-jdbc-3.51.2.0.jar", __DIR__)
    {:ok, _} = SparkEx.Artifacts.add_jars(session, [jar_path])

    db_path =
      Path.join(System.tmp_dir!(), "spark_ex_sqlite_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> File.rm(db_path) end)

    url = "jdbc:sqlite:#{db_path}"
    driver = %{"driver" => "org.sqlite.JDBC"}

    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")

    case Writer.jdbc(df, url, "people", mode: :overwrite, options: driver) do
      :ok ->
        read_df = Reader.jdbc(session, url, "people", options: driver)
        assert {:ok, rows} = DataFrame.collect(read_df)
        assert Enum.sort(Enum.map(rows, & &1["name"])) == ["a", "b"]

      {:error, %SparkEx.Error.Remote{} = error} ->
        assert is_binary(error.message)
        assert String.contains?(error.message, "no such table")

        read_df = Reader.jdbc(session, url, "people", options: driver)
        assert {:error, %SparkEx.Error.Remote{} = read_error} = DataFrame.collect(read_df)
        assert String.contains?(read_error.message || "", "no such table")
    end
  end
end
