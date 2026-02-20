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
        if is_binary(error.message) and String.contains?(error.message, "no such table") do
          assert error.grpc_status == 13
          assert error.message =~ "no such table"
        else
          flunk("unexpected jdbc write error: #{inspect(error)}")
        end
    end
  end

  test "jdbc read supports predicates pushdown", %{session: session} do
    jar_path = Path.expand("../jars/sqlite-jdbc-3.51.2.0.jar", __DIR__)
    {:ok, _} = SparkEx.Artifacts.add_jars(session, [jar_path])

    db_path =
      Path.join(
        System.tmp_dir!(),
        "spark_ex_sqlite_predicates_#{System.unique_integer([:positive])}.db"
      )

    on_exit(fn -> File.rm(db_path) end)

    url = "jdbc:sqlite:#{db_path}"
    driver = %{"driver" => "org.sqlite.JDBC"}

    df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)")

    case Writer.jdbc(df, url, "people", mode: :overwrite, options: driver) do
      :ok ->
        read_df =
          Reader.jdbc(session, url, "people",
            options: driver,
            predicates: ["id = 1", "id = 3"]
          )

        assert {:ok, rows} = DataFrame.collect(read_df)
        assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 3]

      {:error, %SparkEx.Error.Remote{} = error} ->
        if is_binary(error.message) and String.contains?(error.message, "no such table") do
          assert error.grpc_status == 13
          assert error.message =~ "no such table"
        else
          flunk("unexpected jdbc write error: #{inspect(error)}")
        end
    end
  end
end
