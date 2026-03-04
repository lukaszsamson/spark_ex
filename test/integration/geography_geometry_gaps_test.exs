defmodule SparkEx.Integration.GeographyGeometryGapsTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Geography/Geometry type support ──
  # These tests verify Spark 4.x geography/geometry type support.
  # On Spark 3.x these functions are unresolved — we accept that specific error.
  # Spark 3.x returns error_class=nil, so we also check the message.

  defp assert_unresolved_routine_error(%SparkEx.Error.Remote{} = err) do
    class = err.error_class || ""
    msg = err.message || ""

    assert String.contains?(class, "UNRESOLVED_ROUTINE") or
             String.contains?(msg, "Undefined function") or
             String.contains?(msg, "UNRESOLVED_ROUTINE"),
           "expected unresolved routine error, got class=#{inspect(err.error_class)} message=#{inspect(err.message)}"
  end

  describe "geography type" do
    test "ST_Point creates geography value via SQL", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_Point(1.0, 2.0) AS pt
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert row["pt"] != nil

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end

    test "ST_GeomFromText creates geography from WKT", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_GeomFromText('POINT (1.0 2.0)') AS geom
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert row["geom"] != nil

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end

    test "ST_AsText converts geography to WKT", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_AsText(ST_Point(1.0, 2.0)) AS wkt
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert is_binary(row["wkt"])
          assert String.contains?(row["wkt"], "POINT")

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end
  end

  describe "geometry type" do
    test "geometry column in VALUES clause", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_GeomFromText('LINESTRING (0 0, 1 1, 2 2)') AS line
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert row["line"] != nil

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end

    test "geometry polygon", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS poly
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert row["poly"] != nil

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end
  end

  describe "geography/geometry operations" do
    test "ST_Distance between two points", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_Distance(ST_Point(0.0, 0.0), ST_Point(3.0, 4.0)) AS dist
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert is_number(row["dist"])
          assert_in_delta row["dist"], 5.0, 0.1

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end

    test "ST_Contains check", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_Contains(
          ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
          ST_Point(5.0, 5.0)
        ) AS inside
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert row["inside"] == true

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end

    test "ST_Buffer creates buffered geometry", %{session: session} do
      result =
        SparkEx.sql(session, """
        SELECT ST_AsText(ST_Buffer(ST_Point(0.0, 0.0), 1.0)) AS buffered
        """)
        |> DataFrame.collect()

      case result do
        {:ok, [row]} ->
          assert is_binary(row["buffered"])
          assert String.contains?(row["buffered"], "POLYGON")

        {:error, %SparkEx.Error.Remote{} = err} ->
          assert_unresolved_routine_error(err)
      end
    end
  end
end
